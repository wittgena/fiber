# atoa.executor.dsp
import threading
from typing import Any
import contextlib
import logging
import signal
import sys
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import tqdm

from bound.xor.opt.context import runtime, get_context_propagator
from arch.xor.sample import Sample
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class DSPRunner:
    def __init__(
        self,
        num_threads: int | None = None,
        max_errors: int | None = None,
        access_examples: bool = True,
        return_failed_examples: bool = False,
        provide_traceback: bool | None = None,
        disable_progress_bar: bool = False,
        timeout: int = 120,
        straggler_limit: int = 3,
    ):
        super().__init__()
        self.num_threads = num_threads or runtime.num_threads
        self.max_errors = runtime.max_errors if max_errors is None else max_errors
        self.access_examples = access_examples
        self.return_failed_examples = return_failed_examples
        self.provide_traceback = provide_traceback if provide_traceback is not None else runtime.provide_traceback
        self.disable_progress_bar = disable_progress_bar
        self.timeout = timeout
        self.straggler_limit = straggler_limit

        self.error_count = 0
        self.error_lock = threading.Lock()
        self.cancel_jobs = threading.Event()
        self.failed_examples = []
        self.exceptions = []

    def forward(self, exec_pairs: list[tuple[Any, Sample]], num_threads: int | None = None) -> list[Any]:
        num_threads = num_threads if num_threads is not None else self.num_threads
        executor = OptExecutor(
            num_threads=num_threads,
            max_errors=self.max_errors,
            provide_traceback=self.provide_traceback,
            disable_progress_bar=self.disable_progress_bar,
            timeout=self.timeout,
            straggler_limit=self.straggler_limit,
            context_propagator=get_context_propagator()
        )

        def process_pair(pair):
            result = None
            module, example = pair

            if isinstance(example, Sample):
                if self.access_examples:
                    result = module(**example.inputs())
                else:
                    result = module(example)
            elif isinstance(example, dict):
                result = module(**example)
            elif isinstance(example, list) and module.__class__.__name__ == "Parallel":
                result = module(example)
            elif isinstance(example, tuple):
                result = module(*example)
            else:
                raise ValueError(
                    f"Invalid example type: {type(example)}, only supported types are Example, dict, list and tuple"
                )
            return result

        # Execute the processing function over the execution pairs
        results = executor.execute(process_pair, exec_pairs)

        # Populate failed examples and exceptions from the executor
        if self.return_failed_examples:
            for failed_idx in getattr(executor, 'failed_indices', []):
                if failed_idx < len(exec_pairs):
                    _, original_example = exec_pairs[failed_idx]
                    self.failed_examples.append(original_example)
                    
                    exceptions_map = getattr(executor, 'exceptions_map', {})
                    if exception := exceptions_map.get(failed_idx):
                        self.exceptions.append(exception)

            return results, self.failed_examples, self.exceptions
        else:
            return results

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.forward(*args, **kwargs)

class OptExecutor:
    def __init__(
        self,
        num_threads=8,
        max_errors=10,
        disable_progress_bar=False,
        provide_traceback=False,
        compare_results=False,
        timeout=120,
        straggler_limit=3,
        context_propagator=None
    ):
        self.num_threads = num_threads
        self.max_errors = max_errors
        self.disable_progress_bar = disable_progress_bar
        self.provide_traceback = provide_traceback
        self.compare_results = compare_results
        self.timeout = timeout
        self.straggler_limit = straggler_limit
        self.context_propagator = context_propagator

        self.error_count = 0
        self.error_lock = threading.Lock()
        self.cancel_jobs = threading.Event()
        self.failed_indices = []
        self.exceptions_map = {}

    def execute(self, function, data):
        tqdm.tqdm._instances.clear()
        wrapped = self._wrap_function(function)
        if self.num_threads == 1:
            return self._execute_sequential(wrapped, data)
        return self._execute_parallel(wrapped, data)

    def _wrap_function(self, user_function):
        def safe_func(item):
            if self.cancel_jobs.is_set():
                return None
            try:
                return user_function(item)
            except Exception as e:
                with self.error_lock:
                    self.error_count += 1
                    if self.error_count >= self.max_errors:
                        self.cancel_jobs.set()
                if self.provide_traceback:
                    log.error(f"Error for {item}: {e}\n{traceback.format_exc()}")
                else:
                    log.error(f"Error for {item}: {e}. Set `provide_traceback=True` for traceback.")
                return e

        return safe_func

    def _execute_sequential(self, function, data):
        """Execute items sequentially on the main thread."""
        results = [None] * len(data)

        pbar = tqdm.tqdm(
            total=len(data),
            dynamic_ncols=True,
            disable=self.disable_progress_bar,
            file=sys.stdout,
        )

        try:
            for idx, item in enumerate(data):
                if self.cancel_jobs.is_set():
                    break

                outcome = function(item)
                self._process_outcome(results, idx, outcome)
                self._report_progress(pbar, results, len(data))
        except KeyboardInterrupt:
            self.cancel_jobs.set()
            log.warning("SIGINT received. Cancelling.")
            raise
        finally:
            pbar.close()

        if self.cancel_jobs.is_set():
            log.warning("Execution cancelled due to errors or interruption.")
            raise Exception("Execution cancelled due to errors or interruption.")

        return results

    def _execute_parallel(self, function, data):
        results = [None] * len(data)
        job_cancelled = "cancelled"

        # We resubmit at most once per item.
        start_time_map = {}
        start_time_lock = threading.Lock()
        resubmitted = set()

        # This is the worker function each thread will run.
        def worker(submission_id, index, item):
            if self.cancel_jobs.is_set():
                return index, job_cancelled
            
            # Record actual start time
            with start_time_lock:
                start_time_map[submission_id] = time.time()

            # Delegate context propagation to the injected hook (if provided)
            ctx = self.context_propagator() if self.context_propagator else contextlib.nullcontext()
            
            with ctx:
                return index, function(item)

        # Handle Ctrl-C in the main thread
        @contextlib.contextmanager
        def interrupt_manager():
            if threading.current_thread() is threading.main_thread():
                orig_handler = signal.getsignal(signal.SIGINT)

                def handler(sig, frame):
                    self.cancel_jobs.set()
                    log.warning("SIGINT received. Cancelling.")
                    orig_handler(sig, frame)

                signal.signal(signal.SIGINT, handler)
                try:
                    yield
                finally:
                    signal.signal(signal.SIGINT, orig_handler)
            else:
                yield

        executor = ThreadPoolExecutor(max_workers=self.num_threads)
        try:
            with interrupt_manager():
                futures_map = {}
                futures_set = set()
                submission_counter = 0

                for idx, item in enumerate(data):
                    f = executor.submit(worker, submission_counter, idx, item)
                    futures_map[f] = (submission_counter, idx, item)
                    futures_set.add(f)
                    submission_counter += 1

                pbar = tqdm.tqdm(
                    total=len(data),
                    dynamic_ncols=True,
                    disable=self.disable_progress_bar,
                    file=sys.stdout,
                )

                def all_done():
                    return all(r is not None for r in results)

                while futures_set and not self.cancel_jobs.is_set():
                    if all_done():
                        break
                    done, not_done = wait(futures_set, timeout=1, return_when=FIRST_COMPLETED)
                    for f in done:
                        futures_set.remove(f)
                        try:
                            index, outcome = f.result()
                        except Exception:
                            pass
                        else:
                            if outcome != job_cancelled and results[index] is None:
                                self._process_outcome(results, index, outcome)

                        self._report_progress(pbar, results, len(data))

                    if all_done():
                        break

                    # Check stragglers if few remain
                    if 0 < self.timeout and len(not_done) <= self.straggler_limit:
                        now = time.time()
                        for f in list(not_done):
                            if f not in resubmitted:
                                sid, idx, item = futures_map[f]
                                with start_time_lock:
                                    st = start_time_map.get(sid, None)
                                if st and (now - st) >= self.timeout:
                                    resubmitted.add(f)
                                    nf = executor.submit(
                                        worker,
                                        submission_counter,
                                        idx,
                                        item,
                                    )
                                    futures_map[nf] = (submission_counter, idx, item)
                                    futures_set.add(nf)
                                    submission_counter += 1

                pbar.close()

        finally:
            executor.shutdown(wait=False)

        if self.cancel_jobs.is_set():
            log.warning("Execution cancelled due to errors or interruption.")
            raise Exception("Execution cancelled due to errors or interruption.")

        return results

    def _process_outcome(self, results, idx, outcome):
        """Store a single outcome and track errors."""
        if isinstance(outcome, Exception):
            with self.error_lock:
                self.failed_indices.append(idx)
                self.exceptions_map[idx] = outcome
        else:
            results[idx] = outcome

    def _report_progress(self, pbar, results, total):
        """Compute metrics and update the progress bar."""
        if self.compare_results:
            vals = [r[-1] for r in results if r is not None]
            self._update_progress(pbar, sum(vals), len(vals))
        else:
            self._update_progress(
                pbar,
                len([r for r in results if r is not None]),
                total,
            )

    def _update_progress(self, pbar, nresults, ntotal):
        if self.compare_results:
            pct = round(100 * nresults / ntotal, 1) if ntotal else 0
            pbar.set_description(f"Average Metric: {nresults:.2f} / {ntotal} ({pct}%)")
        else:
            pbar.set_description(f"Processed {nresults} / {ntotal} examples")
        pbar.update()