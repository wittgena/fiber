# xor.executor.dsp
## @lineage: xphi.xor.executor.dsp
## @lineage: anchor.phase.executor.dsp
## @lineage: anchor.executor.dsp
## @lineage: xphi.xor.module.executor.runner
import threading
from typing import Any

from xor.opt.context import runtime, get_context_propagator
from arch.xor.sample import Sample
from arch.xor.proto.opt import OptExecutor

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