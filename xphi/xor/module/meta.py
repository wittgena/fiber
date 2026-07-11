# xphi.xor.module.meta
## @lineage: xphi.xor.opt.manifold.module.meta
## @lineage: xphi.xor.opt.module.meta
import inspect
from typing import Any, TextIO, TYPE_CHECKING

from xphi.scope.dsp.context import runtime
from xphi.scope.dsp.usage import track_usage
from xphi.watcher.format import pretty_print_history

from arch.xor.manifold.sample import Sample
from arch.xor.manifold.sample import Prediction
from xphi.xor.module.base import BaseModule
from xphi.xor.opt.callback.base import with_callbacks
from anchor.executor.dsp import DSPRunner

from arch.contract.exp.nest import NestedAttr
from watcher.plane.emitter import get_emitter

log = get_emitter('module.meta')

class ProgramMeta(type):
    def __call__(cls, *args, **kwargs):
        obj = cls.__new__(cls, *args, **kwargs)
        if isinstance(obj, cls):
            Module._base_init(obj)
            cls.__init__(obj, *args, **kwargs)
            if not hasattr(obj, "callbacks"):
                obj.callbacks = []
            if not hasattr(obj, "history"):
                obj.history = []
        return obj


class Module(BaseModule, metaclass=ProgramMeta):
    def _base_init(self):
        self._compiled = False
        self.callbacks = []
        self.history = []

    def __init__(self, callbacks=None):
        self.callbacks = callbacks or []
        self._compiled = False
        self.history = []

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("history", None)
        state.pop("callbacks", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        if not hasattr(self, "history"):
            self.history = []
        if not hasattr(self, "callbacks"):
            self.callbacks = []

    @with_callbacks
    def __call__(self, *args, **kwargs) -> Prediction:
        from xphi.scope.dsp.context import thread_local_overrides

        caller_modules = runtime.caller_modules or []
        caller_modules = list(caller_modules)
        caller_modules.append(self)

        with runtime.bind(caller_modules=caller_modules):
            if runtime.track_usage and thread_local_overrides.get().get("usage_tracker") is None:
                with track_usage() as usage_tracker:
                    output = self.forward(*args, **kwargs)
                tokens = usage_tracker.get_total_tokens()
                self._set_lm_usage(tokens, output)

                return output

            return self.forward(*args, **kwargs)

    @with_callbacks
    async def acall(self, *args, **kwargs) -> Prediction:
        from xphi.scope.dsp.context import _current_context

        caller_modules = runtime.caller_modules or []
        caller_modules = list(caller_modules)
        caller_modules.append(self)

        with runtime.bind(caller_modules=caller_modules):
            if runtime.track_usage and _current_context.get().get("usage_tracker") is None:
                with track_usage() as usage_tracker:
                    output = await self.aforward(*args, **kwargs)
                    tokens = usage_tracker.get_total_tokens()
                    self._set_lm_usage(tokens, output)

                    return output

            return await self.aforward(*args, **kwargs)

    def named_predictors(self):
        from xphi.xor.module.prompter import Predict
        return [(name, param) for name, param in self.named_parameters() if isinstance(param, Predict)]

    def predictors(self):
        return [param for _, param in self.named_predictors()]

    def set_lm(self, lm):
        for _, param in self.named_predictors():
            param.lm = lm

    def get_lm(self):
        all_used_lms = [param.lm for _, param in self.named_predictors()]
        if len(set(all_used_lms)) == 1:
            return all_used_lms[0]

        raise ValueError("Multiple LMs are being used in the module. There's no unique LM to return.")

    def __repr__(self):
        s = []

        for name, param in self.named_predictors():
            s.append(f"{name} = {param}")

        return "\n".join(s)

    def map_named_predictors(self, func):
        for name, predictor in self.named_predictors():
            NestedAttr.set(self, name, func(predictor))
        return self

    def inspect_history(self, n: int = 1, file: "TextIO | None" = None) -> None:
        pretty_print_history(self.history, n, file=file)

    def batch(
        self,
        examples: list[Sample],
        num_threads: int | None = None,
        max_errors: int | None = None,
        return_failed_examples: bool = False,
        provide_traceback: bool | None = None,
        disable_progress_bar: bool = False,
        timeout: int = 120,
        straggler_limit: int = 3,
    ) -> list[Sample] | tuple[list[Sample], list[Sample], list[Exception]]:
        exec_pairs = [(self, example.inputs()) for example in examples]
        parallel_executor = DSPRunner(
            num_threads=num_threads,
            max_errors=max_errors,
            return_failed_examples=return_failed_examples,
            provide_traceback=provide_traceback,
            disable_progress_bar=disable_progress_bar,
            timeout=timeout,
            straggler_limit=straggler_limit,
        )

        if return_failed_examples:
            results, failed_examples, exceptions = parallel_executor.forward(exec_pairs)
            return results, failed_examples, exceptions
        else:
            results = parallel_executor.forward(exec_pairs)
            return results

    def _set_lm_usage(self, tokens: dict[str, Any], output: Any):
        prediction_in_output = None
        if isinstance(output, Prediction):
            prediction_in_output = output
        elif isinstance(output, tuple) and len(output) > 0 and isinstance(output[0], Prediction):
            prediction_in_output = output[0]
        if prediction_in_output:
            prediction_in_output.set_lm_usage(tokens)
        else:
            log.warning("Failed to set LM usage. Please return `spi.prim.Prediction` object from spi.prim.Module to enable usage tracking.")

    def __getattribute__(self, name):
        attr = super().__getattribute__(name)
        if name == "forward" and callable(attr):
            stack = inspect.stack()
            forward_called_directly = len(stack) <= 1 or stack[1].function != "__call__"
            if forward_called_directly:
                log.warning(
                    f"Calling module.forward(...) on {self.__class__.__name__} directly is discouraged. "
                    f"Please use module(...) instead."
                )
        return attr