# bound.xor.opt.context
## @lineage: xor.opt.context
## @lineage: xor.scope.dsp.context
## @lineage: xphi.scope.dsp.context
import contextvars
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, List, Optional, Dict
import cloudpickle
from watcher.plane.emitter import get_emitter

log = get_emitter("dsp.context")

@dataclass
class RuntimeContext:
    lm: Optional[Any] = None
    adapter: Optional[Any] = None
    rm: Optional[Any] = None
    branch_idx: int = 0
    trace: List[Any] = field(default_factory=list)
    callbacks: List[Any] = field(default_factory=list)
    async_max_workers: int = 8
    send_stream: Optional[Any] = None
    disable_history: bool = False
    track_usage: bool = False
    usage_tracker: Optional[Any] = None
    caller_predict: Optional[Any] = None
    caller_modules: Optional[Any] = None
    stream_listeners: List[Any] = field(default_factory=list)
    provide_traceback: bool = False
    num_threads: int = 8
    max_errors: int = 10
    allow_tool_async_sync_conversion: bool = False
    max_history_size: int = 10000
    max_trace_size: int = 10000
    warn_on_type_mismatch: bool = True

_default_context = RuntimeContext()
_current_context = contextvars.ContextVar("dsp_runtime_context", default=_default_context)

class RuntimeState:
    def __getattr__(self, name):
        ctx = _current_context.get()
        if hasattr(ctx, name):
            return getattr(ctx, name)
        raise AttributeError(f"'RuntimeState' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        raise RuntimeError("Use `with runtime.bind(...)` instead")
        
    def get(self, key, default=None):
        ctx = _current_context.get()
        return getattr(ctx, key, default)

    @contextmanager
    def bind(self, **kwargs):
        current_ctx = _current_context.get()
        new_ctx = replace(current_ctx, **kwargs)
        token = _current_context.set(new_ctx)

        try:
            yield
        finally:
            _current_context.reset(token)

    def copy(self):
        return self._get_dict_safe()

    def _get_dict_safe(self, exclude_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        exclude_keys = exclude_keys or []
        ctx = _current_context.get()
        return {k: v for k, v in ctx.__dict__.items() if k not in exclude_keys}

    def save(self, path: str, modules_to_serialize: Optional[List[str]] = None, exclude_keys: Optional[List[str]] = None):
        log.warning("`settings` are serialized using cloudpickle.")
        try:
            for module in (modules_to_serialize or []):
                cloudpickle.register_pickle_by_value(module)

            data = self._get_dict_safe(exclude_keys)
            with open(path, "wb") as f:
                cloudpickle.dump(data, f)
        except Exception as e:
            raise RuntimeError(f"Saving failed with error: {e}")

    @classmethod
    def load(cls, path: str, allow_pickle: bool = False) -> Dict[str, Any]:
        if not allow_pickle:
            raise ValueError("Loading .pkl files can run arbitrary code, which may be dangerous.")
        with open(path, "rb") as f:
            return cloudpickle.load(f)

def get_context_propagator():
    current_kwargs = runtime._get_dict_safe()
    if current_kwargs.get("usage_tracker"):
        import copy
        current_kwargs["usage_tracker"] = copy.deepcopy(current_kwargs["usage_tracker"])
        
    return lambda: runtime.bind(**current_kwargs)

runtime = RuntimeState()