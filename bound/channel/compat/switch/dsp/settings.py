# bound.channel.compat.switch.dsp.settings
import asyncio
import contextvars
import copy
import logging
import threading
from contextlib import contextmanager
from typing import Any
import cloudpickle
from arch.topos.bind.residue import dotdict
from watcher.plane.emitter import get_emitter

log = get_emitter("dspy.settings")

DEFAULT_CONFIG = dotdict(
    lm=None,
    adapter=None,
    rm=None,
    branch_idx=0,
    trace=[],
    callbacks=[],
    async_max_workers=8,
    send_stream=None,
    disable_history=False,
    track_usage=False,
    usage_tracker=None,
    caller_predict=None,
    caller_modules=None,
    stream_listeners=[],
    provide_traceback=False,  # Whether to include traceback information in error logs.
    num_threads=8,  # Number of threads to use for parallel processing.
    max_errors=10,  # Maximum errors before halting operations.
    allow_tool_async_sync_conversion=False,
    max_history_size=10000,
    max_trace_size=10000,
    warn_on_type_mismatch=True,  # Whether to log warnings when a module's input type doesn't match the signature type.
)

# Global base configuration
main_thread_config = copy.deepcopy(DEFAULT_CONFIG)
global_lock = threading.Lock()
thread_local_overrides = contextvars.ContextVar("context_overrides", default=dotdict())

class DSPSettings:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def lock(self):
        return global_lock

    def __getattr__(self, name):
        overrides = thread_local_overrides.get()
        if name in overrides:
            return overrides[name]
        elif name in main_thread_config:
            return main_thread_config[name]
        else:
            raise AttributeError(f"'DSPSettings' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name in ("_instance",):
            super().__setattr__(name, value)
        else:
            # 직접 할당 방지 및 마이그레이션 안내
            raise RuntimeError(
                f"Direct assignment to settings (settings.{name} = ...) is strictly prohibited to ensure thread safety. "
                "Please use `settings.context(...)` or inject dependencies via `managed_scope`."
            )

    def __getitem__(self, key):
        return self.__getattr__(key)

    def __setitem__(self, key, value):
        # 직접 할당 방지 및 마이그레이션 안내
        raise RuntimeError(
            f"Direct assignment to settings (settings['{key}'] = ...) is strictly prohibited to ensure thread safety. "
            "Please use `settings.context(...)` or inject dependencies via `managed_scope`."
        )

    def __contains__(self, key):
        overrides = thread_local_overrides.get()
        return key in overrides or key in main_thread_config

    def get(self, key, default=None):
        try:
            return self[key]
        except AttributeError:
            return default

    def copy(self):
        overrides = thread_local_overrides.get()
        return dotdict({**main_thread_config, **overrides})

    @property
    def config(self):
        return self.copy()

    @contextmanager
    def context(self, **kwargs):
        original_overrides = thread_local_overrides.get().copy()
        new_overrides = dotdict({**main_thread_config, **original_overrides, **kwargs})
        token = thread_local_overrides.set(new_overrides)

        try:
            yield
        finally:
            thread_local_overrides.reset(token)

    def __repr__(self):
        overrides = thread_local_overrides.get()
        combined_config = {**main_thread_config, **overrides}
        return repr(combined_config)

    def save(
        self, path: str,
        modules_to_serialize: list[str] | None = None,
        exclude_keys: list[str] | None = None,
    ):
        log.warning(
            "`settings` are serialized using cloudpickle. Because cloudpickle allows for the "
            "execution of arbitrary code during deserialization, you should only load files from "
            "verified sources within a trusted environment."
        )
        try:
            modules_to_serialize = modules_to_serialize or []
            for module in modules_to_serialize:
                cloudpickle.register_pickle_by_value(module)

            exclude_keys = exclude_keys or []
            data = {key: value for key, value in self.config.items() if key not in exclude_keys}
            with open(path, "wb") as f:
                cloudpickle.dump(data, f)
        except Exception as e:
            raise RuntimeError(
                f"Saving failed with error: {e}. Please remove the non-picklable attributes from the values in the `settings`"
            )

    @classmethod
    def load(cls, path: str, allow_pickle: bool = False) -> dict[str, Any]:
        """Load the settings from a file using cloudpickle"""
        if not allow_pickle:
            raise ValueError(
                "Loading .pkl files can run arbitrary code, which may be dangerous. "
                "Set `allow_pickle=True` if you trust the source of the file."
            )

        with open(path, "rb") as f:
            configs = cloudpickle.load(f)

        return configs

settings = DSPSettings()