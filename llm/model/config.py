# fiber.llm.model.config
import os
import logging
from typing import Any
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("model.config")

class ConfigResolver:
    def __init__(self):
        self._local_overrides = {}

    def get(self, name: str, default: Any = None) -> Any:
        try:
            value = getattr(self, name)
            return default if value is None else value
        except AttributeError:
            return default

    def set_override(self, key: str, value: Any):
        self._local_overrides[key] = value

    def __getattr__(self, name: str):
        if name in self._local_overrides:
            return self._local_overrides[name]

        env_key = name.upper()
        if env_key in os.environ:
            return os.environ[env_key]
        raise AttributeError(f"'{type(self).__name__}' object (brane config) has no attribute '{name}'")
    
    def __setattr__(self, name: str, value: Any):
        if name == "_local_overrides":
            super().__setattr__(name, value)
            return

        self._local_overrides[name] = value

config = ConfigResolver()