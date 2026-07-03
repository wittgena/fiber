# xphi.xor.dsp.handler.memory
## @lineage: xphi.reflect.dsp.handler.memory
## @lineage: xphi.opt.dsp.handler.memory
## @lineage: bound.xor.dsp.handler.memory
## @lineage: xor.dsp.handler.memory
## @lineage: bound.handler.dsp.memory
## @lineage: bound.channel.handler.dsp.memory
## @lineage: anchor.dsp.memory
## @lineage: gov.gateway.io.cache
## @lineage: gov.gate.io.cache
## @lineage: gate.io.cache
## @lineage: gov.gateway.io.store.cache
## @lineage: gov.medium.io.store.cache
## @lineage: gov.io.store.cache
## @lineage: xor.store.cache
from typing import Any
from cachetools import LRUCache
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

class MemoryLRUCache(LRUCache):
    def __init__(self, max_memory: int, max_size: int, *args, **kwargs):
        maxsize = max(1, max_size)
        super().__init__(maxsize=maxsize, *args, **kwargs)
        self.max_memory = max_memory
        self.current_memory = 0

    def _get_size(self, value: Any) -> int:
        if isinstance(value, str):
            return len(value)
        elif isinstance(value, bytes):
            return len(value)
        else:
            try:
                import sys
                return sys.getsizeof(value)
            except Exception:
                return 0

    def __setitem__(self, key: Any, value: Any) -> None:
        new_size = self._get_size(value)
        if new_size > self.max_memory:
            logger.debug(f"Item too large for cache ({new_size} bytes > {self.max_memory} bytes), skipping cache")
            return

        if key in self:
            old_value = self[key]
            self.current_memory -= self._get_size(old_value)

        self.current_memory += new_size
        while self.current_memory > self.max_memory and len(self) > 0:
            self.popitem()
        super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        if key in self:
            old_value = self[key]
            self.current_memory -= self._get_size(old_value)
        super().__delitem__(key)
