# fiber.agent.conver.conv.store.file
## @lineage: surgent.conv.store.file
## @lineage: xphi.xor.stream.store.file
## @lineage: xphi.arch.xor.stream.store.file
## @lineage: arch.xor.stream.store.file
## @lineage: arch.xor.store.file
## @lineage: arch.xor.bridge.store.file
## @lineage: arch.gov.bridge.store.file
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
import os
import shutil
from typing import Any
from cachetools import LRUCache
from collections.abc import Iterator
from contextlib import contextmanager
from filelock import FileLock, Timeout

from xphi.watcher.plane.emitter import get_logger
from xphi.watcher.plane.observer.span import observe

logger = get_logger(__name__)

class FileStore(ABC):
    @abstractmethod
    def write(self, path: str, contents: str | bytes) -> None:
        """Write contents to a file at the specified path"""

    @abstractmethod
    def read(self, path: str) -> str:
        """Read and return the contents of a file as a string"""

    @abstractmethod
    def list(self, path: str) -> list[str]:
        """List all files and directories at the specified path"""

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete the file or directory at the specified path"""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file or directory exists at the specified path"""

    @abstractmethod
    def get_absolute_path(self, path: str) -> str:
        """Get the absolute filesystem path for a given relative path"""

    @abstractmethod
    @contextmanager
    def lock(self, path: str, timeout: float = 30.0) -> Iterator[None]:
        """Acquire an exclusive lock for the given path"""
        yield  # pragma: no cover

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


class LocalFileStore(FileStore):
    root: str
    cache: MemoryLRUCache

    def __init__(
        self,
        root: str,
        cache_limit_size: int = 500,
        cache_memory_size: int = 20 * 1024 * 1024,
    ) -> None:
        if root.startswith("~"):
            root = os.path.expanduser(root)
        root = os.path.abspath(os.path.normpath(root))
        self.root = root
        os.makedirs(self.root, exist_ok=True)
        self.cache = MemoryLRUCache(cache_memory_size, cache_limit_size)

    def get_full_path(self, path: str) -> str:
        # strip leading slash to keep relative under root
        if path.startswith("/"):
            path = path[1:]
        # normalize path separators to handle both Unix (/) and Windows (\) styles
        normalized_path = path.replace("\\", "/")
        full = os.path.abspath(
            os.path.normpath(os.path.join(self.root, normalized_path))
        )
        # ensure sandboxing
        if os.path.commonpath([self.root, full]) != self.root:
            raise ValueError(f"path escapes filestore root: {path}")

        return full

    @observe(name="LocalFileStore.write", span_type="TOOL")
    def write(self, path: str, contents: str | bytes) -> None:
        full_path = self.get_full_path(path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        if isinstance(contents, str):
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(contents)
            self.cache[full_path] = contents
        else:
            with open(full_path, "wb") as f:
                f.write(contents)

    def read(self, path: str) -> str:
        full_path = self.get_full_path(path)

        if full_path in self.cache:
            return self.cache[full_path]

        if not os.path.exists(full_path):
            raise FileNotFoundError(path)

        with open(full_path, encoding="utf-8") as f:
            result = f.read()

        self.cache[full_path] = result
        return result

    @observe(name="LocalFileStore.list", span_type="TOOL")
    def list(self, path: str) -> list[str]:
        full_path = self.get_full_path(path)
        if not os.path.exists(full_path):
            return []

        # If path is a file, return the file itself (S3-consistent behavior)
        if os.path.isfile(full_path):
            return [path]

        # Otherwise it's a directory, return its contents
        files = [os.path.join(path, f) for f in os.listdir(full_path)]
        files = [f + "/" if os.path.isdir(self.get_full_path(f)) else f for f in files]
        return files

    @observe(name="LocalFileStore.delete", span_type="TOOL")
    def delete(self, path: str) -> None:
        try:
            full_path = self.get_full_path(path)
            if not os.path.exists(full_path):
                logger.debug(f"Local path does not exist: {full_path}")
                return

            if os.path.isfile(full_path):
                os.remove(full_path)
                del self.cache[full_path]
                logger.debug(f"Removed local file: {full_path}")
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)
                self.cache.clear()
                logger.debug(f"Removed local directory: {full_path}")

        except Exception as e:
            logger.error(f"Error clearing local file store: {str(e)}")

    def exists(self, path: str) -> bool:
        """Check if a file or directory exists."""
        return os.path.exists(self.get_full_path(path))

    def get_absolute_path(self, path: str) -> str:
        """Get absolute filesystem path."""
        return self.get_full_path(path)

    @contextmanager
    def lock(self, path: str, timeout: float = 30.0) -> Iterator[None]:
        """Acquire file-based lock using flock."""
        lock_path = self.get_full_path(path)
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        file_lock = FileLock(lock_path)
        try:
            with file_lock.acquire(timeout=timeout):
                yield
        except Timeout:
            logger.error(f"Failed to acquire lock within {timeout}s: {lock_path}")
            raise TimeoutError(f"Lock acquisition timed out: {path}")