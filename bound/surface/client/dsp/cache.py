# bound.surface.client.dsp.cache
## @lineage: bound.surface.dsp.cache
## @lineage: anchor.provider.dsp.cache
## @lineage: anchor.registry.provider.dsp.cache
## @lineage: xphi.xor.dsp.handler.cache
import os
import copy
import inspect
import logging
import threading
from pathlib import Path
from functools import wraps
from hashlib import sha256
from typing import Any
import cloudpickle
import orjson
import pydantic
from cachetools import LRUCache
from diskcache import FanoutCache
from anchor.registry.model.config.resolver import config
from watcher.plane.emitter import get_emitter
from xphi.scope.dsp.context import runtime

log = get_emitter(__name__)

config.telemetry = False 
config.cache = None      # 기본 캐시 비활성화 (Gate DiskCache를 대신 사용)

class Cache:
    def __init__(
        self,
        enable_disk_cache: bool,
        enable_memory_cache: bool,
        disk_cache_dir: str,
        disk_size_limit_bytes: int | None = 1024 * 1024 * 10,
        memory_max_entries: int = 1000000,
    ):
        self.enable_disk_cache = enable_disk_cache
        self.enable_memory_cache = enable_memory_cache
        if self.enable_memory_cache:
            if memory_max_entries is None:
                raise ValueError("`memory_max_entries` cannot be None. Use `math.inf` if you need an unbounded cache.")
            elif memory_max_entries <= 0:
                raise ValueError(f"`memory_max_entries` must be a positive number, but received {memory_max_entries}")
            self.memory_cache = LRUCache(maxsize=memory_max_entries)
        else:
            self.memory_cache = {}
        if self.enable_disk_cache:
            self.disk_cache = FanoutCache(
                shards=16,
                timeout=10,
                directory=disk_cache_dir,
                size_limit=disk_size_limit_bytes,
            )
        else:
            self.disk_cache = {}

        self._lock = threading.RLock()

    def __contains__(self, key: str) -> bool:
        """Check if a key is in the cache."""
        return key in self.memory_cache or key in self.disk_cache

    def cache_key(self, request: dict[str, Any], ignored_args_for_cache_key: list[str] | None = None) -> str:
        """
        Obtain a unique cache key for the given request dictionary by hashing its JSON
        representation. For request fields having types that are known to be JSON-incompatible,
        convert them to a JSON-serializable format before hashing.
        """

        ignored_args_for_cache_key = ignored_args_for_cache_key or []

        def transform_value(value):
            if isinstance(value, type) and issubclass(value, pydantic.BaseModel):
                return value.model_json_schema()
            elif isinstance(value, pydantic.BaseModel):
                return value.model_dump(mode="json")
            elif callable(value):
                # Try to get the source code of the callable if available
                import inspect

                try:
                    # For regular functions, we can get the source code
                    return f"<callable_source:{inspect.getsource(value)}>"
                except (TypeError, OSError):
                    # For lambda functions or other callables where source isn't available,
                    # use a string representation
                    return f"<callable:{value.__name__ if hasattr(value, '__name__') else 'lambda'}>"
            elif isinstance(value, dict):
                return {k: transform_value(v) for k, v in value.items()}
            else:
                return value

        params = {k: transform_value(v) for k, v in request.items() if k not in ignored_args_for_cache_key}
        return sha256(orjson.dumps(params, option=orjson.OPT_SORT_KEYS)).hexdigest()

    def get(self, request: dict[str, Any], ignored_args_for_cache_key: list[str] | None = None) -> Any:

        if not self.enable_memory_cache and not self.enable_disk_cache:
            return None

        try:
            key = self.cache_key(request, ignored_args_for_cache_key)
        except Exception:
            log.debug(f"Failed to generate cache key for request: {request}")
            return None

        if self.enable_memory_cache and key in self.memory_cache:
            with self._lock:
                response = self.memory_cache[key]
        elif self.enable_disk_cache and key in self.disk_cache:
            # Found on disk but not in memory cache, add to memory cache
            response = self.disk_cache[key]
            if self.enable_memory_cache:
                with self._lock:
                    self.memory_cache[key] = response
        else:
            return None

        response = copy.deepcopy(response)
        if hasattr(response, "usage"):
            # Clear the usage data when cache is hit, because no LM call is made
            response.usage = {}
            response.cache_hit = True
        return response

    def put(
        self,
        request: dict[str, Any],
        value: Any,
        ignored_args_for_cache_key: list[str] | None = None,
        enable_memory_cache: bool = True,
    ) -> None:
        enable_memory_cache = self.enable_memory_cache and enable_memory_cache

        # Early return to avoid computing cache key if both memory and disk cache are disabled
        if not enable_memory_cache and not self.enable_disk_cache:
            return

        try:
            key = self.cache_key(request, ignored_args_for_cache_key)
        except Exception:
            log.debug(f"Failed to generate cache key for request: {request}")
            return

        if enable_memory_cache:
            with self._lock:
                self.memory_cache[key] = value

        if self.enable_disk_cache:
            try:
                self.disk_cache[key] = value
            except Exception as e:
                # Disk cache writing can fail for different reasons, e.g. disk full or the `value` is not picklable.
                log.debug(f"Failed to put value in disk cache: {value}, {e}")

    def reset_memory_cache(self) -> None:
        if not self.enable_memory_cache:
            return

        with self._lock:
            self.memory_cache.clear()

    def save_memory_cache(self, filepath: str) -> None:
        if not self.enable_memory_cache:
            return

        with self._lock:
            with open(filepath, "wb") as f:
                cloudpickle.dump(self.memory_cache, f)

    def load_memory_cache(self, filepath: str, allow_pickle: bool = False) -> None:
        if not allow_pickle:
            raise ValueError("Loading untrusted .pkl files can run arbitrary code, which may be dangerous. \
            Set `allow_pickle=True` to load if you are running in a trusted environment and the file is from a trusted source.")

        if not self.enable_memory_cache:
            return

        with self._lock:
            with open(filepath, "rb") as f:
                self.memory_cache = cloudpickle.load(f)


def request_cache(
    cache_arg_name: str | None = None,
    ignored_args_for_cache_key: list[str] | None = None,
    enable_memory_cache: bool = True,
    *,  # everything after this is keyword-only
    maxsize: int | None = None,  # legacy / no-op
):
    """
    Decorator for applying caching to a function based on the request argument.

    Args:
        cache_arg_name: The name of the argument that contains the request. If not provided, the entire kwargs is used
            as the request.
        ignored_args_for_cache_key: A list of arguments to ignore when computing the cache key from the request.
        enable_memory_cache: Whether to enable in-memory cache at call time. If False, the memory cache will not be
            written to on new data.
    """
    ignored_args_for_cache_key = ignored_args_for_cache_key or ["api_key", "api_base", "base_url"]
    # Deprecation notice
    if maxsize is not None:
        log.warning(
            "[DEPRECATION] `maxsize` is deprecated and no longer does anything; "
            "the cache is now handled internally by `cache`. "
            "This parameter will be removed in a future release.",
        )

    def decorator(fn):
        @wraps(fn)
        def process_request(args, kwargs):
            # Use fully qualified function name for uniqueness
            fn_identifier = f"{fn.__module__}.{fn.__qualname__}"
            if cache_arg_name:
                # When `cache_arg_name` is provided, use the value of the argument with this name as the request for
                # caching.
                modified_request = copy.deepcopy(kwargs[cache_arg_name])
            else:
                # When `cache_arg_name` is not provided, use the entire kwargs as the request for caching.
                modified_request = copy.deepcopy(kwargs)
                for i, arg in enumerate(args):
                    modified_request[f"positional_arg_{i}"] = arg
            modified_request["_fn_identifier"] = fn_identifier

            return modified_request

        @wraps(fn)
        def sync_wrapper(*args, **kwargs):
            cache = GATE_CACHE
            modified_request = process_request(args, kwargs)

            # Retrieve from cache if available
            cached_result = cache.get(modified_request, ignored_args_for_cache_key)

            if cached_result is not None:
                return cached_result

            # Otherwise, compute and store the result
            # Make a copy of the original request in case it's modified in place, e.g., deleting some fields
            original_request = copy.deepcopy(modified_request)
            result = fn(*args, **kwargs)
            # `enable_memory_cache` can be provided at call time to avoid indefinite growth.
            cache.put(original_request, result, ignored_args_for_cache_key, enable_memory_cache)

            return result

        @wraps(fn)
        async def async_wrapper(*args, **kwargs):
            cache = GATE_CACHE
            modified_request = process_request(args, kwargs)

            # Retrieve from cache if available
            cached_result = cache.get(modified_request, ignored_args_for_cache_key)
            if cached_result is not None:
                return cached_result

            # Otherwise, compute and store the result
            # Make a copy of the original request in case it's modified in place, e.g., deleting some fields
            original_request = copy.deepcopy(modified_request)
            result = await fn(*args, **kwargs)
            cache.put(original_request, result, ignored_args_for_cache_key, enable_memory_cache)
            return result

        if inspect.iscoroutinefunction(fn):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

DISK_CACHE_DIR = os.environ.get("GATE_CACHEDIR") or os.path.join(Path.home(), ".gate_cache")
DISK_CACHE_LIMIT = int(os.environ.get("GATE_CACHE_LIMIT", 3e10))  # 30 GB default

def configure_cache(
    enable_disk_cache: bool | None = True,
    enable_memory_cache: bool | None = True,
    disk_cache_dir: str | None = DISK_CACHE_DIR,
    disk_size_limit_bytes: int | None = DISK_CACHE_LIMIT,
    memory_max_entries: int = 1000000,
):
    GATE_CACHE = Cache(
        enable_disk_cache,
        enable_memory_cache,
        disk_cache_dir,
        disk_size_limit_bytes,
        memory_max_entries,
    )
    # Update the reference to point to the new cache
    runtime.cache = GATE_CACHE

def _get_gate_cache():
    disk_cache_dir = os.environ.get("GATE_CACHEDIR") or os.path.join(Path.home(), ".gate_cache")
    disk_cache_limit = int(os.environ.get("GATE_CACHE_LIMIT", 3e10))

    try:
        _gate_cache = Cache(
            enable_disk_cache=True,
            enable_memory_cache=True,
            disk_cache_dir=disk_cache_dir,
            disk_size_limit_bytes=disk_cache_limit,
            memory_max_entries=1000000,
        )
    except Exception as e:
        logger.warning("Failed to initialize disk cache, falling back to memory-only cache: %s", e)
        _gate_cache = Cache(
            enable_disk_cache=False,
            enable_memory_cache=True,
            disk_cache_dir=disk_cache_dir,
            disk_size_limit_bytes=disk_cache_limit,
            memory_max_entries=1000000,
        )
    return _gate_cache

GATE_CACHE = _get_gate_cache()