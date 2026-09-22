# fiber.dev.ex.agent.protocol.executor
## @lineage: fiber.dev.ex.agent.protocol
from __future__ import annotations

import os
import sys
import shlex
import subprocess
import threading
import atexit
import inspect
import weakref
import platform
import functools
from collections.abc import Callable, Sequence, Generator, Mapping
from contextlib import contextmanager
from typing import Any, Final, Protocol, runtime_checkable, TypeVar, Generic

import anyio
from anyio.from_thread import start_blocking_portal

from xphi.arch.bound.xor.secret.redact import redact_string
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

"""Generic Protocols"""
T_Action = TypeVar("T_Action")
T_Event = TypeVar("T_Event")
T_ToolDef = TypeVar("T_ToolDef")

@runtime_checkable
class AsyncExecutorProtocol(Protocol):
    def run_async(self, awaitable_or_fn: Callable[..., Any] | Any, *args: Any, timeout: float | None = None, **kwargs: Any) -> Any: ...
    def close(self) -> None: ...

@runtime_checkable
class BatchExecutorProtocol(Protocol, Generic[T_Action, T_Event, T_ToolDef]):
    def execute_batch(
        self, 
        action_events: Sequence[T_Action], 
        tool_runner: Callable[[T_Action], list[T_Event]], 
        tools: dict[str, T_ToolDef] | None = None
    ) -> list[list[T_Event]]: ...

"""Execution Utilities"""
_SENSITIVE_ENV_VARS: Final = frozenset({"SESSION_API_KEY"})

def sanitized_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    base_env: dict[str, str] = dict(os.environ) if env is None else dict(env)
    for key in _SENSITIVE_ENV_VARS:
        base_env.pop(key, None)
    if "LD_LIBRARY_PATH_ORIG" in base_env:
        origin = base_env["LD_LIBRARY_PATH_ORIG"]
        if origin:
            base_env["LD_LIBRARY_PATH"] = origin
        else:
            base_env.pop("LD_LIBRARY_PATH", None)
    return base_env

def execute_command(
    cmd: list[str] | str,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
    timeout: float | None = None,
    print_output: bool = True,
) -> subprocess.CompletedProcess:
    if isinstance(cmd, str):
        cmd_to_run, use_shell, cmd_str = cmd, True, cmd
    else:
        cmd_to_run, use_shell, cmd_str = cmd, False, " ".join(shlex.quote(c) for c in cmd)

    log.info("$ %s", redact_string(cmd_str))
    proc = subprocess.Popen(
        cmd_to_run, cwd=cwd, env=sanitized_env(env),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1, shell=use_shell,
    )
    if proc is None:
        raise RuntimeError("Failed to start process")

    if proc.stdout is None or proc.stderr is None:
        raise RuntimeError("Failed to capture stdout/stderr streams")

    stdout_lines, stderr_lines = [], []

    def read_stream(stream, lines, output_stream):
        try:
            for line in stream:
                if print_output:
                    output_stream.write(line)
                    output_stream.flush()
                lines.append(line)
        except Exception as e:
            log.error(f"Failed to read stream: {e}")

    stdout_thread = threading.Thread(target=read_stream, args=(proc.stdout, stdout_lines, sys.stdout))
    stderr_thread = threading.Thread(target=read_stream, args=(proc.stderr, stderr_lines, sys.stderr))
    
    stdout_thread.start()
    stderr_thread.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_thread.join()
        stderr_thread.join()
        return subprocess.CompletedProcess(cmd_to_run, -1, "".join(stdout_lines), "".join(stderr_lines))

    stdout_thread.join(timeout=timeout)
    stderr_thread.join(timeout=timeout)
    
    return subprocess.CompletedProcess(cmd_to_run, proc.returncode, "".join(stdout_lines), "".join(stderr_lines))

@functools.lru_cache(maxsize=1)
def _is_tmux_available() -> bool:
    try:
        result = subprocess.run(
            ["tmux", "-V"],
            capture_output=True, text=True, timeout=5.0, env=sanitized_env()
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

@functools.lru_cache(maxsize=1)
def _is_powershell_available() -> bool:
    powershell_cmd = "powershell" if platform.system() == "Windows" else "pwsh"
    try:
        result = subprocess.run(
            [powershell_cmd, "-Command", "Write-Host 'PowerShell Available'"],
            capture_output=True, text=True, timeout=5.0, env=sanitized_env()
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

# =============================================================================
# Resource & Lock Management
# =============================================================================

DEFAULT_TIMEOUTS: Final[dict[str, float]] = {"file": 30.0, "terminal": 300.0, "mcp": 300.0, "tool": 60.0}
_DEFAULT_TIMEOUT: Final[float] = 30.0

class ResourceLockTimeout(TimeoutError): pass

class ResourceLockManager:
    def __init__(self, timeouts: dict[str, float] | None = None):
        self._locks: dict[str, Any] = {}
        self._meta_lock = threading.Lock()
        self._refcounts: dict[str, int] = {}
        self._timeouts = timeouts or DEFAULT_TIMEOUTS

    def _get_lock(self, key: str) -> Any:
        with self._meta_lock:
            if key not in self._locks: 
                self._locks[key] = threading.RLock()
            self._refcounts[key] = self._refcounts.get(key, 0) + 1
            return self._locks[key]

    def _release_lock(self, key: str):
        with self._meta_lock:
            lock = self._locks.get(key)
            if lock is None: return
            
            lock.release()
            self._refcounts[key] -= 1
            
            if self._refcounts[key] == 0:
                del self._locks[key]
                del self._refcounts[key]

    def _get_timeout(self, key: str) -> float:
        prefix = key.split(":", 1)[0] if ":" in key else key
        return self._timeouts.get(prefix, _DEFAULT_TIMEOUT)

    @contextmanager
    def lock(self, *resource_keys: str) -> Generator[None, None, None]:
        sorted_keys = sorted(set(resource_keys))
        acquired: list[str] = []
        try:
            for key in sorted_keys:
                timeout = self._get_timeout(key)
                target_lock = self._get_lock(key)
                
                if not target_lock.acquire(timeout=timeout):
                    with self._meta_lock:
                        self._refcounts[key] -= 1
                        if self._refcounts[key] == 0:
                            del self._locks[key]
                            del self._refcounts[key]
                    raise ResourceLockTimeout(f"Could not acquire lock for '{key}' within {timeout}s")
                
                acquired.append(key)
            yield
        finally:
            for key in reversed(acquired):
                self._release_lock(key)

# =============================================================================
# Async Execution Engine
# =============================================================================

class AsyncExecutor(AsyncExecutorProtocol):
    """Provides a safe bridge to execute coroutines in a background thread."""
    def __init__(self):
        self._portal = None
        self._portal_cm = None
        self._lock = threading.Lock()
        self._atexit_registered = False

    def _ensure_portal(self):
        with self._lock:
            if self._portal is None:
                self._portal_cm = start_blocking_portal()
                self._portal = self._portal_cm.__enter__()
                if not self._atexit_registered:
                    weak_self = weakref.ref(self)
                    def cleanup():
                        executor = weak_self()
                        if executor is not None:
                            try:
                                executor.close()
                            except Exception:
                                pass
                    atexit.register(cleanup)
                    self._atexit_registered = True
            return self._portal

    def run_async(self, awaitable_or_fn: Callable[..., Any] | Any, *args: Any, timeout: float | None = None, **kwargs: Any) -> Any:
        portal = self._ensure_portal()
        
        if inspect.iscoroutine(awaitable_or_fn):
            coro = awaitable_or_fn
        elif inspect.iscoroutinefunction(awaitable_or_fn):
            coro = awaitable_or_fn(*args, **kwargs)
        else:
            raise TypeError("run_async expects a coroutine or an async function")

        if timeout is not None:
            async def _with_timeout():
                with anyio.fail_after(timeout):
                    return await coro
            return portal.call(_with_timeout)
        else:
            async def _execute():
                return await coro
            return portal.call(_execute)

    def close(self):
        with self._lock:
            portal_cm = self._portal_cm
            self._portal_cm = None
            self._portal = None

        if portal_cm is not None:
            try:
                portal_cm.__exit__(None, None, None)
            except Exception as e:
                log.warning(f"Error closing BlockingPortal: {e}")

    def __enter__(self): 
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
        
    def __del__(self):
        try: 
            self.close()
        except Exception: 
            pass