# agent.space.terminal.session.builder
## @lineage: bound.space.terminal.session.builder
## @lineage: agent.runtime.terminal.session.builder
## @lineage: ator.runtime.session.builder
## @lineage: agent.protocol.builder
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
import warnings
from collections.abc import Callable, Sequence, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable, Literal

import anyio
from anyio.from_thread import start_blocking_portal

from agent.llm.driver.event.observation import AgentErrorEvent
from agent.loop.runtime.protocol.tool.terminal import TerminalObservation
from agent.loop.runtime.protocol.terminal.context import (
    ExecutionContext,
    ExecutionEngine,
    ExecutionMiddleware,
)

if TYPE_CHECKING:
    from agent.llm.driver.event.action import ActionEvent
    from agent.loop.conv.action.builder import DeclaredResources, ActionDefinition

from arch.model.conv.event import Event
from arch.xor.secret.redact import redact_string
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

# =============================================================================
# 1. Protocols & Basic Utilities
# =============================================================================

@runtime_checkable
class AsyncExecutorProtocol(Protocol):
    def run_async(self, awaitable_or_fn: Callable[..., Any] | Any, *args, timeout: float | None = None, **kwargs) -> Any: ...
    def close(self) -> None: ...

@runtime_checkable
class BatchExecutorProtocol(Protocol):
    def execute_batch(self, action_events: Sequence[ActionEvent], tool_runner: Callable[[ActionEvent], list[Event]], tools: dict[str, ActionDefinition] | None = None) -> list[list[Event]]: ...

_SENSITIVE_ENV_VARS = frozenset({"SESSION_API_KEY"})

def sanitized_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    base_env: dict[str, str] = dict(os.environ) if env is None else dict(env)
    for key in _SENSITIVE_ENV_VARS:
        base_env.pop(key, None)
    if "LD_LIBRARY_PATH_ORIG" in base_env:
        origin = base_env["LD_LIBRARY_PATH_ORIG"]
        if origin: base_env["LD_LIBRARY_PATH"] = origin
        else: base_env.pop("LD_LIBRARY_PATH", None)
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
    if proc is None: raise RuntimeError("Failed to start process")

    stdout_lines, stderr_lines = [], []
    if proc.stdout is None or proc.stderr is None: raise RuntimeError("Failed to capture stdout/stderr")

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
    stdout_thread.start(); stderr_thread.start()

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout_thread.join(); stderr_thread.join()
        return subprocess.CompletedProcess(cmd_to_run, -1, "".join(stdout_lines), "".join(stderr_lines))

    stdout_thread.join(timeout=timeout)
    stderr_thread.join(timeout=timeout)
    return subprocess.CompletedProcess(cmd_to_run, proc.returncode, "".join(stdout_lines), "".join(stderr_lines))

def _is_tmux_available() -> bool:
    """Check if tmux is available on the system."""
    try:
        result = subprocess.run(
            ["tmux", "-V"],
            capture_output=True,
            text=True,
            timeout=5.0,
            env=sanitized_env(),
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

def _is_powershell_available() -> bool:
    """Check if PowerShell is available on the system."""
    if platform.system() == "Windows":
        powershell_cmd = "powershell"
    else:
        powershell_cmd = "pwsh"

    try:
        result = subprocess.run(
            [powershell_cmd, "-Command", "Write-Host 'PowerShell Available'"],
            capture_output=True,
            text=True,
            timeout=5.0,
            env=sanitized_env(),
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


# =============================================================================
# 2. Resource & Lock Management
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
# 3. Executors & Factory
# =============================================================================

class AsyncExecutor:
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

    def run_async(self, awaitable_or_fn: Callable[..., Any] | Any, *args, timeout: float | None = None, **kwargs) -> Any:
        portal = self._ensure_portal()
        if inspect.iscoroutine(awaitable_or_fn):
            coro = awaitable_or_fn
        elif inspect.iscoroutinefunction(awaitable_or_fn):
            coro = awaitable_or_fn(*args, **kwargs)
        else:
            raise TypeError("run_async expects a coroutine or async function")

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

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    def __del__(self):
        try: self.close()
        except Exception: pass


class ParallelExecutor:
    def __init__(self, max_workers: int = 1, lock_manager: ResourceLockManager | None = None):
        self._max_workers = max_workers
        self._lock_manager = lock_manager or ResourceLockManager()

    def execute_batch(self, action_events: Sequence[ActionEvent], tool_runner: Callable[[ActionEvent], list[Event]], tools: dict[str, ActionDefinition] | None = None) -> list[list[Event]]:
        if not action_events: return []
        def _resolve(ae: ActionEvent) -> ActionDefinition | None: return tools.get(ae.tool_name) if tools else None
        
        if len(action_events) == 1 or self._max_workers == 1:
            return [self._run_safe(action, tool_runner, _resolve(action), bypass_lock=True) for action in action_events]

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = [executor.submit(self._run_safe, action, tool_runner, _resolve(action)) for action in action_events]
        return [future.result() for future in futures]

    def _run_safe(self, action: ActionEvent, tool_runner: Callable[[ActionEvent], list[Event]], tool: ActionDefinition | None = None, bypass_lock: bool = False) -> list[Event]:
        try:
            if tool is None: return tool_runner(action)
            resources = self._extract_declared_resources(action, tool)
            lock_keys = self._resolve_lock_keys(resources, tool)

            if bypass_lock or not lock_keys:
                return tool_runner(action)

            with self._lock_manager.lock(*lock_keys):
                return tool_runner(action)
                
        except ValueError as e:
            log.info(f"Tool error in '{action.tool_name}': {e}")
            return [AgentErrorEvent(error=f"Error executing tool '{action.tool_name}': {e}", tool_name=action.tool_name, tool_call_id=action.tool_call_id)]
        except Exception as e:
            log.error(f"Unexpected error in tool '{action.tool_name}': {e}", exc_info=True)
            return [AgentErrorEvent(error=f"Error executing tool '{action.tool_name}': {e}", tool_name=action.tool_name, tool_call_id=action.tool_call_id)]

    @staticmethod
    def _extract_declared_resources(action: ActionEvent, tool: ActionDefinition) -> DeclaredResources | None:
        parsed_action = action.action
        return tool.declared_resources(parsed_action) if parsed_action else None

    @staticmethod
    def _resolve_lock_keys(resources: DeclaredResources | None, tool: ActionDefinition) -> list[str]:
        if resources is None or not resources.declared: return [f"tool:{tool.name}"]
        return list(resources.keys)


class ExecutorFactory:
    def __init__(self):
        self._lock = threading.Lock()
        self._shared_async_executor: AsyncExecutor | None = None
        self._shared_lock_manager = ResourceLockManager()

    def get_async_executor(self) -> AsyncExecutorProtocol:
        with self._lock:
            if self._shared_async_executor is None:
                self._shared_async_executor = AsyncExecutor()
            return self._shared_async_executor

    def get_parallel_executor(self, max_workers: int = 4) -> BatchExecutorProtocol:
        return ParallelExecutor(max_workers=max_workers, lock_manager=self._shared_lock_manager)

    def shutdown(self):
        with self._lock:
            if self._shared_async_executor:
                self._shared_async_executor.close()
                self._shared_async_executor = None

executor_factory = ExecutorFactory()


# =============================================================================
# 4. Chain Builder
# =============================================================================

class ChainBuilder(ExecutionEngine):
    def __init__(self, base_engine: ExecutionEngine, middlewares: list[ExecutionMiddleware]):
        self.base_engine = base_engine
        self.middlewares = middlewares

    def execute(self, context: ExecutionContext) -> TerminalObservation:
        """Builds and executes the chain of responsibility."""

        def _build_chain(index: int) -> Callable[[ExecutionContext], TerminalObservation]:
            if index < len(self.middlewares):
                middleware = self.middlewares[index]
                # Pass the current context and the next step in the chain
                return lambda ctx: middleware.process(ctx, _build_chain(index + 1))
            else:
                # Terminal node: The actual low-level execution engine (e.g., PollingExecutionEngine)
                return lambda ctx: self.base_engine.execute(ctx)

        # Initiate the recursive chain from the first middleware
        first_step = _build_chain(0)
        
        try:
            return first_step(context)
        except Exception as e:
            log.error(f"Pipeline execution failed: {str(e)}")
            return TerminalObservation.from_text(
                text=f"Internal pipeline error during execution: {str(e)}",
                command=context.action.command,
                is_error=True
            )


# =============================================================================
# 5. Terminal Session Builder
# =============================================================================

def create_terminal_session(
    work_dir: str,
    username: str | None = None,
    no_change_timeout_seconds: int | None = None,
    terminal_type: Literal["tmux", "subprocess"] | None = None,
    shell_path: str | None = None,
): # Type hint for return 'TerminalSession' is removed locally to avoid circular import, handled dynamically
    
    # Lazy imports to avoid circular dependencies with session & terminal modules
    from agent.loop.runtime.protocol.terminal.session import TerminalSession

    if terminal_type:
        # Force specific session type
        if terminal_type == "tmux":
            if not _is_tmux_available():
                raise RuntimeError("Tmux is not available on this system")
            from agent.space.terminal.tmux.interface import TmuxTerminal

            log.info("Using forced TmuxTerminal")
            terminal = TmuxTerminal(work_dir, username)
            return TerminalSession(terminal, no_change_timeout_seconds)
            
        elif terminal_type == "subprocess":
            from agent.space.terminal.backend import SubprocessTerminal

            log.info("Using forced SubprocessTerminal")
            terminal = SubprocessTerminal(work_dir, username, shell_path)
            return TerminalSession(terminal, no_change_timeout_seconds)
        else:
            raise ValueError(f"Unknown session type: {terminal_type}")

    # Auto-detect based on system capabilities
    system = platform.system()
    if system == "Windows":
        raise NotImplementedError("Windows is not supported yet")
    else:
        # On Unix-like systems, prefer tmux if available, otherwise use subprocess
        if _is_tmux_available():
            from agent.space.terminal.tmux.interface import TmuxTerminal

            log.info("Auto-detected: Using TmuxTerminal (tmux available)")
            terminal = TmuxTerminal(work_dir, username)
            return TerminalSession(terminal, no_change_timeout_seconds)
        else:
            from agent.space.terminal.backend import SubprocessTerminal
            _tmux_warning = (
                "tmux is not installed. Falling back to subprocess-based"
                " terminal, which may be less stable. For best agent"
                " performance, install tmux (e.g. `apt-get install tmux`"
                " or `brew install tmux`)."
            )
            log.warning(_tmux_warning)
            warnings.warn(_tmux_warning, stacklevel=2)
            terminal = SubprocessTerminal(work_dir, username, shell_path)
            return TerminalSession(terminal, no_change_timeout_seconds)