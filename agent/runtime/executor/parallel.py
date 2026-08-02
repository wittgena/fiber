# agent.runtime.executor.parallel
## @lineage: engine.executor.parallel
from __future__ import annotations
from collections.abc import Callable, Sequence, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable
import threading

from engine.protocol.atoa.event.llm.observation import AgentErrorEvent
from agent.state.store.fifo import FIFOLock

if TYPE_CHECKING:
    from engine.protocol.atoa.conv.event import Event
    from engine.protocol.atoa.event.llm_convertible import ActionEvent
    from engine.protocol.action.builder import DeclaredResources, ActionDefinition

from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUTS: Final[dict[str, float]] = {
    "file": 30.0,
    "terminal": 300.0,
    "browser": 300.0,
    "mcp": 300.0,
    "tool": 60.0,
}
_DEFAULT_TIMEOUT: Final[float] = 30.0

class ResourceLockTimeout(TimeoutError):
    """A lock could not be acquired within the allowed timeout."""

class ResourceLockManager:
    def __init__(
        self,
        timeouts: dict[str, float] | None = None,
    ) -> None:
        self._locks: dict[str, FIFOLock] = {}
        self._meta_lock = threading.Lock()
        self._refcounts: dict[str, int] = {}
        self._timeouts = timeouts or DEFAULT_TIMEOUTS

    def _get_lock(self, key: str) -> FIFOLock:
        with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = FIFOLock()
            self._refcounts[key] = self._refcounts.get(key, 0) + 1
            return self._locks[key]

    def _release_lock(self, key: str) -> None:
        """Release the FIFOLock for *key* and clean up if unreferenced."""
        with self._meta_lock:
            lock = self._locks.get(key)
            if lock is None:
                return
            lock.release()
            self._refcounts[key] -= 1
            if self._refcounts[key] == 0 and not lock.locked():
                del self._locks[key]
                del self._refcounts[key]

    def _get_timeout(self, key: str) -> float:
        prefix = key.split(":", 1)[0] if ":" in key else key
        return self._timeouts.get(prefix, _DEFAULT_TIMEOUT)

    @contextmanager
    def lock(self, *resource_keys: str) -> Generator[None]:
        sorted_keys = sorted(set(resource_keys))
        acquired: list[str] = []
        try:
            for key in sorted_keys:
                timeout = self._get_timeout(key)
                if not self._get_lock(key).acquire(timeout=timeout):
                    with self._meta_lock:
                        self._refcounts[key] -= 1
                        if self._refcounts[key] == 0 and not self._locks[key].locked():
                            del self._locks[key]
                            del self._refcounts[key]
                    raise ResourceLockTimeout(
                        f"Could not acquire lock for '{key}' within {timeout}s"
                    )
                acquired.append(key)
            yield
        finally:
            for key in reversed(acquired):
                self._release_lock(key)

@runtime_checkable
class BatchExecutorProtocol(Protocol):
    """ActionBatch에서 도구를 실행하기 위한 공통 인터페이스"""
    def execute_batch(
        self,
        action_events: Sequence[ActionEvent],
        tool_runner: Callable[[ActionEvent], list[Event]],
        tools: dict[str, ActionDefinition] | None = None,
    ) -> list[list[Event]]:
        ...

class ParallelExecutor:
    """외부 I/O 호출(웹, 파일 시스템 등)을 병렬로 처리하고 락(Lock)을 관리하는 실행기"""
    def __init__(
        self,
        max_workers: int = 1,
        lock_manager: ResourceLockManager | None = None,
    ) -> None:
        self._max_workers = max_workers
        self._lock_manager = lock_manager or ResourceLockManager()

    def execute_batch(
        self,
        action_events: Sequence[ActionEvent],
        tool_runner: Callable[[ActionEvent], list[Event]],
        tools: dict[str, ActionDefinition] | None = None,
    ) -> list[list[Event]]:
        if not action_events:
            return []

        def _resolve(ae: ActionEvent) -> ActionDefinition | None:
            return tools.get(ae.tool_name) if tools else None

        if len(action_events) == 1 or self._max_workers == 1:
            return [
                self._run_safe(action, tool_runner, _resolve(action))
                for action in action_events
            ]

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = [
                executor.submit(self._run_safe, action, tool_runner, _resolve(action))
                for action in action_events
            ]

        return [future.result() for future in futures]

    def _run_safe(
        self,
        action: ActionEvent,
        tool_runner: Callable[[ActionEvent], list[Event]],
        tool: ActionDefinition | None = None,
    ) -> list[Event]:
        try:
            if tool is None:
                return tool_runner(action)

            resources = self._extract_declared_resources(action, tool)
            lock_keys = self._resolve_lock_keys(resources, tool)
            
            if not lock_keys:
                return tool_runner(action)
                
            with self._lock_manager.lock(*lock_keys):
                return tool_runner(action)

        except ValueError as e:
            logger.info(f"Tool error in '{action.tool_name}': {e}")
            return [
                AgentErrorEvent(
                    error=f"Error executing tool '{action.tool_name}': {e}",
                    tool_name=action.tool_name,
                    tool_call_id=action.tool_call_id,
                )
            ]
        except Exception as e:
            logger.error(
                f"Unexpected error in tool '{action.tool_name}': {e}",
                exc_info=True,
            )
            return [
                AgentErrorEvent(
                    error=f"Error executing tool '{action.tool_name}': {e}",
                    tool_name=action.tool_name,
                    tool_call_id=action.tool_call_id,
                )
            ]

    @staticmethod
    def _extract_declared_resources(
        action: ActionEvent,
        tool: ActionDefinition,
    ) -> DeclaredResources | None:
        """Call ``tool.declared_resources()`` if the action is parsed."""
        parsed_action = action.action
        return tool.declared_resources(parsed_action) if parsed_action else None

    @staticmethod
    def _resolve_lock_keys(
        resources: DeclaredResources | None,
        tool: ActionDefinition,
    ) -> list[str]:
        if resources is None or not resources.declared:
            return [f"tool:{tool.name}"]
        return list(resources.keys)


# ---------------------------------------------------------
# [NEW] CognitiveExecutor (내부 인지 도구용 실행기)
# ---------------------------------------------------------
class CognitiveExecutor:
    """
    내부 인지 및 제어 흐름(lang, think, finish 등)을 처리하기 위한 동기식 순차 실행기.
    - 메인 스레드에서 즉시 실행됨.
    - I/O Lock(ResourceLockManager)을 사용하지 않음.
    - 불필요한 스레드 풀 오버헤드 방지.
    """
    def execute_batch(
        self,
        action_events: Sequence[ActionEvent],
        tool_runner: Callable[[ActionEvent], list[Event]],
        tools: dict[str, ActionDefinition] | None = None,
    ) -> list[list[Event]]:
        if not action_events:
            return []
            
        results = []
        for action in action_events:
            try:
                # 락 관리나 스레드 위임 없이 즉시 동기 실행
                events = tool_runner(action)
                results.append(events)
                
            except ValueError as e:
                logger.info(f"Cognitive Tool error in '{action.tool_name}': {e}")
                results.append([
                    AgentErrorEvent(
                        error=f"Error executing cognitive tool '{action.tool_name}': {e}",
                        tool_name=action.tool_name,
                        tool_call_id=action.tool_call_id,
                    )
                ])
                
            except Exception as e:
                logger.error(
                    f"Unexpected error in cognitive tool '{action.tool_name}': {e}",
                    exc_info=True,
                )
                results.append([
                    AgentErrorEvent(
                        error=f"Error executing cognitive tool '{action.tool_name}': {e}",
                        tool_name=action.tool_name,
                        tool_call_id=action.tool_call_id,
                    )
                ])
                
        return results