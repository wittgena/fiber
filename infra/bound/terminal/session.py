# fiber.infra.bound.terminal.session
## @lineage: fiber.dev.ex.terminal.session
## @lineage: phase.align.terminal.session
## @lineage: phase.agent.terminal.session
from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable

from fiber.infra.bound.terminal.metadata import (
    CmdOutputMetadata,
    TerminalCommandStatus,
    NO_CHANGE_TIMEOUT_SECONDS,
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

@dataclass
class TerminalRequest:
    """에이전트 스키마와 분리된 순수 터미널 실행 요청 객체"""
    command: str
    is_input: bool = False
    timeout: float | None = None
    reset: bool = False


@dataclass
class TerminalResult:
    """에이전트 스키마와 분리된 순수 터미널 실행 결과 객체"""
    command: str
    output: str
    exit_code: int | None = None
    is_error: bool = False
    timeout: bool = False
    metadata: CmdOutputMetadata = field(default_factory=CmdOutputMetadata)


# ==========================================
# 1. Terminal Escape Sequence Filter
# ==========================================
_DSR_PATTERN = re.compile(rb"\x1b\[6n")
_OSC_QUERY_PATTERN = re.compile(
    rb"\x1b\]\d+(?:;[^;\x07\x1b]*)?;\?(?:\x07|\x1b\\)"
)
_DA_PATTERN = re.compile(rb"\x1b\[0?c")
_DA2_PATTERN = re.compile(rb"\x1b\[>0?c")
_DECRQSS_PATTERN = re.compile(rb"\x1bP\$q[^\x1b]*\x1b\\")
_INCOMPLETE_ESC_PATTERN = re.compile(
    rb"(?:\x1b$|\x1b\[[0-9;>]*$|\x1b\][^\x07]*$|\x1bP(?:[^\x1b]|\x1b(?!\\))*$)"
)

def _filter_complete_queries(output_bytes: bytes) -> bytes:
    output_bytes = _DSR_PATTERN.sub(b"", output_bytes)
    output_bytes = _OSC_QUERY_PATTERN.sub(b"", output_bytes)
    output_bytes = _DA_PATTERN.sub(b"", output_bytes)
    output_bytes = _DA2_PATTERN.sub(b"", output_bytes)
    output_bytes = _DECRQSS_PATTERN.sub(b"", output_bytes)
    return output_bytes


class TerminalQueryFilter:
    def __init__(self) -> None:
        self._pending: bytes = b""

    def reset(self) -> None:
        self._pending = b""

    def filter(self, output: str) -> str:
        output_bytes = output.encode("utf-8", errors="surrogateescape")
        if self._pending:
            output_bytes = self._pending + output_bytes
            self._pending = b""

        match = _INCOMPLETE_ESC_PATTERN.search(output_bytes)
        if match:
            self._pending = output_bytes[match.start() :]
            output_bytes = output_bytes[: match.start()]

        output_bytes = _filter_complete_queries(output_bytes)
        return output_bytes.decode("utf-8", errors="surrogateescape")

    def flush(self) -> str:
        if not self._pending:
            return ""
        pending = self._pending
        self._pending = b""
        filtered = _filter_complete_queries(pending)
        return filtered.decode("utf-8", errors="surrogateescape")


# ==========================================
# 2. Terminal Backend Interface
# ==========================================
class TerminalInterface(ABC):
    work_dir: str
    username: str | None
    _initialized: bool
    _closed: bool

    def __init__(self, work_dir: str, username: str | None = None):
        self.work_dir = work_dir
        self.username = username
        self._initialized = False
        self._closed = False

    @abstractmethod
    def initialize(self) -> None: pass

    @abstractmethod
    def close(self) -> None: pass

    @abstractmethod
    def send_keys(self, text: str, enter: bool = True) -> None: pass

    @abstractmethod
    def read_screen(self) -> str: pass

    @abstractmethod
    def clear_screen(self) -> None: pass

    @abstractmethod
    def interrupt(self) -> bool: pass

    @abstractmethod
    def is_running(self) -> bool: pass

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def closed(self) -> bool:
        return self._closed

    def is_powershell(self) -> bool:
        return False


# ==========================================
# 3. Execution Context & Engine Contracts
# ==========================================
class ExecutionContext:
    def __init__(
        self,
        session: TerminalSession,
        request: TerminalRequest,
        context_data: Any = None,
    ):
        self.session = session
        self.request = request
        self.context_data = context_data
        self.metadata: dict[str, Any] = {}
        self.is_aborted: bool = False


class ExecutionEngine(ABC):
    @abstractmethod
    def execute(self, context: ExecutionContext) -> TerminalResult:
        pass


class ExecutionMiddleware(ABC):
    @abstractmethod
    def process(
        self, 
        context: ExecutionContext, 
        next_call: Callable[[ExecutionContext], TerminalResult]
    ) -> TerminalResult:
        pass


# ==========================================
# 4. Unified Terminal Session
# ==========================================
class TerminalSession:
    terminal: TerminalInterface
    prev_status: TerminalCommandStatus | None
    prev_output: str
    engine: ExecutionEngine

    def __init__(
        self,
        terminal: TerminalInterface,
        no_change_timeout_seconds: int | None = None,
        execution_engine: ExecutionEngine | None = None,
    ):
        self.work_dir = terminal.work_dir
        self.username = terminal.username
        self.no_change_timeout_seconds = no_change_timeout_seconds or NO_CHANGE_TIMEOUT_SECONDS
        self._initialized = False
        self._closed = False
        self._cwd = os.path.abspath(self.work_dir)

        self.terminal = terminal
        self.prev_status = None
        self.prev_output = ""
        self._query_filter = TerminalQueryFilter()
        
        if execution_engine is None:
            from fiber.infra.bound.terminal.polling import PollingExecutionEngine
            self.engine = PollingExecutionEngine()
        else:
            self.engine = execution_engine

    @classmethod
    def attach_to_existing(
        cls,
        terminal: TerminalInterface,
        no_change_timeout_seconds: int | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> TerminalSession:
        session = cls(terminal, no_change_timeout_seconds, execution_engine)
        session._initialized = True
        return session

    def initialize(self) -> None:
        self.terminal.initialize()
        self._initialized = True
        log.debug(f"Unified session initialized with {type(self.terminal).__name__}")

    def close(self) -> None:
        if self._closed:
            return
        self.terminal.close()
        self._closed = True

    def interrupt(self) -> bool:
        return self.terminal.interrupt()

    def is_running(self) -> bool:
        if not self._initialized:
            return False
        return self.prev_status in {
            TerminalCommandStatus.CONTINUE,
            TerminalCommandStatus.NO_CHANGE_TIMEOUT,
            TerminalCommandStatus.HARD_TIMEOUT,
        }

    def execute(
        self, 
        request: TerminalRequest, 
        context_data: Any = None
    ) -> TerminalResult:
        """순수 TerminalRequest를 받아 TerminalResult를 반환합니다."""
        if not self._initialized:
            raise RuntimeError("Unified session is not initialized")
            
        if request.reset:
            self.close()
            self.initialize()
            self.prev_status = None
            self.prev_output = ""
            self._query_filter.reset()

        context = ExecutionContext(
            session=self, 
            request=request, 
            context_data=context_data
        )
        return self.engine.execute(context)

    @property
    def cwd(self) -> str:
        return self._cwd

    def __del__(self) -> None:
        try:
            self.close()
        except ImportError:
            pass

TerminalSessionBase = TerminalSession