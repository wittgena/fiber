# ator.driver.tool.terminal.session
## @lineage: driver.tool.terminal.session
## @lineage: ator.bound.tool.terminal.session
## @lineage: eco.tool.terminal.session
## @lineage: engine.tool.terminal.session
## @lineage: engine.terminal.session
## @lineage: phi.executor.terminal.session
## @lineage: phi.engine.terminal.session
## @lineage: swarm.engine.terminal.session
import re
import traceback
from typing import Any, TYPE_CHECKING
import bashlex
from bashlex.errors import ParsingError

from ator.conv.protocol.tool.terminal import TerminalAction, TerminalObservation
from ator.conv.context.state.protocol import ToolExecutionContextProtocol

from ator.driver.tool.terminal.interface import TerminalInterface, TerminalSessionBase
from ator.driver.tool.terminal.context import ExecutionEngine, ExecutionContext
from ator.driver.tool.terminal.polling import PollingExecutionEngine

from eco.bound.xor.bridge.tool.command.terminal import TerminalCommandStatus
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

_DSR_PATTERN = re.compile(rb"\x1b\[6n")
_OSC_QUERY_PATTERN = re.compile(
    rb"\x1b\]"  # OSC introducer
    rb"\d+"  # Parameter number (10, 11, 4, 12, etc.)
    rb"(?:;[^;\x07\x1b]*)?"  # Optional sub-parameter (e.g., palette index)
    rb";\?"  # Query marker - the key indicator this is a query
    rb"(?:\x07|\x1b\\)"  # BEL or ST terminator
)
_DA_PATTERN = re.compile(rb"\x1b\[0?c")
_DA2_PATTERN = re.compile(rb"\x1b\[>0?c")
_DECRQSS_PATTERN = re.compile(
    rb"\x1bP\$q"  # DCS introducer + DECRQSS
    rb"[^\x1b]*"  # Setting identifier
    rb"\x1b\\"  # ST terminator
)

_INCOMPLETE_ESC_PATTERN = re.compile(
    rb"(?:"
    rb"\x1b$|"  # ESC at end (might be start of any sequence)
    rb"\x1b\[[0-9;>]*$|"  # CSI without command char
    rb"\x1b\][^\x07]*$|"  # OSC without BEL terminator (ST needs \x1b\)
    rb"\x1bP(?:[^\x1b]|\x1b(?!\\))*$"  # DCS without complete ST terminator
    rb")"
)

def _filter_complete_queries(output_bytes: bytes) -> bytes:
    """Filter complete terminal query sequences from output bytes."""
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

class TerminalSession(TerminalSessionBase):
    terminal: TerminalInterface
    prev_status: TerminalCommandStatus | None
    prev_output: str
    engine: ExecutionEngine  # 실행 전략 엔진 (또는 미들웨어 파이프라인)

    def __init__(
        self,
        terminal: TerminalInterface,
        no_change_timeout_seconds: int | None = None,
        execution_engine: ExecutionEngine | None = None,  # 의존성 주입(DI) 포인트
    ):
        super().__init__(
            terminal.work_dir,
            terminal.username,
            no_change_timeout_seconds,
        )
        self.terminal = terminal
        
        # 상태 관리를 위한 변수들 (엔진에서 참조 및 수정함)
        self.prev_status = None
        self.prev_output = ""
        self._query_filter = TerminalQueryFilter()
        
        # 외부에서 주입받은 엔진이 없으면 기본 폴링 엔진으로 초기화
        self.engine = execution_engine or PollingExecutionEngine()

    @classmethod
    def attach_to_existing(
        cls,
        terminal: TerminalInterface,
        no_change_timeout_seconds: int | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> "TerminalSession":
        session = cls(terminal, no_change_timeout_seconds, execution_engine)
        session._initialized = True
        return session

    def initialize(self) -> None:
        """Initialize the terminal backend."""
        self.terminal.initialize()
        self._initialized = True
        log.debug(f"Unified session initialized with {type(self.terminal).__name__}")

    def close(self) -> None:
        """Clean up the terminal backend."""
        if self._closed:
            return
        self.terminal.close()
        self._closed = True

    def interrupt(self) -> bool:
        """Interrupt the currently running command (equivalent to Ctrl+C)."""
        return self.terminal.interrupt()

    def is_running(self) -> bool:
        """Check if a command is currently running."""
        if not self._initialized:
            return False
        return self.prev_status in {
            TerminalCommandStatus.CONTINUE,
            TerminalCommandStatus.NO_CHANGE_TIMEOUT,
            TerminalCommandStatus.HARD_TIMEOUT,
        }

    def execute(
        self, 
        action: TerminalAction, 
        conversation: "ToolExecutionContextProtocol | None" = None
    ) -> TerminalObservation:
        if not self._initialized:
            raise RuntimeError("Unified session is not initialized")
            
        context = ExecutionContext(
            session=self, 
            action=action, 
            conversation=conversation
        )
        return self.engine.execute(context)