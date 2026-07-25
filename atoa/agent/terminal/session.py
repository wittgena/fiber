# atoa.agent.terminal.session
## @lineage: agent.topos.terminal.session
## @lineage: atoa.topos.terminal.session
## @lineage: gov.sandbox.engine.terminal.session
## @lineage: agent.engine.terminal.session
## @lineage: sandbox.executor.terminal.session
## @lineage: gov.sandbox.executor.terminal.session
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from atoa.gov.disc.conv import ToolExecutionContextProtocol

from atoa.gov.disc.action.tool.terminal import TerminalAction, TerminalObservation
from atoa.gov.tool.terminal.interface import TerminalInterface, TerminalSessionBase
from arch.gov.tool.command.terminal import TerminalCommandStatus
from atoa.agent.terminal.utils import TerminalQueryFilter
from atoa.agent.context import ExecutionEngine, ExecutionContext
from atoa.agent.terminal.polling import PollingExecutionEngine

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)


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