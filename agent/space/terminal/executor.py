# agent.space.terminal.executor
## @lineage: bound.space.terminal.executor
## @lineage: agent.runtime.terminal.executor
## @lineage: ator.runtime.terminal.executor
## @lineage: agent.bridge.terminal.executor
## @lineage: agent.space.tool.terminal.executor
import json
import threading
import time
from typing import TYPE_CHECKING, Literal

from fiber.agent.space.action.executor import ActionExecutor
from fiber.agent.space.action.message import TextContent
from fiber.agent.loop.runtime.protocol.tool.terminal import TerminalAction, TerminalObservation
from fiber.agent.loop.runtime.protocol.context import ToolExecutionContextProtocol
from fiber.agent.loop.runtime.protocol.terminal.session import TerminalSession
from fiber.agent.loop.runtime.protocol.terminal.context import ExecutionContext, ExecutionEngine
from fiber.agent.loop.runtime.protocol.terminal.polling import PollingExecutionEngine

from fiber.agent.space.terminal.session.builder import ChainBuilder
from fiber.agent.space.terminal.session.builder import _is_tmux_available, create_terminal_session
from fiber.agent.space.terminal.tmux.pool import DEFAULT_MAX_PANES, PooledTmuxTerminal, TmuxPanePool

from xphi.arch.xor.bridge.command.terminal import TerminalCommandStatus
from xphi.arch.xor.bridge.terminal import CMD_OUTPUT_PS1_END

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)


class TerminalExecutor(ActionExecutor[TerminalAction, TerminalObservation]):
    shell_path: str | None
    pipeline: ExecutionEngine

    def __init__(
        self,
        working_dir: str,
        username: str | None = None,
        no_change_timeout_seconds: int | None = None,
        terminal_type: Literal["tmux", "subprocess"] | None = None,
        shell_path: str | None = None,
        full_output_save_dir: str | None = None,
        max_panes: int = DEFAULT_MAX_PANES,
    ):
        self.shell_path = shell_path
        self._working_dir = working_dir
        self._username = username
        self._no_change_timeout_seconds = no_change_timeout_seconds
        self._terminal_type = terminal_type
        self.full_output_save_dir = full_output_save_dir

        self._pool: TmuxPanePool | None = None
        self._session: TerminalSession | None = None
        self._sessions: dict[int, TerminalSession] = {}
        self._sessions_lock = threading.Lock()

        # [추가됨] 파이프라인 초기화 조립 (DI)
        # 하드코딩되었던 _export_envs와 _mask_observation을 대체할 미들웨어들을 장착합니다.
        self.pipeline = ChainBuilder(
            base_engine=PollingExecutionEngine(),
            middlewares=[
                # 추후 구현될 미들웨어들:
                # EnvInjectionMiddleware(),
                # IntentParserMiddleware(),
                # SecurityGuardrailMiddleware(),
                # OutputMaskingMiddleware(self.full_output_save_dir),
            ]
        )

        use_pool = terminal_type in (None, "tmux") and _is_tmux_available()

        if use_pool:
            self._pool = TmuxPanePool(working_dir, username, max_panes=max_panes)
            self._pool.initialize()
            log.info(
                f"TerminalExecutor initialized (pool mode) "
                f"working_dir: {working_dir}, username: {username}, "
                f"max_panes: {max_panes}"
            )
        else:
            self._session = create_terminal_session(
                work_dir=working_dir,
                username=username,
                no_change_timeout_seconds=no_change_timeout_seconds,
                terminal_type=terminal_type,
                shell_path=shell_path,
            )
            self._session.initialize()
            log.info(
                f"TerminalExecutor initialized with "
                f"working_dir: {working_dir}, "
                f"username: {username}, "
                f"terminal_type: {terminal_type or self._session.__class__.__name__}"
            )

    @property
    def is_pooled(self) -> bool:
        return self._pool is not None

    @property
    def working_dir(self) -> str:
        return self._working_dir

    @property
    def session(self) -> TerminalSession:
        if self._pool is not None:
            raise AttributeError(
                "TerminalExecutor.session is not available in pool mode. "
                "Use the is_pooled property to check mode, or set "
                "terminal_type='subprocess' to disable pool mode."
            )
        assert self._session is not None
        return self._session

    # ------------------------------------------------------------------
    # Pool helpers
    # ------------------------------------------------------------------

    def _wrap_session(self, terminal: PooledTmuxTerminal) -> TerminalSession:
        pane_id = id(terminal)
        with self._sessions_lock:
            if pane_id not in self._sessions:
                session = TerminalSession.attach_to_existing(
                    terminal, self._no_change_timeout_seconds
                )
                self._sessions[pane_id] = session
            return self._sessions[pane_id]

    def _discard_session(self, terminal: PooledTmuxTerminal) -> None:
        with self._sessions_lock:
            session = self._sessions.pop(id(terminal), None)
            if session is not None:
                session._closed = True
                terminal._closed = True

    @staticmethod
    def _prepare_pooled_session(session: TerminalSession) -> None:
        if session.prev_status in (
            TerminalCommandStatus.NO_CHANGE_TIMEOUT,
            TerminalCommandStatus.HARD_TIMEOUT,
            TerminalCommandStatus.CONTINUE,
        ):
            session.terminal.interrupt()
            _max_wait = 2.0
            _poll = 0.05
            _waited = 0.0
            while _waited < _max_wait:
                time.sleep(_poll)
                _waited += _poll
                screen = session.terminal.read_screen()
                if screen.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip()):
                    break
            else:
                log.debug(
                    "Prompt did not reappear within %.1fs after interrupt; "
                    "proceeding anyway",
                    _max_wait,
                )
            session.terminal.clear_screen()
        session.prev_status = None
        session.prev_output = ""

    def _export_envs(
        self,
        action: TerminalAction,
        conversation: "ToolExecutionContextProtocol | None" = None,
        session: TerminalSession | None = None,
    ) -> None:
        if not action.command.strip() or action.is_input:
            return

        env_vars = {}
        if conversation is not None:
            try:
                secret_registry = conversation.state.secret_registry
                env_vars = secret_registry.get_secrets_as_env_vars(action.command)
            except Exception:
                pass

        if not env_vars:
            return

        export_statements = [f"export {k}={json.dumps(v)}" for k, v in env_vars.items()]
        exports_cmd = " && ".join(export_statements)

        log.debug(f"Exporting {len(env_vars)} environment variables before command")

        target = session or self.session
        # 내부적으로 엔진을 직접 호출하지 않고 Context를 만들어 파이프라인에 태웁니다.
        ctx = ExecutionContext(
            session=target,
            action=TerminalAction(command=exports_cmd, is_input=False, timeout=action.timeout),
            conversation=conversation
        )
        _ = self.pipeline.execute(ctx)

    def _mask_observation(
        self,
        observation: TerminalObservation,
        conversation: "ToolExecutionContextProtocol | None" = None,
    ) -> TerminalObservation:
        content_text = observation.text

        if content_text and conversation is not None:
            try:
                secret_registry = conversation.state.secret_registry
                masked_content = secret_registry.mask_secrets_in_output(content_text)
                if masked_content:
                    data = observation.model_dump(exclude={"content", "full_output_save_dir"})
                    return TerminalObservation.from_text(
                        text=masked_content,
                        full_output_save_dir=self.full_output_save_dir,
                        **data,
                    )
            except Exception:
                pass

        return observation

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> TerminalObservation:
        return self._reset_single_session()

    def _reset_single_session(self) -> TerminalObservation:
        assert self._session is not None
        original_work_dir = self._session.work_dir
        original_username = self._session.username
        original_no_change_timeout = self._session.no_change_timeout_seconds

        self._session.close()
        self._session = create_terminal_session(
            work_dir=original_work_dir,
            username=original_username,
            no_change_timeout_seconds=original_no_change_timeout,
            terminal_type=None,
            shell_path=self.shell_path,
        )
        self._session.initialize()

        log.info(f"Terminal session reset successfully with working_dir: {self._working_dir}")

        return TerminalObservation.from_text(
            text="Terminal session has been reset. All previous environment variables and session state have been cleared.",
            command="[RESET]",
            exit_code=0,
        )

    _RESET_TEXT = "Terminal session has been reset. All previous environment variables and session state have been cleared."

    def _execute_single_session(
        self,
        action: TerminalAction,
        conversation: "ToolExecutionContextProtocol | None" = None,
    ) -> TerminalObservation:
        if action.reset or self.session._closed:
            reset_result = self._reset_single_session()

            if action.command.strip():
                session = self.session
                command_action = TerminalAction(
                    command=action.command,
                    timeout=action.timeout,
                    is_input=False,
                    intent=action.intent  # Intent 전파
                )
                self._export_envs(command_action, conversation, session=session)
                
                # [변경됨] Context로 포장하여 파이프라인 실행
                context = ExecutionContext(session=session, action=command_action, conversation=conversation)
                command_result = self.pipeline.execute(context)

                observation = command_result.model_copy(
                    update={
                        "content": [TextContent(text=f"{reset_result.text}\n\n{command_result.text}")],
                        "command": f"[RESET] {action.command}",
                    }
                )
            else:
                observation = reset_result
        else:
            self._export_envs(action, conversation, session=self.session)
            
            # [변경됨] Context로 포장하여 파이프라인 실행
            context = ExecutionContext(session=self.session, action=action, conversation=conversation)
            observation = self.pipeline.execute(context)

        return self._mask_observation(observation, conversation)

    def _execute_pooled(
        self,
        action: TerminalAction,
        conversation: "ToolExecutionContextProtocol | None" = None,
    ) -> TerminalObservation:
        with self._pool.pane() as handle:
            reset_text: str | None = None
            if action.reset or handle.terminal._closed:
                self._discard_session(handle.terminal)
                handle.terminal = self._pool.replace(handle.terminal)
                reset_text = self._RESET_TEXT
                log.info(f"Terminal pane replaced (reset) working_dir: {self._working_dir}")
                
                if not action.command.strip():
                    return TerminalObservation.from_text(text=reset_text, command="[RESET]", exit_code=0)

            session = self._wrap_session(handle.terminal)
            self._prepare_pooled_session(session)

            cmd_action = (
                action if reset_text is None
                else TerminalAction(
                    command=action.command,
                    timeout=action.timeout,
                    is_input=False,
                    intent=action.intent # Intent 전파
                )
            )
            
            self._export_envs(cmd_action, conversation, session=session)
            
            # [변경됨] Context로 포장하여 파이프라인 실행
            context = ExecutionContext(session=session, action=cmd_action, conversation=conversation)
            observation = self.pipeline.execute(context)

            if reset_text is not None:
                observation = observation.model_copy(
                    update={
                        "content": [TextContent(text=f"{reset_text}\n\n{observation.text}")],
                        "command": f"[RESET] {action.command}",
                    }
                )

            return self._mask_observation(observation, conversation)

    def __call__(
        self,
        action: TerminalAction,
        conversation: "ToolExecutionContextProtocol | None" = None,
    ) -> TerminalObservation:
        if action.reset and action.is_input:
            raise ValueError("Cannot use reset=True with is_input=True")

        if self._pool is not None:
            return self._execute_pooled(action, conversation)
        else:
            return self._execute_single_session(action, conversation)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            with self._sessions_lock:
                self._sessions.clear()
        elif self._session is not None:
            self._session.close()