# atoa.gov.context.context
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from atoa.registry import LLMRegistry
from atoa.call.types import (
    ConversationCallbackType,
    ConversationID,
    ConversationTokenCallbackType,
)
from atoa.agent.disc.base.conv import ProtoConv
from atoa.call.driver.tensor import Driver
from atoa.gov.context.message.visualizer import ConversationVisualizer
from atoa.gov.context.state import ConversationState
from atoa.gov.security.analyzer import SecurityAnalyzerBase
from atoa.gov.security.confirm import ConfirmationPolicyBase

from bound.resolver.secret import SecretValue

from eco.call.action.message import Message
from eco.call.disc.action import Action, Observation

from gov.sandbox.agent.workspace import SandboxWorkspace
from xor.secure.secret.validator import Cipher

from arch.contract.event.next import next_id
from atoa.gov.context.adapter import AgentCommunicator, ExecutionController, SecurityManager, EngineContextAdapter


class ConvContext(ProtoConv):
    """
    @desc: Gov 환경에서 대화의 상태(State)와 제어(Adapters), 그리고 로컬 툴(Tools)을 
           응집하는 최상위 런타임 래퍼입니다. (원격 Agent 워커와는 Tunnel로 소통합니다.)
    """
    def __init__(
        self,
        workspace: str | Path | SandboxWorkspace,
        agent_id: str = "default-agent",     # [핵심] Ator 객체 대신 ID 주입
        agent_config: dict[str, Any] | None = None,
        persistence_dir: str | Path | None = None,
        conversation_id: ConversationID | None = None,
        callbacks: list[ConversationCallbackType] | None = None,
        token_callbacks: list[ConversationTokenCallbackType] | None = None,
        max_iteration_per_run: int = 500,
        stuck_detection: bool = True,
        cipher: Cipher | None = None,
        tags: dict[str, str] | None = None,
        topos_bridge: ConversationCallbackType | None = None,
        **kwargs: object,
    ):
        super().__init__()
        self._cleanup_initiated = False

        if isinstance(workspace, (str, Path)):
            workspace = SandboxWorkspace(working_dir=workspace)
        self._workspace = workspace
        ws_path = Path(self._workspace.working_dir)
        if not ws_path.exists():
            ws_path.mkdir(parents=True, exist_ok=True)

        desired_id = conversation_id or next_id()
        desired_id_str = str(desired_id) # [버그 수정] .hex 제거
        
        self._state = ConversationState.create(
            id=desired_id,
            agent_id=agent_id,
            agent_config=agent_config,
            workspace=self._workspace,
            persistence_dir=str(Path(persistence_dir) / desired_id_str) if persistence_dir else None,
            max_iterations=max_iteration_per_run,
            stuck_detection=stuck_detection,
            cipher=cipher,
            tags=tags,
        )

        def _default_callback(e):
            self._state.events.append(e)
            if getattr(e, "source", None) == "user":
                self._state.last_user_message_id = getattr(e, "id", None)
            if topos_bridge:
                try: topos_bridge(e)
                except Exception: pass

        composed = (callbacks or []) + [_default_callback]
        self._on_event = self._compose_callbacks(composed)
        self._on_token = self._compose_callbacks(token_callbacks) if token_callbacks else None
        
        self.llm_registry = LLMRegistry()
        self.tools = {} # [추가] ActionResolver가 Gov용 실행기를 바인딩할 공간

        self._communicator = AgentCommunicator(self)
        self._controller = ExecutionController(self)
        self._security = SecurityManager(self)
        self._engine = EngineContextAdapter(self)

        # [수정] atexit.register(self.close) 제거 (비동기 함수이므로 불가)
        self._start_observability_span(desired_id_str)

    @staticmethod
    def _compose_callbacks(callbacks):
        def composed(event):
            for cb in callbacks:
                if cb: cb(event)
        return composed

    @property
    def id(self) -> ConversationID: return self._state.id
    @property
    def state(self) -> ConversationState: return self._state
    @property
    def workspace(self) -> SandboxWorkspace: return self._workspace
    
    # [수정] @property def ator(self) 완전 삭제 (Decoupling 완성)
    
    @property
    def conversation_stats(self): return self._state.stats

    def _warn_deprecation(self, method_name: str, adapter_name: str):
        warnings.warn(
            f"[Deprecation] Conv.{method_name}() 직접 호출은 금지되었습니다. "
            f"대신 {adapter_name} 어댑터를 사용하세요.",
            DeprecationWarning,
            stacklevel=3
        )

    # ---------------------------------------------------
    # [A] 비동기 전환된 제어 메서드들 (Async Wrappers)
    # ---------------------------------------------------
    async def run(self) -> None:
        self._warn_deprecation("run", "ExecutionController")
        await self._controller.run()

    async def close(self) -> None:
        if self._cleanup_initiated: return
        self._warn_deprecation("close", "ExecutionController")
        await self._controller.close()

    async def switch_profile(self, profile_name: str) -> None:
        self._warn_deprecation("switch_profile", "EngineContextAdapter")
        await self._engine.switch_profile(profile_name)

    # ---------------------------------------------------
    # [B] 동기 유지 제어 메서드들 (Sync Wrappers)
    # ---------------------------------------------------
    def send_message(self, message: str | Message, sender: str | None = None) -> None:
        self._warn_deprecation("send_message", "AgentCommunicator")
        self._communicator.send_message(message, sender)

    def ask(self, question: str) -> str:
        self._warn_deprecation("ask", "AgentCommunicator")
        return self._communicator.ask(question)

    def pause(self) -> None:
        self._warn_deprecation("pause", "ExecutionController")
        self._controller.pause()

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        self._warn_deprecation("rerun_actions", "ExecutionController")
        return self._controller.rerun_actions(rerun_log_path)

    def execute_tool(self, tool_name: str, action: Action) -> Observation:
        self._warn_deprecation("execute_tool", "ExecutionController")
        return self._controller.execute_tool(tool_name, action)

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        self._warn_deprecation("reject_pending_actions", "SecurityManager")
        self._security.reject_pending_actions(reason)

    def update_secrets(self, secrets: Mapping[str, SecretValue]) -> None:
        self._warn_deprecation("update_secrets", "SecurityManager")
        self._security.update_secrets(secrets)

    def set_security_analyzer(self, analyzer: SecurityAnalyzerBase | None) -> None:
        self._warn_deprecation("set_security_analyzer", "SecurityManager")
        self._security.set_security_analyzer(analyzer)

    def set_confirmation_policy(self, policy: ConfirmationPolicyBase) -> None:
        self._warn_deprecation("set_confirmation_policy", "SecurityManager")
        self._security.set_confirmation_policy(policy)

    @property
    def is_confirmation_mode_active(self) -> bool:
        return self._security.is_confirmation_mode_active

    @property
    def confirmation_policy_active(self) -> bool:
        return self._security.confirmation_policy_active

    def condense(self) -> None:
        self._warn_deprecation("condense", "EngineContextAdapter")
        self._engine.condense()

    def generate_title(self, llm: Driver | None = None, max_length: int = 50) -> str:
        self._warn_deprecation("generate_title", "EngineContextAdapter")
        return self._engine.generate_title(llm, max_length)