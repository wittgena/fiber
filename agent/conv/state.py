# agent.conv.state
## @lineage: atoa.conv.state
## @lineage: atoa.gov.context.state
import os
import json
import inspect
from collections.abc import Sequence
from typing import Any, Self, TYPE_CHECKING
from pydantic import Field

from agent.atoa.disc.event.llm.action import ActionEvent
from agent.atoa.disc.event.llm.observation import (
    AgentErrorEvent,
    ObservationEvent,
    UserRejectObservation,
)
from agent.eco.event.base import Event
from agent.eco.event.types import EventID
from agent.atoa.disc.base.workspace import BaseWorkspace
from agent.atoa.disc.status import ConverStatus
from agent.atoa.types import ConversationCallbackType, ConversationID, ConversationTags

from agent.gov.security.confirm import ConfirmationPolicyBase, NeverConfirm
from eco.watcher.stats import ConversationStats

if TYPE_CHECKING:
    from agent.gov.security.analyzer import SecurityAnalyzerBase
    SecurityType = SecurityAnalyzerBase | Any
else:
    SecurityType = Any

from bound.resolver.secret import SecretRegistry
from agent.conv.io import IOManager
from agent.gov.store.log import LogStore, VirtualEventLogProxy
from agent.conv.context.command import (
    StateCommand, 
    TransitionStatus, 
    BlockAction, 
    BlockMessage, 
    ActivateSkill, 
    UpdateAgentState, 
    UpdateTags
)

from arch.topos.bound.surge.disc import SurgeBaseModel
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class ConversationState(SurgeBaseModel):
    id: ConversationID = Field(description="Unique conversation ID")
    
    # [핵심 변경] 무거운 Ator 객체 대신 Agent의 식별자와 설정 메타데이터만 보유
    agent_id: str = Field(
        default="default-agent",
        description="Identifier for the remote Agent node/profile handling this conversation."
    )
    agent_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Serialized configuration or profile metadata of the agent (e.g., allowed tools, LLM model info)."
    )
    
    workspace: BaseWorkspace = Field(
        ...,
        description="Workspace used by the Gov Engine to execute commands and read/write files.",
    )
    persistence_dir: str | None = Field(
        default="workspace/conv",
        description="Directory for persisting state. If None, runs in-memory.",
    )

    max_iterations: int = Field(
        default=5,
        gt=0,
        description="Maximum number of iterations the agent can perform.",
    )
    stuck_detection: bool = Field(default=True)

    execution_status: ConverStatus = Field(default=ConverStatus.IDLE)
    confirmation_policy: ConfirmationPolicyBase = NeverConfirm()
    security_analyzer: SecurityType | Any = Field(default=None)
    activated_knowledge_skills: list[str] = Field(default_factory=list)

    blocked_actions: dict[str, str] = Field(default_factory=dict)
    blocked_messages: dict[str, str] = Field(default_factory=dict)

    last_user_message_id: EventID | None = Field(default=None)
    stats: ConversationStats = Field(default_factory=ConversationStats)
    secret_registry: SecretRegistry = Field(default_factory=SecretRegistry)
    tags: ConversationTags = Field(default_factory=dict)
    agent_state: dict[str, Any] = Field(default_factory=dict)
    
    autosave_enabled: bool = Field(default=False, exclude=True)
    on_state_change: ConversationCallbackType | None = Field(default=None, exclude=True)
    virtual_events: list[Event] = Field(default_factory=list, exclude=True)

    @property
    def env_observation_dir(self) -> str | None:
        if not self.persistence_dir:
            return None
            
        target_dir = os.path.join(self.persistence_dir, str(self.id), "env_observations")
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    @property
    def events(self) -> LogStore | None:
        """이벤트 스토리지는 더 이상 직접 들고있지 않고 매니저에게 요청합니다."""
        return IOManager.get_instance().get_log_store(self.id)

    def inject_virtual_event(self, event: Event) -> None:
        if not event.id.startswith("virtual-"):
            log.warning(f"Virtual event [{event.id}] is injected without 'virtual-' prefix.")
        self.virtual_events.append(event)
        log.debug(f"Injected virtual event [{event.id}]. Total virtual: {len(self.virtual_events)}")

    def get_effective_events(self) -> Sequence[Event]:
        log_store = self.events
        if not self.virtual_events:
            return log_store
        return VirtualEventLogProxy(log_store, self.virtual_events)

    def set_on_state_change(self, callback: ConversationCallbackType | None) -> None:
        self.on_state_change = callback

    def save_base_state(self) -> None:
        """IOManager에게 현재 상태의 디스크 저장을 요청합니다. (비동기 위임)"""
        io_manager = IOManager.get_instance()
        cipher = io_manager.get_cipher(self.id)
        
        context = {"cipher": cipher} if cipher else None

        if not cipher and self.secret_registry.secret_sources:
            log.warning(
                f"Saving conversation state without cipher - "
                f"{len(self.secret_registry.secret_sources)} secret(s) will be redacted."
            )

        payload = self.model_dump_json(exclude_none=True, context=context)
        io_manager.save_base_state(self.id, payload)

    @classmethod
    def create(
        cls: type["ConversationState"],
        id: ConversationID,
        workspace: BaseWorkspace,
        agent_id: str = "default-agent",  # [개선] Ator 런타임 객체 대신 ID 주입
        agent_config: dict[str, Any] | None = None,
        persistence_dir: str | None = None,
        max_iterations: int = 500,
        stuck_detection: bool = True,
        cipher: Any = None,
        tags: dict[str, str] | None = None,
    ) -> "ConversationState":
        io_manager = IOManager.get_instance()
        io_manager.register_conversation(id, persistence_dir, max_iterations, cipher)
        
        base_text = io_manager.read_base_state(id)
        if base_text:
            context = {"cipher": cipher} if cipher else None
            state = cls.model_validate(json.loads(base_text), context=context)
            if state.id != id:
                raise ValueError(
                    f"Conversation ID mismatch: provided {id}, "
                    f"but persisted state has {state.id}"
                )

            # [개선] 메모리 객체 기반의 agent.verify(...) 제거
            state.autosave_enabled = True
            state.agent_id = agent_id
            if agent_config:
                state.agent_config = agent_config
            state.workspace = workspace
            state.max_iterations = max_iterations

            log.info(
                f"Resumed conversation {state.id} from persistent storage.\n"
                f"State: {state.model_dump(exclude={'agent_config'})}\n"
                f"Agent ID: {state.agent_id}"
            )
            return state

        state = cls(
            id=id,
            agent_id=agent_id,
            agent_config=agent_config or {},
            workspace=workspace,
            persistence_dir=persistence_dir,
            max_iterations=max_iterations,
            stuck_detection=stuck_detection,
            tags=tags or {},
        )
        state.stats = ConversationStats()

        state.save_base_state()  # 최초 상태 스냅샷 저장
        state.autosave_enabled = True
        
        log.info(f"Created new conversation {state.id} for Agent {state.agent_id}")
        return state

    def apply(self, command: StateCommand) -> None:
        """상태 변경을 중앙 통제하고 추적하기 위한 단일 창구(Single Source of Truth)"""
        if isinstance(command, TransitionStatus):
            self._transition_status(command)
        elif isinstance(command, UpdateSecurityPolicy):
            if command.confirmation_policy is not None:
                self.confirmation_policy = command.confirmation_policy
            if command.security_analyzer is not None:
                self.security_analyzer = command.security_analyzer
        elif isinstance(command, BlockAction):
            self.blocked_actions = {**self.blocked_actions, command.action_id: command.reason}
            log.info(f"🛡️ [ACTION BLOCKED] {command.action_id} (Reason: {command.reason})")
        elif isinstance(command, BlockMessage):
            self.blocked_messages = {**self.blocked_messages, command.message_id: command.reason}
            log.info(f"🛡️ [MESSAGE BLOCKED] {command.message_id} (Reason: {command.reason})")
        elif isinstance(command, ActivateSkill):
            if command.skill_name not in self.activated_knowledge_skills:
                self.activated_knowledge_skills = [*self.activated_knowledge_skills, command.skill_name]
                log.info(f"🧠 [SKILL ACTIVATED] {command.skill_name}")
        elif isinstance(command, UpdateAgentState):
            self.agent_state = {**self.agent_state, command.key: command.value}
        elif isinstance(command, UpdateTags):
            self.tags = {**self.tags, **command.tags_to_update}
        else:
            log.warning(f"Unknown state command received: {type(command)}")
            return
            
        self.save_base_state()

    def _transition_status(self, command: TransitionStatus) -> None:
        old_status = self.execution_status
        new_status = command.new_status
        
        if old_status == new_status:
            return

        if old_status.is_terminal():
            log.warning(f"Cannot transition from terminal status {old_status} to {new_status}")
            return

        self.execution_status = new_status
        log.info(
            f"🎯 [STATE TRANSITION] {old_status.value} -> {new_status.value} "
            f"(Reason: {command.reason})"
        )

    def __setattr__(self, name, value):
        _sentinel = object()
        old = getattr(self, name, _sentinel)
        
        is_field = name in self.__class__.model_fields
        autosave_enabled = getattr(self, "autosave_enabled", False)

        # 직접 변수 할당을 검출하고 로그를 남기는 로직
        if autosave_enabled and is_field and old is not _sentinel and old != value:
            # 주요 상태 변수를 외부에서 '='로 직접 변경하려 할 때만 추적
            if name in ("execution_status", "blocked_actions", "blocked_messages", "activated_knowledge_skills", "agent_state", "tags"):
                stack = inspect.stack()
                if len(stack) > 1:
                    caller = stack[1]
                    caller_func = caller.function
                    caller_file = caller.filename
                    
                    # 내부 Command 창구나 pydantic의 내부 할당은 무시
                    if caller_func not in ("apply", "_transition_status") and "pydantic" not in caller_file:
                        log.warning(
                            f"🚨 [DEPRECATION] Direct assignment to '{name}' detected! "
                            f"Please refactor to use Command pattern (state.apply). "
                            f"\n  -> File: {caller_file}, Line: {caller.lineno}, Function: {caller_func}()"
                        )

        # 실제 할당 실행
        super().__setattr__(name, value)

        if not (autosave_enabled and is_field):
            return

        # 자동 저장 로직 (점진적 리팩토링이 끝날 때까지 하위 호환성을 위해 유지)
        if old is _sentinel or old != value:
            try:
                self.save_base_state()
            except Exception as e:
                log.exception("Auto-persist base_state failed", exc_info=True)
                raise e

            callback = getattr(self, "on_state_change", None)
            if callback is not None and old is not _sentinel:
                try:
                    from agent.atoa.disc.event.conv.state import ConversationStateUpdateEvent
                    callback(ConversationStateUpdateEvent(key=name, value=value))
                except Exception:
                    log.exception(f"State change callback failed for field {name}", exc_info=True)

    def block_action(self, action_id: str, reason: str) -> None:
        self.blocked_actions = {**self.blocked_actions, action_id: reason}

    def pop_blocked_action(self, action_id: str) -> str | None:
        if action_id not in self.blocked_actions:
            return None
        updated = dict(self.blocked_actions)
        reason = updated.pop(action_id)
        self.blocked_actions = updated
        return reason

    def block_message(self, message_id: str, reason: str) -> None:
        self.blocked_messages = {**self.blocked_messages, message_id: reason}

    def pop_blocked_message(self, message_id: str) -> str | None:
        if message_id not in self.blocked_messages:
            return None
        updated = dict(self.blocked_messages)
        reason = updated.pop(message_id)
        self.blocked_messages = updated
        return reason

    @staticmethod
    def get_unmatched_actions(events: Sequence[Event]) -> list[ActionEvent]:
        observed_action_ids: set[EventID] = set()
        observed_tool_call_ids: set[str] = set()
        unmatched_actions = []

        for event in reversed(events):
            if isinstance(event, (ObservationEvent, UserRejectObservation)):
                observed_action_ids.add(event.action_id)
            elif isinstance(event, AgentErrorEvent):
                observed_tool_call_ids.add(event.tool_call_id)
            elif isinstance(event, ActionEvent):
                if (
                    event.action is not None
                    and event.id not in observed_action_ids
                    and event.tool_call_id not in observed_tool_call_ids
                ):
                    unmatched_actions.insert(0, event)

        return unmatched_actions