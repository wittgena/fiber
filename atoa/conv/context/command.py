# atoa.conv.context.command
## @lineage: atoa.gov.context.command
## @lineage: atoa.context.gov.command
## @lineage: gov.conv.command
"""
@module: gov.conv.command
@desc: ConversationState의 상태 변경을 캡슐화하기 위한 Command 객체 모음
"""
from dataclasses import dataclass
from typing import Any

from atoa.agent.disc.status import ConverStatus
from atoa.gov.security.confirm import ConfirmationPolicyBase

@dataclass(kw_only=True)
class StateCommand:
    """
    모든 상태 변경 명령의 최상위 Base 클래스.
    상태를 변경할 때는 가급적 '왜' 변경하는지에 대한 추적(Traceability)을 위해 reason을 남깁니다.
    """
    reason: str = "No reason provided"

@dataclass(kw_only=True)
class UpdateSecurityPolicy(StateCommand):
    """보안 분석기 및 컨펌 정책 업데이트 명령"""
    confirmation_policy: ConfirmationPolicyBase | None = None
    security_analyzer: Any = None

@dataclass(kw_only=True)
class TransitionStatus(StateCommand):
    """
    대화의 실행 상태(execution_status) 전이를 요청하는 Command.
    (예: IDLE -> RUNNING, RUNNING -> STUCK 등)
    """
    new_status: ConverStatus

@dataclass(kw_only=True)
class BlockAction(StateCommand):
    """
    PreToolUse 훅 등에서 특정 액션(Action)의 실행을 차단하도록 요청하는 Command.
    """
    action_id: str

@dataclass(kw_only=True)
class BlockMessage(StateCommand):
    """
    UserPromptSubmit 훅 등에서 특정 사용자 메시지 처리를 차단하도록 요청하는 Command.
    """
    message_id: str

@dataclass(kw_only=True)
class ActivateSkill(StateCommand):
    """
    에이전트의 특정 지식 스킬(Knowledge Skill) 활성화를 요청하는 Command.
    (activated_knowledge_skills 리스트에 추가)
    """
    skill_name: str

@dataclass(kw_only=True)
class UpdateAgentState(StateCommand):
    """
    에이전트별 런타임 상태 딕셔너리(agent_state)의 특정 키/값 업데이트를 요청하는 Command.
    """
    key: str
    value: Any

@dataclass(kw_only=True)
class UpdateTags(StateCommand):
    """
    대화의 사용자 정의 메타데이터(tags) 업데이트를 요청하는 Command.
    """
    tags_to_update: dict[str, str]