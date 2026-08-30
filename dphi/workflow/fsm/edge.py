# fiber.dphi.workflow.fsm.edge
from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Dict

class EdgePhaseState(Enum):
    INIT = auto()
    COMPUTING = auto()           # 에이전트 과금 및 실행 단계 (Quote -> Invoice -> Balance -> Execute)
    COMPLIANCE_CHECKING = auto() # 규제 및 감사 단계 (Audit -> OTLP)
    SETTLING = auto()            # 자산 정산 단계 (Exchange -> Ledger -> Clearing)
    COMPLETED = auto()
    FAILED = auto()

@dataclass
class StartIntentEvent:
    agent_id: str
    action: str
    max_fuel: int
    source_code: str
    signature: str  # [정합성 회복] 클라이언트(E2E)가 생성한 실제 서명

@dataclass
class ComputePhaseCompletedEvent:
    audit_receipt: Dict[str, Any]
    cost_usd: float

@dataclass
class CompliancePhaseCompletedEvent:
    otlp_hash: str

@dataclass
class SettlementPhaseCompletedEvent:
    tx_hash: str

@dataclass
class PhaseFailedEvent:
    reason: str

@dataclass
class RunComputePhaseCmd:
    agent_id: str
    action: str
    max_fuel: int
    source_code: str
    signature: str  # [정합성 회복] Workflow에 그대로 전달될 서명

@dataclass
class RunCompliancePhaseCmd:
    audit_receipt: Dict[str, Any]

@dataclass
class RunSettlementPhaseCmd:
    agent_id: str
    cost_usd: float

@dataclass
class FinishWorkflowCmd:
    tx_hash: str

@dataclass
class HaltWorkflowCmd:
    reason: str

class EdgePhaseFSM:
    def __init__(self):
        self.state = EdgePhaseState.INIT
        self.context: Dict[str, Any] = {}

    def apply(self, event: Any) -> Any:
        if isinstance(event, PhaseFailedEvent):
            self.state = EdgePhaseState.FAILED
            return HaltWorkflowCmd(reason=event.reason)

        ## Phase 1: Computing (실행 및 과금)
        if self.state == EdgePhaseState.INIT and isinstance(event, StartIntentEvent):
            self.state = EdgePhaseState.COMPUTING
            self.context["agent_id"] = event.agent_id
            return RunComputePhaseCmd(
                agent_id=event.agent_id, 
                action=event.action,
                max_fuel=event.max_fuel, 
                source_code=event.source_code,
                signature=event.signature  # 서명 무결성 전달
            )

        ## Phase 2: Compliance (감사 및 로깅)
        elif self.state == EdgePhaseState.COMPUTING and isinstance(event, ComputePhaseCompletedEvent):
            self.state = EdgePhaseState.COMPLIANCE_CHECKING
            self.context["audit_receipt"] = event.audit_receipt
            self.context["cost_usd"] = event.cost_usd
            return RunCompliancePhaseCmd(audit_receipt=event.audit_receipt)

        ## Phase 3: Settlement (원장 기록 및 정산)
        elif self.state == EdgePhaseState.COMPLIANCE_CHECKING and isinstance(event, CompliancePhaseCompletedEvent):
            self.state = EdgePhaseState.SETTLING
            return RunSettlementPhaseCmd(
                agent_id=self.context["agent_id"],
                cost_usd=self.context.get("cost_usd", 0.0)
            )
        elif self.state == EdgePhaseState.SETTLING and isinstance(event, SettlementPhaseCompletedEvent):
            self.state = EdgePhaseState.COMPLETED
            return FinishWorkflowCmd(tx_hash=event.tx_hash)

        self.state = EdgePhaseState.FAILED
        return HaltWorkflowCmd(reason=f"Invalid Event {event.__class__.__name__} at {self.state.name}")