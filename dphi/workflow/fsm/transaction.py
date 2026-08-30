# fiber.dphi.workflow.fsm.transaction
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

class TransactionState(Enum):
    INIT = auto()               # 초기 상태 (대기)
    SHADOW_EXECUTION = auto()   # 가상 머신(DVM) 섀도우 연산 진행 중
    LEDGER_SEALING = auto()     # 연산 결과 검증 후 L2 롤업 원장 기록 중
    COMPLETED = auto()          # 정산 완료 (최종)
    FAILED = auto()             # 비즈니스 규칙 위반 또는 가상머신 예외로 정지

@dataclass
class StartTransactionIntent:
    caller: str
    charge_amount: int
    target_contract: str
    calldata: str
    active_snapshot: Dict[str, Any]

@dataclass
class DvmResultEvent:
    success: bool
    state_diff: Dict[str, Any] = field(default_factory=dict)
    gas_used: int = 0
    revert_reason: Optional[str] = None

@dataclass
class ExecuteDvmCmd:
    target_contract: str
    active_calldata: str
    active_snapshot: Dict[str, Any]

@dataclass
class LedgerSealCmd:
    caller: str
    target_contract: str
    state_diff: Dict[str, Any]
    gas_used: int

@dataclass
class HaltFsmCmd:
    reason: str

class TransactionFSM:
    def __init__(self, max_gas_limit: int = 200_000):
        self.state: TransactionState = TransactionState.INIT
        self.max_gas_limit = max_gas_limit
        self.caller: str = ""
        self.target_contract: str = ""
        self.charge_amount: int = 0

    def apply_event(self, event: Any) -> Optional[Any]:
        ## Transition 1: INIT -> SHADOW_EXECUTION
        if self.state == TransactionState.INIT and isinstance(event, StartTransactionIntent):
            # 비즈니스 규칙 1: 청구 금액 유효성 검증
            if event.charge_amount <= 0:
                self.state = TransactionState.FAILED
                return HaltFsmCmd(reason="청구 금액(Charge Amount)은 0보다 커야 합니다.")
                
            # 컨텍스트 저장
            self.caller = event.caller
            self.target_contract = event.target_contract
            self.charge_amount = event.charge_amount
            
            # 상태 전이 및 인프라(DVM) 실행 명령 하달
            self.state = TransactionState.SHADOW_EXECUTION
            return ExecuteDvmCmd(
                target_contract=self.target_contract,
                active_calldata=event.calldata,
                active_snapshot=event.active_snapshot
            )
        ## Transition 2: SHADOW_EXECUTION -> LEDGER_SEALING (or FAILED)
        elif self.state == TransactionState.SHADOW_EXECUTION and isinstance(event, DvmResultEvent):
            # 비즈니스 규칙 2: 가상 머신 실행 실패 시 차단
            if not event.success:
                self.state = TransactionState.FAILED
                return HaltFsmCmd(reason=f"REVM Execution Reverted: {event.revert_reason}")
                
            # 비즈니스 규칙 3: 가스 한도 초과(OOG) 방어
            if event.gas_used > self.max_gas_limit:
                self.state = TransactionState.FAILED
                return HaltFsmCmd(reason=f"가스 한도 초과 (Used: {event.gas_used}, Limit: {self.max_gas_limit})")
                
            # 비즈니스 규칙 4: 무의미한 상태 변환(Empty State Diff) 거부
            if not event.state_diff:
                self.state = TransactionState.FAILED
                return HaltFsmCmd(reason="상태 변경이 발생하지 않은(No State Diff) 트랜잭션입니다.")
                
            # 상태 전이 및 인프라(Ledger) 기록 명령 하달
            self.state = TransactionState.LEDGER_SEALING
            return LedgerSealCmd(
                caller=self.caller,
                target_contract=self.target_contract,
                state_diff=event.state_diff,
                gas_used=event.gas_used
            )
        ## Exception: 예기치 않은 이벤트 인입 (Invalid State Transition)
        else:
            current_state_name = self.state.name
            event_name = event.__class__.__name__
            self.state = TransactionState.FAILED
            return HaltFsmCmd(reason=f"허용되지 않은 상태 전이: State({current_state_name}) + Event({event_name})")