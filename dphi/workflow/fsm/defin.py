# fiber.dphi.workflow.fsm.defin
import hashlib
from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, List, Optional

class FsmState(Enum):
    INIT = auto()
    VIRTUAL_EXCHANGE = auto()
    MICRO_BILLING = auto()
    STATE_NETTING = auto()
    SETTLED = auto()
    HALTED = auto()

@dataclass
class FsmStartIntent:
    tenant_address: str
    initial_deposit: float
    target_contract: str

@dataclass
class UtxoAnchoredEvent:
    tx_hash: str

@dataclass
class WasmExecutedEvent:
    success: bool
    remaining_fuel: int
    worker_tx_hashes: List[str]
    error_reason: Optional[str] = None

@dataclass
class MintGenesisUtxoCmd:
    budget: int
    owner: str

@dataclass
class ExecuteParallelWasmCmd:
    concurrent_agents: int
    budget_per_agent: int
    root_tx_hash: str

@dataclass
class SealSettlementCmd:
    tenant: str
    net_debt: float
    merkle_root: str

@dataclass
class FsmHaltCmd:
    reason: str

class DefinFSM:
    def __init__(self, concurrent_agents: int = 3):
        self.state: FsmState = FsmState.INIT
        self.concurrent_agents = concurrent_agents
        self.tenant: str = ""
        self.initial_deposit: float = 0.0
        self.authorized_fuel_budget: int = 0
        self.root_utxo_hash: str = ""
        self.all_tx_hashes: List[str] = []

    def _pure_compute_merkle_root(self, hashes: List[str]) -> str:
        if not hashes:
            return "0x0"
        combined = "".join(hashes).encode('utf-8')
        return hashlib.sha256(combined).hexdigest()

    def apply_event(self, event: Any) -> Optional[Any]:
        if self.state == FsmState.INIT and isinstance(event, FsmStartIntent):
            if event.initial_deposit <= 0:
                self.state = FsmState.HALTED
                return FsmHaltCmd(reason="Initial deposit must be greater than 0")
                
            fuel_ratio = 1_000_000
            self.tenant = event.tenant_address
            self.initial_deposit = event.initial_deposit
            self.authorized_fuel_budget = int(self.initial_deposit * fuel_ratio)
            
            self.state = FsmState.VIRTUAL_EXCHANGE
            return MintGenesisUtxoCmd(budget=self.authorized_fuel_budget, owner=self.tenant)

        elif self.state == FsmState.VIRTUAL_EXCHANGE and isinstance(event, UtxoAnchoredEvent):
            self.root_utxo_hash = event.tx_hash
            self.all_tx_hashes.append(event.tx_hash)
            
            budget_per_agent = self.authorized_fuel_budget // self.concurrent_agents
            self.state = FsmState.MICRO_BILLING
            return ExecuteParallelWasmCmd(
                concurrent_agents=self.concurrent_agents,
                budget_per_agent=budget_per_agent,
                root_tx_hash=self.root_utxo_hash
            )

        elif self.state == FsmState.MICRO_BILLING and isinstance(event, WasmExecutedEvent):
            if not event.success:
                self.state = FsmState.HALTED
                return FsmHaltCmd(reason=f"WASM Execution Reverted: {event.error_reason}")
                
            self.all_tx_hashes.extend(event.worker_tx_hashes)
            total_fuel_consumed = self.authorized_fuel_budget - event.remaining_fuel
            net_debt_usdc = total_fuel_consumed / 1_000_000
            
            if total_fuel_consumed > self.authorized_fuel_budget or net_debt_usdc < 0:
                self.state = FsmState.HALTED
                return FsmHaltCmd(reason="Anomalous fuel calculation detected")

            merkle_root = self._pure_compute_merkle_root(self.all_tx_hashes)
            self.state = FsmState.STATE_NETTING
            return SealSettlementCmd(
                tenant=self.tenant, 
                net_debt=net_debt_usdc, 
                merkle_root=merkle_root
            )

        else:
            self.state = FsmState.HALTED
            return FsmHaltCmd(reason=f"Invalid event {event.__class__.__name__} in state {self.state.name}")