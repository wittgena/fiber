# fiber.dphi.infra.eco.actor
## @lineage: fiber.dphi.eco.infra
import json
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Protocol

from fiber.dphi.infra.config import dphi_env
from xphi.watcher.plane.emitter import get_emitter

from xphi.xor.space.sandbox.protocol import (
    TriadAxis, ProtocolValidator, D3Protocol,
    MsgIngressPledge, MsgDelegateTrust, MsgWasmExecution, 
    MsgExecutionReceipt, MsgSettlementSeal
)
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.utxo import (
    UtxoAdapter, UtxoPointer, UtxoInput, UtxoOutput, UtxoTransaction,
    AgentWallet, compute_merkle_root
)
from xphi.kernel.dphi.ledger.consensus import KernelLedger, SealedKernel, ToposBlob
from xphi.kernel.dphi.ledger.oracle import LedgerOracle

log = get_emitter("eco.infra")

# =====================================================================
# [1] Domain Models & Ports
# =====================================================================
class GrantResource(str, Enum):
    INTENT_QUOTA = "Intent_Quota"               
    SUBSTRATE_BANDWIDTH = "Substrate_Bandwidth" 
    SETTLEMENT_BOND = "Settlement_Bond"         

@dataclass
class IncentiveModel:
    strategic_driver: str
    network_dividend: str
    resource_type: GrantResource
    resource_balance: int = 0  

class ActorState(str, Enum):
    ORPHAN = "Orphan"                 
    PLEDGED = "Pledged"               
    EXECUTING = "Executing"     
    SEALED = "Sealed"

class SettlementVerifier(Protocol):
    """[PORT] 외부 통신 혹은 로컬 E2E 모의 검증을 위한 인터페이스"""
    async def verify(self, payload: dict) -> bool:
        ...

class LocalMockVerifier:
    """[ADAPTER] E2E 환경을 위한 기본 Fallback 검증기"""
    async def verify(self, payload: dict) -> bool:
        if set(payload.get("signers", [])) != set(payload.get("allowed_signers", [])):
            raise RuntimeError("Consensus Failed: Signature verification rejected (Signer mismatch)")
        return True


# =====================================================================
# [2] Generic Actuators 
# =====================================================================
class GenericExecutionActuator:
    def __init__(self, broker: DphiBroker, validator: ProtocolValidator):
        self.broker = broker
        self.validator = validator

    async def execute_task(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        execution_tx_hash = await self.validator.apply_wasm_execution(msg)

        try:
            # 타겟 코드를 추출
            payload = msg.target_wasm if isinstance(msg.target_wasm, str) else json.dumps(msg.target_wasm)
            execution_tier = getattr(msg, "tier", None) or dphi_env.wasm.tier

            # [CRITICAL FIX] 딕셔너리 포장({"action":...})을 제거하고, 순수 문자열 코드(payload)를 인자로 던집니다.
            # 이래야 브로커가 DVM 파싱 크래시를 내지 않고 Python Wasm Sandbox(EXECUTE_CODE)로 정상 라우팅합니다.
            res = await self.broker.execute(
                code=payload, 
                variables={"worker": msg.worker_address, "tx_hash": execution_tx_hash},
                tier=execution_tier
            )

            if not res.success:
                raise RuntimeError(f"Execution Reverted: {res.error}")

        except Exception as e:
            log.error(f"[Actuator:Substrate] Execution Failed: {str(e)}")
            # Wasmtime에서 발생한 에러(Timeout, Net Block 등)를 파이프라인으로 정확히 전파
            raise RuntimeError(f"Sandbox Execution Reverted ({str(e)})")

        sealed = SealedKernel(
            kernel_id=f"ker_{execution_tx_hash[:8]}",
            stream_id=execution_tx_hash,
            executable_payload=payload,
            tension_at_seal=0.1,
            signature="0xGeneratedSignatureForExecution"
        )
        return MsgExecutionReceipt(worker_address=msg.worker_address, execution_tx_hash=execution_tx_hash, sealed_kernel=sealed)


class GenericSettlementActuator:
    def __init__(self, verifier: SettlementVerifier, validator: ProtocolValidator):
        self.verifier = verifier
        self.validator = validator

    async def execute_settlement(self, msg: MsgSettlementSeal) -> str:
        try:
            validation_payload = json.loads(msg.l1_calldata)
        except Exception:
            validation_payload = {"calldata": msg.l1_calldata}

        try:
            await self.verifier.verify(validation_payload)
        except Exception as e:
            raise RuntimeError(f"Settlement execution reverted by verification edge: {str(e)}")

        msg.rollup_blob.details += " | Edge Verified"
        return self.validator.apply_settlement(msg)


# =====================================================================
# [3] Real Protocol Interface & Runners
# =====================================================================
class EcoProtocolInterface(D3Protocol):
    def __init__(self, verifier: Optional[SettlementVerifier] = None):
        self.broker = DphiBroker()
        self.ledger = KernelLedger()
        self.utxo_adapter = UtxoAdapter(broker=self.broker)
        self.oracle = LedgerOracle(broker=self.broker)

        self.validator = ProtocolValidator(self.utxo_adapter, self.ledger, self.oracle)
        self.exec_actuator = GenericExecutionActuator(self.broker, self.validator)
        
        self.settlement_verifier = verifier or LocalMockVerifier()
        self.settle_actuator = GenericSettlementActuator(self.settlement_verifier, self.validator)

    async def publish_pledge(self, msg: MsgIngressPledge) -> str:
        return await self.validator.apply_ingress(msg)
    async def publish_delegation(self, msg: MsgDelegateTrust) -> str:
        return await self.validator.apply_delegation(msg)
    async def request_execution(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        return await self.exec_actuator.execute_task(msg)
    async def publish_settlement(self, msg: MsgSettlementSeal) -> str:
        return await self.settle_actuator.execute_settlement(msg)


class NotaryNode:
    def __init__(self, alias: str, axis: TriadAxis, archetype: str, incentive: IncentiveModel, parent: Optional['NotaryNode'] = None, private_key_hex: Optional[str] = None):
        self.alias = alias
        self.axis = axis
        self.archetype = archetype
        self.incentive = incentive
        self.parent = parent
        self.sub_notaries: List['NotaryNode'] = []
        self.wallet = AgentWallet(private_key=private_key_hex) if private_key_hex else AgentWallet()
        self.utxo_ptrs: List[UtxoPointer] = []
        self.generated_state_roots: List[str] = []

    async def execute_pledge(self, amount: int, interface: D3Protocol) -> str:
        tx_mint = UtxoTransaction(inputs=[], outputs=[UtxoOutput(amount=amount, owner=self.wallet.address, asset_type=self.incentive.resource_type.value)], metadata={"action": "GENESIS_MINT"})
        msg = MsgIngressPledge(axis=self.axis, actor_address=self.wallet.address, pledge_tx=tx_mint)
        tx_hash = await interface.publish_pledge(msg)
        self.utxo_ptrs.append(UtxoPointer(tx_hash, 0))
        self.incentive.resource_balance += amount
        return tx_hash

    async def execute_swarm_task(self, num_workers: int, burn_amount: int, target_payload: Any, interface: D3Protocol, tier: str = "SYSTEM") -> str:
        if not self.utxo_ptrs: return "0x0"
        total_needed = num_workers * burn_amount
        ptr = self.utxo_ptrs.pop(0)
        sig = self.wallet.sign_payload(ptr.to_key())
        worker_wallets = [AgentWallet() for _ in range(num_workers)]
        outputs = [UtxoOutput(amount=burn_amount, owner=w.address, asset_type=self.incentive.resource_type.value) for w in worker_wallets]
        remain_amount = self.incentive.resource_balance - total_needed
        if remain_amount > 0:
            outputs.append(UtxoOutput(amount=remain_amount, owner=self.wallet.address, asset_type=self.incentive.resource_type.value))

        tx_distribute = UtxoTransaction(inputs=[UtxoInput(pointer=ptr, signature=sig, owner_address=self.wallet.address)], outputs=outputs, metadata={"action": "SWARM_DISTRIBUTION"})
        distribute_hash = await interface.publish_delegation(MsgDelegateTrust(delegator_address=self.wallet.address, split_tx=tx_distribute))
        
        if remain_amount > 0:
            self.utxo_ptrs.append(UtxoPointer(distribute_hash, num_workers))
        self.incentive.resource_balance = remain_amount

        receipt_hashes = []
        for i, worker in enumerate(worker_wallets):
            worker_ptr = UtxoPointer(distribute_hash, i)
            tx_exec = UtxoTransaction(
                inputs=[UtxoInput(pointer=worker_ptr, signature=worker.sign_payload(worker_ptr.to_key()), owner_address=worker.address)],
                outputs=[UtxoOutput(amount=0, owner="0xDEAD", asset_type="CONSUME")]
            )
            exec_msg = MsgWasmExecution(worker_address=worker.address, target_wasm=target_payload, execution_tx=tx_exec)
            exec_msg.tier = tier 

            receipt: MsgExecutionReceipt = await interface.request_execution(exec_msg)
            receipt_hashes.append(receipt.execution_tx_hash)

        merkle_root = compute_merkle_root(receipt_hashes)
        self.generated_state_roots.append(merkle_root)
        return merkle_root

    async def aggregate_and_seal_settlement(self, interface: D3Protocol, custom_blob: ToposBlob = None, custom_calldata: str = None) -> str:
        canonical_state_hash = compute_merkle_root(self.generated_state_roots) if self.generated_state_roots else "0x0"
        blob = custom_blob or ToposBlob(action="SETTLEMENT_CLOSURE", from_state="notary", to_state="sealed", tension=0.99, details="")
        calldata = custom_calldata or f"0x00{canonical_state_hash[:56]}"
        msg = MsgSettlementSeal(aggregator_address=self.wallet.address, rollup_blob=blob, consolidated_root=canonical_state_hash, l1_calldata=calldata)
        return await interface.publish_settlement(msg)


class EcosystemActor:
    def __init__(self, alias: str, axis: TriadAxis, resource_type: GrantResource, initial_budget: int, agent_alias: Optional[str] = None):
        self.alias = alias
        self.axis = axis
        self.state = ActorState.ORPHAN
        pkey = dphi_env.get_agent_pkey(agent_alias) if agent_alias else None
        self.wallet = AgentWallet(private_key=pkey) if pkey else AgentWallet()
        self.budget_committed = initial_budget
        self.resource_type = resource_type
        self.notary_node: Optional[NotaryNode] = None
        self.owned_merkle_roots: List[str] = []

    async def pledge_to_interface(self, interface: D3Protocol) -> NotaryNode:
        incentive = IncentiveModel(strategic_driver=f"{self.alias}", network_dividend="", resource_type=self.resource_type, resource_balance=0)
        self.notary_node = NotaryNode(
            alias=self.alias, axis=self.axis, archetype=f"Gen-N:{self.alias}", incentive=incentive,
            private_key_hex=self.wallet.private_key_hex if hasattr(self.wallet, 'private_key_hex') else None
        )
        await self.notary_node.execute_pledge(self.budget_committed, interface)
        self.state = ActorState.PLEDGED
        return self.notary_node