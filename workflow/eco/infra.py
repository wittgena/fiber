# fiber.workflow.eco.infra
import json
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

from fiber.dphi.adapter.config import dphi_env
from xphi.watcher.plane.emitter import get_emitter

from xphi.eco.protocol import (
    TriadAxis, ProtocolValidator, D3Protocol,
    MsgIngressPledge, MsgDelegateTrust, MsgWasmExecution, 
    MsgExecutionReceipt, MsgSettlementSeal
)
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.space.topos.network.bridge import RpcBridge
from xphi.kernel.dphi.adapter.utxo import (
    UtxoAdapter, UtxoPointer, UtxoInput, UtxoOutput, UtxoTransaction,
    AgentWallet, compute_merkle_root
)
from xphi.kernel.dphi.ledger.consensus import KernelLedger, SealedKernel, ToposBlob
from xphi.kernel.dphi.ledger.oracle import LedgerOracle
from xphi.kernel.dphi.method import DphiMethod

log = get_emitter("eco.infra")


# =====================================================================
# [1] Domain Models & Enums (Implementation specific)
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


# =====================================================================
# [2] Generic Actuators (Chain-Agnostic Pass-through)
# =====================================================================
class GenericExecutionActuator:
    """@desc: executes generic sandboxes to physically resolve L2 Intent messages without VM Bias."""
    def __init__(self, broker: DphiBroker, validator: ProtocolValidator):
        self.broker = broker
        self.validator = validator

    async def execute_task(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        # 1. Validate & Consume L1 UTXO
        execution_tx_hash = await self.validator.apply_wasm_execution(msg)

        # 2. Fire physical sandbox (Real DphiBroker 연동)
        try:
            payload = msg.target_wasm if isinstance(msg.target_wasm, str) else json.dumps(msg.target_wasm)

            res = await self.broker.execute(
                code={"action": DphiMethod.EXECUTE_CODE.value, "target": payload}, 
                tier=msg.tier if hasattr(msg, 'tier') else dphi_env.wasm.tier, 
                context={"worker": msg.worker_address, "tx_hash": execution_tx_hash}
            )

            if not res.success:
                raise RuntimeError(f"Execution Reverted: {res.error}")

        except Exception as e:
            log.error(f"[Actuator:Substrate] Execution Failed: {str(e)}")
            raise RuntimeError(f"Sandbox Execution Reverted ({str(e)})")

        # 3. Issue receipt (State Root)
        sealed = SealedKernel(
            kernel_id=f"ker_{execution_tx_hash[:8]}",
            stream_id=execution_tx_hash,
            executable_payload=payload,
            tension_at_seal=0.1,
            signature="0xGeneratedSignatureForExecution"
        )
        return MsgExecutionReceipt(worker_address=msg.worker_address, execution_tx_hash=execution_tx_hash, sealed_kernel=sealed)


class GenericSettlementActuator:
    """@desc: Validates L2 state diffs abstractly and seals the ledger."""
    def __init__(self, bridge: RpcBridge, validator: ProtocolValidator):
        self.bridge = bridge
        self.validator = validator

    async def execute_settlement(self, msg: MsgSettlementSeal) -> str:
        validation_payload = {
            "action": DphiMethod.SEAL_EPOCH.value,
            "aggregator": msg.aggregator_address,
            "calldata_or_proof": msg.l1_calldata,
        }

        # Real RPC Bridge를 통한 L1/DVM 통신
        res = await self.bridge.request({"action": "verify_state", "payload": validation_payload})
        if res.get("status") != 200 or not res.get("data", {}).get("success", True):
            raise RuntimeError("Settlement execution reverted by verification bridge.")

        msg.rollup_blob.details += " | Bridge Verified"
        return self.validator.apply_settlement(msg)


# =====================================================================
# [3] Real Protocol Interface
# =====================================================================
class EcoProtocolInterface(D3Protocol):
    """@desc: 실제 Broker와 Bridge를 주입받아 동작하는 물리 계층 인터페이스"""
    def __init__(self):
        self.broker = DphiBroker()
        self.bridge = RpcBridge(endpoint=dphi_env.rpc.target_url if hasattr(dphi_env, 'rpc') else None)
        self.ledger = KernelLedger()
        self.utxo_adapter = UtxoAdapter(broker=self.broker)
        self.oracle = LedgerOracle(broker=self.broker)

        self.validator = ProtocolValidator(self.utxo_adapter, self.ledger, self.oracle)
        self.exec_actuator = GenericExecutionActuator(self.broker, self.validator)
        self.settle_actuator = GenericSettlementActuator(self.bridge, self.validator)

    async def publish_pledge(self, msg: MsgIngressPledge) -> str:
        return await self.validator.apply_ingress(msg)

    async def publish_delegation(self, msg: MsgDelegateTrust) -> str:
        return await self.validator.apply_delegation(msg)

    async def request_execution(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        return await self.exec_actuator.execute_task(msg)

    async def publish_settlement(self, msg: MsgSettlementSeal) -> str:
        return await self.settle_actuator.execute_settlement(msg)


# =====================================================================
# [4] Client Runners (NotaryNode & EcosystemActor)
# =====================================================================
class NotaryNode:
    def __init__(self, alias: str, axis: TriadAxis, archetype: str, incentive: IncentiveModel, parent: Optional['NotaryNode'] = None):
        self.alias = alias
        self.axis = axis
        self.archetype = archetype
        self.incentive = incentive
        self.parent = parent
        self.sub_notaries: List['NotaryNode'] = []

        self.wallet = AgentWallet()
        self.utxo_ptrs: List[UtxoPointer] = []
        self.generated_state_roots: List[str] = []

    async def execute_pledge(self, amount: int, interface: D3Protocol) -> str:
        tx_mint = UtxoTransaction(
            inputs=[], 
            outputs=[UtxoOutput(amount=amount, owner=self.wallet.address, asset_type=self.incentive.resource_type.value)],
            metadata={"action": "GENESIS_MINT", "node": self.alias}
        )
        msg = MsgIngressPledge(axis=self.axis, actor_address=self.wallet.address, pledge_tx=tx_mint)
        tx_hash = await interface.publish_pledge(msg)

        self.utxo_ptrs.append(UtxoPointer(tx_hash, 0))
        self.incentive.resource_balance += amount
        log.info(f"  └─ 🛡️ [Pledge] {self.alias} submitted pledge of {amount:,}. TxHash: {tx_hash[:8]}...")
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

        tx_distribute = UtxoTransaction(
            inputs=[UtxoInput(pointer=ptr, signature=sig, owner_address=self.wallet.address)],
            outputs=outputs,
            metadata={"action": "SWARM_DISTRIBUTION"}
        )

        distribute_msg = MsgDelegateTrust(delegator_address=self.wallet.address, split_tx=tx_distribute)
        distribute_hash = await interface.publish_delegation(distribute_msg)

        if remain_amount > 0:
            self.utxo_ptrs.append(UtxoPointer(distribute_hash, num_workers))
        self.incentive.resource_balance = remain_amount

        receipt_hashes = []
        for i, worker in enumerate(worker_wallets):
            worker_ptr = UtxoPointer(distribute_hash, i)
            tx_exec = UtxoTransaction(
                inputs=[UtxoInput(pointer=worker_ptr, signature=worker.sign_payload(worker_ptr.to_key()), owner_address=worker.address)],
                outputs=[UtxoOutput(amount=0, owner="0xDEAD", asset_type="CONSUME")], 
                metadata={"action": "INTENT_EXECUTION", "worker": worker.address[:8]}
            )
            exec_msg = MsgWasmExecution(worker_address=worker.address, target_wasm=target_payload, execution_tx=tx_exec)
            exec_msg.tier = tier 

            receipt: MsgExecutionReceipt = await interface.request_execution(exec_msg)
            receipt_hashes.append(receipt.execution_tx_hash)

        merkle_root = compute_merkle_root(receipt_hashes)
        self.generated_state_roots.append(merkle_root)

        log.info(f"  └─ ⚙️ [Intent Execution: {self.alias}] Task Executed. Compressed Root: 0x{merkle_root[:12]}...")
        return merkle_root

    async def aggregate_and_seal_settlement(self, interface: D3Protocol, custom_blob: ToposBlob = None, custom_calldata: str = None) -> str:
        canonical_state_hash = compute_merkle_root(self.generated_state_roots) if self.generated_state_roots else "0x0"

        blob = custom_blob or ToposBlob(
            action="SETTLEMENT_CLOSURE", from_state="notary.swarm.executed", to_state="ledger.sealed",
            tension=0.99, details=f"Consolidated {len(self.generated_state_roots)} Roots"
        )
        calldata = custom_calldata or f"0x00000000{canonical_state_hash[:56]}"

        msg = MsgSettlementSeal(
            aggregator_address=self.wallet.address, rollup_blob=blob,
            consolidated_root=canonical_state_hash, l1_calldata=calldata
        )
        return await interface.publish_settlement(msg)


class EcosystemActor:
    def __init__(self, alias: str, axis: TriadAxis, resource_type: GrantResource, initial_budget: int):
        self.alias = alias
        self.axis = axis
        self.state = ActorState.ORPHAN
        self.wallet = AgentWallet()
        self.budget_committed = initial_budget
        self.resource_type = resource_type
        self.notary_node: Optional[NotaryNode] = None
        self.owned_merkle_roots: List[str] = []

    async def pledge_to_interface(self, interface: D3Protocol) -> NotaryNode:
        incentive = IncentiveModel(
            strategic_driver=f"{self.alias} Driver", 
            network_dividend=f"Support {self.axis.value}", 
            resource_type=self.resource_type, 
            resource_balance=0 
        )
        self.notary_node = NotaryNode(self.alias, self.axis, f"Gen-N:{self.alias}", incentive)
        await self.notary_node.execute_pledge(self.budget_committed, interface)
        self.state = ActorState.PLEDGED
        return self.notary_node