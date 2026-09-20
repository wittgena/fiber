# fiber.gateway.node.notary
import json
import base64
import hashlib
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Protocol
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.gateway.edge.ext.config.exchange import exchange_config
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.sandbox.protocol import (
    TriadAxis, ProtocolValidator, D3Protocol,
    MsgIngressPledge, MsgDelegateTrust, MsgWasmExecution, 
    MsgExecutionReceipt, MsgSettlementSeal
)
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import (
    PtaAdapter, PtaPointer, PtaInput, PtaOutput, 
    PtaTransaction, compute_merkle_root
)
from xphi.state.anchor.consensus import KernelLedger, SealedKernel, ToposBlob
from xphi.state.anchor.oracle import AnchorOracle

log = get_emitter("node.notary")

# =====================================================================
# [1] Domain Models & Enums
# =====================================================================

class GrantResource(str, Enum):
    INTENT_QUOTA = "Intent_Quota"               
    SUBSTRATE_BANDWIDTH = "Substrate_Bandwidth" 
    SETTLEMENT_BOND = "Settlement_Bond"         

@dataclass
class IncentiveModel:
    strategic_driver: str
    resource_type: GrantResource
    network_dividend: str = ""
    resource_balance: int = 0  

class ActorState(str, Enum):
    ORPHAN = "Orphan"                 
    PLEDGED = "Pledged"               
    EXECUTING = "Executing"     
    SEALED = "Sealed"

class SettlementVerifier(Protocol):
    async def verify(self, payload: dict) -> bool: ...

class LocalMockVerifier:
    async def verify(self, payload: dict) -> bool:
        if set(payload.get("signers", [])) != set(payload.get("allowed_signers", [])):
            raise RuntimeError("Consensus Failed: Signature verification rejected (Signer mismatch)")
        return True


# =====================================================================
# [2] Cryptographic Identity & Legacy Swarm
# =====================================================================

class NodeWallet:
    def __init__(self, private_key: Optional[Any] = None):
        if private_key is None:
            self.private_key = ed25519.Ed25519PrivateKey.generate()
        elif isinstance(private_key, str):
            try:
                self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(private_key))
            except ValueError:
                raise ValueError("Invalid private key hex format")
        else:
            self.private_key = private_key

        self.private_key_hex = self.private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption()
        ).hex()

        self.public_key = self.private_key.public_key()
        raw_pub = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
        self.address = f"cosm_{base64.urlsafe_b64encode(raw_pub).decode().rstrip('=')}"

    def sign_payload(self, payload: str) -> str:
        signature = self.private_key.sign(payload.encode('utf-8'))
        return base64.urlsafe_b64encode(signature).decode()


class NotarySwarm:
    """[RESTORED] WASM 커널 및 E2E 테스트와 완벽 호환되는 결정론적(Deterministic) 합의체 스웜"""
    def __init__(self, size: int = 3):
        self.notaries = []
        for i in range(size):
            # WASM 커널이 Genesis State로 신뢰하고 있는 고정된 시드 기반 키 생성
            seed = hashlib.sha256(f"dphi_notary_node_{i}".encode()).digest()
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            public_hex = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex()
            self.notaries.append({"priv": private_key, "pub": public_hex})
        
        # 시스템 글로벌 설정에 인가된 공개키 덮어쓰기
        exchange_config.export_attestation.__class__.witness_pubkeys = property(lambda self: [node["pub"] for node in self.notaries])

    @property
    def public_keys(self) -> List[str]:
        return [node["pub"] for node in self.notaries]

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        """바이트 해시를 직접 서명하여 Hex로 리턴 (기존 구조 완벽 호환)"""
        return [node["priv"].sign(canonical_hash).hex() for node in self.notaries]


# =====================================================================
# [3] Protocol Actuators
# =====================================================================

class GenericExecutionActuator:
    def __init__(self, broker: DphiBroker, validator: ProtocolValidator):
        self.broker = broker
        self.validator = validator

    async def execute_task(self, msg: MsgWasmExecution) -> MsgExecutionReceipt:
        execution_tx_hash = await self.validator.apply_wasm_execution(msg)
        try:
            payload = msg.target_wasm if isinstance(msg.target_wasm, str) else json.dumps(msg.target_wasm)
            execution_tier = getattr(msg, "tier", None) or exchange_config.wasm.tier
            res = await self.broker.execute(
                code=payload, 
                variables={"worker": msg.worker_address, "tx_hash": execution_tx_hash},
                tier=execution_tier
            )
            if not res.success:
                raise RuntimeError(f"Execution Reverted: {res.error}")

        except Exception as e:
            log.error(f"[Actuator:Substrate] Execution Failed: {str(e)}")
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
        validation_payload = json.loads(msg.l1_calldata) if isinstance(msg.l1_calldata, str) else {"calldata": msg.l1_calldata}
        try:
            await self.verifier.verify(validation_payload)
        except Exception as e:
            raise RuntimeError(f"Settlement execution reverted by verification edge: {str(e)}")

        msg.rollup_blob.details += " | Edge Verified"
        return self.validator.apply_settlement(msg)


class EcoProtocolInterface(D3Protocol):
    def __init__(self, verifier: Optional[SettlementVerifier] = None):
        self.broker = DphiBroker()
        self.ledger = KernelLedger()
        self.pta_adapter = PtaAdapter(broker=self.broker)
        self.oracle = AnchorOracle(broker=self.broker)
        self.validator = ProtocolValidator(self.pta_adapter, self.ledger, self.oracle)
        
        self.exec_actuator = GenericExecutionActuator(self.broker, self.validator)
        self.settle_actuator = GenericSettlementActuator(verifier or LocalMockVerifier(), self.validator)

    async def publish_pledge(self, msg: MsgIngressPledge) -> str: return await self.validator.apply_ingress(msg)
    async def publish_delegation(self, msg: MsgDelegateTrust) -> str: return await self.validator.apply_delegation(msg)
    async def request_execution(self, msg: MsgWasmExecution) -> MsgExecutionReceipt: return await self.exec_actuator.execute_task(msg)
    async def publish_settlement(self, msg: MsgSettlementSeal) -> str: return await self.settle_actuator.execute_settlement(msg)


# =====================================================================
# [4] The Node Notary (For specific lifecycle nodes, co-existing with Swarm)
# =====================================================================

class NodeNotary:
    """단일 공증 노드의 생명주기 및 합의를 관장하는 엔티티"""
    def __init__(self, alias: str, axis: TriadAxis, resource_type: GrantResource, initial_budget: int, agent_alias: Optional[str] = None):
        self.alias = alias
        self.axis = axis
        self.state = ActorState.ORPHAN
        
        pkey = exchange_config.get_agent_pkey(agent_alias) if agent_alias else None
        self.wallet = NodeWallet(private_key=pkey)
        
        self.initial_budget = initial_budget
        self.incentive = IncentiveModel(
            strategic_driver=alias, 
            resource_type=resource_type, 
            resource_balance=0
        )
        
        self.pta_ptrs: List[PtaPointer] = []
        self.generated_state_roots: List[str] = []

    async def pledge(self, interface: D3Protocol) -> str:
        tx_mint = PtaTransaction(
            inputs=[], 
            outputs=[PtaOutput(amount=self.initial_budget, owner=self.wallet.address, asset_type=self.incentive.resource_type.value)], 
            metadata={"action": "GENESIS_MINT"}
        )
        msg = MsgIngressPledge(axis=self.axis, actor_address=self.wallet.address, pledge_tx=tx_mint)
        
        tx_hash = await interface.publish_pledge(msg)
        self.pta_ptrs.append(PtaPointer(tx_hash, 0))
        self.incentive.resource_balance += self.initial_budget
        self.state = ActorState.PLEDGED
        return tx_hash

    async def delegate_and_execute(self, num_workers: int, burn_amount: int, target_payload: Any, interface: D3Protocol, tier: str = "SYSTEM") -> str:
        if not self.pta_ptrs: return "0x0"
        self.state = ActorState.EXECUTING
        
        total_needed = num_workers * burn_amount
        if total_needed > self.incentive.resource_balance:
            raise ValueError(f"Insufficient Notary balance. Required: {total_needed}, Available: {self.incentive.resource_balance}")
            
        ptr = self.pta_ptrs.pop(0)
        sig = self.wallet.sign_payload(ptr.to_key())
        
        worker_wallets = [NodeWallet() for _ in range(num_workers)]
        outputs = [PtaOutput(amount=burn_amount, owner=w.address, asset_type=self.incentive.resource_type.value) for w in worker_wallets]
        
        remain_amount = self.incentive.resource_balance - total_needed
        if remain_amount > 0:
            outputs.append(PtaOutput(amount=remain_amount, owner=self.wallet.address, asset_type=self.incentive.resource_type.value))

        tx_distribute = PtaTransaction(
            inputs=[PtaInput(pointer=ptr, signature=sig, owner_address=self.wallet.address)], 
            outputs=outputs, 
            metadata={"action": "SWARM_DISTRIBUTION"}
        )
        distribute_hash = await interface.publish_delegation(MsgDelegateTrust(delegator_address=self.wallet.address, split_tx=tx_distribute))
        
        if remain_amount > 0:
            self.pta_ptrs.append(PtaPointer(distribute_hash, num_workers))
        self.incentive.resource_balance = remain_amount

        receipt_hashes = []
        for i, worker in enumerate(worker_wallets):
            worker_ptr = PtaPointer(distribute_hash, i)
            tx_exec = PtaTransaction(
                inputs=[PtaInput(pointer=worker_ptr, signature=worker.sign_payload(worker_ptr.to_key()), owner_address=worker.address)],
                outputs=[PtaOutput(amount=0, owner="0xDEAD", asset_type="CONSUME")]
            )
            exec_msg = MsgWasmExecution(worker_address=worker.address, target_wasm=target_payload, execution_tx=tx_exec)
            exec_msg.tier = tier 

            receipt: MsgExecutionReceipt = await interface.request_execution(exec_msg)
            receipt_hashes.append(receipt.execution_tx_hash)

        merkle_root = compute_merkle_root(receipt_hashes)
        self.generated_state_roots.append(merkle_root)
        return merkle_root

    async def seal_epoch(self, interface: D3Protocol, custom_blob: ToposBlob = None, custom_calldata: str = None) -> str:
        canonical_state_hash = compute_merkle_root(self.generated_state_roots) if self.generated_state_roots else "0x0"
        blob = custom_blob or ToposBlob(action="SETTLEMENT_CLOSURE", from_state="notary", to_state="sealed", tension=0.99, details="")
        calldata = custom_calldata or f"0x00{canonical_state_hash[:56]}"
        
        msg = MsgSettlementSeal(
            aggregator_address=self.wallet.address, 
            rollup_blob=blob, 
            consolidated_root=canonical_state_hash, 
            l1_calldata=calldata
        )
        tx_hash = await interface.publish_settlement(msg)
        self.state = ActorState.SEALED
        return tx_hash