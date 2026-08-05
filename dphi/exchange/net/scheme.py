# dphi.exchange.net.scheme
import time
import json
import hashlib
import asyncio
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from fastapi.routing import APIRoute
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from dphi.eco.rest import api as rest_app  
from arch.topos.network.bridge import FlowPropagator, RpcBridge
from arch.topos.network.channel.codec import JsonMessageCodec
from arch.topos.network.factory import ProtocolFactory

from kernel.dphi.scheme.runner import WebRunner
from kernel.dphi.adapter.eco import (
    EcoAdapter, ExchangeAdapter, WalletAdapter,
    X402SettlementReceipt, TransactionReceipt
)
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter, flow_scope
from arch.topos.workflow import Workflow, WorkflowMessage, StopMessage, ErrorMessage, step

class ExStartMsg(WorkflowMessage): pass
class ExIngressMsg(WorkflowMessage): pass
class ExEntanglementMsg(WorkflowMessage): pass
class ExSettlementMsg(WorkflowMessage): pass
class ExNexusMsg(WorkflowMessage): pass

class NodeIdentity:
    def __init__(self):
        self.key = ed25519.Ed25519PrivateKey.generate()
        self.pub_hex = self.key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, canonical_bytes: bytes) -> str:
        return self.key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

class ExchangeWorkflow(Workflow):
    def __init__(self, simulate_wallet: bool = True):
        super().__init__(name="EXCHANGE_E2E")
        self.log = get_emitter("workflow.exchange")
        
        self.field_node = NodeIdentity()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field_node.pub_hex)
        self.wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=simulate_wallet)
        
        if not self.wallet_adapter.simulate:
            self.wallet_adapter.fund_wallet()
            
        self.rpc_bridge: Optional[RpcBridge] = None
        self.protocol_transport = None
        self.agent_a = NodeIdentity()
        self.agent_b = NodeIdentity()
        
        self.phase_results = {}
        self.entangled_state = {}
        self.signatures = []
        self.x402_receipt: Optional[X402SettlementReceipt] = None
        self.receipt: Optional[TransactionReceipt] = None

    async def start(self, host: str, port: int) -> bool:
        self.log.info(f"\n=== [START] {self.name}: P2P Order Ingress & Deterministic Settlement ===")
        
        self.rpc_bridge = RpcBridge()
        bootstrap = ProtocolFactory() \
            .child_handler(lambda: FlowPropagator("EXCHANGE_AGENT")) \
            .child_handler(lambda: JsonMessageCodec()) \
            .child_handler(lambda: self.rpc_bridge)
            
        protocol = await bootstrap.connect(host, port)
        await asyncio.sleep(0.1) 
        
        self.protocol_transport = protocol.pipeline.transport if protocol and protocol.pipeline else None
        self.post_message(ExStartMsg())
        await self.run()
        
        return self.receipt is not None

    @step
    async def phase_ingress(self, msg: ExStartMsg) -> WorkflowMessage:
        self.log.info(" -> Running Phase: Ingress")
        res_a = await self.rpc_bridge.request({"action": "init_epoch", "topo": 101})
        res_b = await self.rpc_bridge.request({"action": "init_epoch", "topo": 101})
        self.phase_results['a'] = res_a.get("data", {"phase_id": "FAILED_A"})
        self.phase_results['b'] = res_b.get("data", {"phase_id": "FAILED_B"})
        
        return ExIngressMsg()

    @step
    async def phase_entanglement(self, msg: ExIngressMsg) -> WorkflowMessage:
        self.log.info(" -> Running Phase: Entanglement")
        parity = StateAdapter.build_parity_triplet(
            topos_id=f"clearing_batch_{int(time.time())}",
            phase_id=101,
            nexus_id=1
        )
        
        self.entangled_state = {
            "has_contention": True,
            "repos": {
                "participant_a": self.phase_results['a'].get("phase_id", "0"),
                "participant_b": self.phase_results['b'].get("phase_id", "0")
            },
            "parity": parity
        }
        return ExEntanglementMsg()

    @step
    async def phase_settlement(self, msg: ExEntanglementMsg) -> WorkflowMessage:
        self.log.info(" -> Running Phase: Settlement")
        invoice = EcoAdapter.build_x402_invoice(payee_address="0xFieldNode", amount_usdc="0.05", resource_id="fee")
        self.x402_receipt = EcoAdapter.process_x402_settlement(
            invoice=invoice, agent_wallet_address="0xAgentAWallet", wallet_adapter=self.wallet_adapter
        )
        return ExSettlementMsg()

    @step
    async def phase_nexus(self, msg: ExSettlementMsg) -> WorkflowMessage:
        self.log.info(" -> Running Phase: Nexus (Global Anchoring)")
        parity = self.entangled_state["parity"]
        canonical_bytes = StateAdapter.to_canonical_bytes({"parity": parity})
        self.signatures = [
            self.agent_a.sign(canonical_bytes),
            self.agent_b.sign(canonical_bytes),
            self.field_node.sign(canonical_bytes)
        ]
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity,
            parent_nexus_id=0,
            self_parent_state="genesis",
            repos=self.entangled_state["repos"],
            cached_states={},
            timestamp=time.time(),
            signers=[self.agent_a.pub_hex, self.agent_b.pub_hex, self.field_node.pub_hex],
            signatures=self.signatures,
            threshold=2
        )
        canonical_seal = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
        res = await self.rpc_bridge.request({"action": "seal_epoch", "payload": canonical_seal})
        if res.get("status") != 200:
            return ErrorMessage(f"Nexus Settlement Rejected: {res.get('error', 'Unknown')}")
            
        return ExNexusMsg()

    @step
    async def phase_finalize(self, msg: ExNexusMsg) -> WorkflowMessage:
        self.log.info(" -> Running Phase: Finalize")
        self.receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=self.entangled_state, 
            signatures=self.signatures, 
            cost_metrics={"fuel_consumed": 35000}, 
            tier="SYSTEM"
        )
        
        self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
        self._teardown_network()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self._teardown_network()
        return StopMessage(result=False)

    def _teardown_network(self):
        if self.protocol_transport:
            self.protocol_transport.close()