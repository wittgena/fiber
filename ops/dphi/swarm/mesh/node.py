# ops.dphi.swarm.mesh.node
import asyncio
import json
import uuid
import time
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, Set, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.contract.audit.promise import future, Promise, Validated
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("swarm.mesh")

## [Promises] Topological & P2P Consensus Anchors
p2p_topology_promise = Promise(
    contract="All swarm topological shifts (Splits/Merges) must be validated by the WASM Kernel.",
    invariant="A node shall not migrate or cast a SPLIT_VOTE unless the Kernel cryptographically verifies the network tension.",
    consequence="Unauthorized network forks based on subjective local evaluation will result in cryptographic isolation (Orphan Node).",
)

class NetworkTransport(ABC):
    """
    @xe.desc: Abstract boundary enforcing physical isolation of nodes.
    All state crosses this boundary strictly as serialized bytes.
    """
    @abstractmethod
    async def broadcast(self, topic: str, payload_bytes: bytes) -> None:
        pass

class VirtualMeshTransport(NetworkTransport):
    def __init__(self):
        self.routing_table: Dict[str, asyncio.Queue] = {}
        self.topic_subscribers: Dict[str, Set[str]] = {}

    def bind_socket(self, peer_id: str, topic: str) -> asyncio.Queue:
        socket_buffer = asyncio.Queue()
        self.routing_table[peer_id] = socket_buffer
        self.topic_subscribers.setdefault(topic, set()).add(peer_id)
        return socket_buffer

    async def broadcast(self, topic: str, payload_bytes: bytes) -> None:
        subscribers = self.topic_subscribers.get(topic, set())
        for peer_id in subscribers:
            buffer = self.routing_table.get(peer_id)
            if buffer:
                asyncio.create_task(self._delayed_delivery(buffer, payload_bytes))

    async def _delayed_delivery(self, buffer: asyncio.Queue, payload_bytes: bytes):
        await asyncio.sleep(0.01) # 10ms network latency
        await buffer.put(payload_bytes)

class AutonomousAgentNode:
    """
    @xe.desc: Trustless actor traversing the P2P mesh. 
    Subjective local decisions are removed; all consensus relies on the Kernel.
    """
    def __init__(self, transport: NetworkTransport, broker: WasmBroker, topos_group: str = "global_nexus"):
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.peer_id = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()[:16]
        
        self.topos_group = topos_group
        self.transport = transport
        self.broker = broker  # WASM Kernel Connection
        
        self.socket_buffer = transport.bind_socket(self.peer_id, self.topos_group)
        
        self.local_tension = 0.0
        self.peer_tensions: Dict[str, float] = {}

    def _serialize(self, payload: dict) -> bytes:
        return json.dumps(payload).encode('utf-8')

    def _deserialize(self, raw_bytes: bytes) -> dict:
        return json.loads(raw_bytes.decode('utf-8'))

    def _sign_payload(self, payload_dict: dict) -> str:
        raw_json_bytes = json.dumps(payload_dict, sort_keys=True).encode('utf-8')
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return self.private_key.sign(commit_hash).hex()

    async def start_listening(self):
        log.info(f"[{self.peer_id}] Started listening on topic: {self.topos_group}")
        while True:
            raw_bytes = await self.socket_buffer.get()
            payload = self._deserialize(raw_bytes)
            if payload.get("sender") == self.peer_id:
                continue
            await self._process_message(payload)

    @future(promise=p2p_topology_promise)
    async def _process_message(self, payload: dict):
        """
        @xe.desc: Evaluates incoming Gossip packets via the WASM Kernel.
        """
        msg_type = payload.get("type")
        sender = payload.get("sender")

        if msg_type == "TENSION_BEACON":
            self.peer_tensions[sender] = payload.get("tension_score", 0.0)
            await self._evaluate_swarm_health()
            
        elif msg_type == "PROPOSE_SPLIT":
            new_group = payload.get("new_group")
            validation_context = {
                "action": "VOTE_SPLIT",
                "target_group": new_group,
                "peer_tensions": self.peer_tensions
            }
            canonical_payload = StateAdapter.to_canonical_bytes(validation_context).decode('utf-8')
            
            log.info(f"[{self.peer_id}] Delegating SPLIT proposal validation to WASM Kernel...")
            res = await self.broker.invoke(WasmMethod.VALIDATE_INTENT, canonical_payload)
            
            if res.success:
                kernel_decision = json.loads(res.output)
                if kernel_decision.get("approved", False):
                    log.warning(f"[{self.peer_id}] Kernel Approved. Casting SPLIT_VOTE to '{new_group}'")
                    signature = self._sign_payload({"action": "SPLIT", "new_group": new_group})
                    vote_msg = self._serialize({"type": "SPLIT_VOTE", "sender": self.peer_id, "signature": signature, "new_group": new_group})
                    await self.transport.broadcast(self.topos_group, vote_msg)
                else:
                    log.info(f"[{self.peer_id}] Kernel Rejected Split Proposal: Tension not critical.")

    @future(promise=p2p_topology_promise)
    async def _evaluate_swarm_health(self):
        """
        @xe.desc: Evaluates global swarm tension via Kernel. If the Kernel detects a rupture threshold, initiates split.
        """
        tension_context = {"peer_tensions": self.peer_tensions, "current_group": self.topos_group}
        canonical_payload = StateAdapter.to_canonical_bytes(tension_context).decode('utf-8')
        
        res = await self.broker.invoke(WasmMethod.EVALUATE_TENSION, canonical_payload)
        
        if res.success:
            evaluation = json.loads(res.output)
            if evaluation.get("require_split", False) and self.topos_group == "global_nexus":
                new_sub_group = f"nexus_shard_{int(time.time())}"
                log.critical(f"[{self.peer_id}] KERNEL DETECTED TENSION SPIKE. Proposing Topological Split to '{new_sub_group}'!")
                
                proposal_bytes = self._serialize({
                    "type": "PROPOSE_SPLIT",
                    "sender": self.peer_id,
                    "new_group": new_sub_group
                })
                await self.transport.broadcast(self.topos_group, proposal_bytes)
                
                # 내부 상태 정리
                self.local_tension = 0.0
                self.peer_tensions.clear()

    async def broadcast_telemetry_loop(self):
        for _ in range(3):
            await asyncio.sleep(1)
            self.local_tension += 8.5
            msg_bytes = self._serialize({
                "type": "TENSION_BEACON",
                "sender": self.peer_id,
                "tension_score": self.local_tension
            })
            await self.transport.broadcast(self.topos_group, msg_bytes)

async def run_swarm_simulation():
    log.info("=== Starting WASM-Validated Autonomous Swarm (Actor Model) ===")
    
    mesh_network = VirtualMeshTransport()
    broker = WasmBroker(timeout=5.0) 
    swarm_nodes = [AutonomousAgentNode(transport=mesh_network, broker=broker) for _ in range(5)]
    listen_tasks = [asyncio.create_task(node.start_listening()) for node in swarm_nodes]
    telemetry_tasks = [asyncio.create_task(node.broadcast_telemetry_loop()) for node in swarm_nodes]
    
    await asyncio.gather(*telemetry_tasks)
    for task in listen_tasks:
        task.cancel()

if __name__ == "__main__":
    asyncio.run(run_swarm_simulation())