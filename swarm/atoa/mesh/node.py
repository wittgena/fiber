# swarm.atoa.mesh.node
## @lineage: swarm.mesh.node
## @lineage: swarm.dphi.mesh.node
import asyncio
import json
import uuid
import time
import hashlib
from typing import Dict, Set, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.contract.event.mesh.transport import MeshP2PTransport
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter

log = get_emitter("mesh.node")

class AgentNode:
    def __init__(self, transport: MeshP2PTransport, broker: WasmBroker, topos_group: str = "global_nexus"):
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.peer_id = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()[:16]
        
        self.topos_group = topos_group
        self.transport = transport
        self.broker = broker  # WASM Kernel Connection
        
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
        """
        @xe.desc: Transport 루프를 폴링(Queue)하는 대신, Zenoh Transport에 콜백을 바인딩합니다.
        """
        log.info(f"[{self.peer_id}] Binding to MeshP2PTransport on topic: {self.topos_group}")
        await self.transport.bind_and_start(ingress_callback=self._ingress_callback)
        await self.transport.join_topic(self.topos_group)

    async def _ingress_callback(self, sender_id: str, raw_bytes: bytes):
        """
        @xe.desc: Zenoh Transport로부터 주입되는 Ingress 이벤트 핸들러.
        """
        try:
            payload = self._deserialize(raw_bytes)
            # 자신이 보낸 메시지는 무시
            if payload.get("sender") == self.peer_id:
                return
            await self._process_message(payload)
        except Exception as e:
            log.error(f"[{self.peer_id}] Error in ingress callback: {e}")

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
            
    async def shutdown(self):
        """노드 종료 시 물리 네트워크 연결을 안전하게 해제합니다."""
        await self.transport.close()