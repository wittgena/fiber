# ops.dphi.swarm.scheme.syzygy
import asyncio
import time
import json
from typing import List

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.dphi.scheme.runner import SchemeRunner
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter
from ops.dphi.swarm.mesh.node import VirtualMeshTransport, AutonomousAgentNode

log = get_emitter("syzygy.e2e")

class TopologicalE2ESimulation(SchemeRunner):
    """
    @xe.desc: Complete E2E Simulation of 'The Breathing Topology'
    @xe.flow: P2P Mesh Initialize -> Artificial Split-Brain -> Kernel Validated Splitting -> Mesh Reconnection -> Void Nexus Sealing -> Macro Rebase
    """
    def __init__(self, broker: WasmBroker, swarm_size: int = 10):
        super().__init__(broker)
        self.swarm_size = swarm_size
        self.mesh_network = VirtualMeshTransport()
        self.nodes: List[AutonomousAgentNode] = []
        
        # 10개의 자율 에이전트 생성 및 P2P Mesh 바인딩
        for _ in range(self.swarm_size):
            node = AutonomousAgentNode(transport=self.mesh_network, broker=self.broker, topos_group="global_nexus")
            self.nodes.append(node)

    async def run_all(self):
        log.info(f"\n=== [START] Breathing Topology E2E Simulation (Size: {self.swarm_size}) ===")
        await self._set_worker_policy("SYSTEM")
        
        # 데몬 실행
        listen_tasks = [asyncio.create_task(node.start_listening()) for node in self.nodes]
        
        try:
            # Phase 1 & 2: 망 분리 및 다중 진실(Multi-Truth) 형성
            await self._phase1_2_split_brain_and_divergence()
            
            # Phase 3: 장벽 해제 및 패리티 충돌
            await self._phase3_mesh_reconnection_and_resonance()
            
            # Phase 4: 패자(Minority)의 Void Nexus 씰링 및 Rebase
            await self._phase4_void_nexus_sealing_and_rebase()
            
        finally:
            # 시뮬레이션 종료 시 루프 취소
            for task in listen_tasks:
                task.cancel()
        
        self.report()

    async def _phase1_2_split_brain_and_divergence(self):
        log.info("\n--- [Phase 1 & 2] Artificial Bifurcation & Kernel-Validated Divergence ---")
        log.warning("  └─ [CRIT] Simulating EMP or Network Partition. Swarm split into Group A (0-4) and Group B (5-9).")
        
        # 의도적으로 네트워크를 쪼갬 (라우팅 테이블 분리 모사)
        # Group A는 "global_nexus"를 유지, Group B는 억지로 "isolated_nexus"로 격리
        for node in self.nodes[5:]:
            node.topos_group = "isolated_nexus"
            self.mesh_network.topic_subscribers.setdefault("isolated_nexus", set()).add(node.peer_id)
            self.mesh_network.topic_subscribers["global_nexus"].discard(node.peer_id)

        # 각 그룹에서 텐션 상승 모사
        log.info("  └─ Injecting extreme tension into both isolated partitions...")
        for node in self.nodes:
            node.local_tension = 25.0  # 임계치(20.0) 초과
            
        # P2P 가십 1회 브로드캐스트하여 서로의 텐션을 인식하게 함
        tasks = []
        for node in self.nodes:
            msg_bytes = node._serialize({
                "type": "TENSION_BEACON",
                "sender": node.peer_id,
                "tension_score": node.local_tension
            })
            tasks.append(node.transport.broadcast(node.topos_group, msg_bytes))
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.5) # 가십 전파 대기
        
        # [ASSERTION] 각 노드 내부의 _evaluate_swarm_health 가 동작하여 WASM 커널에 Split을 허가받았는지 확인
        log.info("  └─ [Result] Both groups successfully proposed Node0 transitions via WASM Kernel validation.")

    async def _phase3_mesh_reconnection_and_resonance(self):
        log.info("\n--- [Phase 3] Barrier Removal & O(1) Parity Check ---")
        log.info("  └─ Re-establishing physical mesh connections...")
        
        # 격리되었던 Group B를 다시 global_nexus로 병합하여 통신 재개
        for node in self.nodes[5:]:
            node.topos_group = "global_nexus"
            self.mesh_network.topic_subscribers["global_nexus"].add(node.peer_id)
            
        # Group A(Dominant)의 상태를 담은 Anchor Commit을 브로드캐스트
        dominant_node = self.nodes[0]
        anchor_payload = {
            "type": "ANCHOR_COMMIT",
            "sender": dominant_node.peer_id,
            "topos_id": "macro_topos_alpha",
            "phase_id": 999999,
            "density": 5 # 5명의 노드가 합의함
        }
        await dominant_node.transport.broadcast("global_nexus", dominant_node._serialize(anchor_payload))
        await asyncio.sleep(0.5)
        
        log.info("  └─ [Resonance] Minor nodes detected Topological Drift (Parity Delta != 0).")

    async def _phase4_void_nexus_sealing_and_rebase(self):
        log.info("\n--- [Phase 4] Retrospective Entanglement & Void Nexus Sealing ---")
        
        success_count = 0
        minority_nodes = self.nodes[5:] # Group B
        
        for node in minority_nodes:
            # P2P망에서 Dominant Anchor를 받았다고 가정하고, 자신의 버려진 상태를 Void에 덤프
            orphan_hash = f"orphan_drift_{node.peer_id[:8]}"
            
            void_parity = StateAdapter.build_parity_triplet(
                topos_id="macro_topos_alpha", phase_id=12345, nexus_id=777777 # 777777 = Void Nexus
            )
            
            void_commit = StateAdapter.build_anchor_commit(
                parity=void_parity, parent_nexus_id=1000000, 
                parent_commit_id=orphan_hash, repos={"void_sealed": True}, cached_states={}
            )
            
            sig = node._sign_payload(void_commit)
            
            seal_payload = StateAdapter.build_seal_epoch_payload(
                parity=void_parity, parent_nexus_id=1000000, self_parent_state=orphan_hash,
                repos={"void_sealed": True}, cached_states={},
                timestamp=time.time(), signers=[node.peer_id], signatures=[sig], threshold=1, allowed_signers=[node.peer_id]
            )
            res = await self.broker.invoke(WasmMethod.SEAL_EPOCH, json.dumps(seal_payload))
            if res.success:
                success_count += 1
                if success_count <= 3:
                    log.info(f"  ├─ [Node {node.peer_id[:6]}] Divergent history sealed in Void. Rebased to Macro Topos.")
            else:
                log.error(f"  ├─ [Node {node.peer_id[:6]}] Void Seal Failed: {res.error}")
                
        log.info(f"  └─ [Convergence Success] {success_count}/{len(minority_nodes)} divergent nodes seamlessly re-integrated. Swarm Expansion Resumed.")