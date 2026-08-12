# phase.epoch.scene.cert
import time
import asyncio
import json
import math
from typing import Any, List

from dphi.sandbox.runner import SandboxRunner
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.method import DphiMethod
from watcher.plane.emitter import get_emitter
from phase.epoch.scene.anchor import ActorIdentity # 재사용 가정

log = get_emitter("scene.cert")

class CertProofScene(SandboxRunner):
    """
    @desc: CI/로컬 환경에서 증명 가능한 극한의 한계(Edge) 및 결정론적 무결성 4대 마스터 씬
           (대규모 병렬 부하 및 워커 교차 오염 테스트는 entry.attach의 concurrency 씬으로 위임됨)
    """
    def __init__(self, broker: Any):
        super().__init__(broker)
        # Byzantine 테스트를 위한 Actor 초기화
        self.system = ActorIdentity("System_Core")
        self.node_a = ActorIdentity("Validator_A")
        self.node_b = ActorIdentity("Validator_B")
        self.node_rogue = ActorIdentity("Rogue_Node")

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🚀 [START] Master System Certification Pipeline (4 Core Proofs)")
        log.info("=======================================================\n")

        await self._set_worker_policy("SYSTEM")

        # Domain A: Resource & Payload Boundary
        await self._proof_gas_boundary_trap()
        await self._proof_deserialization_bomb()

        # Domain B: Determinism & Consensus Integrity
        await self._proof_floating_point_determinism()
        await self._proof_byzantine_fault_tolerance()

        # Report Generation (상속받은 메서드)
        self.report()

    # =========================================================================
    # [Domain A] 극한 자원 통제 및 페이로드 한계 증명
    # =========================================================================
    async def _proof_gas_boundary_trap(self):
        log.info("\n--- [Proof 1] Gas Boundary & Memory Trap ---")
        # Fuel을 STANDARD로 낮춰 자원 고갈을 유도
        await self._set_worker_policy("STANDARD")
        
        # [수정됨] try-except를 제거하여, 샌드박스가 자원 초과를 감지하고 프로세스를 
        # 하드 킬(Hard-kill) 하도록 유도합니다.
        toxic_code = """
state_mock = []
while True:
    state_mock.append('A' * 1024)
"""
        res = await self.broker.execute(code=toxic_code)
        
        # 샌드박스가 터져서 res.success가 False가 되어야 성공적인 방어입니다.
        if not getattr(res, 'success', True):
            self._record_success(0, f"Fuel/Memory Trap triggered successfully. Error: {getattr(res, 'error', 'Trap/OOM')}")
        else:
            self._record_fail(0, "System failed to trap infinite resource allocation.", "Gas Boundary")
            
        await self._set_worker_policy("SYSTEM")

    async def _proof_deserialization_bomb(self):
        log.info("\n--- [Proof 2] Deserialization Bomb Defense ---")
        # 악의적으로 중첩된 거대 JSON 딕셔너리 생성 (Zip Bomb 유사)
        deep_dict = {"level_0": "payload"}
        for i in range(100):
            deep_dict = {f"level_{i+1}": deep_dict}
        
        # 패킷 검증 엔드포인트에 투척
        res = await self.broker.invoke(DphiMethod.VERIFY_PACKET.value, deep_dict, timeout=3.0)
        
        if not getattr(res, 'success', True):
            self._record_success(0, "System gracefully rejected JSON bomb without crashing.")
        else:
            self._record_fail(0, "System parsed the bomb and survived, but should have rejected it as oversized/malformed.", "Deserialization Bomb")

    # =========================================================================
    # [Domain B] 로직 결정론 및 비잔틴 합의 무결성 증명
    # =========================================================================
    async def _proof_floating_point_determinism(self):
        log.info("\n--- [Proof 3] Floating-Point Determinism ---")
        # 아키텍처(ARM/x86) 간 부동소수점 오차 발생 가능성이 있는 난해한 연산
        fp_code = """
import math
# 비선형 연산 후 15자리 정밀도로 출력
val = sum(math.sin(i * 0.1) * math.cos(i * 0.05) for i in range(1000))
print(f"{val:.15f}")
"""
        results = []
        for _ in range(3):
            res = await self.broker.execute(code=fp_code)
            if res.success:
                results.append(res.output.strip())

        # 모든 샌드박스의 실행 결과가 단 1비트의 오차도 없이 동일해야 함
        if len(results) == 3 and len(set(results)) == 1:
            self._record_success(0, f"Perfect Determinism achieved. Result: {results[0]}")
        else:
            self._record_fail(0, f"Divergence detected in floating-point operations: {results}", "Determinism")

    async def _proof_byzantine_fault_tolerance(self):
        log.info("\n--- [Proof 4] Byzantine Fault & Quarantine ---")
        
        # 1. 정상적인 에포크 데이터 구성
        parity = StateAdapter.build_parity_triplet("topos_cert", 111, 222)
        valid_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_A"}, {})
        
        # 2. 로그(Rogue) 노드가 데이터를 몰래 변조한 커밋 생성
        rogue_commit = StateAdapter.build_anchor_commit(parity, 0, "genesis", {"repo": "hash_B_MALICIOUS"}, {})
        
        # 3. 서명 수집 (2개는 정상, 1개는 조작된 데이터에 서명)
        signatures = [
            self.node_a.sign(valid_commit),
            self.node_b.sign(valid_commit),
            self.node_rogue.sign(rogue_commit) # 변조된 페이로드에 대한 서명 제출
        ]
        
        # 4. 봉인 시도 (Threshold는 2)
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, parent_nexus_id=0, self_parent_state="genesis",
            repos={"repo": "hash_A"}, cached_states={}, timestamp=int(time.time()),
            signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex],
            signatures=signatures, threshold=2, 
            allowed_signers=[self.node_a.pubkey_hex, self.node_b.pubkey_hex, self.node_rogue.pubkey_hex]
        )
        
        # 브로커(엔드포인트)는 rogue_commit의 서명 불일치를 감지하여 Quarantine 시키고, 
        # 남은 2개의 유효 서명이 Threshold(2)를 만족하므로 합의를 성공시켜야 합니다.
        await self._run_case(
            title="Byzantine Defense: Quarantine rogue signature & accept 2-of-3 threshold",
            target_func=DphiMethod.SEAL_EPOCH.value,
            payload=payload,
            expected_success=True 
        )