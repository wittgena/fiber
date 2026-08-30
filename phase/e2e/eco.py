# fiber.phase.e2e.eco
import time
import json
import asyncio

from fiber.dphi.workflow.scene.anchor import ActorIdentity
from fiber.dphi.infra.eco.actor import (
    EcoProtocolInterface, 
    EcosystemActor, 
    GrantResource,
    SettlementVerifier
)
from fiber.kernel.debug.sandbox import TestScripts

from xphi.arch.eco.protocol import TriadAxis
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.dphi.adapter.state import StateAdapter

log = get_emitter("e2e.eco")

class E2ESettlementVerifier(SettlementVerifier):
    async def verify(self, payload: dict) -> bool:
        if set(payload.get("signers", [])) != set(payload.get("allowed_signers", [])):
            raise RuntimeError("Consensus Failed: Signature verification rejected (Signer mismatch in E2E Mock)")
        return True

class EcoSetupMsg(WorkflowMessage): pass
class EcoIntentFlowMsg(WorkflowMessage): pass
class EcoSettlementFlowMsg(WorkflowMessage): pass
class EcoSubstrateFlowMsg(WorkflowMessage): pass
class EcoNegativeQuotaMsg(WorkflowMessage): pass
class EcoNegativeKineticMsg(WorkflowMessage): pass
class EcoNegativeSettlementMsg(WorkflowMessage): pass

class EcoIntegrationWorkflow(Workflow):
    """@desc: E2E Pipeline Verifier (순수 시나리오 제어 및 통합 테스트)"""
    def __init__(self):
        super().__init__(name="ECO_PIPELINE_VERIFIER")
        self.log = log
        self.mock_verifier = E2ESettlementVerifier()
        self.interface = EcoProtocolInterface(verifier=self.mock_verifier)
        self.aggregator_identity = ActorIdentity("Aggregator_B")
        self.hacker_identity = ActorIdentity("Malicious_Hacker")

    async def start(self) -> bool:
        self.log.info(f"\n{'='*70}\n⚙️ [START] Eco Protocol Verification Pipeline\n{'='*70}")
        self.post_message(EcoSetupMsg())
        await self.run()
        return True

    @step
    async def phase_canonical_intent(self, msg: EcoSetupMsg) -> WorkflowMessage:
        self.log.info("--- [Flow A] Real Intent Resolution ---")
        actor = EcosystemActor("Agent_A", TriadAxis.INTENT, GrantResource.INTENT_QUOTA, 10000)
        await actor.pledge_to_interface(self.interface)

        real_wasm_code = "def resolve_intent(): return 'INTENT_RESOLVED'\nprint(resolve_intent())"
        compute_root = await actor.notary_node.execute_swarm_task(1, 1000, real_wasm_code, self.interface)
        actor.owned_merkle_roots.append(compute_root)

        self.log.info(f"✅ [PASS] Flow A: Real Task Executed -> {compute_root[:16]}...")
        return EcoSettlementFlowMsg()

    @step
    async def phase_canonical_settlement(self, msg: EcoSettlementFlowMsg) -> WorkflowMessage:
        self.log.info("--- [Flow B] Real State Settlement ---")
        actor = EcosystemActor("Aggregator_B", TriadAxis.SETTLEMENT, GrantResource.SETTLEMENT_BOND, 5000)
        await actor.pledge_to_interface(self.interface)

        parity = StateAdapter.build_parity_triplet("eco_topos_1", 101, 777)
        sig_hex = self.aggregator_identity.sign(parity)

        # [FIXED] 필수 메타데이터 파라미터 (parent_nexus_id, self_parent_state 등) 모두 주입
        real_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, 
            parent_nexus_id=0, self_parent_state="0x_genesis", repos={}, cached_states={},
            timestamp=int(time.time() * 1000),
            signers=[self.aggregator_identity.pubkey_hex], signatures=[sig_hex], threshold=1,
            allowed_signers=[self.aggregator_identity.pubkey_hex]
        )

        sealed_hash = await actor.notary_node.aggregate_and_seal_settlement(self.interface, custom_calldata=json.dumps(real_payload))
        actor.owned_merkle_roots.append(sealed_hash)

        self.log.info(f"✅ [PASS] Flow B: Real Ledger Sealed -> {sealed_hash[:16]}...")
        return EcoSubstrateFlowMsg()

    @step
    async def phase_substrate_passiveness(self, msg: EcoSubstrateFlowMsg) -> WorkflowMessage:
        self.log.info("--- [Flow C] Real Substrate Role Isolation ---")
        actor = EcosystemActor("Edge_C", TriadAxis.SUBSTRATE, GrantResource.SUBSTRATE_BANDWIDTH, 1000)
        await actor.pledge_to_interface(self.interface)

        try:
            # 샌드박스의 NET_VIOLATION 에러가 Actuator를 뚫고 파이프라인에 정확히 도달하는지 테스트!
            await actor.notary_node.execute_swarm_task(1, 100, TestScripts.NET_VIOLATION.code, self.interface)
            return ErrorMessage("Substrate breach successful. Isolation failed!")
        except RuntimeError as e:
            self.log.info(f"✅ [PASS] Flow C Blocked correctly: {str(e)}")

        return EcoNegativeQuotaMsg()

    @step
    async def phase_negative_quota(self, msg: EcoNegativeQuotaMsg) -> WorkflowMessage:
        self.log.info("--- [Flow D] Real Quota Exhaustion (Math Trap) ---")
        actor = EcosystemActor("Malicious_D", TriadAxis.INTENT, GrantResource.INTENT_QUOTA, 1000)
        await actor.pledge_to_interface(self.interface)

        try:
            # STANDARD 타임아웃 에러 전파 테스트
            await actor.notary_node.execute_swarm_task(1, 100, TestScripts.INFINITE_LOOP_ATTACK.code, self.interface, tier="STANDARD")
            return ErrorMessage("Quota Exhaustion failed to halt execution!")
        except RuntimeError as e:
            self.log.info(f"✅ [PASS] Flow D Halted correctly: {str(e)}")

        return EcoNegativeKineticMsg()

    @step
    async def phase_negative_kinetic(self, msg: EcoNegativeKineticMsg) -> WorkflowMessage:
        self.log.info("--- [Flow E] Real Kinetic Trap Activation ---")
        actor = EcosystemActor("Hacker_E", TriadAxis.INTENT, GrantResource.INTENT_QUOTA, 1000)
        await actor.pledge_to_interface(self.interface)

        try:
            malware = "import ctypes; ctypes.cast(0, ctypes.py_object)"
            await actor.notary_node.execute_swarm_task(1, 100, malware, self.interface)
            return ErrorMessage("Kinetic Trap Failed. Malware bypassed sandbox.")
        except RuntimeError as e:
            self.log.info(f"✅ [PASS] Flow E Halted correctly: {str(e)}")

        return EcoNegativeSettlementMsg()

    @step
    async def phase_negative_settlement(self, msg: EcoNegativeSettlementMsg) -> WorkflowMessage:
        self.log.info("--- [Flow F] Real DVM Rejection ---")
        actor = EcosystemActor("Hacker_Aggregator", TriadAxis.SETTLEMENT, GrantResource.SETTLEMENT_BOND, 1000)
        await actor.pledge_to_interface(self.interface)

        parity = StateAdapter.build_parity_triplet("eco_topos_1", 101, 777)
        rogue_sig = self.hacker_identity.sign(parity)

        rogue_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity, 
            parent_nexus_id=0, self_parent_state="0x_genesis", repos={}, cached_states={},
            timestamp=int(time.time() * 1000),
            signers=[self.hacker_identity.pubkey_hex], signatures=[rogue_sig], threshold=1,
            allowed_signers=[self.aggregator_identity.pubkey_hex] 
        )

        try:
            await actor.notary_node.aggregate_and_seal_settlement(self.interface, custom_calldata=json.dumps(rogue_payload))
            return ErrorMessage("Settlement Trap failed! Malicious rollup was sealed.")
        except RuntimeError as e:
            self.log.info(f"✅ [PASS] Flow F Halted correctly: {str(e)}")

        self.log.info("\n🎉 ALL PIPELINE BOUNDARIES VERIFIED (Pipeline isolated from Infra).")
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [WORKFLOW HALTED] Verification failure: {msg.msg}")
        return StopMessage(result=False)

def main():
    app = EcoIntegrationWorkflow()
    PhaseReactor.ignite(main_coro_func=app.start)

if __name__ == "__main__":
    main()