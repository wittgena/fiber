# dphi.phase.workflow.edge
## @lineage: dphi.epoch.workflow.edge
## @lineage: dphi.workflow.edge
import hashlib
import time
import uuid
from typing import Callable, Dict, Optional

import httpx

# --- Core Framework & Adapters ---
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from ext.client.http import VerifiedHttpClient
from kernel.dphi.adapter.state import StateAdapter
from kernel.phase.runner import WebRunner

# --- Configurations & Envs ---
from dphi.adapter.config.client import NotarySwarm
from dphi.adapter.config.dphi import mock_env

# --- Ingress & Observability ---
from watcher.tracer.edge import E2EConfig, RouteRegistry, SceneConfig, TargetOp
from watcher.plane.emitter import get_emitter


# =========================================================================
# Workflow Messages
# =========================================================================
class StartSceneMsg(WorkflowMessage): pass
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class GlobalAnchorMsg(WorkflowMessage): pass


# =========================================================================
# Edge E2E Workflow Orchestrator
# =========================================================================
class EdgeWorkflow(Workflow):
    def __init__(
        self, 
        config: E2EConfig, 
        scene_config: SceneConfig, 
        routes: RouteRegistry, 
        client: httpx.AsyncClient, 
        inject_faults: bool = False,
        attestation_injector: Optional[Callable[[httpx.Response], httpx.Response]] = None
    ):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.scene_config = scene_config
        self.inject_faults = inject_faults
        self.routes = routes  # DI: rest_app 의존성 제거
        self.runner = WebRunner(config.base_url, client=client)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")
        self.notary_swarm = NotarySwarm(size=3)
        
        # 🔥 추가됨: 응답 헤더 서명(First-Party Oracle) 훼손 테스트용 카오스 인젝터
        self.attestation_injector = attestation_injector

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        if self.attestation_injector:
            mode_str = "Attestation Rejection (Tampered Headers)"
            
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
        
        if not self.inject_faults:
            self.log.info(f"  └─ Settlement Sink: Chain {mock_env.network.chain_id} (Receptor: {mock_env.contracts.nexus_clearing})")
            self.log.info(f"  └─ Exchange Agents: {mock_env.agents.alpha.did} ⟷ {mock_env.agents.beta.did}")
            self.log.info(f"  └─ Export Notaries: {len(self.notary_swarm.public_keys)} Notary Nodes Loaded")

        self.post_message(StartSceneMsg())
        await self.run()

    def _verify_attestation(self, response: httpx.Response, request_path: str) -> Optional[ErrorMessage]:
        """
        [Helper] Golden Path 200 OK 응답에 대해 서버(First-Party Oracle)의 
        암호학적 서명(X-Dphi-Signature)이 올바른지 검증합니다.
        """
        # 에러 주입 테스트(Negative Path 422 응답 등)이거나 200 OK가 아닌 경우 서명 검증 생략
        if response.status_code != 200:
            return None

        # 🔥 카오스 테스트: 고의로 헤더 서명 훼손
        if self.attestation_injector:
            self.log.warning(f"  └─ 👾 Injecting Chaos: Tampering Attestation Headers for {request_path}")
            response = self.attestation_injector(response)
            
        try:
            self.log.info(f"  └─ 🔍 Verifying First-Party Attestation for {request_path}...")
            verifier = VerifiedHttpClient(client=self.runner.client)
            verifier._verify_header_proof(response, request_path)
            self.log.info("  └─ ✅ Attestation Signature Verified Successfully!")
            return None
        except Exception as e:
            self.log.error(f"  └─ 🚨 Attestation Failed: {e}")
            self.runner.fail_count += 1
            return ErrorMessage(f"Attestation Proof Verification Failed: {e}")

    @step
    async def phase_head_smoke(self, msg: StartSceneMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 1] API Head & MCP Connectivity Sweep ---")
        res = await self.runner.client.get(f"{self.runner.base_url}/openapi.json")
        if res.status_code == 200:
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("API Head is unreachable")

        mcp_res = await self.runner.client.post(f"{self.runner.base_url}/mcp/sse")
        if mcp_res.status_code in [405, 400]: 
            self.runner.success_count += 1
        else:
            self.runner.fail_count += 1
            return ErrorMessage("MCP Server unreachable")
            
        return OtlpIngressMsg()

    @step
    async def phase_otlp_ingress(self, msg: OtlpIngressMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 2] OTLP Telemetry Ingress & WASM Seal ---")
        path = self.routes.url_for(TargetOp.OTLP_INGRESS)
        
        payload = self.scene_config.otlp_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"OTLP Ingress ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"OTLP Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        # 🔥 서명 검증 적용
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
            
        if not self.inject_faults:
            self.state_roots["otlp_root"] = res.headers.get("x-edge-content-hash", "0x_default_otlp_hash")
            
        return D3FiExchangeMsg()

    @step
    async def phase_d3fi_exchange(self, msg: D3FiExchangeMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 3] D3Fi P2P Trade & Settlement (ExchangeNet) ---")
        path = self.routes.url_for(TargetOp.TRADE_INGRESS)
        
        payload = self.scene_config.trade_builder(self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"D3Fi Trade Ingress ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
        
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
             return ErrorMessage(f"D3Fi Ingress Check Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
             
        # 🔥 서명 검증 적용
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
             
        if not self.inject_faults:
            self.state_roots["exchange_root"] = f"d3fi_state_hash_{uuid.uuid4().hex[:8]}"
            
        return LedgerAppendMsg()

    @step
    async def phase_ledger_append(self, msg: LedgerAppendMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 4] Immutable Ledger Stream Append ---")
        path = self.routes.url_for(TargetOp.LEDGER_APPEND)
        
        exchange_root = self.state_roots.get("exchange_root", "0x00")
        payload = self.scene_config.ledger_builder(exchange_root, self.inject_faults)
        expected_status = 422 if self.inject_faults else 200
        test_desc = f"Ledger Append ({'Membrane Strict Block Test' if self.inject_faults else 'Golden Path'})"
            
        res = await self.runner._run_api_case(test_desc, "POST", path, payload, expected_status)
        if not res or res.status_code != expected_status:
            return ErrorMessage(f"Ledger Append Failed: Expected {expected_status}, Got {res.status_code if res else 'None'}")
            
        # 🔥 서명 검증 적용
        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err
            
        if self.inject_faults:
            return GlobalAnchorMsg()

        self.state_roots["ledger_root"] = res.json().get("result", {}).get("hash", "0x_default_ledger_hash")
        return GlobalAnchorMsg()

    @step
    async def phase_global_anchor(self, msg: GlobalAnchorMsg) -> WorkflowMessage:
        self.log.info("\n--- [Phase 5] Export Plug: Attest & Submit Anchor ---")
        if self.inject_faults:
            self.log.info(f"\n[SUCCESS] {self.name} Fault-Injection Scenario Completed.")
            self.runner.report()
            return StopMessage(result=True)
            
        required_keys = ["otlp_root", "exchange_root", "ledger_root"]
        if not all(k in self.state_roots for k in required_keys):
            return ErrorMessage("Cannot anchor: Sub-state roots are missing.")
        
        parity_triplet = {"topos_id": f"epoch_{time.strftime('%Y%m%d')}_batch_01", "nexus_id": 1, "phase_id": 1}
        receptor_id = mock_env.contracts.nexus_clearing
        parent_state_hash = "0x0000000000000000000000000000000000000000000000000000000000000000"
        
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, 
            parent_nexus_id=0, 
            parent_commit_id=parent_state_hash, 
            repos=self.state_roots, 
            cached_states={}
        )
        
        commit_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(anchor_commit)).hexdigest().encode('utf-8')
        signatures = self.notary_swarm.attest_payload(commit_hash)
        
        payload = {
            "receptor_id": receptor_id,  
            "proposed_parity": parity_triplet,
            "parent_nexus_id": 0,
            "self_parent_state": parent_state_hash,
            "repos": self.state_roots,
            "signers": self.notary_swarm.public_keys, 
            "signatures": signatures,
            "timestamp": int(time.time() * 1000)
        }
        
        path = self.routes.url_for(TargetOp.ANCHOR_SEAL)
        res = await self.runner._run_api_case("Export Attested Anchor", "POST", path, payload, 200)
        
        if not res or res.status_code != 200:
            return ErrorMessage("Global Anchor Export Failed")

        attest_err = self._verify_attestation(res, path)
        if attest_err: return attest_err

        self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)