# dphi.workflow.edge
import uuid
import time
import hashlib
from typing import Dict, Any
import httpx

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from phase.epoch.config.builder.phase import NotarySwarm
from phase.epoch.config.dphi import mock_env
from kernel.phase.runner import WebRunner
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter
from receptor.ingress.tracer import E2EConfig, SceneConfig, TargetOp, RouteRegistry

class StartSceneMsg(WorkflowMessage): pass
class OtlpIngressMsg(WorkflowMessage): pass
class D3FiExchangeMsg(WorkflowMessage): pass
class LedgerAppendMsg(WorkflowMessage): pass
class GlobalAnchorMsg(WorkflowMessage): pass

class EdgeWorkflow(Workflow):
    def __init__(self, config: E2EConfig, scene_config: SceneConfig, routes: RouteRegistry, client: httpx.AsyncClient, inject_faults: bool = False):
        super().__init__(name="E2E_SCENE_NET")
        self.config = config
        self.scene_config = scene_config
        self.inject_faults = inject_faults
        self.routes = routes  # DI: rest_app 의존성 제거
        self.runner = WebRunner(config.base_url, client=client)
        self.state_roots: Dict[str, str] = {}
        self.log = get_emitter("workflow.scene_runner")
        self.notary_swarm = NotarySwarm(size=3)

    async def execute(self):
        mode_str = "Negative/Faults" if self.inject_faults else "Golden Path"
        self.log.info(f"\n=== [START] {self.name} ({mode_str}) ===")
        
        if not self.inject_faults:
            self.log.info(f"  └─ Settlement Sink: Chain {mock_env.network.chain_id} (Receptor: {mock_env.contracts.nexus_clearing})")
            self.log.info(f"  └─ Exchange Agents: {mock_env.agents.alpha.did} ⟷ {mock_env.agents.beta.did}")
            self.log.info(f"  └─ Export Notaries: {len(self.notary_swarm.public_keys)} Notary Nodes Loaded")

        self.post_message(StartSceneMsg())
        await self.run()

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

        self.log.info(f"\n[SUCCESS] {self.name} Completed successfully.")
        self.runner.report()
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"\n[HALTED] {self.name} aborted during execution: {msg.msg}")
        self.runner.report()
        return StopMessage(result=False)