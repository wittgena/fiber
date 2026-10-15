# fiber.dev.e2e.gateway.worker.finops
## @lineage: fiber.dev.e2e.worker.finops
import sys
import uuid
import json
import asyncio
import httpx

from fiber.infra.e2e.config import Phase, E2EConfig
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log
from fiber.gateway.mcp.connector import MCPServerConnector
from xphi.state.phase.reactor import PhaseReactor

from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
import fiber.dev.worker.finops as worker_finops

class FinopsIntegrationPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config, 
            name="FinOps System Integration Suite", 
            scope_name="FINOPS_POC_SUITE"
        )
        self.finops_id = "finops-poc-01"
        self.dpop_client = DPoPClientGenerator(key_size=2048) # 보안 헤더 생성기

        self.set_phases([
            Phase("Phase 1: FinOps JWT Issuance (Track B)", self.phase_issue_jwt),
            Phase("Phase 2: Pub/Sub Kill Switch Trigger (Track A)", self.phase_trigger_kill_switch),
        ])

    async def setup_workers(self):
        finops_cmd = f"{sys.executable} -m {worker_finops.__name__}"
        self.connectors.append(
            MCPServerConnector(target_id=self.finops_id, execution_target=finops_cmd, mode="multiplex")
        )

    # =====================================================================
    # Test Phases
    # =====================================================================
    
    async def phase_issue_jwt(self):
        payload = {
            "jsonrpc": "2.0", 
            "id": 100, 
            "method": "tools/call", 
            "params": {"name": "issue_jwt", "arguments": {"tenant_id": "accounting_dept", "budget_usd": 50.0}}
        }
        
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finops_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": nonce, "DPoP": dpop_proof}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            
        if res.status_code != 200: raise RuntimeError(f"Expected 200, got {res.status_code}")
            
        data = res.json()
        result_text = data.get("result", {}).get("content", [])[0].get("text", "{}")
        parsed_result = json.loads(result_text)
        
        jwt_token = parsed_result.get("jwt")
        if not jwt_token: raise RuntimeError("Missing JWT in response")
        log.info(f"  └─ Successfully received PoC JWT: {jwt_token[:30]}...")

    async def phase_trigger_kill_switch(self):
        payload = {
            "jsonrpc": "2.0", 
            "id": 200, 
            "method": "tools/call", 
            "params": {"name": "trigger_kill", "arguments": {"tenant_id": "accounting_dept"}}
        }
        
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.finops_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        headers = {"x-idempotency-key": uuid.uuid4().hex, "x-nonce": nonce, "DPoP": dpop_proof}
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            
        if res.status_code != 200: raise RuntimeError(f"Expected 200, got {res.status_code}")
            
        data = res.json()
        result_text = data.get("result", {}).get("content", [])[0].get("text", "{}")
        parsed_result = json.loads(result_text)
        
        if parsed_result.get("status") != "KILL_SIGNAL_SENT":
            raise RuntimeError("Kill switch failed.")
        log.info("  └─ Successfully emitted Pub/Sub Kill Switch signal.")

class FinopsSuiteRunner:
    def __init__(self):
        self.log = log

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        results = await FinopsIntegrationPipeline(config=net_config).run_pipeline()
        
        if all(r.passed for r in results):
            self.log.info("🎉 FINOPS SYSTEM POC VALIDATED SUCCESSFULLY.")
        else:
            self.log.critical("💥 FINOPS POC FAILURE DETECTED.")

def main(args_list: list[str] = None):
    app = FinopsSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()