# fiber.dev.trace.mcp.ingress
import sys
import asyncio
import uuid
import httpx
import uvicorn
from typing import List
from contextlib import suppress

from fiber.dev.infra.config import PipelineRunner, ManagedTestServer, TestResult, E2EConfig, Phase
from fiber.dphi.edge.payload import create_app, Config
from fiber.dphi.worker.connector import WorkerConnector

from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.state.phase.reactor import PhaseReactor
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.ledger.consensus import KernelLedger
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("tracer.mcp_ingress")

class McpIngressTracer(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="MCP Ingress & Routing Tracer", scope_name="TRACER_INGRESS")
        self.config = config
        self.local_url = f"{self.config.protocol}://{self.config.host}:{self.config.port}"
        
        self.target_worker_id = "ingress-dummy-worker"
        self.dpop_client = DPoPClientGenerator()
        
        self.server = None
        self._server_task = None
        self.connector = None
        self._connector_task = None
        
        self.set_phases([
            Phase("Ingress Infrastructure Ignition", self.phase_ignition),
            Phase("JSON-RPC Routing & State Resolution", self.phase_mcp_routing)
        ])

    def _get_auth_headers(self, target_url: str, method: str = "POST") -> dict:
        nonce = uuid.uuid4().hex
        valid_dpop_proof = self.dpop_client.generate_proof(url=target_url, method=method, nonce=nonce)
        return {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "DPoP": valid_dpop_proof,
            "x-spiffe-id": "spiffe://e2e/test/agent"
        }

    async def phase_ignition(self):
        log.info(f"[{self.scope_name}] Bootstrapping Gateway & Connector...")
        self.tunnel = await TunnelFactory.get_default()
        self.ledger = KernelLedger()
        
        # Gateway 기동
        self.rest_app = create_app(config=Config(), tunnel=self.tunnel, ledger=self.ledger)
        u_config = uvicorn.Config(app=self.rest_app, host=self.config.host, port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        
        async with httpx.AsyncClient() as client:
            for _ in range(20):
                try:
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: break
                except Exception: pass
                await asyncio.sleep(0.2)

        # 초경량 Dummy Worker (JSON-RPC 응답만 반환하는 파이썬 스크립트)
        dummy_cmd = f"""{sys.executable} -c "import sys, json; req=json.loads(sys.stdin.readline()); sys.stdout.write(json.dumps({{'jsonrpc': '2.0', 'id': req.get('id'), 'result': 'TRACED_SUCCESS'}})+'\\n')" """
        
        self.connector = WorkerConnector(
            target_id=self.target_worker_id, 
            execution_target=dummy_cmd, 
            mode="ephemeral", 
            transport_type="stdio"  # NetworkTransport 대신 검증된 Stdio 사용
        )
        self._connector_task = asyncio.create_task(self.connector.run())
        await asyncio.sleep(0.5)

    async def phase_mcp_routing(self):
        """Gateway를 통해 JSON-RPC 인텐트가 워커까지 도달하고 성공적으로 응답(RESOLVED)되는지 추적"""
        payload = {"jsonrpc": "2.0", "id": 888, "method": "tools/call", "params": {"name": "trace_ping"}}
        target_path = f"/v1/mcp-gateway/{self.target_worker_id}/invoke"
        target_url = f"{self.local_url}{target_path}"
        headers = self._get_auth_headers(target_url=target_url, method="POST")
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(target_path, json=payload, headers=headers)
            
            if res.status_code in (401, 403, 423):
                raise RuntimeError(f"MCP Security/Auth Blocked: {res.status_code} - {res.text}")
            
            response_data = res.json()
            if response_data.get("result") == "TRACED_SUCCESS":
                log.info(f"✅ Ingress & Routing successfully resolved via WorkerConnector.")
            else:
                raise RuntimeError(f"Trace failed or unexpected payload: {response_data}")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Tracer: {self.name} ===")
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                try:
                    await phase.action()
                    results.append(TestResult("INGRESS", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("INGRESS", phase.name, False, True))
                    break 
        finally:
            if self.connector:
                self.connector.running = False
                if self._connector_task: 
                    self._connector_task.cancel()
                    with suppress(Exception): await self._connector_task
            if self.server:
                self.server.should_exit = True
                if self._server_task: await self._server_task
            with suppress(Exception): await TunnelFactory.close_all()
            
        return results

def main():
    config = E2EConfig(host="127.0.0.1", port=8360, protocol="http")
    app = McpIngressTracer(config)
    PhaseReactor.ignite(main_coro_func=app.run_pipeline)

if __name__ == "__main__":
    main()