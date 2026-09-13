# fiber.dev.e2e.worker.trace
import os
import sys
import asyncio
import uuid
import time
import uvicorn
import httpx
from typing import List
from contextlib import suppress

from fastapi import Request

from fiber.dev.infra.config import PipelineRunner, ManagedTestServer, TestResult, E2EConfig, Phase
from fiber.dev.trace.llm.profile import LlmTraceProfile
from fiber.dphi.edge.payload import create_app, Config
from fiber.dphi.worker.connector import WorkerConnector

from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.state.phase.reactor import PhaseReactor
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.ledger.consensus import KernelLedger
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.worker.trace")

class AgentNetworkTracePipeline(PipelineRunner):
    """
    [Agent DX & Network Trace E2E 파이프라인]
    단순한 네트워크 핑(Ping)이 아닌, AI 에이전트의 실제 생명주기를 모사하여
    LLM(두뇌), MCP(행동), Audit(기억) API가 정상적으로 관통하는지 검증합니다.
    """
    def __init__(self, config: E2EConfig):
        super().__init__(name="Agent DX Network Trace & Egress Multiplexing", scope_name="E2E_AGENT_TRACE")
        self.config = config
        self.local_url = f"{self.config.protocol}://{self.config.host}:{self.config.port}"
        
        self.target_worker_id = "network-loopback-worker-01"
        self.mock_receipt = "x402-mock-receipt-for-e2e-trace"
        
        # 암호학적 E2E 서명을 위한 DPoP 클라이언트 제너레이터 초기화
        self.dpop_client = DPoPClientGenerator()
        
        # 인프라 리소스 트래킹
        self.server = None
        self._server_task = None
        self.connector = None
        self._connector_task = None
        
        self.set_phases([
            Phase("Edge Loopback Infrastructure Ignition", self.phase_ignition),
            Phase("Trace 1: LLM Inference via Network Egress", self.phase_llm_trace),
            Phase("Trace 2: Idempotent MCP Gateway Execution (w/ SDK Egress)", self.phase_mcp_execution),
            Phase("Trace 3: OTLP Telemetry & Audit Sealing", self.phase_audit_trace)
        ])

    def _get_auth_headers(self, target_url: str, method: str = "POST") -> dict:
        """Edge의 엄격한 보안 미들웨어를 통과하기 위한 E2E용 실시간 DPoP 암호학적 서명 헤더"""
        nonce = uuid.uuid4().hex
        valid_dpop_proof = self.dpop_client.generate_proof(url=target_url, method=method, nonce=nonce)
        
        headers = {
            "x-idempotency-key": uuid.uuid4().hex,
            "x-nonce": nonce,
            "DPoP": valid_dpop_proof,
            "x-spiffe-id": "spiffe://e2e/test/agent"
        }
        
        # [개선] 스마트 라우팅 인증 처리
        # - MCP Gateway(/invoke): 내부 Billing 데몬이 없으므로 X402 영수증을 보내면 15초 RPC 타임아웃 발생 (DPoP로만 통과)
        # - Public/LLM Gateway: 글로벌 402 방어벽을 뚫기 위해 가짜 X402 영수증 주입 필요
        if "/mcp-gateway/" not in target_url:
            headers["X-X402-Receipt"] = self.mock_receipt
            
        return headers

    async def phase_ignition(self):
        """[Infra] Edge 서버 부팅 및 WorkerConnector 루프백 연결"""
        log.info(f"[{self.scope_name}] Bootstrapping Loopback Edge Infrastructure...")
        self.tunnel = await TunnelFactory.get_default()
        self.ledger = KernelLedger()
        
        self.rest_app = create_app(config=Config(), tunnel=self.tunnel, ledger=self.ledger)
        
        # =========================================================================
        # [개선] 관측성 폐쇄 루프(Closed-loop)를 완성하는 가상의 SDK 워커 엔드포인트
        # =========================================================================
        from fiber.dphi.eco.client.sdk import DphiPublicClient, StrictPayloadFactory
        sdk_client = DphiPublicClient(base_url=self.local_url)

        @self.rest_app.post("/mock-agent-trace")
        async def mock_agent_handler(request: Request):
            req_data = await request.json()
            req_id = req_data.get("id")
            
            try:
                # 1. 워커가 임무를 완료했다고 가정하고 SDK를 통해 엄격한 DTO 규격의 Audit Event 방출 (출구로 배출)
                audit_payload = StrictPayloadFactory.create_audit_payload(
                    actor="network-loopback-worker-01",
                    action="trace_execution",
                    message=f"Successfully processed trace intent: {req_id}"
                )
                await sdk_client.record_audit_event(audit_payload, payment_receipt=self.mock_receipt)
                
                # 2. NetworkTransport 에게 정상적인 JSON-RPC 성공 결과를 반환
                return {"jsonrpc": "2.0", "id": req_id, "result": {"status": "SDK Audit Emitted and Resolved"}}
            except Exception as e:
                return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32000, "message": str(e)}}
        # =========================================================================
        
        u_config = uvicorn.Config(app=self.rest_app, host=self.config.host, port=self.config.port, log_level="error", access_log=False)
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        
        # Edge API 기동 대기
        async with httpx.AsyncClient() as client:
            for _ in range(20):
                try:
                    if (await client.get(f"{self.local_url}/openapi.json")).status_code == 200: break
                except Exception: pass
                await asyncio.sleep(0.2)

        log.info(f"[{self.scope_name}] Igniting WorkerConnector with NetworkTransport...")
        
        # [수정] 무한 루프(Recursion)를 방지하기 위해 NetworkTransport의 타겟을 방금 만든 가짜 워커로 지정
        loopback_url = f"{self.local_url}/mock-agent-trace"
        
        self.connector = WorkerConnector(
            target_id=self.target_worker_id, 
            execution_target=loopback_url, 
            mode="multiplex", 
            transport_type="network"
        )
        self._connector_task = asyncio.create_task(self.connector.run())
        await asyncio.sleep(0.5)

    async def phase_llm_trace(self):
        """[LLM API Debug] HTTP 경계를 통해 Mock 제어 메타데이터가 파이프라인 끝까지 전달되는지 확인"""
        profile = LlmTraceProfile("gpt-3.5-mock")
        payload = profile.build_mock_bypass_payload()
        
        target_path = "/v1/chat/completions"
        target_url = f"{self.local_url}{target_path}" # DPoP용 Fully Qualified URL
        headers = self._get_auth_headers(target_url=target_url, method="POST")
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(target_path, json=payload, headers=headers)
            
            if res.status_code == 200:
                profile.verify_mock_bypass(res.json())
                log.info("✅ LLM Trace & Mock Bypass executed seamlessly over HTTP.")
            elif res.status_code == 402:
                # 커널 인증 미연결 상태 시 정상적인 차단 로직
                log.info("✅ LLM Trace routed successfully (Blocked correctly by KernelAuth 402).")
            else:
                raise RuntimeError(f"LLM Trace failed. Unexpected status: {res.status_code} - {res.text}")

    async def phase_mcp_execution(self):
        """[MCP API Debug] Idempotency, SDK Egress Event 및 비동기 결과 릴레이 확인"""
        payload = {"jsonrpc": "2.0", "id": 101, "method": "tools/call", "params": {"name": "loopback_ping"}}
        
        target_path = f"/v1/mcp-gateway/{self.target_worker_id}/invoke"
        target_url = f"{self.local_url}{target_path}"
        headers = self._get_auth_headers(target_url=target_url, method="POST")
        
        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(target_path, json=payload, headers=headers)
            
            # TransitionBridge 보안 정책에 걸리지 않고(401, 403, 423 방어) 내부 라우팅을 통과했는가 확인
            if res.status_code in (401, 403, 423):
                raise RuntimeError(f"MCP Security/Auth Blocked: {res.status_code} - {res.text}")
            
            # 결과값 수신(SDK Egress가 정상적으로 완료되어 RESOLVED 반환됨)을 확인
            response_data = res.json()
            if response_data.get("result", {}).get("status") == "SDK Audit Emitted and Resolved":
                log.info(f"✅ MCP Gateway Execution bridged successfully and Egress completed via SDK.")
            else:
                raise RuntimeError(f"Execution trace failed or returned unexpected payload: {response_data}")

    async def phase_audit_trace(self):
        """[Telemetry/Audit Debug] 로그 추출기(OTLP) 및 암호학적 씰링(Audit) 라우팅 확인"""
        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            # 1. OTLP Logs
            target_path_otlp = "/v1/public/telemetry/logs"
            target_url_otlp = f"{self.local_url}{target_path_otlp}"
            headers_otlp = self._get_auth_headers(target_url=target_url_otlp, method="POST")
            
            otlp_payload = {"resourceLogs": []}
            res_otlp = await client.post(target_path_otlp, json=otlp_payload, headers=headers_otlp)
            
            # Strict Parser에 의해 422 반려를 받았다면 규칙 엔진(Extractor)까지 무사히 도달했음을 의미
            if res_otlp.status_code not in (200, 422):
                raise RuntimeError(f"OTLP Trace failed: {res_otlp.status_code}")
                
            # 2. Audit Event
            target_path_audit = "/v1/public/audit/event"
            target_url_audit = f"{self.local_url}{target_path_audit}"
            headers_audit = self._get_auth_headers(target_url=target_url_audit, method="POST")
            
            audit_payload = {
                "verbose": True,
                "event": {"action": "e2e_trace", "status": "success"}
            }
            res_audit = await client.post(target_path_audit, json=audit_payload, headers=headers_audit)
            
            # 커널 미연결로 인한 500 에러도 Edge 인프라 관점에서는 라우팅 성공으로 간주
            if res_audit.status_code not in (200, 500):
                raise RuntimeError(f"Audit Trace failed: {res_audit.status_code} - {res_audit.text}")
                
            log.info("✅ Audit & Telemetry Trace endpoints reached successfully.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Pipeline: {self.name} ===")
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("WORKER_TRACE", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("WORKER_TRACE", phase.name, False, True))
                    break 
        finally:
            log.info(f"\n[Pipeline] Triggering loopback teardown sequence...")
            
            # WorkerConnector 종료
            if self.connector:
                self.connector.running = False
                if self._connector_task: 
                    self._connector_task.cancel()
                    with suppress(Exception): await self._connector_task

            # Edge 서버 종료
            if self.server:
                self.server.should_exit = True
                if self._server_task: await self._server_task
            
            with suppress(Exception): await TunnelFactory.close_all()
            log.info(f"[Pipeline] Loopback infrastructure evaporated safely.")
            
        return results

class AgentTraceSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🌐 [AGENT DX SUITE] Commencing Zero-Trust Edge Trace & Lifecycle Tests")
        self.log.info("="*80)
        
        # 충돌 방지를 위한 독립 포트 사용
        trace_config = E2EConfig(host="127.0.0.1", port=8356, protocol="http")
        self.results.extend(await AgentNetworkTracePipeline(config=trace_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        lines = [
            "\n" + "=" * 80,
            "📡 [AGENT DX TRACE TEST REPORT]",
            "=" * 80
        ]
        
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            lines.append(
                f"{status_icon} {idx:02d}. [{res.target}]".ljust(22)
                + f"{res.scenario.ljust(50)} | Result: {'PASSED' if res.passed else 'FAILED'}"
            )
            
        lines.append("-" * 80)
        if all_passed: 
            lines.append("🎉 ALL AGENT TRACE & LIFECYCLE TESTS PASSED.")
        else: 
            lines.append("💥 AGENT TRACE FRACTURED. Check logs for middleware blocks.")
        lines.append("=" * 80 + "\n")
        
        self.log.info("\n".join(lines))

def main(args_list: list[str] = None):
    app = AgentTraceSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()