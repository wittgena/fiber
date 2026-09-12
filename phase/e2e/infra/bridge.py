# fiber.phase.e2e.infra.bridge
import os
import sys
import time
import json
import asyncio
from typing import Any, List, Dict

import uvicorn

from fiber.dphi.eco.client.rpc import InternalRpcClient
from fiber.phase.e2e.infra.config import (
    PipelineRunner, 
    ManagedTestServer, 
    TestResult, 
    E2EConfig
)

import fiber.dphi.rpc.registry as rpc_registry
from fiber.dphi.daemon.rpc import RpcWorkerDaemon
from fiber.dphi.edge.payload import create_app, Config

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("infra.e2e.bridge")

class MockStream:
    def __init__(self, handle_id: str, target: str, fuel: int):
        self.id = handle_id
        self.metadata = {"target": target, "fuel_locked": fuel, "risk_score": 0}

class MockLedger:
    def __init__(self):
        self.states = {}
        self.yield_timestamps = {}
        self.stale_timeout = 60.0

    async def query_state(self, handle_id: str):
        return self.states.get(handle_id)

    def set_resolved(self, handle_id: str, payload: dict):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "RESOLVED"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps.pop(handle_id, None)

    def set_faulted(self, handle_id: str, error_detail: str):
        class DummyState:
            action = "dphi.transition.resolve"
            metadata = {"status": "FAULTED", "error_detail": error_detail}
            def __init__(self): self.payload = None
        self.states[handle_id] = DummyState()
        self.yield_timestamps.pop(handle_id, None)

    def set_yielded(self, handle_id: str, payload: dict):
        class DummyState:
            action = "dphi.transition.yield"
            metadata = {"status": "YIELD"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps[handle_id] = time.time()

    async def query_stale_streams(self, current_time: float, thresholds: dict) -> List[MockStream]:
        stale = []
        for hid, ts in list(self.yield_timestamps.items()):
            if current_time - ts > self.stale_timeout:
                stale.append(MockStream(hid, "legacy-01", 100))
        return stale

    async def force_transition(self, handle_id: str, new_action: str, metadata: dict):
        class DummyState:
            action = new_action
            def __init__(self, md): self.metadata = md
            self.payload = {}
        self.states[handle_id] = DummyState(metadata)
        self.yield_timestamps.pop(handle_id, None)


class BaseBridgePipeline(PipelineRunner):
    """
    Gateway E2E 테스트를 위한 공통 파이프라인.
    공통 인프라(RPC, REST, Daemon)의 라이프사이클 관리를 책임집니다.
    """
    def __init__(self, config: E2EConfig, name: str, scope_name: str):
        super().__init__(name=name, scope_name=scope_name)
        self.config = config
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        
        # 공통 의존성
        self.mock_ledger = MockLedger()
        self.rpc = None
        
        # 라이프사이클 컨테이너
        self.connectors = []
        self._connector_tasks = []
        self.server = None
        self._server_task = None
        self.worker_daemon = None
        self._worker_task = None
        
        # 상태 추적용
        self.captured_handle_ids: Dict[str, str] = {}

    # -----------------------------------------------------------------
    # Template Methods (자식 클래스에서 오버라이딩)
    # -----------------------------------------------------------------
    async def setup_custom_context(self):
        """보안 컨텍스트 설정 등 자식 클래스에서 필요한 초기화 로직"""
        pass
        
    async def setup_workers(self):
        """자식 클래스에서 필요한 WorkerConnector 인스턴스를 초기화하고 self.connectors에 등록"""
        pass
        
    async def teardown_custom(self):
        """자식 클래스 전용 티어다운 로직 (예: Sentinel 종료)"""
        pass

    # -----------------------------------------------------------------
    # Core Infrastructure Setup
    # -----------------------------------------------------------------
    async def _setup_rpc_bus(self):
        self.rpc = InternalRpcClient()
        tunnel = await TunnelFactory.get_default()

        # --- E2E Mock Functions (상태 추적 관련만 남김) ---
        async def mock_mcp_state_query(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            state = await self.mock_ledger.query_state(handle_id)
            if not state: 
                return {"exists": False}
            
            meta = getattr(state, "metadata", {})
            return {
                "exists": True, 
                "status": meta.get("status"),
                "error_detail": meta.get("error_detail", ""),
                "executable_payload": getattr(state, "payload", {}),
                "action": getattr(state, "action", meta.get("action", ""))
            }

        async def mock_mcp_state_pending_seal(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            return {"success": True, "handle_id": handle_id}

        async def mock_resolve_state(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            status = params.get("status")
            payload = params.get("executable_payload", {})
            error_detail = params.get("error_detail", "Unknown Fault")

            self.captured_handle_ids["latest"] = handle_id

            if status == "RESOLVED": self.mock_ledger.set_resolved(handle_id, payload)
            elif status == "YIELD": self.mock_ledger.set_yielded(handle_id, payload)
            else: self.mock_ledger.set_faulted(handle_id, error_detail)

            # 브릿지로 결과 브로드캐스트
            await tunnel.publish(f"mcp.intent.reply.{handle_id}", json.dumps({
                "status": status,
                "executable_payload": payload,
                "error_detail": error_detail
            }))
            
            return {"success": True, "status": status}

        original_builder = rpc_registry.build_internal_rpc_registry
        
        def bridge_mock_registry_builder(*args, **kwargs):
            # 먼저 원본 레지스트리를 얻어옵니다. 
            # (이 안에는 분리된 진짜 receipt.py 핸들러가 포함되어 있습니다)
            registry = original_builder(*args, **kwargs)
            
            # 그 위에 E2E 상태 머신 Mock들만 덮어씌웁니다.
            # [수정됨] mock_billing_receipt_validate 덮어쓰기 제거 -> 원본 통과
            registry["mcp.state.query"] = mock_mcp_state_query
            registry["mcp.state.pending.seal"] = mock_mcp_state_pending_seal
            registry["mcp.bridge.resolve_state"] = mock_resolve_state
            
            return registry
            
        # 레지스트리 빌더 팩토리 덮어쓰기
        rpc_registry.build_internal_rpc_registry = bridge_mock_registry_builder

        # [핵심 방어] 데몬 모듈 자체에 패치된 빌더를 명시적으로 주입하여,
        # 데몬이 원본이 아닌 완벽하게 조합된 라우팅 테이블(Mock Ledger + Real Receipt)을 사용하도록 강제합니다.
        import fiber.dphi.daemon.rpc as daemon_module
        daemon_module.build_internal_rpc_registry = bridge_mock_registry_builder

        # Daemon 컨텍스트 및 워커 기동
        class MockDaemonCtx: broker = None; store = None
        self.worker_daemon = RpcWorkerDaemon(ctx=MockDaemonCtx())
        self.worker_daemon.running = True  
        self._worker_task = asyncio.create_task(self.worker_daemon.run())
        
        log.info("[BasePipeline] RPC Bus & Core Daemons Ignited (Mock Ledger & Real Validators Aligned).")

    async def _setup_rest_edge(self):
        tunnel = await TunnelFactory.get_default()
        rest_app = create_app(
            config=Config(wasm_timeout=5.0),
            tunnel=tunnel,            
            ledger=self.mock_ledger   
        )
        u_config = uvicorn.Config(
            app=rest_app, host="127.0.0.1", port=self.config.port, 
            log_level="error", access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        log.info("[BasePipeline] REST Edge Bootstrapped.")

    # -----------------------------------------------------------------
    # Standard Execution Flow
    # -----------------------------------------------------------------
    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Suite: {self.name} ===")
        
        # 1. 인프라 부트스트랩 (자식 클래스의 setup_custom_context가 먼저 실행되어 Validator 패치가 먼저 이뤄집니다)
        await self.setup_custom_context()
        await self._setup_rpc_bus()  # 여기서 Bridge의 Mock 패치가 덧씌워집니다.
        await self._setup_rest_edge()
        await self.setup_workers()
        await asyncio.sleep(1.0)

        # 2. 커넥터(워커) 기동
        log.info(f"[BasePipeline] Igniting {len(self.connectors)} Connectors...")
        for conn in self.connectors:
            self._connector_tasks.append(asyncio.create_task(conn.run()))
        await asyncio.sleep(2.0)

        # 3. 페이즈 실행
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult(self.scope_name, phase.name, True, True))
                    log.info("  └─ Status: PASSED ✅")
                except Exception as e:
                    log.error(f"  └─ Halted: {str(e)} ❌", exc_info=True)
                    results.append(TestResult(self.scope_name, phase.name, False, True))
        finally:
            await self._teardown_all()

        return results

    async def _teardown_all(self):
        log.info(f"\n[BasePipeline] Teardown sequence initiated...")
        
        # 자식 클래스 커스텀 티어다운
        await self.teardown_custom()

        # 커넥터 종료
        for conn in self.connectors: conn.running = False
        for task in self._connector_tasks: task.cancel()

        # 백그라운드 데몬 종료
        if self.worker_daemon: self.worker_daemon.running = False
        if self._worker_task: self._worker_task.cancel()

        # 서버 종료
        if self.server: self.server.should_exit = True
        if self._server_task: await self._server_task
            
        log.info(f"[BasePipeline] Teardown complete.")