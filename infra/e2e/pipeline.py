# fiber.infra.e2e.pipeline
import os
import sys
import time
import json
import asyncio
from typing import Any, List, Dict, Optional, Callable

import uvicorn

from fiber.gateway.rest.payload import create_app, Config

from xphi.arch.bound.client.rpc import InternalRpcClient
from fiber.infra.e2e.config import (
    PipelineRunner, 
    ManagedTestServer, 
    TestResult, 
    E2EConfig
)

from xphi.arch.bound.client.rpc import InternalRpcClient
import xphi.node.rpc.registry as rpc_registry
from xphi.node.ops.daemon.rpc import RpcWorkerDaemon
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("infra.e2e.bridge")


class MockStream:
    def __init__(self, handle_id: str, target: str, fuel: int):
        self.id = handle_id
        self.metadata = {"target": target, "fuel_locked": fuel, "risk_score": 0}


class MockPhaseStore:
    def __init__(self):
        self.states = {}
        self.yield_timestamps = {}
        self.stale_timeout = 60.0

    async def query_state(self, handle_id: str):
        return self.states.get(handle_id)

    def set_pending(self, handle_id: str, payload: dict, target_server_id: str = "legacy-01"):
        class DummyState:
            action = "dphi.transition.pending"
            metadata = {"status": "PENDING", "target": target_server_id}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)

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
        effective_ttl = min(
            self.stale_timeout,
            thresholds.get("dphi.transition.pending", self.stale_timeout)
        )
        for hid, ts in list(self.yield_timestamps.items()):
            if current_time - ts > effective_ttl:
                stale.append(MockStream(hid, "legacy-01", 100))
        return stale

    async def force_transition(self, handle_id: str, new_action: str, metadata: dict):
        class DummyState:
            action = new_action
            def __init__(self, md):
                self.metadata = md
                self.payload = {}
        self.states[handle_id] = DummyState(metadata)
        self.yield_timestamps.pop(handle_id, None)


class BaseBridgePipeline(PipelineRunner):
    """Gateway E2E 테스트를 위한 공통 파이프라인 - 공통 인프라(RPC, REST, Daemon)의 라이프사이클 관리"""
    def __init__(self, config: E2EConfig, name: str, scope_name: str):
        super().__init__(name=name, scope_name=scope_name)
        self.config = config
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"
        
        self.mock_store = MockPhaseStore()
        self.rpc: Optional[InternalRpcClient] = None
        
        # 라이프사이클 컨테이너
        self.connectors = []
        self._connector_tasks = []
        self.server = None
        self._server_task = None
        self.worker_daemon = None
        self._worker_task = None
        
        # 몽키패치 복원용 레퍼런스 (Zero Side-Effect 보장)
        self._original_rpc_call: Optional[Callable] = None
        self._original_registry_builder: Optional[Callable] = None

        # 상태 추적
        self.captured_handle_ids: Dict[str, str] = {}

    # Template Methods
    async def setup_custom_context(self):
        """보안 컨텍스트 설정 등 자식 클래스에서 필요한 초기화 로직"""
        pass
        
    async def setup_workers(self):
        """자식 클래스에서 필요한 WorkerConnector 인스턴스를 초기화하고 self.connectors에 등록"""
        pass
        
    async def teardown_custom(self):
        """자식 클래스 전용 티어다운 로직 (예: Sentinel 종료)"""
        pass

    # Infra Setup
    async def _setup_rpc_bus(self):
        self.rpc = InternalRpcClient()
        tunnel = await TunnelFactory.get_default()

        async def mock_mcp_state_query(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            state = await self.mock_store.query_state(handle_id)
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
            payload = params.get("payload", {})
            target_server_id = params.get("target_server_id", "legacy-01")
            if handle_id:
                self.captured_handle_ids["latest"] = handle_id
                self.mock_store.set_pending(handle_id, payload, target_server_id)
            return {"success": True, "handle_id": handle_id}

        async def mock_resolve_state(params: dict, ctx: Any = None):
            handle_id = params.get("handle_id")
            status = params.get("status")
            payload = params.get("executable_payload", {})
            error_detail = params.get("error_detail", "Unknown Fault")

            self.captured_handle_ids["latest"] = handle_id

            if status == "RESOLVED":
                self.mock_store.set_resolved(handle_id, payload)
            elif status == "YIELD":
                self.mock_store.set_yielded(handle_id, payload)
            else:
                self.mock_store.set_faulted(handle_id, error_detail)

            # 브릿지로 결과 브로드캐스트
            await tunnel.publish(f"mcp.intent.reply.{handle_id}", json.dumps({
                "status": status,
                "executable_payload": payload,
                "error_detail": error_detail
            }))
            
            return {"success": True, "status": status}

        # [핵심 아키텍처 정렬: Hybrid Split-Routing]
        # 1. E2E 테스트 전용 임시 트랜잭션 상태(mcp.state.*, mcp.bridge.resolve_state)는
        #    백그라운드에 부트된 실제 노드의 영구 원장(Anchor Ledger)을 오염시키거나
        #    외부 노드에 가로채여 Split-Brain(Current status: None)을 유발하지 않도록 MockPhaseStore로 직결합니다.
        # 2. 반면 validate.attest, fuel.receipt.validate, compute.margin.calculate 등
        #    실제 보안/과금/비즈니스 RPC는 100% Redis Bus(internal.rpc.queue)를 그대로 통과하여
        #    실제 부트된 노드(또는 로컬 워커)의 실전 파이프라인을 검증합니다.
        ephemeral_state_handlers = {
            "mcp.state.query": mock_mcp_state_query,
            "mcp.state.pending.seal": mock_mcp_state_pending_seal,
            "mcp.bridge.resolve_state": mock_resolve_state,
        }

        self._original_rpc_call = InternalRpcClient.call
        orig_call_ref = self._original_rpc_call

        async def _hybrid_live_node_rpc_call(client_self, method: Any, params: dict = None, *args, **kwargs):
            method_key = getattr(method, "value", str(method))
            if method_key in ephemeral_state_handlers:
                return await ephemeral_state_handlers[method_key](params or {})
            return await orig_call_ref(client_self, method, params, *args, **kwargs)

        InternalRpcClient.call = _hybrid_live_node_rpc_call

        self._original_registry_builder = rpc_registry.build_internal_rpc_registry
        orig_builder_ref = self._original_registry_builder
        
        def bridge_mock_registry_builder(*args, **kwargs):
            registry = orig_builder_ref(*args, **kwargs)
            registry.update(ephemeral_state_handlers)
            return registry
            
        rpc_registry.build_internal_rpc_registry = bridge_mock_registry_builder
        import fiber.infra.daemon.rpc as daemon_module
        daemon_module.build_internal_rpc_registry = bridge_mock_registry_builder

        class MockDaemonCtx:
            broker = None
            store = None

        self.worker_daemon = RpcWorkerDaemon(ctx=MockDaemonCtx())
        self.worker_daemon.running = True  
        self._worker_task = asyncio.create_task(self.worker_daemon.run())
        
        log.info("[BasePipeline] RPC Bus & Core Daemons Ignited (Mock PhaseStore & Real Validators Aligned).")

    async def _setup_rest_edge(self):
        tunnel = await TunnelFactory.get_default()
        rest_app = create_app(
            config=Config(wasm_timeout=5.0),
            tunnel=tunnel,            
            phase_store=self.mock_store   
        )
        u_config = uvicorn.Config(
            app=rest_app, host="127.0.0.1", port=self.config.port, 
            log_level="error", access_log=False
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        log.info("[BasePipeline] REST Edge Bootstrapped.")

    # Standard Execution Flow
    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Suite: {self.name} ===")
        
        # 인프라 부트스트랩
        await self.setup_custom_context()
        await self._setup_rpc_bus()
        await self._setup_rest_edge()
        await self.setup_workers()
        await asyncio.sleep(1.0)

        # 커넥터(워커) 기동
        log.info(f"[BasePipeline] Igniting {len(self.connectors)} Connectors...")
        for conn in self.connectors:
            self._connector_tasks.append(asyncio.create_task(conn.run()))
        await asyncio.sleep(2.0)

        # 페이즈 실행
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n[PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult(self.scope_name, phase.name, True, True))
                    log.info("  └─ Status: PASSED")
                except Exception as e:
                    log.error(f"  └─ Halted: {str(e)} X", exc_info=True)
                    results.append(TestResult(self.scope_name, phase.name, False, True))
        finally:
            await self._teardown_all()

        return results

    async def _teardown_all(self):
        log.info(f"\n[BasePipeline] Teardown sequence initiated...")
        
        await self.teardown_custom()
        for conn in self.connectors:
            conn.running = False
        for task in self._connector_tasks:
            if not task.done():
                task.cancel()
        if self._connector_tasks:
            await asyncio.gather(*self._connector_tasks, return_exceptions=True)

        # background daemon stop
        if self.worker_daemon:
            self.worker_daemon.running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)

        # server down
        if self.server:
            self.server.should_exit = True
        if self._server_task:
            await asyncio.gather(self._server_task, return_exceptions=True)

        # RPC 후킹 및 빌더 원상 복구 (Zero Side-Effect)
        if self._original_rpc_call:
            InternalRpcClient.call = self._original_rpc_call
            self._original_rpc_call = None

        if self._original_registry_builder:
            rpc_registry.build_internal_rpc_registry = self._original_registry_builder
            import fiber.infra.daemon.rpc as daemon_module
            daemon_module.build_internal_rpc_registry = self._original_registry_builder
            self._original_registry_builder = None
            
        log.info(f"[BasePipeline] Teardown complete.")