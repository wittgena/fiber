# fiber.dev.e2e.base.pipeline
## @lineage: fiber.infra.bound.e2e.pipeline
import os
import sys
import time
import json
import asyncio
from typing import Any, List, Dict, Optional, Callable

import uvicorn

from fiber.gateway.payload import create_app, Config
from fiber.dev.e2e.base.config import (
    PipelineRunner,
    ManagedTestServer,
    TestResult,
    E2EConfig,
)

from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.state.anchor.store import PhaseStore, AnchorRole
import xphi.node.rpc.registry as rpc_registry
import xphi.node.ops.daemon.rpc as daemon_module
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
            action = "transition.pending"
            metadata = {"status": "PENDING", "target": target_server_id}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)

    def set_resolved(self, handle_id: str, payload: dict):
        class DummyState:
            action = "transition.resolve"
            metadata = {"status": "RESOLVED"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps.pop(handle_id, None)

    def set_faulted(self, handle_id: str, error_detail: str):
        class DummyState:
            action = "transition.resolve"
            metadata = {"status": "FAULTED", "error_detail": error_detail}
            def __init__(self): self.payload = None
        self.states[handle_id] = DummyState()
        self.yield_timestamps.pop(handle_id, None)

    def set_yielded(self, handle_id: str, payload: dict):
        class DummyState:
            action = "transition.yield"
            metadata = {"status": "YIELD"}
            def __init__(self, p): self.payload = p
        self.states[handle_id] = DummyState(payload)
        self.yield_timestamps[handle_id] = time.time()

    async def query_stale_streams(self, current_time: float, thresholds: dict) -> List[MockStream]:
        stale = []
        effective_ttl = min(
            self.stale_timeout,
            thresholds.get("transition.pending", self.stale_timeout)
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


# =========================================================================
# 1. MockPipeline: 인메모리 MockPhaseStore 및 RPC 몽키패치 기반 격리 파이프라인
# =========================================================================
class MockPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig, name: str, scope_name: str):
        super().__init__(name=name, scope_name=scope_name)
        self.config = config
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"

        self.mock_store = MockPhaseStore()
        self.rpc: Optional[InternalRpcClient] = None

        self.connectors = []
        self._connector_tasks = []
        self.server = None
        self._server_task = None
        self.worker_daemon = None
        self._worker_task = None

        self._original_rpc_call: Optional[Callable] = None
        self._original_registry_builder: Optional[Callable] = None
        self.captured_handle_ids: Dict[str, str] = {}

    async def setup_custom_context(self):
        pass

    async def setup_workers(self):
        pass

    async def teardown_custom(self):
        pass

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
                "action": getattr(state, "action", meta.get("action", "")),
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

            await tunnel.publish(
                f"mcp.intent.reply.{handle_id}",
                json.dumps({
                    "status": status,
                    "executable_payload": payload,
                    "error_detail": error_detail,
                }),
            )
            return {"success": True, "status": status}

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
        daemon_module.build_internal_rpc_registry = bridge_mock_registry_builder

        class MockDaemonCtx:
            broker = None
            store = None

        self.worker_daemon = RpcWorkerDaemon(ctx=MockDaemonCtx())
        self.worker_daemon.running = True
        self._worker_task = asyncio.create_task(self.worker_daemon.run())

        log.info("[MockPipeline] RPC Bus & Core Daemons Ignited (Mock PhaseStore & Real Validators Aligned).")

    async def _setup_rest_edge(self):
        tunnel = await TunnelFactory.get_default()
        rest_app = create_app(
            config=Config(wasm_timeout=5.0),
            tunnel=tunnel,
            phase_store=self.mock_store,
        )
        u_config = uvicorn.Config(
            app=rest_app,
            host="127.0.0.1",
            port=self.config.port,
            log_level="error",
            access_log=False,
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        log.info("[MockPipeline] REST Edge Bootstrapped.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Suite: {self.name} ===")

        await self.setup_custom_context()
        await self._setup_rpc_bus()
        await self._setup_rest_edge()
        await self.setup_workers()
        await asyncio.sleep(1.0)

        log.info(f"[BasePipeline] Igniting {len(self.connectors)} Connectors...")
        for conn in self.connectors:
            self._connector_tasks.append(asyncio.create_task(conn.run()))
        await asyncio.sleep(2.0)

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
        log.info("\n[BasePipeline] Teardown sequence initiated...")

        await self.teardown_custom()
        for conn in self.connectors:
            conn.running = False
        for task in self._connector_tasks:
            if not task.done():
                task.cancel()
        if self._connector_tasks:
            await asyncio.gather(*self._connector_tasks, return_exceptions=True)

        if self.worker_daemon:
            self.worker_daemon.running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)

        if self.server:
            self.server.should_exit = True
        if self._server_task:
            await asyncio.gather(self._server_task, return_exceptions=True)

        if self._original_rpc_call:
            InternalRpcClient.call = self._original_rpc_call
            self._original_rpc_call = None

        if self._original_registry_builder:
            rpc_registry.build_internal_rpc_registry = self._original_registry_builder
            daemon_module.build_internal_rpc_registry = self._original_registry_builder
            self._original_registry_builder = None

        log.info("[BasePipeline] Teardown complete.")


# =========================================================================
# 2. BasePipeline (BaseBridgePipeline): 실제 PhaseStore 및 Live Node 연동 파이프라인
# =========================================================================
class BasePipeline(PipelineRunner):
    """
    몽키패치 없이 실제 PhaseStore와 Redis RPC Bus(internal.rpc.queue)를 사용하는 실전 파이프라인.
    - 외부 노드(node.ops.boot)가 이미 떠서 원장 Lock(PROPOSER 모드)과 RPC 큐를 처리 중이면
      자체 RpcWorkerDaemon을 중복 기동하지 않아 Split-Brain 및 큐 가로채기를 원천 방지합니다.
    - 외부 노드가 없는 단독 실행 환경에서는 자체 PhaseStore(SEALER)와 RpcWorkerDaemon을 기동합니다.
    """

    def __init__(self, config: E2EConfig, name: str, scope_name: str):
        super().__init__(name=name, scope_name=scope_name)
        self.config = config
        self.local_url = f"{self.config.protocol}://127.0.0.1:{self.config.port}"

        self.phase_store: Optional[PhaseStore] = None
        self.rpc: Optional[InternalRpcClient] = None
        self.external_node_active: bool = False

        self.connectors = []
        self._connector_tasks = []
        self.server = None
        self._server_task = None
        self.worker_daemon: Optional[RpcWorkerDaemon] = None
        self._worker_task: Optional[asyncio.Task] = None

        self.captured_handle_ids: Dict[str, str] = {}

    async def setup_custom_context(self):
        pass

    async def setup_workers(self):
        pass

    async def teardown_custom(self):
        pass

    async def _is_live_node_running(self) -> bool:
        """원장 Role(PROPOSER 여부) 또는 Tunnel의 활성 데몬 키로 외부 마스터 노드 생존 여부 판별"""
        if self.phase_store and getattr(self.phase_store, "role", None) == AnchorRole.PROPOSER:
            return True
        try:
            tunnel = await TunnelFactory.get_default()
            raw = await tunnel.get("mcp:registry:active_targets")
            if raw:
                return True
        except Exception:
            pass
        return False

    async def _setup_rpc_bus(self):
        self.rpc = InternalRpcClient()
        self.phase_store = PhaseStore()
        self.external_node_active = await self._is_live_node_running()

        if self.external_node_active:
            log.info(
                "[BasePipeline] 🔗 Live Master Node detected (Operating as PROPOSER). "
                "Delegating RPC & Real PhaseStore transitions directly to live node."
            )
        else:
            class RealDaemonCtx:
                broker = None
                store = self.phase_store

            self.worker_daemon = RpcWorkerDaemon(ctx=RealDaemonCtx())
            self.worker_daemon.running = True
            self._worker_task = asyncio.create_task(self.worker_daemon.run())
            log.info("[BasePipeline] Standalone Mode: Real PhaseStore (SEALER) & local RpcWorkerDaemon ignited.")

    async def _setup_rest_edge(self):
        tunnel = await TunnelFactory.get_default()
        rest_app = create_app(
            config=Config(wasm_timeout=5.0),
            tunnel=tunnel,
            phase_store=self.phase_store,
        )
        u_config = uvicorn.Config(
            app=rest_app,
            host="127.0.0.1",
            port=self.config.port,
            log_level="error",
            access_log=False,
        )
        self.server = ManagedTestServer(u_config)
        self._server_task = asyncio.create_task(self.server.serve())
        log.info("[BasePipeline] REST Edge Bootstrapped with Real PhaseStore.")

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Suite: {self.name} ===")

        await self.setup_custom_context()
        await self._setup_rpc_bus()
        await self._setup_rest_edge()
        await self.setup_workers()
        await asyncio.sleep(1.0)

        log.info(f"[BasePipeline] Igniting {len(self.connectors)} Connectors...")
        for conn in self.connectors:
            self._connector_tasks.append(asyncio.create_task(conn.run()))
        if self.connectors:
            await asyncio.sleep(2.0)

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
        log.info("\n[BasePipeline] Teardown sequence initiated...")

        await self.teardown_custom()
        for conn in self.connectors:
            conn.running = False
        for task in self._connector_tasks:
            if not task.done():
                task.cancel()
        if self._connector_tasks:
            await asyncio.gather(*self._connector_tasks, return_exceptions=True)

        if self.worker_daemon:
            self.worker_daemon.running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            await asyncio.gather(self._worker_task, return_exceptions=True)

        if self.server:
            self.server.should_exit = True
        if self._server_task:
            await asyncio.gather(self._server_task, return_exceptions=True)

        if self.phase_store and not self.external_node_active:
            try:
                self.phase_store.close()
            except Exception:
                pass

        log.info("[BasePipeline] Teardown complete.")