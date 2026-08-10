# phase.node.boot
import os
import asyncio
import uvicorn  # 🌟 REST 서버 구동을 위해 추가

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry

from kernel.bind.redirector import PhaseAirlock
from kernel.phase.runtime.executor.swarm import SwarmExecutor
from kernel.phase.runtime.flow.executor import FlowExecutor
from kernel.dphi.ledger.consensus import KernelLedger
from kernel.phase.signal import PhaseSignal
from kernel.phase.reactor import PhaseReactor
from kernel.phase.runtime.node import NodeRuntime

from watcher.plane.regulator import default_plane
from watcher.plane.emitter import get_emitter
from watcher.receptor.bootstrap import receptor_bootstrap
from watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision

# 🌟 신규: Edge/Membrane REST API 앱 임포트
from receptor.rest import api as rest_app

log = get_emitter("phase.boot")

_node_instance = None
_rest_server_instance = None  # 🌟 Uvicorn 서버 인스턴스 보관용

class KernelGateway:
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        broker_facade = await TunnelFactory.get_default()
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())
        
        if topology == "EXT_PROC":
            # ExtProcStreamHandler가 정의되어 있다고 가정합니다.
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            log.info(f"[Orchestrator] Running in {topology} mode. (Ingress Receptor removed)")

class RoutingExecutor(BaseExecutor):
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.swarm_executor = SwarmExecutor(completion_signal)
        self.flow_executor = FlowExecutor(completion_signal)
        self._node = None

    @property
    def node(self): return self._node

    @node.setter
    def node(self, value):
        self._node = value
        self.swarm_executor.node = value
        self.flow_executor.node = value

    async def execute(self, psi) -> list:
        if not hasattr(psi, 'carrier') or psi.carrier.kind != "COMMAND":
            return []

        context = psi.carrier.payload.get("_context", {})
        command = context.get("command") or psi.carrier.tag
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list:
            return await self.swarm_executor.execute(psi)

        task_type = task_info_list[0].get("type", "cli")
        if task_type == "flow":
            try:
                return await self.flow_executor.execute(psi)
            except Exception as e:
                log.error(f"[Router] FlowExecutor failed with {e}. Falling back to SwarmExecutor.")
                return await self.swarm_executor.execute(psi)
        else:
            return await self.swarm_executor.execute(psi)


# 🌟 신규: REST API(Edge 서버)를 비동기로 구동하는 함수
async def start_rest_membrane():
    global _rest_server_instance
    log.info("[Boot] Igniting REST Edge Membrane (FastAPI/Uvicorn) on Port 8000...")
    
    # uvicorn Config 객체를 통해 설정. 로그 포맷 충돌을 막기 위해 uvicorn 자체 접근 로그를 제한할 수 있습니다.
    config = uvicorn.Config(
        app=rest_app, 
        host="0.0.0.0", 
        port=8000, 
        loop="uvloop", 
        log_level="warning",  # Dphi 자체 로거(get_emitter)를 활용하므로 Uvicorn의 내부 로그는 최소화
        access_log=False
    )
    _rest_server_instance = uvicorn.Server(config)
    
    # 서버를 백그라운드 태스크로 구동 (블로킹 방지)
    asyncio.create_task(_rest_server_instance.serve())
    log.info("[Boot] REST Edge Membrane listening on http://0.0.0.0:8000")


async def main_async():
    global _node_instance
    
    log.info("[Boot] Initiating Control Plane (Master Watcher)...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    log.info("[Boot] Attaching Gateway Topology...")
    await KernelGateway.assemble(None)
    
    log.info("[Boot] Igniting Physical Membrane Receptor...")
    tunnel = await TunnelFactory.get_default()
    asyncio.create_task(receptor_bootstrap(tunnel))

    # 🌟 추가: Embedded Node 시작 전에 Edge REST 서버 띄우기
    await start_rest_membrane()

    # [핵심 변경점] Subprocess 대신 단일 프로세스 내에 Embedded NodeRuntime 마운트
    log.info("[Boot] Igniting Embedded Phase Runtime Node...")
    completion_signal = asyncio.Event()
    
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = NodeRuntime(executor=executor)
    await _node_instance.start()

    log.info("[Boot] Control Plane, Embedded Node & REST Membrane fully operational.")
    log.info("[Boot] Entering observation mode...")
    
    # 노드의 라이프사이클을 메인 루프가 대기하도록 설정
    await _node_instance.wait_until_stopped()


async def teardown():
    global _node_instance, _rest_server_instance
    log.info("[Boot] Releasing system resources...")
    
    # 🌟 추가: REST Edge 서버 Graceful Shutdown
    if _rest_server_instance:
        log.info("[Boot] Shutting down REST Edge Membrane...")
        _rest_server_instance.should_exit = True
    
    # Embedded Node의 Graceful Shutdown 우선 수행
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()
        
    await TunnelFactory.close_all()
    try:
        KernelLedger().close()
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    log.info("[Boot] Resource cleanup complete.")


if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)