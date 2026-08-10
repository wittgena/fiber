# phase.node.boot
import os
import asyncio

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry

from kernel.phase.runtime.executor.swarm import SwarmExecutor
from kernel.phase.runtime.flow.executor import FlowExecutor
from kernel.phase.signal import PhaseSignal
from kernel.phase.reactor import PhaseReactor
from kernel.phase.runtime.node import NodeRuntime

from watcher.plane.regulator import default_plane
from watcher.plane.emitter import get_emitter
from watcher.receptor.bootstrap import receptor_bootstrap
from watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh

log = get_emitter("phase.boot")

_node_instance = None
_rest_server_instance = None

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
            # ExtProcStreamHandler가 정의되어 있는 경우 지연 로딩
            from watcher.receptor.policy.router import ExtProcStreamHandler
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


async def clear_zombie_port(port: int):
    # [지연 로딩] 프로세스를 통제하는 Reaper 모듈은 실행 시점에만 가져옵니다.
    from phase.node.reaper import SystemOps
    
    reaper = SystemOps(redis_conn=None, tag="boot.reaper")
    pids = await reaper.get_pids_from_port(port)
    my_pid = str(os.getpid())
    for pid in pids:
        if pid == my_pid:
            continue
            
        log.warning(f"[Boot] Port {port} is occupied by PID {pid} (Possibly suspended). Reaping...")
        await reaper._execute_kill(pid, force=True)
        
    if pids:
        await asyncio.sleep(0.5)


async def start_rest_membrane():
    global _rest_server_instance
    target_port = int(os.getenv("REST_PORT", 8000))
    await clear_zombie_port(target_port)

    # [핵심 변경: 지연 로딩] 
    # REST API(FastAPI)와 Uvicorn은 Worker 스폰 과정에서 절대 평가(Evaluation)되지 않도록 
    # 오직 Master가 8000번 포트를 열 때만 메모리에 로딩합니다.
    import uvicorn
    from receptor.rest import api as rest_app

    log.info(f"[Boot] Igniting REST Edge Membrane (FastAPI/Uvicorn) on Port {target_port}...")
    config = uvicorn.Config(
        app=rest_app, 
        host="0.0.0.0", 
        port=target_port, 
        loop="uvloop", 
        log_level="warning", 
        access_log=False
    )
    _rest_server_instance = uvicorn.Server(config)
    asyncio.create_task(_rest_server_instance.serve())
    log.info(f"[Boot] REST Edge Membrane listening on http://0.0.0.0:{target_port}")


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
    
    # Master 프로세스에서만 REST 서버가 로드되고 열립니다.
    await start_rest_membrane()

    log.info("[Boot] Igniting Embedded Phase Runtime Node...")
    completion_signal = asyncio.Event()
    
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = NodeRuntime(executor=executor)
    await _node_instance.start()

    log.info("[Boot] Control Plane, Embedded Node & REST Membrane fully operational.")
    log.info("[Boot] Entering observation mode...")
    await _node_instance.wait_until_stopped()


async def teardown():
    global _node_instance, _rest_server_instance
    log.info("[Boot] Releasing system resources...")
    
    if _rest_server_instance:
        log.info("[Boot] Shutting down REST Edge Membrane...")
        _rest_server_instance.should_exit = True
    
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()

    await TunnelFactory.close_all()
    try:
        from kernel.dphi.ledger.consensus import KernelLedger
        KernelLedger().close()
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    log.info("[Boot] Resource cleanup complete.")


if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)