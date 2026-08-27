# fiber.kernel.boot
import os
import asyncio
from typing import Optional

# --- Events & Bus ---
from xphi.arch.contract.event.next import LogEvent
from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.contract.event.bus import AsyncEventBus

# --- Executors & Registry ---
from xphi.arch.contract.executor import BaseExecutor
from xphi.arch.contract.registry.unified import registry

# --- Watcher & Emitter ---
from xphi.watcher.plane.observer.event import EventObserver
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.regulator import default_plane
from xphi.watcher.ingress.gateway import DphiGatewayServer, GatewaySettings
from xphi.watcher.receptor.bootstrap import receptor_bootstrap
from xphi.watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh

# --- Kernel & Phase ---
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.phase.runtime.executor.swarm import SwarmExecutor
from xphi.kernel.phase.runtime.flow.executor import FlowExecutor
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.kernel.phase.runtime.node import NodeRuntime

# ==========================================
# Globals
# ==========================================
log = get_emitter("kernel.boot")

_node_instance = None
_rest_server_instance = None
_gateway_instance = None

# ==========================================
# Phase Signal Component
# ==========================================
class PhaseSignal(EventObserver):
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        self.log = get_emitter("anchor.bridge", phase="BRANE")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        payload.update({
            "bridged_from": "Surface.LogEvent",
            "original_source": getattr(event, 'source_id', 'unknown'),
            "boundary": "brane_to_kernel"
        })

        carrier = PsiCarrier(
            kind="SIGNAL",          
            tag=event_message,      
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=getattr(event, 'parent_id', None),
            source_id="anchor.phase.bridge", 
            scope=getattr(event, 'scope', 'GLOBAL'),
            tick=getattr(event, 'tick', 0) or 0,
            phase_id=getattr(event, 'phase_id', 0), # 상위 계층의 phase_id가 있다면 승계
            carrier=carrier,
            context={"domain": "boundary.crossing"}
        )

        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
            loop.create_task(self._async_dispatch(psi_event, event_message))
        except RuntimeError as e:
            self.log.error(f"[PhaseBridge] Cannot dispatch SIGNAL. Event loop unavailable: {e}")

    async def _async_dispatch(self, psi_event: PsiEvent, original_msg: str):
        try:
            symbol = getattr(psi_event, 'symbol', original_msg)
            self.log.trace(f"[PhaseBridge] Elevating SIGNAL -> Psi: {symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[PhaseBridge] Dispatch failed for {original_msg}: {e}", exc_info=True)

# ==========================================
# Kernel Components
# ==========================================
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
            from xphi.watcher.receptor.policy.router import ExtProcStreamHandler
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

# ==========================================
# Boot Sequences & Services
# ==========================================
async def clear_zombie_port(port: int):
    from fiber.kernel.reaper import SystemOps
    
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

    import uvicorn
    from fiber.dphi.receptor.rest import create_app, Config

    log.info(f"[Boot] Injecting runtime configurations for REST Edge on Port {target_port}...")
    resolved_internal_url = os.getenv("INTERNAL_EDGE_URL", f"http://127.0.0.1:{target_port}")
    runtime_config = Config(internal_edge_url=resolved_internal_url)
    injected_app = create_app(runtime_config)

    config = uvicorn.Config(
        app=injected_app,
        host="127.0.0.1",
        port=target_port, 
        loop="uvloop", 
        log_level="warning", 
        access_log=False
    )
    _rest_server_instance = uvicorn.Server(config)
    asyncio.create_task(_rest_server_instance.serve())
    
    log.info(f"[Boot] Internal REST Edge listening safely on http://127.0.0.1:{target_port}")
    log.info(f"[Boot] Routing internal traffic to: {resolved_internal_url}")

async def start_public_gateway():
    global _gateway_instance
    
    settings = GatewaySettings()
    await clear_zombie_port(settings.proxy_port)
    await clear_zombie_port(settings.mcp_port)

    log.info("[Boot] Igniting Public Gateway & MCP Control Plane...")
    _gateway_instance = DphiGatewayServer(settings)
    asyncio.create_task(_gateway_instance.start_dual_servers())

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
    
    await start_rest_membrane()
    await start_public_gateway()

    log.info("[Boot] Igniting Embedded Phase Runtime Node...")
    completion_signal = asyncio.Event()
    
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = NodeRuntime(executor=executor)
    await _node_instance.start()

    log.info("[Boot] Control Plane, Embedded Node, REST Backend & Public Gateway fully operational.")
    log.info("[Boot] Entering observation mode...")
    await _node_instance.wait_until_stopped()

async def teardown():
    global _node_instance, _rest_server_instance, _gateway_instance
    log.info("[Boot] Releasing system resources...")
    
    if _rest_server_instance:
        log.info("[Boot] Shutting down Internal REST Edge...")
        _rest_server_instance.should_exit = True
        
    if _gateway_instance:
        log.info("[Boot] Shutting down Public Gateway...")
        if getattr(_gateway_instance, "client_session", None):
            await _gateway_instance.client_session.close()
    
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()

    await TunnelFactory.close_all()
    try:
        from xphi.kernel.dphi.ledger.consensus import KernelLedger
        KernelLedger().close()
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)