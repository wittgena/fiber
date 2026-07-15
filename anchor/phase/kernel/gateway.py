# anchor.phase.kernel.gateway
## @lineage: anchor.phase.gateway
import os
import asyncio

from anchor.phase.ingress.receptor import PolymorphicReceptor

from bound.bridge.memory.factory import BridgeFactory
from bound.bridge.mock.ext import ignite
from bound.surface.mesh import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision

from arch.topos.bound.tunnel import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from watcher.plane.emitter import get_emitter

log = get_emitter("kernel.gateway")

class KernelGateway:
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        ignite()
        broker_facade = await TunnelFactory.get_default()
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        bridge = BridgeFactory.resolve_bridge(topology, policy_engine, state_mesh)
        event_bus = getattr(node, 'bus', AsyncEventBus())
        receptor = PolymorphicReceptor(bus=event_bus, bridge=bridge)

        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())

        if topology == "EXT_PROC":
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            asyncio.create_task(receptor.listen())