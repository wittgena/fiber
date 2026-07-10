# anchor.bind.bootstrap
"""
@module: anchor.bind.surface.matrix
@desc: Distributed Policy Orchestrator & System Bootstrapper
"""
from bound.surface.bridge.channel.ext import ignite
from bound.ingress.receptor import PolymorphicReceptor
from bound.surface.bridge.memory import BridgeFactory

from phase.runtime.node import NodeRuntime

class GatewayBootstrap:
    """
    @role: Grand Unified Bootstrapper.
    @desc: Resolves topology, injects dependencies, and wires Ingress, Bridge, and Matrix together.
    """
    @classmethod
    async def bootstrap(cls) -> NodeRuntime:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Bootstrapping Unified Membrane in {topology} mode.")

        ## System Immunity & Base Runtime
        ignite()
        node = NodeRuntime()
        
        ## Provision Infrastructure Substrate
        broker_facade = await TunnelFactory.get_default()

        ## Control Plane Initialization
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        ## Topology Bridge Resolution (Data Plane <-> Control Plane)
        ## 만약 ACP 기반 연동이 필요하다면 여기에 acp_conn 인스턴스를 넘깁니다.
        bridge = BridgeFactory.resolve_bridge(topology, policy_engine, state_mesh)

        ## Data Plane Assembly
        receptor = PolymorphicReceptor(node, bridge)

        ## Ignite Background Loops
        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())
        await node.start()

        if topology == "EXT_PROC":
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            asyncio.create_task(receptor.listen())

        return node