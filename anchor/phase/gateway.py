# anchor.phase.gateway
import os
import asyncio
from bound.bridge.ext import ignite
from bound.bridge.memory import BridgeFactory
from anchor.phase.ingress.receptor import PolymorphicReceptor
from bound.surface.mesh import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from arch.topos.bound.sandbox.tunnel import TunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("phase.gateway")

class PhaseGateway:
    """
    @role: Topology Assembler.
    @desc: boot로부터 주입받은 NodeRuntime(핵심 엔진)에 Ingress, Bridge, Matrix 토폴로지를 결합합니다.
    """
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        ## System Immunity (전역 설정 유지)
        ignite()
        
        ## Provision Infrastructure Substrate
        broker_facade = await TunnelFactory.get_default()

        ## Control Plane Initialization
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        ## Topology Bridge Resolution (Data Plane <-> Control Plane)
        bridge = BridgeFactory.resolve_bridge(topology, policy_engine, state_mesh)

        ## Data Plane Assembly
        # 주입받은 node를 Receptor에 연결하여 실제 트래픽이 Executor로 흐르게 함
        receptor = PolymorphicReceptor(node, bridge)

        ## Ignite Background Loops (node.start()는 boot에게 위임)
        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())

        if topology == "EXT_PROC":
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            asyncio.create_task(receptor.listen())