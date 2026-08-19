# ator.conv.flow
import asyncio
import json
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager, AsyncExitStack

from eco.observer.proof.flow import ManifoldFolder, FlowTransition
from phase.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt

from ator.runtime.loop.executor import LoopExecutor
from ator.runtime.space.manager import SpaceNode, space_provider
from ator.runtime.node import RuntimeNode
from ator.conv.registry import get_surface_class, SurfaceConfig

from arch.xor.surge.blueprint import SurgeBlueprint
from arch.contract.model.graph import EntryNode
from arch.topos.node.gan import Message, GanNode
from arch.model.sealer import EpochSealer
from kernel.dphi.broker import DphiBroker
from kernel.dphi.cgroup import Tier
from watcher.plane.emitter import get_emitter
from watcher.tracer.scope import scope_trace, get_current_trace_path

log = get_emitter("conv.flow")

class ToposController:
    """Topology Orchestration, Event Dispatching, and Cryptographic Sealing"""

    @staticmethod
    def assemble_and_mount(topos: GanNode, run_context: dict, broker: Any, topology_spec: dict) -> None:
        use_proxy, target_model = run_context.get("use_proxy", False), run_context.get("target_model")
        log.info(f"[ToposController] Wiring hybrid nodes (Proxy Mode: {use_proxy}, Model: {target_model})")
        
        active_nodes = {
            "ConfigPolicyNode": LoopExecutor("ConfigPolicyNode"),
            "ConfigSettingsNode": RuntimeNode("ConfigSettingsNode", use_proxy=use_proxy, target_model=target_model),
            "DockerWorkspaceNode": SpaceNode("DockerWorkspaceNode", provider=space_provider, use_proxy=use_proxy)
        }
        ManifoldFolder(broker=broker).fold_manifold(active_nodes=active_nodes, topology_spec=topology_spec)
        for node in active_nodes.values():
            topos.mount(node)
        log.info(f"[ToposController] Topology spatial folding complete.")

    @staticmethod
    def dispatch_payload(policy_node: Any, blueprint: Optional[SurgeBlueprint], instruction: str, settings: dict) -> None:
        if blueprint:
            context_msg = Message("set_context")
            context_msg.entry_node = EntryNode(
                entry=blueprint.topology_name, focus=blueprint.focus,
                depth=blueprint.depth_limit, relations=blueprint.relations_constraint.split(',') if blueprint.relations_constraint else []
            )
            policy_node.post_message(context_msg)
            
            run_msg = Message("execute_events")
            run_msg.events = blueprint.nodes
            run_msg.settings = settings
            run_msg.system_instructions = getattr(blueprint, 'system_instructions', None)
            policy_node.post_message(run_msg)
        else:
            run_msg = Message("run_conversation")
            run_msg.instruction = instruction
            run_msg.settings = settings
            policy_node.post_message(run_msg)

    @staticmethod
    async def settle_dominium(
        origin_name: str,
        transition: FlowTransition,
        broker: DphiBroker,
        exchange_adapter: ExchangeAdapter,
        cost: float,
        fuel_consumed: int
    ) -> Optional[TransactionReceipt]:
        """@desc: 에포크를 봉인하고 영지(Dominium)에 도달하기 위한 합의 및 정산 과정을 총괄합니다."""
        entangled_state = {
            "parity": {"topos_id": f"task_{transition.id}", "phase_id": 0, "nexus_id": 0},
            "repos": {}
        }
        canonical_payload = EpochSealer.generate_seal_payload(entangled_state, parent_commit_id="genesis")
        res = await broker.invoke("seal_epoch", canonical_payload)
        
        signatures = []
        if res.success:
            try:
                signatures = json.loads(res.output).get("signatures", [])
            except json.JSONDecodeError:
                log.warning(f"[{origin_name}] Unparseable response from seal_epoch.")
        else:
            log.warning(f"[{origin_name}] Epoch sealing friction: {res.error}")

        receipt = exchange_adapter.finalize_settlement(
            entangled_state=entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        log.info(f"[{origin_name}] 🧾 Transaction Receipt Issued: {receipt.job_id}")
        transition.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{transition.id}")
        return receipt

    @staticmethod
    def handle_rupture(origin_name: str, transition: FlowTransition, source: str, error: str):
        log.critical(f"[{origin_name}] 🚨 Fatal topological rupture originating from [{source}]. Error: {error}")
        transition.record(f"System Error: {source} failed -> {error}")
        transition.fracture_topology(lmbda=0.2, tau=1.0, force_collapse=True)

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
            surface_class = get_surface_class("local")
            
        self.impl = surface_class(config)

    async def up(self):
        if asyncio.iscoroutinefunction(self.impl.up):
            await self.impl.up()
        else:
            self.impl.up()

    async def down(self):
        if asyncio.iscoroutinefunction(self.impl.down):
            await self.impl.down()
        else:
            self.impl.down()
    
    def get_engine(self):
        return self.impl.get_engine()

@asynccontextmanager
async def managed_scope(**surface_kwargs):
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await asyncio.sleep(0.1)
            await manager.down()
            log.info("[+] Context Manager closed safely.")