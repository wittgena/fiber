# phase.agent.workflow
## @lineage: agent.nexus.workflow
## @lineage: nexus.agent.workflow
## @lineage: meta.agent.workflow
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from phase.agent.runtime.blueprint import BlueprintType, TaskResolver, BLUEPRINT_REGISTRY
from phase.agent.topos.scope.manager import FlowTransition, ToposController, managed_scope
from phase.agent.topos.model.tier import model_tier_registry

from arch.model.surge.blueprint import SurgeBlueprint
from arch.topos.node.gan import Message
from arch.topos.workflow import Workflow, WorkflowMessage, StopMessage, ErrorMessage, step
from kernel.phase.reactor import PhaseReactor
from kernel.dphi.broker import DphiBroker
from kernel.dphi.cgroup import Tier
from kernel.dphi.exchange.transaction import ExchangeAdapter, TransactionReceipt
from watcher.plane.emitter import get_emitter

log = get_emitter("agent.workflow")

DEFAULT_TOPOS_SPEC: Dict[str, Dict[str, Any]] = {
    "ConfigPolicyNode": {"location": "local", "edges": ["ConfigSettingsNode", "DockerWorkspaceNode"]},
    "DockerWorkspaceNode": {"location": "local", "edges": ["ConfigPolicyNode"]},
    "ConfigSettingsNode": {"location": "local", "edges": ["ConfigPolicyNode"]}
}

@dataclass
class TopologyResidue:
    edge_state: str
    trajectory_trace: str
    receipt: Optional[TransactionReceipt]

class SystemBootMessage(WorkflowMessage):
    def __init__(self, prompt: str = "", blueprint: Optional[SurgeBlueprint] = None, **kwargs):
        super().__init__(**kwargs)
        self.prompt = prompt
        self.blueprint = blueprint

class AgentConfiguredMessage(WorkflowMessage):
    def __init__(self, settings: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.settings = settings

class DeployWorkspaceMessage(WorkflowMessage):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class WorkspaceReadyMessage(WorkflowMessage):
    def __init__(self, workspace_ref: str = "", **kwargs):
        super().__init__(**kwargs)
        self.workspace_ref = workspace_ref

class TaskConvergedMessage(WorkflowMessage):
    def __init__(self, cost: float = 0.0, fuel_consumed: int = 0, is_proxy: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.cost = cost
        self.fuel_consumed = fuel_consumed
        self.is_proxy = is_proxy

class NodeErrorMessage(WorkflowMessage):
    def __init__(self, source_node: str = "Unknown", error: str = "Unknown Error", **kwargs):
        super().__init__(**kwargs)
        self.source_node = source_node
        self.error = error

class LauncherWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        super().__init__(name=name, timeout=600.0, **kwargs)
        self.run_context = run_context
        
        self.broker = DphiBroker()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key="local_clearing_pub_key")
        self.transition = FlowTransition(origin=name)
        
        self._quorum_target = 2
        self._config_count = 0
        self._workspace_deployed = False
        
        self._initial_instruction: str = ""
        self._active_blueprint: Optional[SurgeBlueprint] = None
        self._injected_settings = None  

    def mount_topology(self):
        ToposController.assemble_and_mount(
            topos=self, run_context=self.run_context, 
            broker=self.broker, topology_spec=DEFAULT_TOPOS_SPEC
        )
        return self

    async def on_agent_configured(self, message: Message):
        self.post_message(AgentConfiguredMessage(settings=getattr(message, 'settings', None)))

    async def on_workspace_ready(self, message: Message):
        self.post_message(WorkspaceReadyMessage(workspace_ref=getattr(message, 'workspace_ref', '')))

    async def on_task_completed(self, message: Message):
        self.post_message(TaskConvergedMessage(cost=getattr(message, 'cost', 0.0), is_proxy=getattr(message, 'is_proxy', False)))

    async def on_task_converged(self, message: Message):
        self.post_message(TaskConvergedMessage(cost=getattr(message, 'cost', 0.0), is_proxy=getattr(message, 'is_proxy', False)))

    async def on_node_error(self, message: Message):
        self.post_message(NodeErrorMessage(
            source_node=getattr(message, 'source_node', 'Unknown'), 
            error=getattr(message, 'error', 'Unknown Error')
        ))

    @step
    async def execute_boot(self, msg: SystemBootMessage) -> None:
        self._initial_instruction = msg.prompt
        self._active_blueprint = msg.blueprint
        self.transition.unbind_and_reset()
        
        log.info(f"[{self.name}] 🛡️ Enforcing standard spatial density via WASM Cgroup (Tier: STANDARD).")
        await self.broker.update_policy(Tier.STANDARD.value)
        
        log.info(f"[{self.name}] 🚀 Booting hybrid system layers")
        for child in self.children:
            child.post_message(Message("boot"))

    @step
    async def evaluate_quorum(self, msg: AgentConfiguredMessage) -> None:
        if self._workspace_deployed:
            return

        self._config_count += 1
        if msg.settings:
            self._injected_settings = msg.settings
            log.info(f"[{self.name}] 📥 Engine asset captured.")

        if self._config_count >= self._quorum_target:  
            log.info(f"[{self.name}] 🚦 Quorum reached. Signaling Workspace creation wave.")
            self._workspace_deployed = True  # 플래그 잠금
            self.post_message(DeployWorkspaceMessage())

    @step
    async def deploy_workspace(self, msg: DeployWorkspaceMessage) -> None:
        log.info(f"[{self.name}] 🌐 Triggering Workspace Provisioning...")
        for child in self.children:
            child.post_message(Message("start_workspace"))

    @step
    async def ignite_agent_loop(self, msg: WorkspaceReadyMessage) -> None:
        log.info(f"[{self.name}] ⚙️ Workspace Ready (Ref: {msg.workspace_ref}). Igniting Agent Loop...")
        for child in self.children:
            if type(child).__name__ == "LoopExecutor":
                ToposController.dispatch_payload(
                    policy_node=child, 
                    blueprint=self._active_blueprint, 
                    instruction=self._initial_instruction, 
                    settings=self._injected_settings
                )

    @step
    async def settle_and_terminate(self, msg: TaskConvergedMessage) -> None:
        mode_tag = "Proxy Channel" if msg.is_proxy else "Local Resource"
        log.info(f"[{self.name}] ✅ Task converged (Cost: {msg.cost}, Incurred via: {mode_tag})")
        self.transition.record(f"Task Completed. Cost: {msg.cost} ({mode_tag})")
        
        receipt = await ToposController.settle_dominium(
            origin_name=self.name, transition=self.transition,
            broker=self.broker, exchange_adapter=self.exchange_adapter,
            cost=msg.cost, fuel_consumed=msg.fuel_consumed
        )
        
        residue = TopologyResidue(
            edge_state=self.transition.edge.value,
            trajectory_trace=str(self.transition.edge),
            receipt=receipt
        )
        self._print_verification_report(residue)
        
        self.post_message(StopMessage(result=residue))

    @step
    async def handle_rupture(self, msg: NodeErrorMessage) -> None:
        if "Connection refused" in msg.error and "DockerWorkspaceNode" in msg.source_node:
            log.info(f"[{self.name}] 💡 HINT: Docker daemon is unreachable. Use '--proxy' for remote execution.")
            
        log.warning(f"[{self.name}] 💥 Rupture Detected in {msg.source_node}: {msg.error}. Halting or shifting to recovery...")
        ToposController.handle_rupture(self.name, self.transition, msg.source_node, msg.error)
        self.post_message(StopMessage(result=None))

    def _print_verification_report(self, residue: TopologyResidue):
        log.info("\n" + "="*60)
        log.info(f"🌌 [TOPOLOGY FINALIZED] Dominium Edge State: {residue.edge_state}")
        if residue.receipt:
            log.info(f"  Receipt ID (Topos): {residue.receipt.job_id}")
            log.info(f"  Fuel Burned (Compute): {residue.receipt.fuel_consumed}")
        log.info("="*60 + "\n")

class AgentApplication:
    def __init__(self, prompt: str, blueprint: Optional[SurgeBlueprint], scope_kwargs: dict, run_context: dict):
        self.prompt = prompt
        self.blueprint = blueprint
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        self.workflow: Optional[LauncherWorkflow] = None

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            self.workflow = LauncherWorkflow("RootAgentApp", self.run_context).mount_topology()
            workflow_task = asyncio.create_task(self.workflow.run())
            
            self.workflow.post_message(SystemBootMessage(prompt=self.prompt, blueprint=self.blueprint))
            await workflow_task

    async def _teardown_hook(self):
        if self.workflow:
            log.info("🧹 Reclaiming launcher resources...")
            for child in list(self.workflow.children):
                await self.workflow.unmount(child)
            self.workflow.stop()

    def execute(self):
        log.info("🚀 Igniting Launcher Workflow via KernelReactor...")
        PhaseReactor.ignite(
            main_coro_func=self._startup_hook,
            teardown_hook=self._teardown_hook
        )


def get_environment_context(args: argparse.Namespace) -> Tuple[dict, dict, Optional[SurgeBlueprint]]:
    resolver = TaskResolver()
    surge_dag = None
    required_score = 2
    
    if not args.prompt:
        b_type, category = BlueprintType.SCHEME, "agent"
        if args.resolution:
            b_type, category = BlueprintType.RESOLUTION, "resolution_hacking"
        elif args.transaction:
            b_type, category = BlueprintType.TRANSACTION, args.transaction
        elif args.scenario:
            b_type, category = BlueprintType.SCHEME, args.scenario
        
        surge_dag, required_score = resolver.resolve(category, b_type)

    def check_online(host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        try:
            import socket
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    is_online = check_online()
    resolved_model = args.model
    if not is_online:
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=True, min_cognitive_score=required_score)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": args.proxy if is_online else False, "show_logs": True}
    run_context = {"use_proxy": scope_kwargs["use_proxy"], "surface_type": "sandbox", "target_model": resolved_model}
    
    return scope_kwargs, run_context, surge_dag


def main():
    parser = argparse.ArgumentParser(description="Meta Agent Entry CLI - Workflow & KernelReactor Bootstrapper")
    parser.add_argument("-p", "--prompt", type=str, help="Execute a single specific task instruction.")
    parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use.")
    parser.add_argument("-s", "--scenario", type=str, choices=list(BLUEPRINT_REGISTRY[BlueprintType.SCHEME].keys()))
    parser.add_argument("-t", "--transaction", type=str, choices=list(BLUEPRINT_REGISTRY[BlueprintType.TRANSACTION].keys()))
    parser.add_argument("-r", "--resolution", action="store_true", help="Trigger Semantic Resolution Hacking Funnel.")
    args = parser.parse_args()

    scope_kwargs, run_context, blueprint = get_environment_context(args)
    app = AgentApplication(
        prompt=args.prompt or "",
        blueprint=blueprint,
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()

if __name__ == "__main__":
    main()