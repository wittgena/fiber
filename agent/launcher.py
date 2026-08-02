# agent.launcher
from __future__ import annotations

import argparse
import asyncio
import json
import socket
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from agent.resolver.context import BlueprintType, SchemeCategory, TaskResolver, TransactionDomain
from agent.resolver.model.tier import model_tier_registry
from agent.runtime.scope.manager import managed_scope

from dphi.topos.flow import FlowTransition, ToposController

from arch.model.surge.blueprint import SurgeBlueprint
from arch.topos.node.gan import GanNode, Message
from phase.executor.flow.event import AgentConfigured
from watcher.dphi.adapter.exchange import ExchangeAdapter, TransactionReceipt
from watcher.dphi.broker import WasmBroker
from watcher.dphi.cgroup import Tier
from watcher.plane.emitter import get_emitter

log = get_emitter("agent.launcher")

DEFAULT_TOPOLOGY_SPEC: Dict[str, Dict[str, Any]] = {
    "ConfigPolicyNode": {
        "location": "local",
        "edges": ["ConfigSettingsNode", "DockerWorkspaceNode"]
    },
    "DockerWorkspaceNode": {
        "location": "local",
        "edges": ["ConfigPolicyNode"]
    },
    "ConfigSettingsNode": {
        "location": "local",
        "edges": ["ConfigPolicyNode"]
    }
}


@dataclass
class TopologyResidue:
    edge_state: str
    trajectory_trace: str
    receipt: Optional[TransactionReceipt]

class Entry(GanNode):
    def __init__(self, name: str, run_context: dict):
        super().__init__(name)
        self.run_context = run_context
        
        # [WASM Kernel & Exchange]
        self.broker = WasmBroker()
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key="local_clearing_pub_key")
        self.last_receipt: Optional[TransactionReceipt] = None
        
        # [State Management]
        self.config_count = 0
        self.target_configs = 2  
        self._active_blueprint: Optional[SurgeBlueprint] = None
        self._initial_instruction: str = ""
        self._injected_settings = None  
        self.transition = FlowTransition(origin=name)

    def setup_default_nodes(self):
        ToposController.assemble_and_mount(
            topos=self, 
            run_context=self.run_context, 
            broker=self.broker, 
            topology_spec=DEFAULT_TOPOLOGY_SPEC
        )
        return self

    async def _execute_pipeline(self, prompt: str = "", blueprint: Optional[SurgeBlueprint] = None) -> TopologyResidue:
        """@desc: Unified execution pipeline bounded by WASM Cgroup Policies."""
        self._initial_instruction = prompt
        self._active_blueprint = blueprint
        self.transition.unbind_and_reset()
        
        log.info(f"[{self.name}] 🛡️ Enforcing standard spatial density via WASM Cgroup (Tier: STANDARD).")
        await self.broker.update_policy(Tier.STANDARD.value)
        
        app_task = asyncio.create_task(self.run())
        self.post_message(Message("boot"))
        
        log.info(f"[{self.name}] Awaiting fluid topology convergence...")
        trajectory_result = await self.transition.await_convergence(timeout=600.0)
        log.info(f"[{self.name}] Initiating teardown sequence. Current Edge State: {self.transition.edge.value}")
        self.post_message(Message("shutdown"))
        
        if not app_task.done():
            await app_task
            
        residue = TopologyResidue(
            edge_state=self.transition.edge.value,
            trajectory_trace=trajectory_result,
            receipt=self.last_receipt
        )
        self._print_verification_report(residue)
        return residue

    def _print_verification_report(self, residue: TopologyResidue):
        log.info("\n" + "="*60)
        log.info(f"🌌 [TOPOLOGY FINALIZED] Dominium Edge State: {residue.edge_state}")
        log.info("-" * 60)
        log.info("[EXECUTION TRAJECTORY]")
        for line in residue.trajectory_trace.split('\n'):
            log.info(f"  {line}")
            
        log.info("-" * 60)
        log.info("[CRYPTOGRAPHIC SETTLEMENT RECEIPT]")
        if residue.receipt:
            log.info(f"  Receipt ID   (Topos): {residue.receipt.job_id}")
            log.info(f"  State Root   (Parity): {residue.receipt.unified_parity_hash}")
            log.info(f"  Signatures   (M-of-N): {len(residue.receipt.clearing_signatures)} validators")
            log.info(f"  Fuel Burned  (Compute): {residue.receipt.fuel_consumed}")
            log.info(f"  Status       : {residue.receipt.settlement_status}")
        else:
            log.warning("  ⚠️ No receipt generated (Epoch sealing fractured or bypassed).")
        log.info("="*60 + "\n")

    async def run_task(self, instruction: str) -> TopologyResidue:
        return await self._execute_pipeline(prompt=instruction)

    async def run_scheme(self, blueprint: SurgeBlueprint) -> TopologyResidue:
        log.info(f"[{self.name}] 🎯 Aligning topology context: {blueprint.topology_name} (Focus: {blueprint.focus})")
        return await self._execute_pipeline(blueprint=blueprint)

    # ---------------------------------------------------------
    # Node Lifecycle Message Handlers
    # ---------------------------------------------------------

    async def on_boot(self, message: Message):
        log.info(f"[{self.name}] 🚀 Booting hybrid system layers")
        for child in self.children:
            child.post_message(Message("boot"))

    async def on_agent_configured(self, message: Message):
        self.config_count += 1
        if isinstance(message, AgentConfigured) and message.settings:
            self._injected_settings = message.settings
            log.info(f"[{self.name}] 📥 Engine asset captured.")

        if self.config_count == self.target_configs:
            log.info(f"[{self.name}] 🚦 Quorum reached. Signaling Workspace creation wave.")
            for child in self.children:
                child.post_message(Message("start_workspace"))

    async def on_workspace_ready(self, message: Message):
        log.info(f"[{self.name}] 🌐 Execution space aligned.")
        for child in self.children:
            if type(child).__name__ == "LoopExecutor":
                ToposController.dispatch_payload(
                    policy_node=child, 
                    blueprint=self._active_blueprint, 
                    instruction=self._initial_instruction, 
                    settings=self._injected_settings
                )

    async def on_llm_event(self, message: Message):
        raw_content = getattr(message, 'llm_message', '')
        self.transition.record(f"LLM: {str(raw_content)}")

    async def on_task_completed(self, message: Message):
        """@desc: ToposController에 Ledger Sealing 및 Settlement 정산 로직을 위임합니다."""
        cost = getattr(message, 'cost', 0.0)
        fuel_consumed = getattr(message, 'fuel_consumed', 0)
        mode_tag = "Proxy Channel" if getattr(message, 'is_proxy', False) else "Local Resource"
        
        log.info(f"[{self.name}] ✅ Task converged (Cost: {cost}, Incurred via: {mode_tag})")
        self.transition.record(f"Task Completed. Cost: {cost} ({mode_tag})")
        
        self.last_receipt = await ToposController.settle_dominium(
            origin_name=self.name,
            transition=self.transition,
            broker=self.broker,
            exchange_adapter=self.exchange_adapter,
            cost=cost,
            fuel_consumed=fuel_consumed
        )

    async def on_node_error(self, message: Message):
        """@desc: 에러 발생 시 ToposController의 붕괴 로직으로 위임합니다."""
        source = getattr(message, 'source_node', 'Unknown')
        error = getattr(message, 'error', 'Unknown Error')
        if "Connection refused" in str(error) and "DockerWorkspaceNode" in source:
            log.info(f"[{self.name}] 💡 HINT: Docker daemon is unreachable. Use '--proxy' for remote execution.")
            
        ToposController.handle_rupture(self.name, self.transition, source, error)

    async def on_shutdown(self, message: Message):
        log.info(f"[{self.name}] 💤 Purging system manifolds and reclaiming resources.")
        if self.transition.future and not self.transition.future.done():
            self.transition.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            
        for child in list(self.children):
            await self.unmount(child)
        self.stop()


class EnvironConfigurator:
    LOCAL_MODEL = "ollama/local-gemma-3"
    DEFAULT_FALLBACK_MODEL = "gemini/gemini-3.1-flash-lite" 
    
    @classmethod
    def _check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve(
        cls, 
        requested_model: Optional[str], 
        requested_proxy: bool, 
        min_cognitive_score: int = 1
    ) -> Tuple[Dict[str, Any], str]:
        
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls._check_network_connectivity()

        if not is_online:
            log.warning("🚨 [System Offline] Network connectivity lost. Forcing fallback to Local Engine.")
            resolved_model = cls.LOCAL_MODEL
            use_proxy = False
            
        elif not resolved_model:
            optimal_result = model_tier_registry.get_optimal_model(
                requires_tools=True, 
                min_cognitive_score=min_cognitive_score
            )
            
            if optimal_result:
                if isinstance(optimal_result, tuple):
                    provider, opt_model = optimal_result
                    resolved_model = f"{provider}/{opt_model}"
                else:
                    resolved_model = f"gemini/{optimal_result}"
                log.info(f"✅ Registry resolved optimal model (Req Score >= {min_cognitive_score}): {resolved_model}")
            else:
                log.warning(f"⚠️ Registry exhausted or no model meets cognitive score {min_cognitive_score}. Defaulting to: {cls.DEFAULT_FALLBACK_MODEL}")
                resolved_model = cls.DEFAULT_FALLBACK_MODEL
                use_proxy = requested_proxy

        scope_kwargs = {
            "use_proxy": use_proxy,
            "show_logs": True
        }
        return scope_kwargs, resolved_model


class AppCLI:
    @classmethod
    def parse(cls) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Meta Agent Entry CLI - System Bootstrapper")
        parser.add_argument("-p", "--prompt", type=str, help="Execute a single specific task instruction.")
        parser.add_argument("-i", "--interactive", action="store_true", help="Enter interactive CLI loop.")
        parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")
        parser.add_argument("-m", "--model", type=str, default=None, help="Target LLM model to use.")
        parser.add_argument("-s", "--scenario", type=str, choices=[c.value for c in SchemeCategory], help="Trigger specific Scheme dimension.")
        parser.add_argument("-t", "--transaction", type=str, choices=[t.value for t in TransactionDomain], help="Trigger a deterministic Transaction boundary.")
        parser.add_argument("-r", "--resolution", action="store_true", help="Trigger the Semantic Resolution Hacking Funnel.")
        return parser.parse_args()


class SystemBootstrapper:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.resolver = TaskResolver()

    def _determine_blueprint(self) -> Tuple[Optional[Any], int]:
        if self.args.interactive or self.args.prompt:
            return None, 2

        b_type, category = BlueprintType.SCHEME, SchemeCategory.AGENT
        if self.args.resolution:
            b_type, category = BlueprintType.RESOLUTION, "resolution_hacking"
        elif self.args.transaction:
            b_type, category = BlueprintType.TRANSACTION, TransactionDomain(self.args.transaction)
        elif self.args.scenario:
            b_type, category = BlueprintType.SCHEME, SchemeCategory(self.args.scenario)
        
        surge_dag, score = self.resolver.resolve(category, b_type)
        return surge_dag, score

    async def launch(self):
        surge_dag, required_score = self._determine_blueprint()
        scope_kwargs, target_model = EnvironConfigurator.resolve(
            requested_model=self.args.model, 
            requested_proxy=self.args.proxy,
            min_cognitive_score=required_score
        )
        
        try:
            async with managed_scope(**scope_kwargs):
                run_context = {
                    "use_proxy": scope_kwargs.get("use_proxy", False),
                    "surface_type": "sandbox",
                    "target_model": target_model
                }
                await self._execute_target(run_context, surge_dag, required_score)
                
        except ConnectionError as ce:
            log.error(f"❌ Target surface connection failed: {ce}")
        except Exception as e:
            log.error(f"❌ Fatal execution error in launcher: {e}")

    async def _execute_target(self, run_context: Dict[str, Any], surge_dag: Optional[Any], required_score: int):
        app = Entry("RootAgentApp", run_context=run_context).setup_default_nodes()
        
        if self.args.interactive:
            await self._run_interactive_loop(app, run_context.get("use_proxy", False))
            return
            
        if self.args.prompt:
            trace = await app.run_task(instruction=self.args.prompt)
            self._print_residue("TRACE", self.args.prompt[:30], trace)
            return

        if surge_dag:
            log.info(f"🚀 Launching Executable Surge DAG: {surge_dag.topology_name} (Cognitive Difficulty: LV.{required_score})")
            trace = await app.run_scheme(blueprint=surge_dag)
            self._print_residue("SCHEME", surge_dag.topology_name, trace)
        else:
            log.error("❌ Failed to locate or compile requested topology blueprint.")

    async def _run_interactive_loop(self, app: Entry, use_proxy: bool):
        log.info(f"Interactive CLI Mode started (Proxy Mode: {use_proxy}). Type 'exit' to terminate.")
        while True:
            try:
                prompt = await asyncio.to_thread(input, "\n🤖 [Agent Prompt]> ")
                prompt = prompt.strip()
                if not prompt: 
                    continue
                if prompt.lower() in ['exit', 'quit']: 
                    break
                
                trace = await app.run_task(instruction=prompt)
                self._print_residue("TRACE", prompt[:30], trace)
            except (KeyboardInterrupt, EOFError):
                log.info("\nSession context disrupted. Exiting...")
                break

    def _print_residue(self, trace_type: str, context_name: str, trace: Any):
        log.info("\n" + "="*50)
        log.info(f"FINAL HYBRID RESIDUE ({trace_type}) FOR: '{context_name}'")
        log.info(trace)
        log.info("="*50)


async def main():
    args = AppCLI.parse()
    bootstrapper = SystemBootstrapper(args)
    await bootstrapper.launch()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n## System gracefully terminated by user.")