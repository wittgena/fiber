# bound.resolver.task
## @lineage: anchor.registry.resolver.task
import asyncio
import socket
from typing import Any, Tuple, Type, Dict, Optional
from watcher.plane.emitter import get_emitter
from anchor.registry.model.tier import model_tier_registry

from agent.handler.scheme.manager import UniversalBlueprintManager
from arch.topos.state.surge.blueprint import SurgeBlueprint

log = get_emitter("resolver.task")

class TaskResolver:
    """
    @desc: Unified Orchestrator for Context Resolution and Task Execution.
           Diagnoses environment state, maps fallbacks, and executes prompts/DAGs.
    """
    ULTIMATE_LOCAL_MODEL = "local-gemma-3"
    
    def __init__(self, run_context: dict, launcher_cls: Type[Any], blueprint_manager: UniversalBlueprintManager):
        self.run_context = run_context
        self.launcher_cls = launcher_cls
        self.blueprint_manager = blueprint_manager

    @classmethod
    def _check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        """Validates physical network boundaries."""
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve_environment(cls, requested_model: Optional[str], requested_proxy: bool) -> Tuple[Dict[str, Any], str, bool]:
        """Returns the optimal (Scope Params, Resolved Model, Use Proxy) layout."""
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls._check_network_connectivity()

        if not is_online:
            log.warning("🚨 [System Offline] Network connectivity lost.")
            log.warning(f"🔄 Forcing fallback to Local Engine: {cls.ULTIMATE_LOCAL_MODEL}")
            resolved_model = cls.ULTIMATE_LOCAL_MODEL
            if use_proxy:
                log.warning("⚠️ Remote proxy cannot be used offline. Disabling proxy surface.")
                use_proxy = False
        elif not resolved_model:
            optimal_model = model_tier_registry.get_optimal_model(requires_tools=True)
            if optimal_model:
                resolved_model = f"gemini/{optimal_model}"
                log.info(f"✅ Registry resolved optimal model: {resolved_model}")
            else:
                log.warning(f"⚠️ Registry exhausted. Falling back to local model: {cls.ULTIMATE_LOCAL_MODEL}")
                resolved_model = cls.ULTIMATE_LOCAL_MODEL
                if use_proxy:
                    log.info("ℹ️ Disabling remote proxy to align with local model execution.")
                    use_proxy = False

        scope_kwargs = {
            "use_proxy": use_proxy,
            "show_logs": True,
            "model": resolved_model  
        }
        return scope_kwargs, resolved_model, use_proxy

    # ==========================================
    # 2. Execution Pipelines
    # ==========================================
    async def execute_prompt(self, instruction: str):
        """Standard Text-based Execution Pipeline (Interactive/Legacy)."""
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_task(instruction=instruction)
        
        log.info("\n" + "="*50)
        log.info(f"FINAL HYBRID RESIDUE (TRACE) FOR: '{instruction[:30]}...'")
        log.info(trace)
        log.info("="*50)

    async def run_surge_blueprint(self, blueprint: SurgeBlueprint):
        """Structured DAG Execution Pipeline (Schemes, Transactions, Resolutions)."""
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_scheme(blueprint=blueprint)
        
        log.info("\n" + "="*50)
        entry_name = getattr(blueprint, "topology_name", "Structured Scheme")
        log.info(f"FINAL HYBRID RESIDUE (TRACE) FOR SCHEME: '{entry_name}'")
        log.info(trace)
        log.info("="*50)

    async def start_interactive_mode(self):
        """Interactive CLI loop manifold."""
        use_proxy = self.run_context.get("use_proxy", False)
        log.info(f"Interactive CLI Mode started (Proxy Mode: {use_proxy}). Type 'exit' to terminate.")
        
        while True:
            try:
                prompt = await asyncio.to_thread(input, "\n🤖 [Agent Prompt]> ")
                prompt = prompt.strip()
                
                if not prompt: 
                    continue
                if prompt.lower() in ['exit', 'quit']:
                    log.info("Exiting interactive manifold...")
                    break
                    
                await self.execute_prompt(prompt)
            except (KeyboardInterrupt, EOFError):
                log.info("\nSession context disrupted. Exiting...")
                break