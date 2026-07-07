# xphi.agent.resolver.context
import asyncio
import socket
from typing import List, Optional, Tuple, Dict, Any, Type, Protocol

from anchor.provider.tier.registry import model_tier_registry
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.context")

class SchemeResolverProtocol(Protocol):
    def select(self, is_external: bool = False) -> Tuple[str, str]:
        ...

class ContextResolver:
    """@desc: Diagnoses infrastructure state to determine optimal model architecture and Surface Scope parameters."""
    ULTIMATE_LOCAL_MODEL = "local-gemma-3"

    @classmethod
    def check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        """Validates network (internet) connectivity."""
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve(cls, requested_model: Optional[str], requested_proxy: bool) -> Tuple[Dict[str, Any], str, bool]:
        """Returns the optimal (Scope Params, Resolved Model, Use Proxy) tuple based on network and quota states."""
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls.check_network_connectivity()

        ## Fallback Matrix Logic
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
                log.warning(f"⚠️ Registry exhausted. Falling back to ultimate local model: {cls.ULTIMATE_LOCAL_MODEL}")
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


class TaskOrchestrator:
    """@desc: Routes and executes sub-tasks based on request type within the mounted context manager sandbox."""
    
    # SchemeResolverProtocol을 타입으로 지정하여 외부에서 주입받음
    def __init__(self, run_context: dict, launcher_cls: Type[Any], scheme_resolver: SchemeResolverProtocol):
        self.run_context = run_context
        self.launcher_cls = launcher_cls
        self.scheme_resolver = scheme_resolver

    async def execute_prompt(self, instruction: str):
        """Single prompt execution task."""
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_task(instruction=instruction)
        
        log.info("\n" + "="*50)
        log.info(f"FINAL HYBRID RESIDUE (TRACE) FOR: '{instruction[:30]}...'")
        log.info(trace)
        log.info("="*50)

    async def start_interactive_mode(self):
        """Interactive CLI loop task."""
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

    async def run_benchmark_scenario(self, is_external: bool):
        """Automated benchmark task for default scenarios."""
        pool_name, selected_scenario = self.scheme_resolver.select(is_external=is_external)
        
        log.info("\n" + "🚀"*17)
        log.info(f"Initiating {pool_name} Auto-Benchmark Sequence.")
        log.info(f"Selected Scenario: [ {selected_scenario} ]")
        log.info("🚀"*17 + "\n")
        
        await self.execute_prompt(selected_scenario)