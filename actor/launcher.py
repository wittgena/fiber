# actor.launcher
## @lineage: topos.agent.launcher
import asyncio
import argparse
import socket
from typing import Any, Tuple, Dict, Optional

from actor.topos.resolver.model.tier import model_tier_registry
from actor.topos.resolver.task import TaskResolver, BlueprintType, SchemeCategory, TransactionDomain
from actor.topos.scope.manager import managed_scope
from actor.root import AgentTopos

from watcher.plane.emitter import get_emitter

log = get_emitter("agent.launcher")

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
        
        # [방어 3] Tuple 언패킹을 통해 Pydantic의 setattr 크래시 원천 회피
        surge_dag, score = self.resolver.resolve(category, b_type)
        return surge_dag, score

    async def launch(self):
        # 1. Blueprint 컴파일 및 난이도 도출
        surge_dag, required_score = self._determine_blueprint()

        # 2. 난이도 기반 타겟 모델 파악 (provider/model 규격)
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
        app = AgentTopos("RootAgentApp", run_context=run_context).setup_default_nodes()
        
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

    async def _run_interactive_loop(self, app: AgentTopos, use_proxy: bool):
        log.info(f"Interactive CLI Mode started (Proxy Mode: {use_proxy}). Type 'exit' to terminate.")
        while True:
            try:
                prompt = await asyncio.to_thread(input, "\n🤖 [Agent Prompt]> ")
                prompt = prompt.strip()
                if not prompt: continue
                if prompt.lower() in ['exit', 'quit']: break
                
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