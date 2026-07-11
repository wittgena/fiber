# bound.resolver.task
## @lineage: anchor.registry.resolver.task
import asyncio
from typing import Any, Protocol, Tuple, Type
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.task")

class SchemeResolverProtocol(Protocol):
    def select(self, category: Any) -> Tuple[str, Any]:
        ...

class TaskResolver:
    """@desc: Routes and executes sub-tasks based on request type within the mounted context manager sandbox."""
    
    def __init__(self, run_context: dict, launcher_cls: Type[Any], scheme_resolver: SchemeResolverProtocol):
        self.run_context = run_context
        self.launcher_cls = launcher_cls
        self.scheme_resolver = scheme_resolver

    async def execute_prompt(self, instruction: str):
        """단일 텍스트 프롬프트 실행 파이프라인 (Legacy / Interactive)"""
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_task(instruction=instruction)
        
        log.info("\n" + "="*50)
        log.info(f"FINAL HYBRID RESIDUE (TRACE) FOR: '{instruction[:30]}...'")
        log.info(trace)
        log.info("="*50)

    async def execute_scheme(self, blueprint: Any):
        """구조화된 객체(SchemeBlueprint) 실행 파이프라인"""
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        # 런처에 새로 추가된 run_scheme 메서드 호출
        trace = await app.setup_default_nodes().run_scheme(blueprint=blueprint)
        
        log.info("\n" + "="*50)
        
        # blueprint 객체가 context(EntryNode)를 가지고 있다고 가정하고 로깅
        entry_name = getattr(blueprint.context, "entry", "Unknown Context") if hasattr(blueprint, "context") else "Structured Scheme"
        log.info(f"FINAL HYBRID RESIDUE (TRACE) FOR SCHEME: '{entry_name}'")
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

    async def run_benchmark_scenario(self, category: Any):
        """Automated benchmark task for Topos dimension scenarios."""
        pool_name, selected_scenario = self.scheme_resolver.select(category=category)
        
        log.info("\n" + "🚀"*17)
        log.info(f"Initiating [ {pool_name.upper()} ] Auto-Benchmark Sequence.")
        if isinstance(selected_scenario, str):
            log.info(f"Selected Prompt: \n[ {selected_scenario} ]")
            log.info("🚀"*17 + "\n")
            await self.execute_prompt(selected_scenario)
        else:
            log.info(f"Selected Blueprint Target: [ {category.value} ]")
            log.info("🚀"*17 + "\n")
            await self.execute_scheme(blueprint=selected_scenario)