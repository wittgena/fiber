# ops.agent.launcher
## @lineage: meta.ops.agent.launcher
import asyncio
import argparse

from topos.bound.resolver.spec import SchemeCategory, TransactionDomain
from topos.bound.resolver.task import TaskResolver
from ops.scope.manager import managed_scope
from ops.scope.flow.graph.root import AgentTopos

from watcher.plane.emitter import get_emitter

log = get_emitter("agent.launcher")

class AppCLI:
    """@desc: CLI argument parsing interface mapped to Topos Dimensions"""
    @classmethod
    def parse(cls) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Meta Agent Entry CLI - System Bootstrapper")
        parser.add_argument("-p", "--prompt", type=str, help="Execute a single specific task instruction.")
        parser.add_argument("-i", "--interactive", action="store_true", help="Enter interactive CLI loop.")
        parser.add_argument("--proxy", action="store_true", help="Enable remote proxy extension layout.")
        parser.add_argument("-m", "--model", type=str, default=None, 
                            help="Target LLM model to use.")
        parser.add_argument("-s", "--scenario", type=str, choices=[c.value for c in SchemeCategory],
                            help="Trigger specific Scheme dimension (agent/gov/meta/autopoiesis).")
        parser.add_argument("-t", "--transaction", type=str, choices=[t.value for t in TransactionDomain],
                            help="Trigger a deterministic Transaction boundary (e.g., code_auditor).")
        parser.add_argument("-r", "--resolution", action="store_true",
                            help="Trigger the Semantic Resolution Hacking Funnel.")
        return parser.parse_args()


class SystemBootstrapper:
    """
    @desc: Core application lifecycle manager.
    @flow: Resolves Environment -> Opens Context Scope -> Injects Dependencies -> Delegates to TaskResolver.
    """
    def __init__(self, args: argparse.Namespace):
        self.args = args

    async def launch(self):
        # 1. 시스템 상태 기반 타겟 환경 및 한계선(Scope) 도출 (무상태 정적 호출)
        scope_kwargs, resolved_model, use_proxy = TaskResolver.resolve_environment(
            requested_model=self.args.model, 
            requested_proxy=self.args.proxy
        )
        
        try:
            # 2. 물리적 Scope(Context) 진입 (managed_scope가 자원 할당/해제를 보장)
            async with managed_scope(**scope_kwargs):
                
                run_context = {
                    "use_proxy": use_proxy,
                    "surface_type": "sandbox",
                    "target_model": resolved_model
                }

                # 3. 통합 TaskResolver 초기화 (엔진과 컨텍스트 주입)
                resolver = TaskResolver(
                    run_context=run_context, 
                    launcher_cls=AgentTopos
                )
                
                # 4. CLI Arguments를 그대로 넘겨 라우팅 및 실행을 위임
                await resolver.route_and_execute(self.args)
                
        except ConnectionError as ce:
            log.error(f"❌ Target surface connection failed: {ce}")
            log.info("💡 Hint: If the remote sandbox is down, run without '--proxy' to use local DphiSurface.")
        except Exception as e:
            log.error(f"❌ Fatal execution error in launcher: {e}")


async def main():
    args = AppCLI.parse()
    bootstrapper = SystemBootstrapper(args)
    await bootstrapper.launch()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n## System gracefully terminated by user.")