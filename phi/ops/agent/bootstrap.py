# phi.ops.agent.bootstrap
## @lineage: phi.runtime.bootstrap
import asyncio
import argparse
import socket
from typing import Any, Tuple, Dict, Optional

from bound.resolver.model.tier import model_tier_registry
from topos.bound.resolver.spec import SchemeCategory, TransactionDomain
from topos.bound.resolver.task import TaskResolver, BlueprintType
from topos.bound.scope.manager import managed_scope
from topos.flow.graph.root import AgentTopos

from watcher.plane.emitter import get_emitter

log = get_emitter("agent.bootstrap")

class EnvironmentConfigurator:
    """
    @desc: 인프라 및 실행 환경 구성을 담당합니다. (기존 TaskResolver에서 분리됨)
    """
    ULTIMATE_LOCAL_MODEL = "local-gemma-3"
    
    @classmethod
    def _check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        """물리적 네트워크 경계(온오프라인 상태)를 검증합니다."""
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve(cls, requested_model: Optional[str], requested_proxy: bool) -> Tuple[Dict[str, Any], str]:
        """
        [Static] 시스템 상태에 따라 최적의 환경(Surface 설정, 결정된 모델)을 도출합니다.
        Returns:
            scope_kwargs: managed_scope에 안전하게 전달 가능한 설정 딕셔너리 (model 파라미터 배제)
            resolved_model: 실제 엔진 구동 시 사용할 타겟 모델명
        """
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls._check_network_connectivity()

        if not is_online:
            log.warning("🚨 [System Offline] Network connectivity lost. Forcing fallback to Local Engine.")
            resolved_model = cls.ULTIMATE_LOCAL_MODEL
            use_proxy = False
        elif not resolved_model:
            optimal_model = model_tier_registry.get_optimal_model(requires_tools=True)
            if optimal_model:
                resolved_model = f"gemini/{optimal_model}"
                log.info(f"✅ Registry resolved optimal model: {resolved_model}")
            else:
                log.warning(f"⚠️ Registry exhausted. Falling back to local model: {cls.ULTIMATE_LOCAL_MODEL}")
                resolved_model = cls.ULTIMATE_LOCAL_MODEL
                use_proxy = False

        # 🚨 최초 오류 원인 제거: scope_kwargs에 'model' 키를 넣지 않음으로써 SurfaceConfig 파라미터 충돌 방지
        scope_kwargs = {
            "use_proxy": use_proxy,
            "show_logs": True
        }
        return scope_kwargs, resolved_model


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
    @flow: Resolves Environment -> Opens Context Scope -> Orchestrates Execution (Prompt vs DAG).
    """
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.resolver = TaskResolver()  # 순수 Blueprint 제공자로 초기화

    async def launch(self):
        # 1. 시스템 상태 기반 환경 도출
        scope_kwargs, target_model = EnvironmentConfigurator.resolve(
            requested_model=self.args.model, 
            requested_proxy=self.args.proxy
        )
        
        try:
            # 2. 물리적 Scope(Context) 진입
            async with managed_scope(**scope_kwargs):
                
                run_context = {
                    "use_proxy": scope_kwargs.get("use_proxy", False),
                    "surface_type": "sandbox",
                    "target_model": target_model
                }
                
                # 3. 타겟 실행 로직 라우팅
                await self._execute_target(run_context)
                
        except ConnectionError as ce:
            log.error(f"❌ Target surface connection failed: {ce}")
            log.info("💡 Hint: If the remote sandbox is down, run without '--proxy' to use local DphiSurface.")
        except Exception as e:
            log.error(f"❌ Fatal execution error in launcher: {e}")

    async def _execute_target(self, run_context: Dict[str, Any]):
        """CLI 커맨드 유형에 맞춰 AgentTopos를 초기화하고 실행을 주도합니다."""
        
        # 엔진 (AgentTopos) 초기화
        app = AgentTopos("RootAgentApp", run_context=run_context).setup_default_nodes()

        # A. 대화형 인터랙티브 모드
        if self.args.interactive:
            await self._run_interactive_loop(app, run_context.get("use_proxy", False))
            return
            
        # B. 단일 프롬프트 실행 모드
        if self.args.prompt:
            trace = await app.run_task(instruction=self.args.prompt)
            self._print_residue("TRACE", self.args.prompt[:30], trace)
            return

        # C. DAG(Blueprint) 토폴로지 실행 모드
        b_type, category = BlueprintType.SCHEME, SchemeCategory.AGENT  # Default

        if self.args.resolution:
            b_type, category = BlueprintType.RESOLUTION, "resolution_hacking"
        elif self.args.transaction:
            b_type, category = BlueprintType.TRANSACTION, TransactionDomain(self.args.transaction)
        elif self.args.scenario:
            b_type, category = BlueprintType.SCHEME, SchemeCategory(self.args.scenario)
        
        # 순수 Resolver를 통해 대상 Blueprint 도출
        surge_dag = self.resolver.resolve(category, b_type)

        if surge_dag:
            log.info(f"🚀 Launching Executable Surge DAG: {surge_dag.topology_name}")
            trace = await app.run_scheme(blueprint=surge_dag)
            self._print_residue("SCHEME", surge_dag.topology_name, trace)
        else:
            log.error(f"❌ Failed to locate or compile requested topology blueprint: {category} ({b_type})")

    async def _run_interactive_loop(self, app: AgentTopos, use_proxy: bool):
        """UI 로직: 터미널 기반의 지속적인 입출력 제어"""
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
        """출력 포매팅 로직"""
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