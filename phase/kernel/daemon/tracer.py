# fiber.phase.kernel.daemon.tracer
import os
import asyncio
from contextlib import suppress

from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.contract.registry.tracer import TracerRegistry

log = get_emitter("daemon.tracer")

@contract.daemon("tracer_controller")
class TracerControllerDaemon(AbstractDaemon):
    """
    @desc: `fiber trace` 명령어를 통해 실행되는 카오스 엔지니어링 및 진단용 하이퍼바이저 데몬.
    주어진 타겟(TRACE_TARGET)에 맞는 Tracer(OOMTracer, ReproTracer 등)를 인스턴스화하고 실행을 지휘합니다.
    """
    def __init__(self, ctx):
        super().__init__("TracerControllerDaemon")
        self.ctx = ctx
        self.target = os.getenv("TRACE_TARGET")
        self._trace_task = None

    def _ensure_tracer_config(self, target: str):
        """
        [안전장치] 대상 시나리오에 대한 Config가 Registry에 없을 경우, 
        초기화 시 KeyError로 인해 데몬이 뻗는 것을 막기 위해 최소한의 Fallback Mocks를 주입합니다.
        """
        if TracerRegistry.get(target):
            return

        log.warning(f"[{self.name}] No config found for '{target}' in TracerRegistry. Injecting safe fallback config.")
        
        if target == "oom_tracer":
            TracerRegistry.register(target, {
                "desc": "OOM Isolation Trace (Fallback)",
                "infra_type": "docker",
                "image_name": "busybox",
                "container_name": "fiber_oom_target_test",
                "workspace_path": "/tmp",
                "verify_type": "rustc_recursion",
                "mem_limit": "64m"
            })
        elif target == "repro_worker":
            TracerRegistry.register(target, {
                "desc": "Deadlock Reproduction Trace (Fallback)",
                "infra_type": "compose",
                "compose_file": "docker-compose.yml",
                "container_name": "fiber_repro_worker_test",
                "workspace_path": "/tmp"
            })

    async def run(self):
        log.info(f"[{self.name}] Initiating Tracer Controller...")
        
        if not self.target:
            log.error(f"[{self.name}] TRACE_TARGET environment variable is missing. Evaporating...")
            return

        log.info(f"[{self.name}] 🎯 Targeting Trace Scenario: {self.target}")
        
        # [개선] 지연 로딩을 통해 평상시 다른 노드(EDGE/COMPUTE)가 뜰 때
        # 무거운 Tracer 인프라(Docker/Kube SDK 등)가 불필요하게 임포트되는 것을 방지합니다.
        try:
            from fiber.phase.debug.tracer.infra import OOMTracer, ReproTracer
        except ImportError as e:
            log.error(f"[{self.name}] Failed to load tracer infra dependencies: {e}")
            return

        tracer_instance = None
        
        # 1. 시나리오 라우팅 및 인스턴스화
        self._ensure_tracer_config(self.target)
        
        if self.target == "oom_tracer":
            tracer_instance = OOMTracer(target_name=self.target)
        elif self.target == "repro_worker":
            tracer_instance = ReproTracer(target_name=self.target)
        else:
            log.error(f"[{self.name}] Unknown TRACE_TARGET: {self.target}. Supported: oom_tracer, repro_worker.")
            return

        # 2. 메인 실행 루프
        try:
            # 백그라운드 태스크로 실행하여 NodeRuntime의 Cancel 시그널(Ctrl+C)에 안전하게 반응
            self._trace_task = asyncio.create_task(tracer_instance.execute())
            
            while self.running:
                if self._trace_task.done():
                    exc = self._trace_task.exception()
                    if exc:
                        log.error(f"[{self.name}] Tracer execution crashed: {exc}", exc_info=exc)
                    else:
                        log.info(f"[{self.name}] ✅ Trace Scenario '{self.target}' completed successfully.")
                    
                    # 추적이 끝났으므로 데몬을 자발적으로 종료(Evaporate)하여 리소스 반환
                    break 
                    
                await asyncio.sleep(1.0)
                
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received. Aborting trace scenario.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal execution error: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing Tracer Controller resources...")
        if self._trace_task and not self._trace_task.done():
            self._trace_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._trace_task
        log.info(f"[{self.name}] Tracer Controller safely evaporated.")