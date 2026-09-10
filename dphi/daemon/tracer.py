# fiber.dphi.daemon.tracer
## @lineage: fiber.dphi.infra.daemon.tracer
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
    def __init__(self, ctx):
        super().__init__("TracerControllerDaemon")
        self.ctx = ctx
        self.target = os.getenv("TRACE_TARGET")
        self._trace_task = None

    def _ensure_tracer_config(self, target: str):
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
        
        try:
            from xphi.watcher.tracer.infra.topos import OOMTracer, ReproTracer
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