# fiber.infra.ops.plane.kube.tracer
## @lineage: fiber.infra.watcher.plane.kube.tracer
## @lineage: xphi.watcher.plane.tracer.kube
import asyncio
from typing import Optional
from xphi.watcher.plane.tracer.base import BaseTracer, BaseStreamAuditor, SystemBound
from xphi.watcher.plane.emitter import get_emitter

class KubeStatusAuditor(BaseStreamAuditor):
    """지정된 주기로 Kubernetes 네임스페이스의 Pod 상태를 관측하는 특화된 옵저버"""
    def __init__(self, target: str, boundary: SystemBound, namespace: str, delay: int = 5):
        super().__init__(target=target, boundary=boundary, delay=delay)
        self.namespace = namespace
        self.log = get_emitter("auditor.kube")

    async def run_stream(self, *args, **kwargs) -> None:
        try:
            while True:
                cmd = ["kubectl", "get", "pods", "-n", self.namespace]
                code, out, _ = await self.boundary.run_command(cmd, capture=True)
                if code == 0 and out:
                    self.log.info(f"\n[Pod Status Watch: {self.namespace}] ------------------\n{out}")
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            pass

class KubeTracer(BaseTracer):
    """Kubernetes 환경의 커맨드 실행 및 라이프사이클을 추적하는 기반 클래스"""
    def __init__(self, tracer_name: str, timeout: int = 600, boundary: Optional[SystemBound] = None):
        super().__init__(tracer_name=tracer_name, timeout=timeout, boundary=boundary)