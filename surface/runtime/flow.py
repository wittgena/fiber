# surface.runtime.flow
## @lineage: runtime.tenant.flow
## @lineage: mesh.runtime.flow
## @lineage: phi.runtime.flow
import os
import time
import uuid
import asyncio
import urllib.parse
import orjson
from typing import Optional, Dict, Any, AsyncGenerator
from dataclasses import dataclass, field
from pathlib import Path

from arch.topos.tunnel.surface import SurfaceMQ
from arch.xor.parser.ruleset import LifecycleRegexParser
from kernel.bind.resolver import resolve_path
from watcher.tracer.bound import SystemBound, log_streamer
from watcher.plane.emitter import get_emitter

log = get_emitter("flow.runtime")

KEY_STATE_PIDS = "system:xphi:pids"
PROCESS_NAME = "phix-node"

FRAMEWORK_LIFECYCLE_RULESET = {
    "targets": [
        {
            "tag": "framework-ready",
            "keywords": [
                {"AND": ["Netty started", "port"]},         
                {"AND": ["Started", "seconds", "JVM"]}      
            ]
        },
        {
            "tag": "framework-fatal",
            "keywords": [
                {"AND": ["io.netty.util.internal.OutOfDirectMemoryError", "failed to allocate"]}, 
                {"AND": ["reactor.blockhound.BlockingOperationError", "Blocking call!"]} 
            ]
        }
    ]
}

@dataclass
class TaskContext:
    """실행 엔진으로 전달되는 표준화된 작업 단위"""
    payload: Dict[str, Any]
    task_type: str = "default"
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

@dataclass
class SearchResult:
    """탐색 쿼리에 대한 반환 데이터 구조"""
    score: float
    block: str
    section: str
    file: str

class PhiRuntime:
    def __init__(self, jar_root: Path, mq_surface: Optional[SurfaceMQ] = None):
        self.jar_root = jar_root
        self.mq_surface = mq_surface
        self.process_name = PROCESS_NAME

        self.boundary = SystemBound()
        self.jar_path = self._resolve_jar()
        self.ready_event = asyncio.Event()

        self.rule_engine = LifecycleRegexParser().parse_ruleset(FRAMEWORK_LIFECYCLE_RULESET)

    def _resolve_jar(self) -> str:
        jars = sorted(self.jar_root.glob("phix-*.jar"))
        if not jars:
            raise RuntimeError("JVM execution core (jar) is missing from the library root.")
        return str(jars[-1])

    @log_streamer([
        "bash", "-c",
        "exec -a {process_name} java -Dreaper.tag={process_name} -jar {jar_path}"
    ])
    async def _stream_and_sense(self, line: str):
        """JVM stdout을 스트리밍하며 컴파일된 엔진을 통해 라이프사이클 이벤트를 라우팅합니다."""
        if not line: return
        
        matched_tags = self.rule_engine.execute(line)
        if "framework-ready" in matched_tags and not self.ready_event.is_set():
            log.info("[JvmRuntime] 🟢 WebFlux Readiness condition met. Engine is active.")
            self.ready_event.set()
        elif "framework-fatal" in matched_tags:
            log.crit(f"[JvmRuntime] 🚨 Fatal Framework Rupture detected: {line}")
            self.boundary.collapse()

    async def ensure(self):
        log.info(f"[JvmRuntime] Starting Java core process: {self.jar_path}")
        asyncio.create_task(self._stream_and_sense())
        await asyncio.sleep(0.1)
        if not self.boundary.process_pool:
            raise RuntimeError("Failed to inject JVM into SystemBound process pool.")
        
        pid = self.boundary.process_pool[-1].pid
        try:
            if self.mq_surface:
                self.mq_surface.register_state(KEY_STATE_PIDS, str(pid))
                log.info(f"[JvmRuntime] Core PID {pid} anchored to State Ledger.")
        except Exception as e:
            log.warning(f"[JvmRuntime] State anchoring failed. Engine proceeding in isolated mode: {e}")

        log.info("[JvmRuntime] Awaiting WebFlux/Netty core startup readiness...")
        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout=15.0)
            log.info("[JvmRuntime] Traffic routing enabled.")
        except asyncio.TimeoutError:
            log.warning("[JvmRuntime] Readiness sensor timed out, but proceeding anyway.")

    def shutdown(self):
        self.boundary.collapse()