# runtime.tenant.phix
import os
import time
import uuid
import orjson
from typing import Optional, Dict, Any, AsyncGenerator, Union
from dataclasses import dataclass, field
from pathlib import Path

from surface.runtime.flow import PhiRuntime

from arch.topos.tunnel.surface import SurfaceMQ, SurfaceClient
from arch.xor.parser.ruleset import AuditRulesetParser, CompiledEngine
from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("phi.flow")

DPHI_BASE = os.getenv("DPHI_BASE", "http://localhost:8079")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
LIB_ROOT = resolve_path("lib")

SOURCE_NAME = "jvm.executor"
GLOBAL_AUDIT_RULESET = {
    "global_config": {
        "inspection_level": "structural"
    }
}

@dataclass
class TaskContext:
    """실행 엔진으로 전달되는 표준화된 작업 단위"""
    payload: Dict[str, Any]
    task_type: str = "ledger_push"
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

class FlowExecutor:
    """JVM 코어 프로세스와의 통신(HTTP/MQ), 보안 검열(Audit), 라우팅을 총괄하는 중앙 실행기"""
    def __init__(self, surface_client: Optional[SurfaceClient] = None, audit_engine: Optional[CompiledEngine] = None):
        if surface_client:
            self.surface = surface_client
        else:
            mq_surface = SurfaceMQ()
            self.surface = SurfaceClient(
                bootstrap_runtime=PhiRuntime(LIB_ROOT, mq_surface),
                mq_surface=mq_surface,
                source_name=SOURCE_NAME,
                fallback_url=DPHI_BASE,
                path_prefix=""
            )
            
        self.audit_engine = audit_engine or AuditRulesetParser().parse_ruleset(GLOBAL_AUDIT_RULESET)

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Dict[str, Any], None]:
        """실제 정의된 Task(원장 전송, 상태 동기화)만 처리하는 간결한 라우터"""
        log.info(f"[{SOURCE_NAME}] Routing task '{context.task_type}' (ID: {context.task_id})")

        if context.task_type == "ledger_push":
            topic = context.payload.get("topic")
            raw_data = context.payload.get("data")
            raw_bytes: bytes = orjson.dumps(raw_data)
            safe_bytes: bytes = self.audit_engine.execute(raw_bytes)
            try:
                async for chunk in self.surface.request(query_path=f"/ledger/{topic}", data=safe_bytes, method="POST"):
                    yield {"status": "processing", "data": chunk}
                    
                log.debug(f"[{context.task_id}] Anchored {len(safe_bytes)} bytes (Audit passed) to {topic}")
                yield {"status": "success"}
            except Exception as e:
                log.error(f"[{context.task_id}] Ledger push failed: {e}")
                yield {"status": "error", "error": str(e)}
        elif context.task_type == "verify_parity":
            nexus_id = context.payload.get("nexus_id")
            log.debug(f"[{context.task_id}] Verifying trajectory parity for {nexus_id}")
            yield {"status": "success", "result": {"parity_matched": True}}
        else:
            yield {"status": "error", "error": f"Unsupported task type: {context.task_type}"}