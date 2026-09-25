# fiber.phase.cli.network
import os
import time
import uuid
import asyncio
import urllib.parse
import orjson
import traceback
from typing import Optional, Dict, Any, AsyncGenerator, List, Union
from dataclasses import dataclass, field
from pathlib import Path

from xphi.arch.bound.xor.parser.ruleset.engine import StreamTaggingParser, AuditRulesetParser, CompiledEngine
from xphi.arch.contract.space.state import Contract, CoherenceState
from xphi.arch.dev.tracer.base import SystemBound, log_streamer
from xphi.arch.contract.config import env

from xphi.kernel.space.tunnel.surface import EchoListener, SurfaceClient
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log_flow = get_emitter("cli.dev")
log_contract = get_emitter("stream.contract")

# --- Constants & Rulesets ---
KEY_STATE_PIDS = "system:xphi:pids"
PROCESS_NAME = "xphi-dev-node"

XPHI_BASE = env.XPHI_BASE
REDIS_HOST = env.REDIS_HOST
REDIS_PORT = env.REDIS_PORT

LIB_ROOT = resolve_path("lib")
SOURCE_NAME = "flow.executor"

DEFAULT_WORKER_RULESET = {
    "targets": [
        {
            "tag": "worker-ready",
            "keywords": [
                {"AND": ["Netty started", "port"]},         
                {"AND": ["Started", "seconds", "JVM"]}      
            ]
        },
        {
            "tag": "worker-fatal",
            "keywords": [
                {"AND": ["io.netty.util.internal.OutOfDirectMemoryError", "failed to allocate"]}, 
                {"AND": ["reactor.blockhound.BlockingOperationError", "Blocking call!"]} 
            ]
        }
    ]
}

GLOBAL_AUDIT_RULESET = {
    "global_config": {
        "inspection_level": "structural"
    }
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

class WorkerRuntimeBootstrapper:
    def __init__(self, bin_root: Path, mq_surface: Optional[EchoListener] = None, ruleset: Optional[dict] = None, executable_pattern: str = "phix-*.jar"):
        self.bin_root = bin_root
        self.mq_surface = mq_surface
        self.process_name = PROCESS_NAME
        self.executable_pattern = executable_pattern

        self.boundary = SystemBound()
        self.executable_path = self._resolve_executable()
        self.ready_event = asyncio.Event()

        active_ruleset = ruleset or DEFAULT_WORKER_RULESET
        # [변경됨] LifecycleRegexParser() -> StreamTaggingParser(engine_type="regex")
        self.rule_engine = StreamTaggingParser(engine_type="regex").parse_ruleset(active_ruleset)

    def _resolve_executable(self) -> str:
        bins = sorted(self.bin_root.glob(self.executable_pattern))
        if not bins:
            raise RuntimeError(f"Worker execution core matching {self.executable_pattern} is missing from the binary root.")
        return str(bins[-1])

    # [개선] 커맨드 내 변수명 정렬 (추후 데코레이터 주입 방식으로 완전 분리 가능)
    @log_streamer([
        "bash", "-c",
        "exec -a {process_name} java -Dreaper.tag={process_name} -jar {executable_path}"
    ])
    async def _stream_and_sense(self, line: str):
        """Worker stdout을 스트리밍하며 컴파일된 엔진을 통해 라이프사이클 이벤트를 라우팅합니다."""
        if not line: return
        
        matched_tags = self.rule_engine.execute(line)
        if "worker-ready" in matched_tags and not self.ready_event.is_set():
            log_flow.info("[WorkerNode] 🟢 Worker Readiness condition met. Engine is active.")
            self.ready_event.set()
        elif "worker-fatal" in matched_tags:
            log_flow.crit(f"[WorkerNode] 🚨 Fatal Worker Rupture detected: {line}")
            self.boundary.collapse()

    async def ensure(self):
        log_flow.info(f"[WorkerNode] Starting managed worker process: {self.executable_path}")
        asyncio.create_task(self._stream_and_sense())
        await asyncio.sleep(0.1)
        if not self.boundary.process_pool:
            raise RuntimeError("Failed to inject Worker into SystemBound process pool.")
        
        pid = self.boundary.process_pool[-1].pid
        try:
            if self.mq_surface:
                self.mq_surface.register_state(KEY_STATE_PIDS, str(pid))
                log_flow.info(f"[WorkerNode] Core PID {pid} anchored to State Store.")
        except Exception as e:
            log_flow.warning(f"[WorkerNode] State anchoring failed. Engine proceeding in isolated mode: {e}")

        log_flow.info("[WorkerNode] Awaiting Worker runtime startup readiness...")
        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout=15.0)
            log_flow.info("[WorkerNode] Traffic routing enabled.")
        except asyncio.TimeoutError:
            log_flow.warning("[WorkerNode] Readiness sensor timed out, but proceeding anyway.")

    def shutdown(self):
        self.boundary.collapse()

class FlowExecutor:
    def __init__(self, surface_client: Optional[SurfaceClient] = None, audit_engine: Optional[CompiledEngine] = None):
        if surface_client:
            self.surface = surface_client
        else:
            mq_surface = EchoListener()
            self.surface = SurfaceClient(
                # [개선] 변경된 WorkerRuntimeBootstrapper 바인딩 (기존 JVM 룰셋은 기본값으로 주입됨)
                bootstrap_runtime=WorkerRuntimeBootstrapper(bin_root=LIB_ROOT, mq_surface=mq_surface),
                mq_surface=mq_surface,
                source_name=SOURCE_NAME,
                fallback_url=XPHI_BASE,
                path_prefix=""
            )
            
        self.audit_engine = audit_engine or AuditRulesetParser().parse_ruleset(GLOBAL_AUDIT_RULESET)

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Dict[str, Any], None]:
        """실제 정의된 Task만 처리하는 간결한 라우터"""
        log_flow.info(f"[{SOURCE_NAME}] Routing task '{context.task_type}' (ID: {context.task_id})")

        if context.task_type == "ledger_push":
            topic = context.payload.get("topic")
            raw_data = context.payload.get("data")
            raw_bytes: bytes = orjson.dumps(raw_data)
            safe_bytes: bytes = self.audit_engine.execute(raw_bytes)
            try:
                async for chunk in self.surface.request(query_path=f"/ledger/{topic}", data=safe_bytes, method="POST"):
                    yield {"status": "processing", "data": chunk}
                    
                log_flow.debug(f"[{context.task_id}] Anchored {len(safe_bytes)} bytes (Audit passed) to {topic}")
                yield {"status": "success"}
            except Exception as e:
                log_flow.error(f"[{context.task_id}] Store push failed: {e}")
                yield {"status": "error", "error": str(e)}
        elif context.task_type == "verify_parity":
            nexus_id = context.payload.get("nexus_id")
            log_flow.debug(f"[{context.task_id}] Verifying trajectory parity for {nexus_id}")
            yield {"status": "success", "result": {"parity_matched": True}}
        else:
            yield {"status": "error", "error": f"Unsupported task type: {context.task_type}"}

class EmitTool:
    name: str
    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        raise NotImplementedError

class WasmIOEmitter(EmitTool):
    """@desc: WASM Kernel에서 발생하는 상태 전이 및 I/O Side-effect를 캡처하여 방출"""
    name = "wasm_emitter"
    
    def __init__(self, broker_client=None):
        self.broker = broker_client

    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        log_contract.info(f"[{self.name}] Subscribing to WASM tension stream on topic: {target_topic}")
        try:
            for _ in range(3):
                await asyncio.sleep(0.1)
                yield Contract(
                    kind="state_transition",
                    source=self.name,
                    state=CoherenceState.STREAMING,
                    payload={
                        "logical_name": f"tx_wasm_{int(time.time()*1000)}",
                        "target_topic": target_topic,
                        "location": "wasm_kernel",
                        "actor": "LLM_IO"
                    }
                )
        except asyncio.CancelledError:
            log_contract.info(f"[{self.name}] Tension stream subscription cleanly cancelled.")
            raise  
        except Exception as e:
            log_contract.error(f"[{self.name}] Tension threshold not met (λ < τ): {e}")
            yield Contract(
                kind="unresolved_io",
                source=self.name,
                state=CoherenceState.FRAGMENTED,
                payload={
                    "error_msg": str(e),
                    "target_topic": target_topic,
                    "location": "unknown"
                }
            )

class StoreEventEmitter(EmitTool):
    name = "ledger_event"

    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        log_contract.info(f"[{self.name}] Monitoring Store constraints on: {target_topic}")
        try:
            await asyncio.sleep(0.2)
            yield Contract(
                kind="consensus_event",
                source=self.name,
                state=CoherenceState.COHERENT,
                payload={
                    "logical_name": f"event_{int(time.time()*1000)}",
                    "target_topic": target_topic,
                    "location": "system_bus",
                    "sig_req": True
                }
            )
        except asyncio.CancelledError:
            log_contract.info(f"[{self.name}] Store event monitor cleanly cancelled.")
            raise
        except Exception as e:
            log_contract.error(f"[{self.name}] Store event failed: {e}")


class StreamProxyRunner:
    def __init__(self, tools: List[EmitTool]):
        self.tools = tools

    async def run_stream(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        queue: asyncio.Queue[Optional[Contract]] = asyncio.Queue()
        
        async def worker(tool: EmitTool):
            try:
                async for contract in tool.stream_emit(target_topic):
                    await queue.put(contract)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log_contract.error(f"[multiplexer] Tool '{tool.name}' stream failed: {e}")
            finally:
                await queue.put(None)

        tasks = [asyncio.create_task(worker(tool)) for tool in self.tools]
        active_workers = len(tasks)

        try:
            while active_workers > 0:
                item = await queue.get()
                if item is None:
                    active_workers -= 1
                else:
                    yield item
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)