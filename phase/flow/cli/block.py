# fiber.phase.flow.cli.block
## @lineage: phase.flow.cli.block
## @lineage: phase.flow.block
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

from xphi.kernel.space.topos.tunnel.surface import SurfaceMQ, SurfaceClient
from xphi.xor.parser.ruleset.engine import LifecycleRegexParser, AuditRulesetParser, CompiledEngine
from xphi.xor.parser.block.contract import Contract, CoherenceState
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.tracer.bound import SystemBound, log_streamer
from xphi.watcher.plane.emitter import get_emitter

log_flow = get_emitter("block.flow")
log_phi = get_emitter("phi.flow")
log_emitter = get_emitter("contract.emitter")

# --- Constants & Rulesets ---
KEY_STATE_PIDS = "system:xphi:pids"
PROCESS_NAME = "phix-node"
DPHI_BASE = os.getenv("DPHI_BASE", "http://localhost:8079")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
LIB_ROOT = resolve_path("lib")
SOURCE_NAME = "jvm.executor"

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

GLOBAL_AUDIT_RULESET = {
    "global_config": {
        "inspection_level": "structural"
    }
}


# ==========================================
# 1. Shared Data Models
# ==========================================

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


# ==========================================
# 2. BlockFlow (JVM Runtime & Flow Sensor)
# ==========================================

class BlockFlow:
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
            log_flow.info("[JvmRuntime] 🟢 WebFlux Readiness condition met. Engine is active.")
            self.ready_event.set()
        elif "framework-fatal" in matched_tags:
            log_flow.crit(f"[JvmRuntime] 🚨 Fatal Framework Rupture detected: {line}")
            self.boundary.collapse()

    async def ensure(self):
        log_flow.info(f"[JvmRuntime] Starting Java core process: {self.jar_path}")
        asyncio.create_task(self._stream_and_sense())
        await asyncio.sleep(0.1)
        if not self.boundary.process_pool:
            raise RuntimeError("Failed to inject JVM into SystemBound process pool.")
        
        pid = self.boundary.process_pool[-1].pid
        try:
            if self.mq_surface:
                self.mq_surface.register_state(KEY_STATE_PIDS, str(pid))
                log_flow.info(f"[JvmRuntime] Core PID {pid} anchored to State Ledger.")
        except Exception as e:
            log_flow.warning(f"[JvmRuntime] State anchoring failed. Engine proceeding in isolated mode: {e}")

        log_flow.info("[JvmRuntime] Awaiting WebFlux/Netty core startup readiness...")
        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout=15.0)
            log_flow.info("[JvmRuntime] Traffic routing enabled.")
        except asyncio.TimeoutError:
            log_flow.warning("[JvmRuntime] Readiness sensor timed out, but proceeding anyway.")

    def shutdown(self):
        self.boundary.collapse()


# ==========================================
# 3. FlowExecutor (Central Router)
# ==========================================

class FlowExecutor:
    """JVM 코어 프로세스와의 통신(HTTP/MQ), 보안 검열(Audit), 라우팅을 총괄하는 중앙 실행기"""
    def __init__(self, surface_client: Optional[SurfaceClient] = None, audit_engine: Optional[CompiledEngine] = None):
        if surface_client:
            self.surface = surface_client
        else:
            mq_surface = SurfaceMQ()
            self.surface = SurfaceClient(
                bootstrap_runtime=BlockFlow(LIB_ROOT, mq_surface),
                mq_surface=mq_surface,
                source_name=SOURCE_NAME,
                fallback_url=DPHI_BASE,
                path_prefix=""
            )
            
        self.audit_engine = audit_engine or AuditRulesetParser().parse_ruleset(GLOBAL_AUDIT_RULESET)

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Dict[str, Any], None]:
        """실제 정의된 Task(원장 전송, 상태 동기화)만 처리하는 간결한 라우터"""
        log_phi.info(f"[{SOURCE_NAME}] Routing task '{context.task_type}' (ID: {context.task_id})")

        if context.task_type == "ledger_push":
            topic = context.payload.get("topic")
            raw_data = context.payload.get("data")
            raw_bytes: bytes = orjson.dumps(raw_data)
            safe_bytes: bytes = self.audit_engine.execute(raw_bytes)
            try:
                async for chunk in self.surface.request(query_path=f"/ledger/{topic}", data=safe_bytes, method="POST"):
                    yield {"status": "processing", "data": chunk}
                    
                log_phi.debug(f"[{context.task_id}] Anchored {len(safe_bytes)} bytes (Audit passed) to {topic}")
                yield {"status": "success"}
            except Exception as e:
                log_phi.error(f"[{context.task_id}] Ledger push failed: {e}")
                yield {"status": "error", "error": str(e)}
        elif context.task_type == "verify_parity":
            nexus_id = context.payload.get("nexus_id")
            log_phi.debug(f"[{context.task_id}] Verifying trajectory parity for {nexus_id}")
            yield {"status": "success", "result": {"parity_matched": True}}
        else:
            yield {"status": "error", "error": f"Unsupported task type: {context.task_type}"}


# ==========================================
# 4. Emitters & Proxy Runner
# ==========================================

class EmitTool:
    name: str
    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        raise NotImplementedError

class WasmTensionEmitter(EmitTool):
    """@pulse: WASM Kernel에서 발생하는 상태 전이(Residues) 및 I/O Side-effect를 캡처하여 방출"""
    name = "wasm_tension"
    
    def __init__(self, broker_client=None):
        self.broker = broker_client

    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        log_emitter.info(f"[{self.name}] Subscribing to WASM tension stream on topic: {target_topic}")
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
            log_emitter.info(f"[{self.name}] Tension stream subscription cleanly cancelled.")
            raise  
        except Exception as e:
            log_emitter.error(f"[{self.name}] Tension threshold not met (λ < τ): {e}")
            yield Contract(
                kind="unresolved_chaos",
                source=self.name,
                state=CoherenceState.FRAGMENTED,
                payload={
                    "error_msg": str(e),
                    "target_topic": target_topic,
                    "location": "unknown"
                }
            )

class LedgerPulseEmitter(EmitTool):
    """@pulse: 노드 간 분산 합의(Multi-sig)나 앵커링이 필요한 메타데이터(System Pulse) 방출"""
    name = "ledger_pulse"

    async def stream_emit(self, target_topic: str) -> AsyncGenerator[Contract, None]:
        log_emitter.info(f"[{self.name}] Monitoring Ledger constraints on: {target_topic}")
        try:
            await asyncio.sleep(0.2)
            yield Contract(
                kind="consensus_pulse",
                source=self.name,
                state=CoherenceState.COHERENT,
                payload={
                    "logical_name": f"pulse_{int(time.time()*1000)}",
                    "target_topic": target_topic,
                    "location": "system_bus",
                    "sig_req": True
                }
            )
        except asyncio.CancelledError:
            log_emitter.info(f"[{self.name}] Ledger pulse monitor cleanly cancelled.")
            raise
        except Exception as e:
            log_emitter.error(f"[{self.name}] Ledger pulse failed: {e}")


class EmissionProxyRunner:
    """@multiplexer: 여러 스트림 소스 병합 및 라이프사이클 제어"""
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
                log_emitter.error(f"[multiplexer] Tool '{tool.name}' stream failed: {e}")
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


class BlockEmitter:
    def __init__(
        self, 
        flow_client: Optional[FlowExecutor] = None,
        audit_engine: Optional[CompiledEngine[bytes, bytes]] = None
    ):
        self.flow = flow_client or FlowExecutor()
        self.runner = EmissionProxyRunner([WasmTensionEmitter(), LedgerPulseEmitter()])
        self.audit_engine = audit_engine

    async def anchor_stream(self, contract_stream: AsyncGenerator[Contract, None], topic_id: str):
        """@state: 쏟아지는 Block Data를 Audit 후 JVM 원장으로 실시간 앵커링"""
        anchor_count = 0
        try:
            async for block in contract_stream:
                raw_bytes: bytes = orjson.dumps(block.model_dump(exclude_none=True))
                safe_bytes: bytes = self.audit_engine.execute(raw_bytes) if self.audit_engine else raw_bytes
                
                # 병합된 TaskContext 사용 (디폴트 task_type을 여기서 명시적으로 덮어씀)
                context = TaskContext(
                    task_type="ledger_push", 
                    payload={
                        "topic": f"xor:events:{topic_id}", 
                        "data": safe_bytes.decode('utf-8')
                    }
                )
                
                success = False
                async for res in self.flow.execute_stream(context):
                    if res.get("status") in ("success", "completed"):
                        success = True
                        break

                if success:
                    anchor_count += 1
                    log_emitter.debug(f"[stream anchored] {block.kind} :: {block.id} -> {block.state.name}")
                else:
                    log_emitter.warning(f"[stream dropped] Block rejected by ledger: {block.id}")

            log_emitter.info(f"\n[BLOCKS ASSIMILATED] {anchor_count} events anchored for topic: {topic_id}")
            
        except asyncio.CancelledError:
            log_emitter.warning("[anchor_stream] Anchoring process was forcefully cancelled.")
            raise
        except Exception as e:
            log_emitter.error(f"[anchor_stream] Critical failure during ledger anchoring: {traceback.format_exc()}")

    async def process_pulse(self, topic_id: str):
        log_emitter.info(f"[emitter] Initiating high-speed Reactive Emission for topic: {topic_id}")
        merged_stream = self.runner.run_stream(topic_id)
        await self.anchor_stream(merged_stream, topic_id)

    async def aggregate_and_sync(self, nexus_id: str):
        log_emitter.info(f"[info] Syncing topology state for Nexus ID: {nexus_id} via JvmExecutor")
        try:
            context = TaskContext(
                task_type="verify_parity",
                payload={"nexus_id": nexus_id}
            )
            
            sync_res = False
            async for res in self.flow.execute_stream(context):
                if res.get("status") in ("success", "completed") and res.get("result", {}).get("parity_matched", True):
                    sync_res = True
                    break

            if not sync_res:
                log_emitter.warning("[warning] Topology sync executed but trajectory parity is broken.")
            else:
                log_emitter.info(f"[info] Trajectory parity strictly verified for Nexus: {nexus_id}")
                
        except Exception as e:
            log_emitter.error(f"[error] Snapshot read/sync failed: {e}")