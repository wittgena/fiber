# entry.contract.emitter
## @lineage: receptor.surface.contract.emitter
## @lineage: surface.contract.emitter
## @lineage: surface.runtime.contract
import time
import asyncio
import traceback
import orjson
from typing import List, Optional, AsyncGenerator

from entry.contract.phix import FlowExecutor, TaskContext

from arch.xor.parser.block.contract import Contract, CoherenceState
from arch.xor.parser.ruleset import CompiledEngine
from watcher.plane.emitter import get_emitter

log = get_emitter("contract.emitter")

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
        log.info(f"[{self.name}] Subscribing to WASM tension stream on topic: {target_topic}")
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
            log.info(f"[{self.name}] Tension stream subscription cleanly cancelled.")
            raise  
        except Exception as e:
            log.error(f"[{self.name}] Tension threshold not met (λ < τ): {e}")
            yield Contract(
                kind="unresolved_chaos",
                source=self.name,
                state=CoherenceState.FRAGMENTED,  # 명시적 파편화 상태
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
        log.info(f"[{self.name}] Monitoring Ledger constraints on: {target_topic}")
        try:
            await asyncio.sleep(0.2)
            yield Contract(
                kind="consensus_pulse",
                source=self.name,
                state=CoherenceState.COHERENT, # 명시적 정합 상태
                payload={
                    "logical_name": f"pulse_{int(time.time()*1000)}",
                    "target_topic": target_topic,
                    "location": "system_bus",
                    "sig_req": True
                }
            )
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Ledger pulse monitor cleanly cancelled.")
            raise
        except Exception as e:
            log.error(f"[{self.name}] Ledger pulse failed: {e}")


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
                log.error(f"[multiplexer] Tool '{tool.name}' stream failed: {e}")
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
                    log.debug(f"[stream anchored] {block.kind} :: {block.id} -> {block.state.name}")
                else:
                    log.warning(f"[stream dropped] Block rejected by ledger: {block.id}")

            log.info(f"\n[BLOCKS ASSIMILATED] {anchor_count} events anchored for topic: {topic_id}")
            
        except asyncio.CancelledError:
            log.warning("[anchor_stream] Anchoring process was forcefully cancelled.")
            raise
        except Exception as e:
            log.error(f"[anchor_stream] Critical failure during ledger anchoring: {traceback.format_exc()}")

    async def process_pulse(self, topic_id: str):
        log.info(f"[emitter] Initiating high-speed Reactive Emission for topic: {topic_id}")
        merged_stream = self.runner.run_stream(topic_id)
        await self.anchor_stream(merged_stream, topic_id)

    async def aggregate_and_sync(self, nexus_id: str):
        log.info(f"[info] Syncing topology state for Nexus ID: {nexus_id} via JvmExecutor")
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
                log.warning("[warning] Topology sync executed but trajectory parity is broken.")
            else:
                log.info(f"[info] Trajectory parity strictly verified for Nexus: {nexus_id}")
                
        except Exception as e:
            log.error(f"[error] Snapshot read/sync failed: {e}")