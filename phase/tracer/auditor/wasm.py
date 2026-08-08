# phase.tracer.auditor.wasm
## @lineage: entry.tracer.auditor.wasm
## @lineage: receptor.surface.tracer.auditor.wasm
## @lineage: surface.tracer.auditor.wasm
## @lineage: surface.auditor.wasm
## @lineage: watcher.wasm.auditor
import asyncio
import struct
import json
from typing import Union, Any, Dict, List

from watcher.tracer.bound import BaseStreamAuditor, BaseBoundary
from watcher.plane.emitter import get_emitter
from arch.contract.event.next import next_id

class WasmTelemetryAuditor(BaseStreamAuditor):
    def __init__(self, interpreter, boundary: Union[BaseBoundary, Any]):
        super().__init__(target="wasm_memory", boundary=boundary, delay=0)
        self.interpreter = interpreter
        self.log = get_emitter("auditor.wasm_telemetry", phase="agent")
        
        self.max_depth = 0
        self.health_flag = 0
        self.is_collapsed = False
        self.poll_interval = 0.1

    async def run_stream(self) -> None:
        while not self.is_collapsed:
            try:
                store = getattr(self.interpreter, 'store', None)
                instance = getattr(self.interpreter, 'instance', None)
                if not store or not instance:
                    await asyncio.sleep(self.poll_interval)
                    continue

                get_telemetry = instance.exports(store).get("get_telemetry_ptr")
                memory = instance.exports(store).get("memory")
                
                if get_telemetry and memory:
                    ptr = get_telemetry(store)
                    raw_data = memory.read(store, ptr, ptr + 12)
                    recursion_depth, max_type_depth, health_flag = struct.unpack("<III", raw_data)
                    
                    self.max_depth = max_type_depth
                    self.health_flag = health_flag
                    
                    if health_flag == 2:  # 2 = Collapsed
                        self.is_collapsed = True
                        self.log.crit(f"  [FATAL] WASM Semantic Collapse Confirmed! (Depth: {max_type_depth})")
                        break
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

class WasmEntropyAuditor(BaseStreamAuditor):
    """@desc: [Energy Axis] Wasmtime 엔진의 Fuel(가스) 소모량을 실시간 관측"""
    def __init__(self, interpreter, boundary: Union[BaseBoundary, Any]):
        super().__init__(target="wasm_fuel", boundary=boundary, delay=0)
        self.interpreter = interpreter
        self.log = get_emitter("auditor.wasm_entropy", phase="agent")
        self.total_fuel_used = 0
        self.is_exhausted = False
        self.poll_interval = 0.5

    async def run_stream(self) -> None:
        while not self.is_exhausted:
            try:
                store = getattr(self.interpreter, 'store', None)
                if store:
                    fuel_consumed = store.fuel_consumed()
                    if fuel_consumed is not None:
                        self.total_fuel_used = fuel_consumed
                        cg_policy = getattr(getattr(self.interpreter, 'cg', None), 'policy', None)
                        fuel_limit = cg_policy.fuel_limit if cg_policy else 10_000_000
                        
                        if fuel_consumed >= fuel_limit:
                            self.is_exhausted = True
                            self.log.crit(f"  [ENTROPY FATAL] Fuel Exhausted: {fuel_consumed}/{fuel_limit}")
                            break
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)


class CanonicalProofAuditor(BaseStreamAuditor):
    """
    @desc: [Cryptographic Axis] Meta-Boundary(Tracer/Tester)로부터 직접 상태를 투영받아
           결정론적 상태 전이(Canonical Log)를 추출하고 연산 증명(Proof)을 생성합니다.
           (실행계의 로깅에 의존하는 자기 참조 모순을 제거한 순수 관측기)
    """
    def __init__(self, boundary: Union[BaseBoundary, Any] = None):
        super().__init__(target="canonical_proof", boundary=boundary, delay=0)
        self.log = get_emitter("auditor.canonical_proof", phase="agent")
        
        self.flow_id = next_id()
        self.canonical_records: List[Dict[str, Any]] = []
        
        # WasmTester 생명주기에 맞추기 위한 상태 플래그
        self.is_collapsed = False
        self.is_exhausted = False

    def project_state(self, action: str, metrics: dict) -> None:
        if not metrics:
            return
            
        fuel = metrics.get("fuel_consumed")
        if fuel is not None:
            record = {
                "action": action,
                "tier": metrics.get("tier", "UNKNOWN"),
                "fuel": fuel,
                "mem_usage": metrics.get("mem_usage_bytes"),
                "mem_peak": metrics.get("mem_peak_bytes")
            }
            # None 값 필터링 (결정론적 해싱을 위해 스키마를 정규화)
            clean_record = {k: v for k, v in record.items() if v is not None}
            self.canonical_records.append(clean_record)

    def generate_payload(self) -> str:
        return json.dumps(self.canonical_records, sort_keys=True, separators=(',', ':'))

    async def run_stream(self) -> None:
        try:
            while not (self.is_collapsed or self.is_exhausted):
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            self.canonical_records.clear()