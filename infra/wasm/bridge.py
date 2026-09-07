# fiber.infra.wasm.bridge
import json
import threading
import ctypes
from typing import Any, Dict, List
from pathlib import Path

try:
    import wasmtime
except ImportError:
    wasmtime = None

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("wasm.bridge")

class GatewayRuptureError(Exception):
    pass

class MemoryBoundaryError(GatewayRuptureError):
    pass

class WasmBridge:
    """
    [Lock-Free Architecture]
    글로벌 락(Mutex)을 완전히 제거했습니다.
    WASM 모듈은 클래스 레벨에서 1회만 AOT 컴파일되어 공유되며,
    실행 컨텍스트(Store, Instance, Memory)는 각 비동기 워커 스레드별로 독립(Thread-Local) 할당됩니다.
    """
    _engine = None
    _module = None
    _compile_lock = threading.Lock()
    
    MAX_INPUT_SIZE = 1024 * 1024 
    
    def __init__(self, wasm_filename: str = "gateway.wasm"):
        if wasmtime is None:
            raise ImportError("wasmtime is strictly required.")
            
        # [핵심] 스레드별로 격리된 메모리 공간을 보장하는 Thread-Local Storage
        self._tls = threading.local()
        
        self._initialize_module(wasm_filename)

    @classmethod
    def _initialize_module(cls, wasm_filename: str):
        """JIT/AOT 컴파일은 애플리케이션 부트스트랩 시 1회만 수행 (Thread-Safe)"""
        with cls._compile_lock:
            if cls._engine is None:
                config = wasmtime.Config()
                cls._engine = wasmtime.Engine(config)
            
            if cls._module is None:
                wasm_path = str(resolve_path("time") / wasm_filename)
                if not Path(wasm_path).exists():
                    raise FileNotFoundError(f"Gateway WASM not found: {wasm_path}")
                cls._module = wasmtime.Module.from_file(cls._engine, wasm_path)

    def _get_local_instance(self) -> Any:
        """
        현재 스레드에 WASM 인스턴스가 없다면 새로 생성하여 캐싱합니다.
        락(Lock) 없이 100% 병렬 확장이 가능해집니다.
        """
        if not hasattr(self._tls, "is_initialized"):
            # 현재 스레드 전용 Store 및 빈 Linker 생성
            self._tls.store = wasmtime.Store(self._engine)
            linker = wasmtime.Linker(self._engine)
            
            self._tls.instance = linker.instantiate(self._tls.store, self._module)
            self._tls.memory = self._tls.instance.exports(self._tls.store)["memory"]
            
            exports = self._tls.instance.exports(self._tls.store)
            self._tls.alloc = exports.get("alloc")
            self._tls.execute_gateway = exports.get("execute_gateway")
            self._tls.dealloc_c_string = exports.get("dealloc_c_string") 
            
            # 현재 스레드 전용 1MB 버퍼(Zero-Alloc) 선할당
            self._tls.input_ptr = self._tls.alloc(self._tls.store, self.MAX_INPUT_SIZE)
            self._tls.is_initialized = True
            
        return self._tls

    def _read_c_string_fast(self, tls: Any, ptr: int) -> str:
        try:
            host_base_addr = ctypes.cast(tls.memory.data_ptr(tls.store), ctypes.c_void_p).value
            target_addr = host_base_addr + ptr
            raw_bytes = ctypes.string_at(target_addr)
            return raw_bytes.decode('utf-8', errors='replace')
        except Exception as e:
            raise GatewayRuptureError(f"C-String direct read failed: {e}")

    def invoke_raw_ffi(self, payload_str: str) -> str:
        """Lock 없이 현재 스레드의 전용 WASM 메모리를 타격"""
        payload_bytes = payload_str.encode('utf-8') + b'\0'
        req_len = len(payload_bytes)
        
        if req_len > self.MAX_INPUT_SIZE:
            raise MemoryBoundaryError(f"Payload exceeds boundary ({self.MAX_INPUT_SIZE}B).")

        # 현재 스레드의 독립된 WASM 컨텍스트 로드 (Lock-Free)
        tls = self._get_local_instance()
        res_ptr = None
        
        try:
            # 1. Thread-Local 메모리 덮어쓰기 (경합 없음)
            tls.memory.write(tls.store, payload_bytes, tls.input_ptr)
            
            # 2. Execute
            res_ptr = tls.execute_gateway(tls.store, tls.input_ptr)
            if res_ptr == 0:
                raise GatewayRuptureError("Gateway Panic: Null pointer returned.")
                
            # 3. Fast Read
            return self._read_c_string_fast(tls, res_ptr)
            
        finally:
            if res_ptr:
                try:
                    tls.dealloc_c_string(tls.store, res_ptr)
                except Exception as e:
                    log.error(f"[Gateway] C-String dealloc failed: {e}")

    def evaluate_intent(self, dimension: int, base_friction: float, raw_payload: str, state_vector: List[int]) -> Dict[str, Any]:
        req_payload = {
            "dimension": dimension,
            "base_friction": base_friction,
            "raw_payload": raw_payload,
            "state_vector": state_vector
        }
        try:
            json_str = json.dumps(req_payload, separators=(',', ':'))
            res_str = self.invoke_raw_ffi(json_str)
            return json.loads(res_str)
            
        except MemoryBoundaryError as mbe:
            log.warning(f"🛡️ [Circuit Breaker Triggered] {mbe}")
            return {"success": False, "revert_reason": "Memory Boundary Exceeded"}
        except GatewayRuptureError as gre:
            log.warning(f"⚠️ [Gateway Rupture] {gre}")
            return {"success": False, "revert_reason": str(gre)}
        except Exception as e:
            log.error(f"❌ [Gateway Fatal] {e}")
            return {"success": False, "revert_reason": "Internal Execution Error"}