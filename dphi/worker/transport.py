# fiber.dphi.worker.transport
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional
import httpx

from fiber.phase.cli.network import WorkerRuntimeBootstrapper
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.tunnel.surface import EchoListener, SurfaceClient

# 통합된 모듈로 로거 네임스페이스 통일
log = get_emitter("worker.transport")

# =====================================================================
# 1. Dummy Process (For Backward Compatibility)
# =====================================================================
class DummyProcess:
    """
    기존 WorkerConnector가 transport.process.pid에 접근하여 로깅하는 
    하위 호환성을 유지하기 위한 초경량 더미 객체입니다.
    """
    def __init__(self, pid: int = -1):
        self.pid = pid
        self.returncode = None


# =====================================================================
# 2. Network Transport (Async HTTP Multiplexing)
# =====================================================================
class NetworkTransport:
    """
    Asynchronous HTTP/Network multiplexing transport layer for WorkerConnector.
    
    Supports two distinct operational models:
      1. Managed Mode: Bootstraps and manages a local binary via SurfaceClient.
      2. Unmanaged Mode: Proxies traffic directly to an external/loopback HTTP endpoint.
    """
    def __init__(self, execution_target: str, handle_id: str, executable_pattern: str = "phix-*.jar"):
        self.execution_target = str(execution_target)
        self.handle_id = handle_id
        
        # 내부 스트림 처리를 위한 비동기 큐 (VirtualProcess 제거됨)
        self._response_queue: asyncio.Queue = asyncio.Queue()
        self.process = DummyProcess()
        
        # Determine operational mode based on the target schema
        self.is_managed = not self.execution_target.startswith("http")
        
        if self.is_managed:
            self.mq_surface = EchoListener()
            self.bootstrapper = WorkerRuntimeBootstrapper(
                bin_root=Path(self.execution_target),
                mq_surface=self.mq_surface,
                executable_pattern=executable_pattern
            )
            self.surface_client: Optional[SurfaceClient] = None
            self.http_client = None
        else:
            self.bootstrapper = None
            self.surface_client = None
            self.http_client = httpx.AsyncClient(timeout=30.0)

    async def start(self):
        if self.is_managed:
            log.info(f"[Transport:{self.handle_id}] Booting managed network worker node...")
            await self.bootstrapper.ensure()
            
            if self.bootstrapper.boundary.process_pool:
                self.process.pid = self.bootstrapper.boundary.process_pool[-1].pid
                
            self.surface_client = SurfaceClient(
                bootstrap_runtime=self.bootstrapper,
                mq_surface=self.mq_surface,
                source_name=f"connector.{self.handle_id}",
                fallback_url="http://localhost:8079", 
                path_prefix=""
            )
            log.info(f"[Transport:{self.handle_id}] Managed worker ready (PID: {self.process.pid}). Surface routing active.")
        else:
            log.info(f"[Transport:{self.handle_id}] Attaching to unmanaged remote endpoint: {self.execution_target}")
            self.process.pid = 99999  # Ephemeral pseudo-PID for remote targets
            log.info(f"[Transport:{self.handle_id}] Unmanaged worker attached. Direct HTTP multiplexing active.")

    async def send_payload(self, safe_payload: Dict[str, Any]):
        try:
            raw_bytes = json.dumps(safe_payload).encode('utf-8')
            req_id = safe_payload.get("id", "unknown")
        except TypeError as e:
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failure: {e}", exc_info=True)
            raise RuntimeError(f"Serialization Exception: {e}")

        # Non-blocking Multiplexing
        asyncio.create_task(self._dispatch_http_request(req_id, raw_bytes))

    async def _dispatch_http_request(self, req_id: str, data: bytes):
        try:
            if self.is_managed:
                target_path = "/mcp/invoke"
                response_buffer = []
                async for chunk in self.surface_client.request(query_path=target_path, data=data, method="POST"):
                    response_buffer.append(chunk)
                full_response = "".join(response_buffer)
            else:
                res = await self.http_client.post(
                    self.execution_target, 
                    content=data, 
                    headers={"Content-Type": "application/json"}
                )
                
                if res.status_code not in (200, 202):
                    error_fallback = json.dumps({
                        "jsonrpc": "2.0", 
                        "id": req_id, 
                        "error": {"code": res.status_code, "message": f"HTTP {res.status_code}: {res.text}"}
                    })
                    await self._response_queue.put(error_fallback)
                    return
                    
                full_response = res.text
            
            await self._response_queue.put(full_response)
            
        except Exception as e:
            log.error(f"[Transport:{self.handle_id}] Egress HTTP Dispatch Fractured [{req_id}]: {e}")
            error_fallback = json.dumps({
                "jsonrpc": "2.0", 
                "id": req_id, 
                "error": {"code": -32000, "message": f"Transport Egress Error: {str(e)}"}
            })
            await self._response_queue.put(error_fallback)

    async def receive_raw(self) -> str:
        """Ephemeral mode extraction: Synchronously awaits the egress response."""
        raw_output = await self._response_queue.get()
        if raw_output is None:
            raise RuntimeError(f"HTTP virtual stream closed for handle: {self.handle_id}.")
        return raw_output

    async def read_egress_stream(self) -> bytes:
        """[통합/개선] 큐의 데이터를 읽어 Stdio와 동일한 바이트 스트림 반환 (다형성 지원)"""
        item = await self._response_queue.get()
        if item is None:
            return b""  # EOF
        return item.encode('utf-8') + b"\n"

    async def close(self):
        log.info(f"[Transport:{self.handle_id}] Initiating graceful teardown of network transport...")
        if self.is_managed and self.bootstrapper:
            self.bootstrapper.shutdown()
        elif not self.is_managed and self.http_client:
            await self.http_client.aclose()
            
        await self._response_queue.put(None)


# =====================================================================
# 3. Stdio Transport (Legacy Subprocess Sandbox)
# =====================================================================
class StdioTransport:
    """
    Subprocess-based transport layer for standard WorkerConnector.
    Handles raw POSIX standard I/O (STDIN/STDOUT/STDERR).
    """
    def __init__(self, command: str, handle_id: str):
        self.command = command
        self.handle_id = handle_id
        self.process: Optional[asyncio.subprocess.Process] = None

    async def start(self):
        log.info(f"[Transport:{self.handle_id}] Booting legacy sandbox: {self.command}")
        self.process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        asyncio.create_task(self._monitor_stderr())
        log.info(f"[Transport:{self.handle_id}] Sandbox running (PID: {self.process.pid})")

    async def _monitor_stderr(self):
        """JSON 스트림을 파싱하여 중앙 로그 시스템으로 릴레이"""
        while self.process and not self.process.stderr.at_eof():
            try:
                line = await self.process.stderr.readline()
                if not line:
                    continue
                    
                raw_str = line.decode('utf-8').strip()
                if not raw_str:
                    continue
                
                try:
                    log_data = json.loads(raw_str)
                    level = log_data.get("level", "INFO").upper()
                    msg = log_data.get("message", "")
                    worker_req_id = log_data.get("req_id")
                    logger_name = log_data.get("logger", "worker")
                    
                    log_ctx = {
                        "handle_id": self.handle_id, 
                        "worker_req_id": worker_req_id, 
                        "worker_source": logger_name
                    }
                    
                    if level == "DEBUG":
                        log.debug(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level in ("WARN", "WARNING"):
                        log.warning(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level == "ERROR":
                        log.error(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level == "CRITICAL":
                        log.critical(f"[Sandbox] {msg}", extra=log_ctx)
                    else:
                        log.info(f"[Sandbox] {msg}", extra=log_ctx)
                        
                except json.JSONDecodeError:
                    log.warning(f"[Sandbox:RAW] {raw_str}", extra={"handle_id": self.handle_id})
                    
            except Exception as e:
                log.error(f"[Transport:{self.handle_id}] STDERR Relay fractured: {e}")
                break

    async def send_payload(self, safe_payload: Dict[str, Any]):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError(f"Legacy process {self.handle_id} is dead.")
            
        try:
            raw_msg = json.dumps(safe_payload) + "\n"
        except TypeError as e:
            safe_keys = list(safe_payload.keys())
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failed: {e}. Top-level Keys: {safe_keys}", exc_info=True)
            raise RuntimeError(f"Serialization failed for transport payload: {e}")

        self.process.stdin.write(raw_msg.encode('utf-8'))
        await self.process.stdin.drain()

    async def receive_raw(self) -> str:
        """Ephemeral 모드 전용: STDOUT을 동기적으로 읽음"""
        raw_output = await self.process.stdout.readline()
        if not raw_output:
            raise RuntimeError(f"EOF reached while reading stdout for {self.handle_id}.")
        return raw_output.decode('utf-8')

    async def read_egress_stream(self) -> bytes:
        """[통합/개선] STDOUT 파이프에서 한 줄을 읽어 반환 (다형성 지원)"""
        if self.process and self.process.stdout:
            return await self.process.stdout.readline()
        return b""

    async def close(self):
        if self.process and self.process.returncode is None:
            log.info(f"[Transport:{self.handle_id}] Terminating sandbox (PID: {self.process.pid})")
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                self.process.kill()