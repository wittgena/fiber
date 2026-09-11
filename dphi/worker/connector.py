# fiber.dphi.worker.connector
## @lineage: fiber.agent.worker.connector
## @lineage: fiber.dphi.edge.gateway.connector
## @lineage: fiber.dphi.infra.gateway.connector
import os
import sys
import json
import asyncio
import logging
import argparse
from typing import Dict, Any, Optional

from fiber.dphi.eco.client.rpc import InternalRpcClient
from fiber.dphi.edge.gateway.quarantine import QuarantineRegistry

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("gateway.connector")

class LegacyTransport:
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
                    # 워커가 발생시킨 JSON 포맷의 구조화된 로그 파싱
                    log_data = json.loads(raw_str)
                    
                    level = log_data.get("level", "INFO").upper()
                    msg = log_data.get("message", "")
                    worker_req_id = log_data.get("req_id")
                    logger_name = log_data.get("logger", "worker")
                    
                    # Trace용 Context 포맷팅
                    log_ctx = {
                        "handle_id": self.handle_id, 
                        "worker_req_id": worker_req_id, 
                        "worker_source": logger_name
                    }
                    
                    # 릴레이 발행
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
                    # JSON 형식이 아닌 에러(C 레벨 세그폴트, 트레이스백 등)는 원문 그대로 방출
                    log.warning(f"[Sandbox:RAW] {raw_str}", extra={"handle_id": self.handle_id})
                    
            except Exception as e:
                log.error(f"[Transport:{self.handle_id}] STDERR Relay fractured: {e}")
                break

    async def send_payload(self, safe_payload: Dict[str, Any]):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError(f"Legacy process {self.handle_id} is dead.")
            
        try:
            # [보강 1] 직렬화 실패 지점 가시성 확보 (AnyUrl 에러 등 추적용)
            raw_msg = json.dumps(safe_payload) + "\n"
        except TypeError as e:
            safe_keys = list(safe_payload.keys())
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failed: {e}. Top-level Keys: {safe_keys}", exc_info=True)
            raise RuntimeError(f"Serialization failed for transport payload: {e}")

        self.process.stdin.write(raw_msg.encode('utf-8'))
        await self.process.stdin.drain()

    async def receive_raw(self) -> str:
        """ephemeral 모드 전용: STDOUT을 동기적(Await)으로 읽음"""
        raw_output = await self.process.stdout.readline()
        if not raw_output:
            raise RuntimeError(f"EOF reached while reading stdout for {self.handle_id}.")
        return raw_output.decode('utf-8')

    async def close(self):
        if self.process and self.process.returncode is None:
            log.info(f"[Transport:{self.handle_id}] Terminating sandbox (PID: {self.process.pid})")
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                self.process.kill()


class WorkerConnector:
    def __init__(self, target_id: str, legacy_command: str, mode: str = "ephemeral"):
        self.target_id = target_id
        self.legacy_command = legacy_command
        self.mode = mode.lower()
        
        if self.mode not in ("ephemeral", "linear", "multiplex"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be ephemeral, linear, or multiplex.")
        
        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False
        
        self.quarantine = QuarantineRegistry.get_adapter(self.target_id)
        
        self.active_sandboxes: Dict[str, LegacyTransport] = {}
        self.shared_transport: Optional[LegacyTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.linear_lock = asyncio.Lock()

    async def run(self):
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)
        
        self.running = True
        log.info(f"[Connector:{self.target_id}] 🚀 Listening for Intents on DPHI Bus (Mode: {self.mode.upper()})")

        if self.mode in ("linear", "multiplex"):
            self.shared_transport = LegacyTransport(self.legacy_command, f"shared-{self.target_id}")
            await self.shared_transport.start()
            asyncio.create_task(self._shared_stdout_listener())

        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg and msg["type"] == "message":
                    intent_data = json.loads(msg["data"])
                    asyncio.create_task(self.process_intent(intent_data))
                    
        except asyncio.CancelledError:
            log.info("[Connector] Shutdown signal received.")
        except Exception as e:
            log.error(f"[Connector] Fatal Bus Error: {e}", exc_info=True)
        finally:
            await pubsub.unsubscribe(self.listen_channel)
            await pubsub.close()
            
            if self.shared_transport:
                await self.shared_transport.close()
            for transport in self.active_sandboxes.values():
                await transport.close()

    async def _shared_stdout_listener(self):
        """linear/multiplex 모드에서 백그라운드로 STDOUT을 수신하여 Future를 Resolve"""
        while self.running and self.shared_transport and self.shared_transport.process:
            try:
                raw_output = await self.shared_transport.process.stdout.readline()
                if not raw_output:
                    break
                
                raw_str = raw_output.decode('utf-8')
                response = self.quarantine.translate_egress(raw_str)
                req_id = response.get("id")

                if response.get("method") == "rpc_delegate":
                    asyncio.create_task(self._handle_agent_rpc_delegation(response))
                    continue

                if response.get("method") and "elicitation" in response.get("method", ""):
                    log.warning(f"[Connector] ⏸️ TRAP: Elicitation (YIELD) detected for {req_id}")
                    if self.mode == "linear":
                        log.error(f"🚨 FATAL: Linear mode worker yielded! This blocks the entire queue.")
                    
                    await self.rpc.call("mcp.bridge.resolve_state", {
                        "handle_id": req_id,
                        "status": "YIELD",
                        "executable_payload": response
                    })
                    
                    future = self.pending_requests.pop(req_id, None)
                    if future and not future.done():
                        future.cancel()
                    continue
                
                future = self.pending_requests.pop(req_id, None)
                if future and not future.done():
                    future.set_result(response)
                    
            except Exception as e:
                log.error(f"[Connector] Shared Listener Fracture: {e}")
                await asyncio.sleep(0.1)

    async def _handle_agent_rpc_delegation(self, rpc_req: Dict[str, Any]):
        call_id = rpc_req.get("id")
        params = rpc_req.get("params", {})
        target_method = params.get("target_method")
        payload = params.get("data", {})

        try:
            core_res = await self.rpc.call(target_method, payload)
            feedback_payload = {"jsonrpc": "2.0", "id": call_id, "result": core_res}
            await self.shared_transport.send_payload(feedback_payload)
        except Exception as e:
            log.error(f"[Connector] Failed to delegate agent RPC: {e}")
            error_payload = {"jsonrpc": "2.0", "id": call_id, "error": {"code": -32000, "message": f"Core delegation failure: {str(e)}"}}
            await self.shared_transport.send_payload(error_payload)

    async def process_intent(self, intent_data: Dict[str, Any]):
        handle_id = intent_data.get("handle_id")
        action = intent_data.get("action", "EXECUTE")
        payload = intent_data.get("payload", {})

        if not handle_id:
            return

        try:
            if action == "EXECUTE":
                if self.mode == "ephemeral":
                    await self._execute_ephemeral(handle_id, payload)
                elif self.mode == "linear":
                    await self._execute_shared(handle_id, payload, use_lock=True)
                elif self.mode == "multiplex":
                    await self._execute_shared(handle_id, payload, use_lock=False)
            elif action == "RESUME":
                if self.mode == "linear":
                    log.warning(f"RESUME not supported in Linear Mode. Ignoring {handle_id}.")
                    return
                elif self.mode == "multiplex":
                    log.info(f"[Connector] Multiplexing RESUME to Shared Transport: {handle_id}")
                    await self._resume_multiplex(handle_id, payload)
                elif self.mode == "ephemeral":
                    transport = self.active_sandboxes.get(handle_id)
                    if not transport:
                        log.error(f"Cannot RESUME {handle_id}: Sandbox not found.")
                        return
                    log.info(f"[Connector] Resuming Parked Intent: {handle_id}")
                    await self._cycle_io(handle_id, payload, transport)
            elif action in ("RESUME_OR_KILL", "FORCE_ROLLBACK"):
                if self.mode in ("linear", "multiplex"):
                    future = self.pending_requests.pop(handle_id, None)
                    if future and not future.done():
                        future.cancel()
                    await self._report_fault(handle_id, "SYSTEM_SENTINEL_TIMEOUT: Request aborted.")
                else:
                    transport = self.active_sandboxes.get(handle_id)
                    if transport:
                        log.warning(f"⚠️ [Connector] Sentinel enforced ROLLBACK on {handle_id}")
                        await self._cycle_io(handle_id, payload, transport, is_rollback=True)

        except Exception as e:
            log.error(f"[Connector] Lifecycle Crash for {handle_id}: {e}", exc_info=True)
            await self._report_fault(handle_id, str(e))
            if self.mode == "ephemeral":
                await self._destroy_sandbox(handle_id)

    async def _execute_shared(self, handle_id: str, payload: Dict[str, Any], use_lock: bool):
        payload["id"] = handle_id
        safe_payload = self.quarantine.translate_ingress(payload)
        
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_requests[handle_id] = future
        
        if use_lock:
            async with self.linear_lock:
                log.debug(f"[Connector] Linear Enqueue: {handle_id}")
                await self.shared_transport.send_payload(safe_payload)
                await self._wait_and_resolve_shared(handle_id, future)
        else:
            log.debug(f"[Connector] Multiplexing Intent: {handle_id}")
            await self.shared_transport.send_payload(safe_payload)
            await self._wait_and_resolve_shared(handle_id, future)

    async def _resume_multiplex(self, handle_id: str, payload: Dict[str, Any]):
        payload["id"] = handle_id
        payload["action"] = "RESUME"
        safe_payload = self.quarantine.translate_ingress(payload)
        
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_requests[handle_id] = future
        
        await self.shared_transport.send_payload(safe_payload)
        await self._wait_and_resolve_shared(handle_id, future)

    async def _wait_and_resolve_shared(self, handle_id: str, future: asyncio.Future):
        try:
            response = await future
            
            # [보강 2] 에러 응답 은닉 방지 - Sandbox의 에러 내역 상세 로깅
            if "error" in response:
                status = "FAULTED"
                log.error(f"[Connector:FAULT] Shared Sandbox execution failed. Error details: {response['error']}", extra={"handle_id": handle_id})
            else:
                status = "RESOLVED"
                
            log.info(f"[Connector] ⏹️ Shared Intent {handle_id} {status}.")
            
            await self.rpc.call("mcp.bridge.resolve_state", {
                "handle_id": handle_id,
                "status": status,
                "executable_payload": response
            })
        except asyncio.CancelledError:
            log.warning(f"Shared request {handle_id} was cancelled or yielded.")

    async def _execute_ephemeral(self, handle_id: str, payload: Dict[str, Any]):
        if handle_id in self.active_sandboxes:
            log.warning(f"[Connector] Duplicate EXECUTE ignored for {handle_id}")
            return
        
        transport = LegacyTransport(self.legacy_command, handle_id)
        await transport.start()
        self.active_sandboxes[handle_id] = transport
        
        log.debug(f"[Connector] Injecting New Ephemeral Intent: {handle_id}")
        await self._cycle_io(handle_id, payload, transport)

    async def _cycle_io(self, handle_id: str, payload: Dict[str, Any], transport: LegacyTransport, is_rollback: bool = False):
        try:
            safe_payload = self.quarantine.translate_ingress(payload)
            await transport.send_payload(safe_payload)
            
            raw_output = await transport.receive_raw()
            response = self.quarantine.translate_egress(raw_output)

            while response.get("method") == "rpc_delegate":
                target_method = response["params"].get("target_method")
                req_data = response["params"].get("data", {})
                call_id = response.get("id")
                
                try:
                    core_res = await self.rpc.call(target_method, req_data)
                    await transport.send_payload({"jsonrpc": "2.0", "id": call_id, "result": core_res})
                except Exception as e:
                    await transport.send_payload({
                        "jsonrpc": "2.0", "id": call_id, 
                        "error": {"code": -32000, "message": f"Core delegation failure: {e}"}
                    })
                    
                raw_output = await transport.receive_raw()
                response = self.quarantine.translate_egress(raw_output)

            if response.get("method") and "elicitation" in response.get("method", ""):
                log.warning(f"[Connector] ⏸️ TRAP: Elicitation detected. Parking {handle_id} (YIELD).")
                await self.rpc.call("mcp.bridge.resolve_state", {
                    "handle_id": handle_id,
                    "status": "YIELD",
                    "executable_payload": response
                })
            else:
                # [보강 3] 에러 응답 은닉 방지 - Sandbox의 에러 내역 상세 로깅 (Ephemeral)
                if is_rollback or "error" in response:
                    status = "FAULTED"
                    if "error" in response:
                        log.error(f"[Connector:FAULT] Ephemeral Sandbox execution failed. Error details: {response['error']}", extra={"handle_id": handle_id})
                else:
                    status = "RESOLVED"
                    
                log.info(f"[Connector] ⏹️ Intent {handle_id} {status}.")
                
                await self.rpc.call("mcp.bridge.resolve_state", {
                    "handle_id": handle_id,
                    "status": status,
                    "executable_payload": response
                })
                await self._destroy_sandbox(handle_id)
                
        except Exception as e:
            raise RuntimeError(f"IO Cycle Failed: {e}")

    async def _destroy_sandbox(self, handle_id: str):
        transport = self.active_sandboxes.pop(handle_id, None)
        if transport:
            await transport.close()

    async def _report_fault(self, handle_id: str, error_detail: str):
        try:
            await self.rpc.call("mcp.bridge.resolve_state", {
                "handle_id": handle_id,
                "status": "FAULTED",
                "error_detail": error_detail
            })
        except Exception as rpc_e:
            log.critical(f"[Connector] Failed to report FAULT to Core: {rpc_e}")

def main():
    parser = argparse.ArgumentParser(description="Fiber Worker Egress Sidecar Connector")
    parser.add_argument("--target", required=True, help="Target ID (e.g., db-server-01)")
    parser.add_argument("--exec", required=True, help="Legacy command (e.g., 'python -m agent.finlib')")
    parser.add_argument(
        "--mode", 
        default="ephemeral", 
        choices=["ephemeral", "linear", "multiplex"], 
        help="Execution mode: ephemeral (isolation), linear (sequential queue), multiplex (async routing)"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    connector = WorkerConnector(target_id=args.target, legacy_command=args.exec, mode=args.mode)
    
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        log.info("[Connector] Exiting gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()