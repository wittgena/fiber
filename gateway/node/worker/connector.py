# fiber.gateway.node.worker.connector
import os
import sys
import json
import asyncio
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, Protocol

from fiber.gateway.edge.rpc.client import InternalRpcClient
from fiber.gateway.node.worker.registry.quarantine import QuarantineRegistry

from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("worker.connector")

# ==========================================
# 1. Transport Protocol (Interface)
# ==========================================
class WorkerTransport(Protocol):
    """WorkerConnector가 통신 방식을 몰라도 되도록 보장하는 Duck-Typing 인터페이스"""
    async def start(self) -> None: ...
    async def send_payload(self, safe_payload: Dict[str, Any]) -> None: ...
    async def receive_raw(self) -> str: ...
    async def read_egress_stream(self) -> bytes: ...  # [개선/추가] 표준 출력 다형성 인터페이스
    async def close(self) -> None: ...
    
    # Process 모니터링 및 로깅을 위한 속성 (PID 등) - 하위 호환성 유지
    process: Any 

# ==========================================
# 2. Worker Connector
# ==========================================
class WorkerConnector:
    def __init__(self, target_id: str, execution_target: str, mode: str = "ephemeral", transport_type: str = "stdio"):
        self.target_id = target_id
        self.execution_target = execution_target
        self.mode = mode.lower()
        self.transport_type = transport_type.lower()
        
        if self.mode not in ("ephemeral", "linear", "multiplex"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be ephemeral, linear, or multiplex.")
            
        if self.transport_type not in ("stdio", "network"):
            raise ValueError(f"Invalid transport: {self.transport_type}. Must be stdio or network.")
        
        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False
        
        self.quarantine = QuarantineRegistry.get_adapter(self.target_id)
        
        self.active_sandboxes: Dict[str, WorkerTransport] = {}
        self.shared_transport: Optional[WorkerTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.linear_lock = asyncio.Lock()

    def _create_transport(self, handle_id: str) -> WorkerTransport:
        """Transport 팩토리: 설정된 타입에 따라 적절한 전송 계층 객체를 동적으로 생성"""
        # [개선] 통합된 fiber.dphi.worker.transport 모듈에서 로드
        if self.transport_type == "network":
            from fiber.gateway.node.worker.transport import NetworkTransport
            return NetworkTransport(execution_target=self.execution_target, handle_id=handle_id)
        else:
            from fiber.gateway.node.worker.transport import StdioTransport
            return StdioTransport(command=self.execution_target, handle_id=handle_id)

    async def run(self):
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)
        
        self.running = True
        log.info(f"[Connector:{self.target_id}] 🚀 Listening for Intents on DPHI Bus (Mode: {self.mode.upper()}, Transport: {self.transport_type.upper()})")

        try:
            # Shared Transport 부팅 단계
            if self.mode in ("linear", "multiplex"):
                self.shared_transport = self._create_transport(f"shared-{self.target_id}")
                await self.shared_transport.start()
                asyncio.create_task(self._shared_stdout_listener())

            # 인텐트 메시지 수신 루프
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
        """linear/multiplex 모드에서 백그라운드로 Egress Stream을 수신하여 Future를 Resolve"""
        # [개선] self.shared_transport.process 체크를 제거하고 프로토콜 자체에 의존
        while self.running and self.shared_transport:
            try:
                # [개선] process.stdout.readline() 에 직접 접근하는 추상화 누수 제거
                # 다형성 인터페이스인 read_egress_stream()을 사용하여 데이터를 읽음
                raw_output = await self.shared_transport.read_egress_stream()
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
        
        transport = self._create_transport(handle_id)
        await transport.start()
        self.active_sandboxes[handle_id] = transport
        
        log.debug(f"[Connector] Injecting New Ephemeral Intent: {handle_id}")
        await self._cycle_io(handle_id, payload, transport)

    async def _cycle_io(self, handle_id: str, payload: Dict[str, Any], transport: WorkerTransport, is_rollback: bool = False):
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

# ==========================================
# 3. CLI Entry Point
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Fiber Worker Egress Sidecar Connector")
    parser.add_argument("--target", required=True, help="Target ID (e.g., db-server-01)")
    parser.add_argument("--exec", required=True, help="Legacy command OR Binary root path (e.g., 'python -m agent', '/opt/bin')")
    parser.add_argument(
        "--mode", 
        default="ephemeral", 
        choices=["ephemeral", "linear", "multiplex"], 
        help="Execution mode: ephemeral (isolation), linear (sequential queue), multiplex (async routing)"
    )
    parser.add_argument(
        "--transport",
        default="stdio",
        choices=["stdio", "network"],
        help="Transport layer: stdio (subprocess I/O) or network (HTTP multiplexing)"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    connector = WorkerConnector(
        target_id=args.target, 
        execution_target=args.exec, 
        mode=args.mode,
        transport_type=args.transport
    )
    
    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        log.info("[Connector] Exiting gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()