# fiber.infra.worker.connector
import os
import sys
import json
import asyncio
import logging
import argparse
from typing import Dict, Any, Optional

from fiber.dphi.rpc.client import InternalRpcClient
from fiber.infra.worker.quarantine import QuarantineRegistry
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("worker.connector")

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
        """표준 에러(stderr)는 정상적인 로깅용으로 간주하고 백그라운드 출력"""
        while self.process and not self.process.stderr.at_eof():
            try:
                line = await self.process.stderr.readline()
                if line:
                    log.warning(f"[Legacy STDERR | {self.handle_id}] {line.decode('utf-8').strip()}")
            except Exception:
                break

    async def send_payload(self, safe_payload: Dict[str, Any]):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError(f"Legacy process {self.handle_id} is dead.")
        # [핵심 2] 전달받은 순수/변환된 딕셔너리를 직렬화하여 전송
        raw_msg = json.dumps(safe_payload) + "\n"
        self.process.stdin.write(raw_msg.encode('utf-8'))
        await self.process.stdin.drain()

    async def receive_raw(self) -> str:
        """[핵심 3] 쓰레기 데이터 무시 로직 완전 제거. 있는 그대로의 스트림만 반환하여 Quarantine에 위임"""
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
        
        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False
        
        # [핵심 4] 타겟 ID에 맞는 부패 방지 계층(Quarantine Adapter) 장착
        self.quarantine = QuarantineRegistry.get_adapter(self.target_id)
        
        self.active_sandboxes: Dict[str, LegacyTransport] = {}
        self.daemon_transport: Optional[LegacyTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}

    async def run(self):
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)
        
        self.running = True
        log.info(f"[Connector:{self.target_id}] 🚀 Listening for Intents on DPHI Bus (Mode: {self.mode.upper()})")

        if self.mode == "daemon":
            self.daemon_transport = LegacyTransport(self.legacy_command, f"daemon-{self.target_id}")
            await self.daemon_transport.start()
            asyncio.create_task(self._daemon_stdout_listener())

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
            
            if self.daemon_transport:
                await self.daemon_transport.close()
            for transport in self.active_sandboxes.values():
                await transport.close()

    async def _daemon_stdout_listener(self):
        while self.running and self.daemon_transport and self.daemon_transport.process:
            try:
                raw_output = await self.daemon_transport.process.stdout.readline()
                if not raw_output:
                    break
                
                raw_str = raw_output.decode('utf-8')
                
                # [적용] Egress 데이터를 격리 구역에서 검증 및 파싱 (에러 시 빠른 실패, JSONDecodeError 발생)
                response = self.quarantine.translate_egress(raw_str)

                req_id = response.get("id")

                if response.get("method") == "rpc_delegate":
                    asyncio.create_task(self._handle_agent_rpc_delegation(response))
                    continue

                if response.get("method") and "elicitation" in response.get("method", ""):
                    log.warning(f"[Connector] ⏸️ Daemon TRAP: Elicitation detected for {req_id}")
                    await self.rpc.call("mcp.bridge.resolve_state", {
                        "handle_id": req_id,
                        "status": "YIELD",
                        "executable_payload": response
                    })
                    continue
                
                future = self.pending_requests.pop(req_id, None)
                if future and not future.done():
                    future.set_result(response)
                    
            except Exception as e:
                log.error(f"[Connector] Daemon Listener Fracture: {e}")
                # Daemon 프로세스는 죽이지 않되, 실패한 파싱으로 인한 로그를 남기고 대기
                await asyncio.sleep(0.1)

    async def _handle_agent_rpc_delegation(self, rpc_req: Dict[str, Any]):
        call_id = rpc_req.get("id")
        params = rpc_req.get("params", {})
        target_method = params.get("target_method")
        payload = params.get("data", {})

        try:
            core_res = await self.rpc.call(target_method, payload)
            feedback_payload = {
                "jsonrpc": "2.0",
                "id": call_id,
                "result": core_res
            }
            await self.daemon_transport.send_payload(feedback_payload)
        except Exception as e:
            log.error(f"[Connector] Failed to delegate agent RPC: {e}")
            error_payload = {
                "jsonrpc": "2.0",
                "id": call_id,
                "error": {"code": -32000, "message": f"Core delegation failure: {str(e)}"}
            }
            await self.daemon_transport.send_payload(error_payload)

    async def process_intent(self, intent_data: Dict[str, Any]):
        handle_id = intent_data.get("handle_id")
        action = intent_data.get("action", "EXECUTE")
        payload = intent_data.get("payload", {})

        if not handle_id:
            return

        try:
            if action == "EXECUTE":
                if self.mode == "daemon":
                    await self._execute_daemon(handle_id, payload)
                else:
                    await self._execute_ephemeral(handle_id, payload)

            elif action == "RESUME":
                if self.mode == "daemon":
                    log.warning(f"RESUME not supported in Daemon Mode. Ignoring {handle_id}.")
                    return
                
                transport = self.active_sandboxes.get(handle_id)
                if not transport:
                    log.error(f"Cannot RESUME {handle_id}: Sandbox not found.")
                    return
                
                log.info(f"[Connector] Resuming Parked Intent: {handle_id}")
                await self._cycle_io(handle_id, payload, transport)

            elif action in ("RESUME_OR_KILL", "FORCE_ROLLBACK"):
                if self.mode == "daemon":
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
            log.error(f"[Connector] Lifecycle Crash for {handle_id}: {e}")
            await self._report_fault(handle_id, str(e))
            if self.mode != "daemon":
                await self._destroy_sandbox(handle_id)

    async def _execute_daemon(self, handle_id: str, payload: Dict[str, Any]):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_requests[handle_id] = future
        
        payload["id"] = handle_id
        
        log.debug(f"[Connector] Multiplexing Intent: {handle_id}")
        
        # [적용] Ingress 데이터를 격리 구역을 통해 안전하게 변환 후 전송 (유연성 지원)
        safe_payload = self.quarantine.translate_ingress(payload)
        await self.daemon_transport.send_payload(safe_payload)
        
        try:
            response = await future
            status = "FAULTED" if "error" in response else "RESOLVED"
            log.info(f"[Connector] ⏹️ Daemon Intent {handle_id} {status}.")
            
            await self.rpc.call("mcp.bridge.resolve_state", {
                "handle_id": handle_id,
                "status": status,
                "executable_payload": response
            })
        except asyncio.CancelledError:
            log.warning(f"Daemon request {handle_id} was cancelled.")

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
            # [적용] Ingress 변환
            safe_payload = self.quarantine.translate_ingress(payload)
            await transport.send_payload(safe_payload)
            
            # [적용] Egress 변환 및 검증 (STDOUT 오염 시 즉각 Fail-Fast 발동)
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
                status = "FAULTED" if is_rollback or "error" in response else "RESOLVED"
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
    parser.add_argument("--mode", default="ephemeral", choices=["ephemeral", "daemon"], help="Execution mode for the sandbox.")
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