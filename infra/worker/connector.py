# fiber.infra.worker.connector
import os
import sys
import json
import asyncio
import logging
import argparse
from typing import Dict, Any, Optional

from fiber.dphi.rpc.client import InternalRpcClient
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
        while self.process and not self.process.stderr.at_eof():
            try:
                line = await self.process.stderr.readline()
                if line:
                    log.warning(f"[Legacy STDERR | {self.handle_id}] {line.decode('utf-8').strip()}")
            except Exception:
                break

    async def send_payload(self, payload: Dict[str, Any]):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError(f"Legacy process {self.handle_id} is dead.")
        raw_msg = json.dumps(payload) + "\n"
        self.process.stdin.write(raw_msg.encode('utf-8'))
        await self.process.stdin.drain()

    async def receive_response(self) -> Dict[str, Any]:
        """(Ephemeral 모드 전용) 단일 요청에 대한 응답 대기"""
        raw_output = await self.process.stdout.readline()
        if not raw_output:
            raise RuntimeError(f"EOF reached while reading stdout for {self.handle_id}.")
        return json.loads(raw_output.decode('utf-8').strip())

    async def close(self):
        """프로세스 완전 소멸 및 자원 반환"""
        if self.process and self.process.returncode is None:
            log.info(f"[Transport:{self.handle_id}] Terminating sandbox (PID: {self.process.pid})")
            self.process.terminate()
            try:
                ## 좀비 프로세스 방지를 위해 종료 대기 (최대 2초)
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                self.process.kill()


class WorkerConnector:
    def __init__(self, target_id: str, legacy_command: str, mode: str = "ephemeral"):
        self.target_id = target_id
        self.legacy_command = legacy_command
        self.mode = mode.lower()  # "ephemeral" or "daemon"
        
        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False
        
        # [Ephemeral Mode] 트랜잭션별 1:1 프로세스 샌드박스
        self.active_sandboxes: Dict[str, LegacyTransport] = {}
        
        # [Daemon Mode] 단일 프로세스 및 다중화 Future 매핑
        self.daemon_transport: Optional[LegacyTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}

    async def run(self):
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)
        
        self.running = True
        log.info(f"[Connector:{self.target_id}] 🚀 Listening for Intents on DPHI Bus (Mode: {self.mode.upper()})")

        # Daemon 모드일 경우 상주형 샌드박스를 미리 기동
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
                    ## 백그라운드 태스크 분리: 동시 다발적 요청 병렬 처리
                    asyncio.create_task(self.process_intent(intent_data))
                    
        except asyncio.CancelledError:
            log.info("[Connector] Shutdown signal received.")
        except Exception as e:
            log.error(f"[Connector] Fatal Bus Error: {e}", exc_info=True)
        finally:
            await pubsub.unsubscribe(self.listen_channel)
            await pubsub.close()
            
            # 셧다운 시 모든 프로세스 파괴
            if self.daemon_transport:
                await self.daemon_transport.close()
            for transport in self.active_sandboxes.values():
                await transport.close()

    async def _daemon_stdout_listener(self):
        """[Daemon Mode 전용] 백그라운드에서 끊임없이 stdout을 읽어 Future(결과)를 매핑하거나 RPC 역호출 처리"""
        while self.running and self.daemon_transport and self.daemon_transport.process:
            try:
                raw_output = await self.daemon_transport.process.stdout.readline()
                if not raw_output:
                    break  # 프로세스 종료 (EOF)
                
                response = json.loads(raw_output.decode('utf-8').strip())
                req_id = response.get("id")

                # =====================================================================
                # [개선] 에이전트가 코어 RPC 질의를 역으로 요청한 경우 (Interception)
                # 규격: {"method": "rpc_delegate", "params": {"target_method": "...", "data": {...}}, "id": "..."}
                # =====================================================================
                if response.get("method") == "rpc_delegate":
                    asyncio.create_task(self._handle_agent_rpc_delegation(response))
                    continue

                # =====================================================================
                # [개선] 에이전트가 데이터 결핍(Elicitation / Yield)을 선언한 경우
                # =====================================================================
                if response.get("method") and "elicitation" in response.get("method", ""):
                    log.warning(f"[Connector] ⏸️ Daemon TRAP: Elicitation detected for {req_id}")
                    await self.rpc.call("mcp.bridge.resolve_state", {
                        "handle_id": req_id,
                        "status": "YIELD",
                        "executable_payload": response
                    })
                    continue
                
                # 매핑된 Future 객체를 찾아 결과를 반환 (resolve)
                future = self.pending_requests.pop(req_id, None)
                if future and not future.done():
                    future.set_result(response)
                    
            except Exception as e:
                log.error(f"[Connector] Daemon Listener Fracture: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    async def _handle_agent_rpc_delegation(self, rpc_req: Dict[str, Any]):
        """에이전트의 역호출을 수신하여 InternalRpcClient로 전달하고, 그 결과를 에이전트 stdin으로 재주입"""
        call_id = rpc_req.get("id")
        params = rpc_req.get("params", {})
        target_method = params.get("target_method")
        payload = params.get("data", {})

        try:
            # 코어 RPC 호출 (예: 텐션 관측값, 원장 조회 등)
            core_res = await self.rpc.call(target_method, payload)
            # 에이전트 stdin으로 결과 피드백
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
                    # Daemon 모드에서는 Future만 강제 취소 (프로세스는 죽이지 않음)
                    future = self.pending_requests.pop(handle_id, None)
                    if future and not future.done():
                        future.cancel()
                    await self._report_fault(handle_id, "SYSTEM_SENTINEL_TIMEOUT: Request aborted.")
                else:
                    # Ephemeral 모드: 좀비 샌드박스 강제 롤백 및 사살
                    transport = self.active_sandboxes.get(handle_id)
                    if transport:
                        log.warning(f"⚠️ [Connector] Sentinel enforced ROLLBACK on {handle_id}")
                        await self._cycle_io(handle_id, payload, transport, is_rollback=True)

        except Exception as e:
            log.error(f"[Connector] Lifecycle Crash for {handle_id}: {e}")
            await self._report_fault(handle_id, str(e))
            if self.mode != "daemon":
                await self._destroy_sandbox(handle_id)

    # -------------------------------------------------------------------------
    # Execution Strategies (Daemon vs Ephemeral)
    # -------------------------------------------------------------------------
    async def _execute_daemon(self, handle_id: str, payload: Dict[str, Any]):
        """[Daemon Mode] 단일 파이프라인(stdin)에 요청을 밀어넣고 퓨처 매핑 대기 (Lock-Free)"""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_requests[handle_id] = future
        
        # 내부 라우팅을 위해 JSON-RPC ID를 handle_id로 덮어쓰기
        payload["id"] = handle_id
        
        log.debug(f"[Connector] Multiplexing Intent: {handle_id}")
        await self.daemon_transport.send_payload(payload)
        
        # 백그라운드 리스너가 결과를 매핑해 줄 때까지 비동기 대기
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
        """[Ephemeral Mode] 요청마다 새로운 샌드박스(프로세스)를 생성하여 실행 및 파괴"""
        if handle_id in self.active_sandboxes:
            log.warning(f"[Connector] Duplicate EXECUTE ignored for {handle_id}")
            return
        
        transport = LegacyTransport(self.legacy_command, handle_id)
        await transport.start()
        self.active_sandboxes[handle_id] = transport
        
        log.debug(f"[Connector] Injecting New Ephemeral Intent: {handle_id}")
        await self._cycle_io(handle_id, payload, transport)

    async def _cycle_io(self, handle_id: str, payload: Dict[str, Any], transport: LegacyTransport, is_rollback: bool = False):
        """(Ephemeral 전용) 샌드박스 I/O 사이클 및 상태 관리"""
        try:
            await transport.send_payload(payload)
            response = await transport.receive_response()

            # =====================================================================
            # [개선] Ephemeral 모드에서도 rpc_delegate 지원 (1회성 역호출 후 응답 대기)
            # =====================================================================
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
                response = await transport.receive_response() # 다음 응답(최종 or 추가요청) 대기

            ## TRAP & YIELD (Stateful)
            if response.get("method") and "elicitation" in response.get("method", ""):
                log.warning(f"[Connector] ⏸️ TRAP: Elicitation detected. Parking {handle_id} (YIELD).")
                await self.rpc.call("mcp.bridge.resolve_state", {
                    "handle_id": handle_id,
                    "status": "YIELD",
                    "executable_payload": response
                })
                ## 주의: 샌드박스를 파괴하지 않고 유지(Park)

            else:
                ## RESOLVED (Stateless / Finalized)
                status = "FAULTED" if is_rollback or "error" in response else "RESOLVED"
                log.info(f"[Connector] ⏹️ Intent {handle_id} {status}.")
                await self.rpc.call("mcp.bridge.resolve_state", {
                    "handle_id": handle_id,
                    "status": status,
                    "executable_payload": response
                })
                ## 목적 달성 시 샌드박스 파괴
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