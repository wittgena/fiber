# fiber.gateway.mcp.connector
import sys
import asyncio
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, Protocol

import orjson

from fiber.infra.rpc.method import RpcMethod
from fiber.gateway.mcp.payload import McpPayloadBuilder
from fiber.gateway.mcp.registry import AdapterRegistry

from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mcp.connector")

class MCPServerTransport(Protocol):
    async def start(self) -> None: ...
    async def send_payload(self, safe_payload: Dict[str, Any]) -> None: ...
    async def receive_raw(self) -> str: ...
    async def read_egress_stream(self) -> bytes: ...
    async def close(self) -> None: ...
    process: Any


class MCPServerConnector:
    def __init__(self, target_id: str, execution_target: str, mode: str = "ephemeral", transport_type: str = "stdio"):
        self.target_id = target_id
        self.execution_target = execution_target.strip()
        self.mode = mode.lower().strip()
        self.transport_type = transport_type.lower().strip()

        if self.mode not in ("ephemeral", "linear", "multiplex"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be ephemeral, linear, or multiplex.")

        if self.transport_type not in ("stdio", "network"):
            raise ValueError(f"Invalid transport: {self.transport_type}. Must be stdio or network.")

        # Unmanaged HTTP(S) URL 여부 판별 (표준 JSON-RPC 규격 준수 분기용)
        self.is_remote_url = (
            self.transport_type == "network"
            and self.execution_target.startswith(("http://", "https://"))
        )

        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False

        self.quarantine = AdapterRegistry.get_adapter(self.target_id)

        self.active_sandboxes: Dict[str, MCPServerTransport] = {}
        self.shared_transport: Optional[MCPServerTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.linear_lock = asyncio.Lock()
        self._listener_task: Optional[asyncio.Task] = None

    def _create_transport(self, handle_id: str) -> MCPServerTransport:
        """Transport 팩토리: 설정된 타입에 따라 적절한 전송 계층 객체를 동적으로 생성"""
        if self.transport_type == "network":
            from xphi.state.phase.network.transport import NetworkTransport
            return NetworkTransport(execution_target=self.execution_target, handle_id=handle_id)
        else:
            from xphi.state.phase.network.transport import StdioTransport
            return StdioTransport(command=self.execution_target, handle_id=handle_id)

    # MCP Payload 정규화 및 위임(Delegation) 공통 헬퍼
    def _prepare_ingress_payload(
        self, handle_id: str, payload: Dict[str, Any], is_resume: bool = False
    ) -> Dict[str, Any]:
        """
        1) 요청 ID(handle_id) 바인딩
        2) RESUME 액션일 때 내부 워커(최상단 키) vs 원격 표준 MCP 서버(params._meta) 분기 처리
        3) 어댑터를 통한 데이터 정제 및 인증 헤더(_http_headers) 주입
        """
        rpc_payload = dict(payload)
        rpc_payload["id"] = handle_id

        if is_resume:
            if not self.is_remote_url:
                rpc_payload["action"] = "RESUME"
            elif isinstance(rpc_payload.get("params"), dict):
                params_copy = dict(rpc_payload["params"])
                meta_copy = dict(params_copy.get("_meta", {}))
                meta_copy["fiber_action"] = "RESUME"
                params_copy["_meta"] = meta_copy
                rpc_payload["params"] = params_copy

        return self.quarantine.translate_ingress(rpc_payload)

    async def _delegate_to_core(self, rpc_req: Dict[str, Any]) -> Dict[str, Any]:
        """에이전트의 rpc_delegate 요청을 코어 RPC로 위임하고 McpPayloadBuilder로 표준 응답 생성"""
        call_id = rpc_req.get("id")
        params = rpc_req.get("params") or {}
        target_method = params.get("target_method")
        req_data = params.get("data") or {}

        try:
            core_res = await self.rpc.call(target_method, req_data)
            return McpPayloadBuilder.result(call_id, core_res)
        except Exception as e:
            log.error(f"[Connector] Failed to delegate agent RPC ({target_method}): {e}")
            return McpPayloadBuilder.error(call_id, -32000, f"Core delegation failure: {e}")

    async def _resolve_bridge_state(
        self,
        handle_id: str,
        status: str,
        executable_payload: Optional[Dict[str, Any]] = None,
        error_detail: Optional[str] = None,
    ) -> None:
        """Core 브릿지로 인텐트 최종 상태(RESOLVED / YIELD / FAULTED) 보고"""
        state_msg: Dict[str, Any] = {"handle_id": handle_id, "status": status}
        if executable_payload is not None:
            state_msg["executable_payload"] = executable_payload
        if error_detail is not None:
            state_msg["error_detail"] = error_detail
        await self.rpc.call(RpcMethod.MCP_BRIDGE_RESOLVE_STATE, state_msg)

    @staticmethod
    def _is_elicitation(response: Dict[str, Any]) -> bool:
        method = response.get("method")
        return bool(method and "elicitation" in str(method))

    # Connector Lifecycle & IO Loops
    async def run(self):
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)

        self.running = True
        log.info(
            f"[Connector:{self.target_id}] Listening for Intents on DPHI Bus "
            f"(Mode: {self.mode.upper()}, Transport: {self.transport_type.upper()})"
        )

        try:
            if self.mode in ("linear", "multiplex"):
                self.shared_transport = self._create_transport(f"shared-{self.target_id}")
                await self.shared_transport.start()
                self._listener_task = asyncio.create_task(self._shared_stdout_listener())

            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg and msg["type"] == "message":
                    intent_data = orjson.loads(msg["data"])
                    asyncio.create_task(self.process_intent(intent_data))

        except asyncio.CancelledError:
            log.info("[Connector] Shutdown signal received.")
        except Exception as e:
            log.error(f"[Connector] Fatal Bus Error: {e}", exc_info=True)
        finally:
            self.running = False
            await pubsub.unsubscribe(self.listen_channel)
            await pubsub.close()

            for future in self.pending_requests.values():
                if not future.done():
                    future.cancel()
            self.pending_requests.clear()

            if self.shared_transport:
                await self.shared_transport.close()
            if self._listener_task and not self._listener_task.done():
                self._listener_task.cancel()
                await asyncio.gather(self._listener_task, return_exceptions=True)

            for transport in list(self.active_sandboxes.values()):
                await transport.close()
            self.active_sandboxes.clear()

    async def _shared_stdout_listener(self):
        """linear/multiplex 모드에서 백그라운드로 Egress Stream을 수신하여 Future를 Resolve"""
        while self.running and self.shared_transport:
            try:
                raw_output = await self.shared_transport.read_egress_stream()
                if not raw_output:
                    break

                raw_str = raw_output.decode("utf-8").strip()
                if not raw_str:
                    continue

                response = self.quarantine.translate_egress(raw_str)
                if not response:
                    # SSE 제어 프레임('event: message', ': keep-alive') 등 빈 dict 반환 시 즉시 스킵
                    continue
                if not isinstance(response, dict):
                    log.warning(f"[Connector] Ignored non-dict egress payload: {response}")
                    continue

                req_id = response.get("id")

                # In-Bus Worker Chaining (rpc_delegate) 처리
                if response.get("method") == "rpc_delegate":
                    asyncio.create_task(self._handle_agent_rpc_delegation(response))
                    continue

                # MRTR Elicitation (YIELD) 감지 및 파킹
                if self._is_elicitation(response):
                    log.warning(f"[Connector] TRAP: Elicitation (YIELD) detected for {req_id}")
                    if self.mode == "linear":
                        log.error("FATAL: Linear mode worker yielded! This blocks the entire queue.")

                    await self._resolve_bridge_state(req_id, "YIELD", executable_payload=response)
                    future = self.pending_requests.pop(req_id, None)
                    if future and not future.done():
                        future.cancel()
                    continue

                future = self.pending_requests.pop(req_id, None)
                if future and not future.done():
                    future.set_result(response)
                else:
                    log.debug(f"[Connector] Unmatched or already resolved response ID: {req_id}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[Connector] Shared Listener Fracture: {e}")
                await asyncio.sleep(0.1)

    async def _handle_agent_rpc_delegation(self, rpc_req: Dict[str, Any]):
        if not self.shared_transport:
            log.error(f"[Connector] Cannot send RPC delegation feedback for {rpc_req.get('id')}: shared_transport is None.")
            return
        feedback_payload = await self._delegate_to_core(rpc_req)
        await self.shared_transport.send_payload(feedback_payload)

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
                    await self._execute_shared(handle_id, payload, use_lock=False, is_resume=True)
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
                        log.warning(f"[Connector] Sentinel enforced ROLLBACK on {handle_id}")
                        await self._cycle_io(handle_id, payload, transport, is_rollback=True)
        except Exception as e:
            if not self.running:
                log.debug(f"[Connector] Intent {handle_id} aborted cleanly due to graceful shutdown (Expected EOF).")
                return

            log.error(f"[Connector] Lifecycle Crash for {handle_id}: {e}", exc_info=True)
            await self._report_fault(handle_id, str(e))
            if self.mode == "ephemeral":
                await self._destroy_sandbox(handle_id)

    async def _execute_shared(
        self, handle_id: str, payload: Dict[str, Any], use_lock: bool, is_resume: bool = False
    ):
        safe_payload = self._prepare_ingress_payload(handle_id, payload, is_resume=is_resume)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_requests[handle_id] = future

        try:
            if use_lock:
                async with self.linear_lock:
                    log.debug(f"[Connector] Linear Enqueue: {handle_id}")
                    await self.shared_transport.send_payload(safe_payload)
                    await self._wait_and_resolve_shared(handle_id, future)
            else:
                log.debug(f"[Connector] Multiplexing Intent: {handle_id}")
                await self.shared_transport.send_payload(safe_payload)
                await self._wait_and_resolve_shared(handle_id, future)
        except Exception:
            self.pending_requests.pop(handle_id, None)
            raise

    async def _wait_and_resolve_shared(self, handle_id: str, future: asyncio.Future):
        try:
            response = await future
            if "error" in response:
                status = "FAULTED"
                log.error(
                    f"[Connector:FAULT] Shared Sandbox execution failed. Error details: {response['error']}",
                    extra={"handle_id": handle_id},
                )
            else:
                status = "RESOLVED"

            log.info(f"[Connector] Shared Intent {handle_id} {status}.")
            await self._resolve_bridge_state(handle_id, status, executable_payload=response)
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

    async def _cycle_io(
        self, handle_id: str, payload: Dict[str, Any], transport: MCPServerTransport, is_rollback: bool = False
    ):
        try:
            safe_payload = self._prepare_ingress_payload(handle_id, payload)
            await transport.send_payload(safe_payload)

            raw_output = await transport.receive_raw()
            response = self.quarantine.translate_egress(raw_output)

            while response.get("method") == "rpc_delegate":
                feedback_payload = await self._delegate_to_core(response)
                await transport.send_payload(feedback_payload)
                raw_output = await transport.receive_raw()
                response = self.quarantine.translate_egress(raw_output)

            if self._is_elicitation(response):
                log.warning(f"[Connector] TRAP: Elicitation detected. Parking {handle_id} (YIELD).")
                await self._resolve_bridge_state(handle_id, "YIELD", executable_payload=response)
            else:
                if is_rollback or "error" in response:
                    status = "FAULTED"
                    if "error" in response:
                        log.error(
                            f"[Connector:FAULT] Ephemeral Sandbox execution failed. Error details: {response['error']}",
                            extra={"handle_id": handle_id},
                        )
                else:
                    status = "RESOLVED"

                log.info(f"[Connector] Intent {handle_id} {status}.")
                await self._resolve_bridge_state(handle_id, status, executable_payload=response)
                await self._destroy_sandbox(handle_id)

        except Exception as e:
            raise RuntimeError(f"IO Cycle Failed: {e}")

    async def _destroy_sandbox(self, handle_id: str):
        transport = self.active_sandboxes.pop(handle_id, None)
        if transport:
            await transport.close()

    async def _report_fault(self, handle_id: str, error_detail: str):
        try:
            await self._resolve_bridge_state(handle_id, "FAULTED", error_detail=error_detail)
        except Exception as rpc_e:
            log.critical(f"[Connector] Failed to report FAULT to Core: {rpc_e}")


def main():
    parser = argparse.ArgumentParser(description="Fiber MCP Worker Egress Connector")
    parser.add_argument("--target", required=True, help="Target ID (e.g., db-server-01)")
    parser.add_argument("--exec", required=True, help="Legacy command, Binary root path, or HTTP(S) URL")
    parser.add_argument(
        "--mode",
        default=None,
        choices=["ephemeral", "linear", "multiplex"],
        help="Execution mode: ephemeral (isolation), linear (sequential queue), multiplex (async routing)",
    )
    parser.add_argument(
        "--transport",
        default=None,
        choices=["stdio", "network"],
        help="Transport layer: stdio (subprocess I/O) or network (HTTP multiplexing)",
    )
    args = parser.parse_args()
    exec_target = args.exec.strip()
    is_http_url = exec_target.startswith(("http://", "https://"))
    is_managed_bin_dir = not is_http_url and Path(exec_target).is_dir()

    resolved_transport = args.transport or ("network" if (is_http_url or is_managed_bin_dir) else "stdio")
    resolved_mode = args.mode or ("multiplex" if resolved_transport == "network" else "ephemeral")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    connector = MCPServerConnector(
        target_id=args.target,
        execution_target=exec_target,
        mode=resolved_mode,
        transport_type=resolved_transport,
    )

    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        log.info("[Connector] Exiting gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()