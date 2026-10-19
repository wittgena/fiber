# fiber.infra.mcp.connector
## @lineage: fiber.gateway.mcp.connector
import sys
import asyncio
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional, Protocol

import orjson

from fiber.gateway.mcp.payload import McpPayloadBuilder
from fiber.gateway.mcp.registry import AdapterRegistry
from fiber.infra.mcp.intent import IntentLifecycleFSM, IntentState

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
    def __init__(
        self,
        target_id: str,
        execution_target: str,
        mode: str = "ephemeral",
        transport_type: str = "stdio",
        preflight_pattern: Optional[str] = None
    ):
        self.target_id = target_id
        self.execution_target = execution_target.strip()
        self.mode = mode.lower().strip()
        self.transport_type = transport_type.lower().strip()

        if self.mode not in ("ephemeral", "linear", "multiplex"):
            raise ValueError(f"Invalid mode: {self.mode}. Must be ephemeral, linear, or multiplex.")

        if self.transport_type not in ("stdio", "network"):
            raise ValueError(f"Invalid transport: {self.transport_type}. Must be stdio or network.")

        self.is_remote_url = (
            self.transport_type == "network"
            and self.execution_target.startswith(("http://", "https://"))
        )

        self.tunnel = None
        self.rpc = InternalRpcClient()
        
        # [확장] IntentLifecycleFSM에 preflight_pattern을 주입하여 서드파티 워커 대리 검증 기능 활성화
        self.fsm = IntentLifecycleFSM(self.target_id, self.rpc, preflight_pattern=preflight_pattern)
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False

        self.quarantine = AdapterRegistry.get_adapter(self.target_id)

        self.active_sandboxes: Dict[str, MCPServerTransport] = {}
        self.shared_transport: Optional[MCPServerTransport] = None
        self.pending_requests: Dict[str, asyncio.Future] = {}
        self.linear_lock = asyncio.Lock()
        self._listener_task: Optional[asyncio.Task] = None

    def _create_transport(self, handle_id: str) -> MCPServerTransport:
        if self.transport_type == "network":
            from xphi.state.phase.network.transport import NetworkTransport
            return NetworkTransport(execution_target=self.execution_target, handle_id=handle_id)
        else:
            from xphi.state.phase.network.transport import StdioTransport
            return StdioTransport(command=self.execution_target, handle_id=handle_id)

    def _prepare_ingress_payload(
        self, handle_id: str, payload: Dict[str, Any], is_resume: bool = False
    ) -> Dict[str, Any]:
        """
        내부 워커(AsyncWorkerProtocol)의 RESUME 언래핑 규격({"action": "RESUME", "payload": {...}})과
        원격 표준 MCP 서버(params._meta.fiber_action) 규격을 정확히 분리하여 원본 prompt_id 유실을 방지합니다.
        """
        if is_resume:
            if not self.is_remote_url:
                rpc_payload = {
                    "id": handle_id,
                    "action": "RESUME",
                    "payload": dict(payload),  # 내부의 {"id": "prompt_txn_...", "result": ...} 보존
                }
            else:
                rpc_payload = dict(payload)
                rpc_payload["id"] = handle_id
                if isinstance(rpc_payload.get("params"), dict):
                    params_copy = dict(rpc_payload["params"])
                    meta_copy = dict(params_copy.get("_meta", {}))
                    meta_copy["fiber_action"] = "RESUME"
                    params_copy["_meta"] = meta_copy
                    rpc_payload["params"] = params_copy
        else:
            rpc_payload = dict(payload)
            rpc_payload["id"] = handle_id

        return self.quarantine.translate_ingress(rpc_payload)

    async def _delegate_to_core(self, rpc_req: Dict[str, Any]) -> Dict[str, Any]:
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

    @staticmethod
    def _is_elicitation(response: Dict[str, Any]) -> bool:
        method = response.get("method")
        return bool(method and "elicitation" in str(method))

    def _clear_pending_future(self, handle_id: str):
        future = self.pending_requests.pop(handle_id, None)
        if future and not future.done():
            future.cancel()

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

            for handle_id in list(self.pending_requests.keys()):
                self._clear_pending_future(handle_id)
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
        while self.running and self.shared_transport:
            try:
                raw_output = await self.shared_transport.read_egress_stream()
                if not raw_output:
                    break

                raw_str = raw_output.decode("utf-8").strip()
                if not raw_str:
                    continue

                response = self.quarantine.translate_egress(raw_str)
                if not response or not isinstance(response, dict):
                    continue

                req_id = response.get("id")

                if response.get("method") == "rpc_delegate":
                    asyncio.create_task(self._handle_agent_rpc_delegation(response))
                    continue

                if self._is_elicitation(response):
                    # [정렬] multiplex/linear 큐 모드에서 prompt_ 접두사를 제거하여 원본 트랜잭션 handle_id를 복원합니다.
                    canonical_id = str(req_id).removeprefix("prompt_")
                    log.warning(f"[Connector] TRAP: Elicitation (YIELD) detected for {canonical_id}")
                    if self.mode == "linear":
                        log.error("FATAL: Linear mode worker yielded! This blocks the entire queue.")

                    await self.fsm.transition(canonical_id, IntentState.YIELDED, executable_payload=response)
                    self._clear_pending_future(canonical_id)
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
                is_preflight_trapped = await self.fsm.check_preflight_trap(handle_id, payload)
                if is_preflight_trapped:
                    return

                if await self.fsm.transition(handle_id, IntentState.EXECUTING):
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

                verified_payload = await self.fsm.verify_preflight_resume(handle_id, payload)
                target_payload = verified_payload if verified_payload else payload
                if await self.fsm.transition(handle_id, IntentState.RESUMING):
                    is_resume_flag = False if verified_payload else True

                    if self.mode == "multiplex":
                        log.info(f"[Connector] Multiplexing RESUME to Shared Transport: {handle_id}")
                        await self._execute_shared(handle_id, target_payload, use_lock=False, is_resume=is_resume_flag)
                    elif self.mode == "ephemeral":
                        transport = self.active_sandboxes.get(handle_id)
                        if not transport:
                            if verified_payload:
                                await self._execute_ephemeral(handle_id, target_payload)
                                return
                            log.error(f"Cannot RESUME {handle_id}: Sandbox not found.")
                            await self.fsm.transition(handle_id, IntentState.FAULTED, error_detail="Sandbox not found for RESUME")
                            return
                        log.info(f"[Connector] Resuming Parked Intent: {handle_id}")
                        await self._cycle_io(handle_id, target_payload, transport, is_resume=is_resume_flag)

            elif action in ("RESUME_OR_KILL", "FORCE_ROLLBACK"):
                if await self.fsm.transition(handle_id, IntentState.ROLLED_BACK, error_detail="SYSTEM_SENTINEL_TIMEOUT: Request aborted."):
                    self._clear_pending_future(handle_id)
                    if self.mode == "ephemeral":
                        log.warning(f"[Connector] Sentinel enforced ROLLBACK on {handle_id}. Terminating parked sandbox.")
                        await self._destroy_sandbox(handle_id)
                    self.fsm.cleanup(handle_id)

        except Exception as e:
            if not self.running:
                log.debug(f"[Connector] Intent {handle_id} aborted cleanly due to graceful shutdown (Expected EOF).")
                return

            if self.fsm.get_state(handle_id) == IntentState.ROLLED_BACK:
                log.debug(f"[Connector] Suppressed expected EOF for rolled-back intent: {handle_id}")
                return

            log.error(f"[Connector] Lifecycle Crash for {handle_id}: {e}", exc_info=True)
            await self.fsm.transition(handle_id, IntentState.FAULTED, error_detail=str(e))
            self._clear_pending_future(handle_id)
            if self.mode == "ephemeral":
                await self._destroy_sandbox(handle_id)
            self.fsm.cleanup(handle_id)

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
                    await self.shared_transport.send_payload(safe_payload)
                    await self._wait_and_resolve_shared(handle_id, future)
            else:
                await self.shared_transport.send_payload(safe_payload)
                await self._wait_and_resolve_shared(handle_id, future)
        except Exception:
            self._clear_pending_future(handle_id)
            raise

    async def _wait_and_resolve_shared(self, handle_id: str, future: asyncio.Future):
        try:
            response = await future
            if "error" in response:
                await self.fsm.transition(handle_id, IntentState.FAULTED, error_detail=str(response["error"]))
            else:
                await self.fsm.transition(handle_id, IntentState.RESOLVED, executable_payload=response)
        except asyncio.CancelledError:
            log.warning(f"Shared request {handle_id} was cancelled or yielded.")
        finally:
            if self.fsm.get_state(handle_id) in self.fsm.TERMINAL_STATES:
                self.fsm.cleanup(handle_id)

    async def _execute_ephemeral(self, handle_id: str, payload: Dict[str, Any]):
        if handle_id in self.active_sandboxes:
            log.warning(f"[Connector] Duplicate EXECUTE ignored for {handle_id}")
            return

        transport = self._create_transport(handle_id)
        await transport.start()
        self.active_sandboxes[handle_id] = transport

        await self._cycle_io(handle_id, payload, transport, is_resume=False)

    async def _cycle_io(
        self, handle_id: str, payload: Dict[str, Any], transport: MCPServerTransport, is_resume: bool = False
    ):
        try:
            safe_payload = self._prepare_ingress_payload(handle_id, payload, is_resume=is_resume)
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
                await self.fsm.transition(handle_id, IntentState.YIELDED, executable_payload=response)
            else:
                if "error" in response:
                    error_detail = response["error"]
                    log.error(f"[Connector:FAULT] Ephemeral Sandbox execution failed: {error_detail}", extra={"handle_id": handle_id})
                    await self.fsm.transition(handle_id, IntentState.FAULTED, error_detail=str(error_detail))
                else:
                    log.info(f"[Connector] Intent {handle_id} RESOLVED.")
                    await self.fsm.transition(handle_id, IntentState.RESOLVED, executable_payload=response)

                await self._destroy_sandbox(handle_id)
                self.fsm.cleanup(handle_id)

        except Exception as e:
            raise RuntimeError(f"IO Cycle Failed: {e}")

    async def _destroy_sandbox(self, handle_id: str):
        transport = self.active_sandboxes.pop(handle_id, None)
        if transport:
            await transport.close()


def main():
    parser = argparse.ArgumentParser(description="Fiber MCP Worker Egress Connector")
    parser.add_argument("--target", required=True, help="Target ID")
    parser.add_argument("--exec", required=True, help="Command, Binary root path, or HTTP(S) URL")
    parser.add_argument("--mode", default=None, choices=["ephemeral", "linear", "multiplex"])
    parser.add_argument("--transport", default=None, choices=["stdio", "network"])
    parser.add_argument("--preflight", default=None, help="Regex pattern to trigger YIELD before worker execution")
    
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
        preflight_pattern=args.preflight
    )

    try:
        asyncio.run(connector.run())
    except KeyboardInterrupt:
        log.info("[Connector] Exiting gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()