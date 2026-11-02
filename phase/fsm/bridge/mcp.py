# fiber.phase.fsm.mcp.intent
## @lineage: fiber.infra.mcp.intent
from enum import Enum, auto
from typing import Dict, Any, Optional

from xphi.node.rpc.method import RpcMethod
from fiber.infra.mcp.guard.preflight import PreflightGuard
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.watcher.plane.emitter import get_emitter


class IntentState(Enum):
    PENDING = auto()
    EXECUTING = auto()
    YIELDED = auto()
    RESUMING = auto()
    RESOLVED = auto()
    FAULTED = auto()
    ROLLED_BACK = auto()


class IntentLifecycleFSM:
    """@transition: PENDING -> EXECUTING -> YIELDED -> RESUMING -> RESOLVED/FAULTED"""
    TERMINAL_STATES = frozenset({
        IntentState.RESOLVED,
        IntentState.FAULTED,
        IntentState.ROLLED_BACK,
    })

    def __init__(self, target_id: str, rpc: InternalRpcClient, preflight_pattern: Optional[str] = None):
        self.target_id = target_id
        self.rpc = rpc
        self.states: Dict[str, IntentState] = {}
        self.log = get_emitter(f"mcp.fsm.{self.target_id}")
        self.preflight = PreflightGuard(target_id=self.target_id, rpc=self.rpc, pattern=preflight_pattern)

    def get_state(self, handle_id: str) -> IntentState:
        return self.states.get(handle_id, IntentState.PENDING)

    async def check_preflight_trap(self, handle_id: str, payload: Dict[str, Any]) -> bool:
        synthetic_prompt = self.preflight.inspect_and_stash(handle_id, payload)
        if not synthetic_prompt:
            return False

        await self.transition(handle_id, IntentState.YIELDED, executable_payload=synthetic_prompt)
        return True

    async def verify_preflight_resume(self, handle_id: str, resume_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return await self.preflight.verify_and_pop(handle_id, resume_payload)
        except Exception as e:
            await self.transition(handle_id, IntentState.FAULTED, error_detail=str(e))
            raise PermissionError(f"[FSM:PreFlight] Attestation failed: {e}") from e

    async def transition(
        self,
        handle_id: str,
        target_state: IntentState,
        executable_payload: Optional[Dict[str, Any]] = None,
        error_detail: Optional[str] = None,
    ) -> bool:
        current_state = self.get_state(handle_id)

        if current_state in self.TERMINAL_STATES:
            self.log.warning(f"[FSM:Reject] {handle_id} locked in {current_state.name} (Target: {target_state.name}).")
            return False

        if target_state == IntentState.RESUMING and current_state != IntentState.YIELDED:
            self.log.warning(f"[FSM:Reject] Invalid RESUME for {handle_id} from {current_state.name}.")
            return False

        self.states[handle_id] = target_state
        rpc_status = {
            IntentState.YIELDED: "YIELD",
            IntentState.RESOLVED: "RESOLVED",
            IntentState.FAULTED: "FAULTED",
            IntentState.ROLLED_BACK: "FAULTED",
        }.get(target_state)

        if rpc_status:
            state_msg: Dict[str, Any] = {"handle_id": handle_id, "status": rpc_status}
            if executable_payload is not None:
                state_msg["executable_payload"] = executable_payload
            if error_detail is not None:
                state_msg["error_detail"] = error_detail
            await self.rpc.call(RpcMethod.MCP_BRIDGE_RESOLVE_STATE, state_msg)

        return True

    def cleanup(self, handle_id: str) -> None:
        self.states.pop(handle_id, None)
        self.preflight.clear(handle_id)