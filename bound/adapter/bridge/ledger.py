# bound.adapter.bridge.ledger
"""
@desc: 
- Interceptor Bridge middleware decoupling Agent execution from Kernel Ledger sealing
- Acts as a strict gatekeeper: translates arbitrary agent actions into LogicStreams,
  evaluates tension, and returns a binary execution authorization (Go/No-Go).
"""
import uuid
from typing import Any, Dict, Optional
from watcher.plane.emitter import get_emitter
from watcher.kernel.ledger import ToposLedger, LogicStream, SealedKernel

log = get_emitter("kernel.bridge", phase="KERNEL")

class LedgerBridge:
    """
    @desc: compliant middleware. Agents interact ONLY with this bridge.
    @architecture.pattern: Adapter & Proxy
    """
    def __init__(self, ledger: Optional[ToposLedger] = None):
        ## @injection: Use the provided ledger or instantiate the default architecture
        self.ledger = ledger or ToposLedger()

    async def authorize(self, action_id: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        @desc: The single choke-point for agent action validation.
        @flow: Agent Action -> LogicStream -> ToposLedger -> Boolean Signal
        """
        if metadata is None:
            metadata = {}

        ## Adapt: Translate agent's raw context into a topological LogicStream
        stream = LogicStream(
            id=action_id or str(uuid.uuid4()),
            payload=payload,
            metadata=metadata
        )

        log.debug(f"[Bridge] Requesting ledger authorization for stream: {stream.id}")

        ## Evaluate & Seal: Delegate the heavy lifting to the Kernel Ledger
        try:
            sealed_kernel: Optional[SealedKernel] = await self.ledger.compile_kernel(stream)

            ## Gatekeep: Interpret the ledger's output
            if sealed_kernel is not None:
                log.info(f"[Bridge] AUTHORIZED: Stream {stream.id} successfully sealed into {sealed_kernel.kernel_id}.")
                return True
            else:
                log.warning(f"[Bridge] BLOCKED: Stream {stream.id} failed topological sealing (Insufficient tension).")
                return False

        except Exception as e:
            ## @failure.mode: Fail-Closed. Any error in the ledger denies execution.
            log.error(f"[Bridge] Ledger evaluation failed with exception: {e}. Defaulting to BLOCKED.")
            return False


class BypassBridge(LedgerBridge):
    """
    @desc: A development/testing dummy bridge. Always authorizes actions without invoking the heavy Merkle ledger computations
    @usage: Inject this during local debugging or unit testing
    """
    async def authorize(self, action_id: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        log.debug(f"[Bridge: Bypass] Auto-authorizing action {action_id} (DEV MODE).")
        return True