# bound.adapter.compiler
"""
@desc: 
- Interceptor Bridge middleware decoupling Agent/Ingress execution from Kernel Compiler sealing.
- Acts as a structural adapter: translates both flat legacy payloads and strict 
  ingress logic streams into the topological kernel context.
"""
import uuid
from typing import Any, Dict, Optional
from anchor.phase.ingress.proxy.schema import LogicStream as IngressLogicStream
from watcher.kernel.compiler import ToposCompiler, LogicStream as KernelLogicStream, SealedKernel
from watcher.plane.emitter import get_emitter

log = get_emitter("kernel.bridge", phase="KERNEL")

class CompilerBridge:
    """@desc: Compliant middleware & Adapter bridging Ingress to the ToposCompiler"""
    def __init__(self, compiler: Optional[ToposCompiler] = None):
        self.compiler = compiler or ToposCompiler()

    async def authorize_ingress(self, stream: IngressLogicStream) -> bool:
        """
        @desc: Structural adapter for Ingress validation.
        - Unpacks the 3D rigid Ingress schema into the 2D flat context required by the core Compiler.
        """
        action_id = str(stream.meta.stream_id)
        payload = {
            "intent": stream.payload.intent.value,
            "parameters": stream.payload.parameters
        }
        metadata = {
            "is_authenticated": stream.identity.is_authenticated,
            "stateless_token": stream.identity.stateless_token_id,
            "client_ip": stream.meta.client_ip,
            "protocol_version": stream.meta.original_protocol.value
        }
        
        ## @action: Delegate to the core authorization choke-point
        return await self.authorize(action_id=action_id, payload=payload, metadata=metadata)

    async def authorize(self, action_id: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        @desc: The single choke-point for agent action validation.
        @flow: Raw Context -> KernelLogicStream -> ToposCompiler -> Boolean Signal
        """
        if metadata is None:
            metadata = {}

        ## Adapt: Translate raw context into a topological KernelLogicStream
        kernel_stream = KernelLogicStream(
            id=action_id or str(uuid.uuid4()),
            payload=payload,
            metadata=metadata
        )

        log.debug(f"[Bridge] Requesting compiler authorization for stream: {kernel_stream.id}")

        try:
            # 💡 [Aligned] Delegate to the compiler's sealing routine
            sealed_kernel: Optional[SealedKernel] = await self.compiler.compile_kernel(kernel_stream)

            if sealed_kernel is not None:
                log.info(f"[Bridge] AUTHORIZED: Stream {kernel_stream.id} successfully sealed into {sealed_kernel.kernel_id}.")
                return True
            else:
                log.warning(f"[Bridge] BLOCKED: Stream {kernel_stream.id} failed topological sealing (Insufficient tension).")
                return False

        except Exception as e:
            ## @failure.mode: Fail-Closed. Any error in the compiler denies execution.
            log.error(f"[Bridge] Compiler evaluation failed with exception: {e}. Defaulting to BLOCKED.")
            return False

class BypassBridge(CompilerBridge):
    """@desc: A development/testing dummy bridge. Auto-authorizes actions (DEV MODE)."""
    async def authorize(self, action_id: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        log.debug(f"[Bridge: Bypass] Auto-authorizing action {action_id} (DEV MODE).")
        return True