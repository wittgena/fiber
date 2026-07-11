# bound.ingress.router
## @lineage: xphi.proxy.ingress.router
## @lineage: bound.transport.stream.ingress.router
"""
@manifold: bound.transport.stream.ingress.router
@desc: Ingress entry point. 
Intercepts macroscopic network traffic, enforces strict spec invariants, 
and bridges validated logic streams into the core ledger matrix.
"""
from bound.ingress.stream.transducer import SpecValidator
from bound.adapter.compiler import CompilerBridge
from xphi.watcher.topos import unified_flow_span
from watcher.plane.emitter import get_emitter

log = get_emitter("ingress.router")

class IngressRouter:
    """
    @desc: Orchestrates transduction and topological sealing.
    Maintains clean boundaries by delegating structural adaptation to the Bridge.
    """
    def __init__(self, bridge: CompilerBridge):
        self.validator = SpecValidator()
        self.bridge = bridge

    async def handle_request(self, headers: dict, raw_body: bytes, client_ip: str):
        ## @action: Initialize native spatial flow.
        with unified_flow_span("ingress.router.pipeline", auto_flush=True, client_ip=client_ip) as flow_ctx:
            try:
                ## Transduce & Validate (Rigid Shield)
                safe_stream = self.validator.process_ingress(headers, raw_body, client_ip)
                
                flow_ctx["stream_id"] = str(safe_stream.meta.stream_id)
                flow_ctx["intent"] = safe_stream.payload.intent.value

                ## Authorize via Bridge (Native Stream Adaptation)
                is_authorized = await self.bridge.authorize_ingress(safe_stream)
                
                ## Evaluate & Return
                if not is_authorized:
                    flow_ctx["ledger_status"] = "denied"
                    log.warning("Topological sealing denied by the core Ledger.")
                    return {"status": "denied"}
                    
                flow_ctx["ledger_status"] = "authorized"
                log.info("Request successfully sealed into the manifold.")
                return {"status": "success", "intent": safe_stream.payload.intent.value}
                
            except ValueError as ve:
                log.warning(f"Ingress boundary dropped payload: {ve}")
                return {"status": "dropped", "reason": "Spec invariant violation"}
                
            except Exception as e:
                log.error(f"Manifold routing collapsed: {e}")
                return {"status": "error", "reason": "Internal boundary failure"}