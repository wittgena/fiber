# xphi.proxy.ingress.transducer
## @lineage: bound.transport.stream.ingress.transducer
"""
@manifold: bound.transport.stream.ingress.transducer
@desc: 
- Ingress boundary defense
- Strips external protocol shells and folds chaotic raw byte-streams into deterministic topological logic streams
"""
import json
from typing import Dict
from pydantic import ValidationError
from xphi.proxy.ingress.schema import (
    LogicStream,
    StreamMetadata,
    StreamIdentity,
    LogicPayload,
    ProtocolSource,
    ActionIntent
)
from bound.watcher.topos import span_context
from watcher.plane.emitter import get_emitter

log = get_emitter("stream.transducer", phase="SYSTEM")

class StreamTransducer:
    """
    @desc: L7 Volumetric guard and semantic parser. 
    Neutralizes malformed vectors before formal schema projection.
    """
    MAX_PAYLOAD_SIZE = 5242880  # 5MB

    def __init__(self):
        self._intent_router = {
            "initialize": ActionIntent.INITIALIZE,
            "invoke": ActionIntent.INVOKE_TOOL,
            "read_resource": ActionIntent.READ_RESOURCE
        }

    def process_ingress(self, headers: Dict[str, str], raw_body: bytes, client_ip: str) -> LogicStream:
        """@action: Strip transport shell and cast to a 1D LogicStream."""
        
        body_length = len(raw_body)
        if body_length > self.MAX_PAYLOAD_SIZE:
            raise ValueError("Volumetric invariant breached. Payload exceeds absolute limit.")

        auth_header = headers.get("authorization")
        if not auth_header:
            raise PermissionError("Boundary breach: Missing stateless authorization.")

        parsed_json = json.loads(raw_body)
        raw_action = parsed_json.get("action")
        intent = self._intent_router.get(raw_action)
        
        if not intent:
            raise ValueError(f"Topological ambiguity: Unrecognized action '{raw_action}'")

        claimed_version = parsed_json.get("protocolVersion", "unknown")
        protocol_source = ProtocolSource(claimed_version) if claimed_version in ["1.0", "2.0"] else ProtocolSource.UNKNOWN

        meta = StreamMetadata(
            original_protocol=protocol_source,
            content_length=body_length,
            client_ip=client_ip
        )
        
        identity = StreamIdentity(
            is_authenticated=True,
            stateless_token_id=auth_header.replace("Bearer ", "")
        )
        
        payload = LogicPayload(
            intent=intent,
            parameters=parsed_json.get("params", {})
        )

        ## @action: Trigger Pydantic schema validation (Raises ValidationError on failure)
        return LogicStream(meta=meta, identity=identity, payload=payload)


class SpecValidator:
    """
    @desc: Formal topological validator. 
    Enforces schema invariants within an observable phase context.
    """
    def __init__(self):
        self.transducer = StreamTransducer()

    def process_ingress(self, headers: Dict[str, str], raw_body: bytes, client_ip: str) -> LogicStream:
        """@action: Fold raw vectors into an immutable Spec and record spatial tension."""
        
        ## @action: Anchor evaluation within the native Topos manifold
        with span_context("ingress.spec.validation", attributes={"client_ip": client_ip, "body_size": len(raw_body)}):
            try:
                valid_stream = self.transducer.process_ingress(headers, raw_body, client_ip)
                log.info(f"Spec invariant confirmed. Stream ID: {valid_stream.meta.stream_id}")
                return valid_stream
            except ValidationError as ve:
                ## @failure: Smuggling or OOM vector detected via strict schema rejection
                log.error(f"Spec Violation (Smuggling/OOM Attempt): {ve.errors()}")
                raise ValueError("Payload violates strict topological ingress spec.") from ve
            except Exception as e:
                log.critical(f"Transduction collapse: {str(e)}")
                raise