# dphi.receptor.ingress.server.middleware
## @lineage: receptor.ingress.server.middleware
import hashlib
import os
import time
from typing import Callable
from urllib.parse import urlparse

from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
from starlette.types import ASGIApp

from kernel.dphi.adapter.sign import NodeSigner
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter
from watcher.plane.observer.span import start_active_span, end_active_span

log = get_emitter("server.middleware")

class WasTelemetry(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        conv_id = self._extract_conversation_id(request.url.path)
        
        if not conv_id:
            return await call_next(request)

        span_name = f"HTTP {request.method} {request.url.path}"
        start_active_span(name=span_name, session_id=conv_id)

        try:
            start_time = time.perf_counter()
            response = await call_next(request)
            latency = time.perf_counter() - start_time
            log.info(
                f"[@observe] conv:{conv_id} | status:{response.status_code} | latency:{latency:.4f}s",
                context={
                    "phase": "api_dispatch",
                    "conv_id": conv_id,
                    "status_code": response.status_code,
                    "latency_ms": round(latency * 1000, 2)
                }
            )
            
            return response
        except Exception as e:
            log.error(f"[@observe] API Error in conv:{conv_id} - {str(e)}")
            raise e
        finally:
            end_active_span()

    def _extract_conversation_id(self, path: str) -> str:
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "conversations":
            return parts[1]
        return ""

class LocalMiddleware(CORSMiddleware):
    def __init__(self, app: ASGIApp, allow_origins: list[str]) -> None:
        super().__init__(
            app,
            allow_origins=allow_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def is_allowed_origin(self, origin: str) -> bool:
        if origin and not self.allow_origins and not self.allow_origin_regex:
            parsed = urlparse(origin)
            hostname = parsed.hostname or ""
            if hostname in ["localhost", "127.0.0.1"]:
                return True

            docker_host_addr = os.environ.get("DOCKER_HOST_ADDR")
            if docker_host_addr and hostname == docker_host_addr:
                return True

        result: bool = super().is_allowed_origin(origin)
        return result

class AttestationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)
        if isinstance(response, StreamingResponse):
            return response

        body_bytes = b""
        async for chunk in response.body_iterator:
            body_bytes += chunk
            
        if not body_bytes or response.status_code >= 400:
            return self._reconstruct_response(response, body_bytes)

        timestamp = int(time.time())
        body_hash = hashlib.sha256(body_bytes).hexdigest()
        request_path = request.url.path
        signature_payload = {
            "path": request_path,
            "timestamp": timestamp,
            "body_hash": body_hash
        }
        canonical_bytes = StateAdapter.to_canonical_bytes(signature_payload)

        signer = NodeSigner.get_instance()
        try:
            signature_hex = signer.sign_payload(canonical_bytes)
        except Exception as e:
            log.error(f"[Attestation] Failed to sign response payload: {e}")
            return self._reconstruct_response(response, body_bytes)

        response.headers["X-Dphi-Signature"] = signature_hex
        response.headers["X-Dphi-Timestamp"] = str(timestamp)
        response.headers["X-Dphi-Signer"] = signer.pubkey_hex
        
        response.headers["X-Dphi-Content-Hash"] = body_hash
        log.debug(f"[Attestation] Payload signed for {request_path}. Hash: {body_hash[:8]}")
        return self._reconstruct_response(response, body_bytes)

    def _reconstruct_response(self, response: Response, body_bytes: bytes) -> Response:
        """소비된 body_iterator를 다시 생성하여 Response 객체를 복구합니다."""
        async def new_body_iterator():
            yield body_bytes
        response.body_iterator = new_body_iterator()
        return response