# receptor.ingress.server.middleware
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

        # 1. Laminar/OTel 분산 추적(Span) 시작
        span_name = f"HTTP {request.method} {request.url.path}"
        start_active_span(name=span_name, session_id=conv_id)

        try:
            start_time = time.perf_counter()
            
            # 다음 라우터로 넘김 (I/O 파싱 없음)
            response = await call_next(request)
            
            latency = time.perf_counter() - start_time

            # 2. Emitter를 통한 도메인 이벤트 방출
            # -> otel_log_interceptor에 의해 현재 Span의 Event로 자동 기록됨
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
            # 예외 발생 시 에러 로깅 (이 역시 OTel Span Status에 ERROR로 자동 매핑됨)
            log.error(f"[@observe] API Error in conv:{conv_id} - {str(e)}")
            raise e

        finally:
            # 3. Span 종료
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
    """
    HTTP Response Payload(Body)를 가로채어, 서버의 프라이빗 키(NodeSigner)로 서명한 뒤
    X-Dphi-Signature 헤더를 주입하는 First-Party Oracle 증명 미들웨어입니다.
    """
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # 1. 다음 미들웨어 또는 라우터(엔드포인트) 실행
        response = await call_next(request)
        
        # 2. 예외 처리: StreamingResponse는 버퍼에 담을 수 없으므로 서명 제외
        if isinstance(response, StreamingResponse):
            return response

        # 3. HTTP 응답 바디(Payload) 추출
        # Starlette 구조상 body_iterator를 순회하여 바디를 읽어내야 합니다.
        body_bytes = b""
        async for chunk in response.body_iterator:
            body_bytes += chunk
            
        # 4. 빈 바디이거나 에러 상태(4xx, 5xx)인 경우 선택적으로 서명을 생략할 수 있습니다.
        # (원한다면 에러 메시지까지 증명할 수 있으나 통상적으로 성공(2xx) 응답에만 증명 부여)
        if not body_bytes or response.status_code >= 400:
            return self._reconstruct_response(response, body_bytes)

        # 5. 서명을 위한 Context 구성
        timestamp = int(time.time())
        body_hash = hashlib.sha256(body_bytes).hexdigest()
        request_path = request.url.path

        # 6. 정규화 (Canonicalization) - StateAdapter 활용
        # StateAdapter.to_canonical_bytes()를 통해 JSON 키 정렬 및 공백 제거 강제
        signature_payload = {
            "path": request_path,
            "timestamp": timestamp,
            "body_hash": body_hash
        }
        canonical_bytes = StateAdapter.to_canonical_bytes(signature_payload)

        # 7. 서명 생성 (Ed25519) - NodeSigner 활용
        signer = NodeSigner.get_instance()
        try:
            signature_hex = signer.sign_payload(canonical_bytes)
        except Exception as e:
            log.error(f"[Attestation] Failed to sign response payload: {e}")
            return self._reconstruct_response(response, body_bytes)

        # 8. 헤더 주입
        # X-Dphi-Attestation 헤더 하나에 JSON 구조로 담거나, 개별 헤더로 나눌 수 있습니다.
        # 파싱의 용이성을 위해 개별 헤더를 권장합니다.
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