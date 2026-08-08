# receptor.ingress.server.middleware
## @lineage: epoch.time.ingress.server.middleware
## @lineage: dphi.gov.server.middleware
## @lineage: arch.gov.server.middleware
## @lineage: kernel.topos.gov.server.middleware
## @lineage: kernel.arch.gov.server.middleware
## @lineage: arch.kernel.gov.middleware
## @lineage: arch.server.gov.middleware
## @lineage: arch.topos.server.middleware
## @lineage: topos.xelog.middleware
import time
import hashlib
import json
import os
from urllib.parse import urlparse
from typing import Dict, List
from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware

from starlette.types import ASGIApp
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from watcher.plane.observer.span import start_active_span, end_active_span
from watcher.plane.emitter import get_emitter

log = get_emitter("was.middleware")

class WasTelemetry(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
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
