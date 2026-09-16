# fiber.phase.contract.server
## @lineage: xphi.arch.contract.server
import os
import time
import json
import inspect
import hashlib
from typing import Any, Optional, List, Callable
from urllib.parse import urlparse
import httpx

from fastapi import FastAPI, Request, Response
from fastapi.routing import APIRoute
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from starlette.types import Scope, Receive, Send, ASGIApp

from mcp_types import TextContent
from mcp.server.mcpserver.server import MCPServer

from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.observer.span import start_active_span, end_active_span
from xphi.arch.dev.transport.sentinel import SpecValidator
from xphi.state.anchor.gateway import StoreGateway
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.bound.adapter.state import StateAdapter

log = get_emitter("arch.contract.server", phase="NETWORK")

# ============================================================================
# 1. MIDDLEWARES (Security, Telemetry, CORS, Attestation)
# ============================================================================

class SentinelFirewallMiddleware:
    """ASGI Middleware integrating strict validation and Volumetric defense."""
    def __init__(self, app: ASGIApp, max_body_size: int = 1024 * 1024 * 5):
        self.app = app
        self.max_body_size = max_body_size
        self.validator = SpecValidator()

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        headers = dict(scope.get("headers", []))
        if b"chunked" in headers.get(b"transfer-encoding", b""):
            response = JSONResponse({"detail": "Chunked encoding not permitted by Membrane"}, status_code=411)
            return await response(scope, receive, send)

        content_length = int(headers.get(b"content-length", 0))
        if content_length > self.max_body_size:
            response = JSONResponse({"detail": "Payload Too Large"}, status_code=413)
            return await response(scope, receive, send)
            
        path = scope.get("path", "")
        if path.startswith("/custom"):
            if b"authorization" not in headers:
                response = JSONResponse({"detail": "Unauthorized Custom Route"}, status_code=401)
                return await response(scope, receive, send)

        await self.app(scope, receive, send)


class WasTelemetry(BaseHTTPMiddleware):
    """Telemetry middleware to track latency and manage active spans."""
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
    """Custom CORS Middleware handling local and dynamic docker host origins."""
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
    """Cryptographically signs HTTP responses using the Node's private key."""
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
        """Restores the consumed body_iterator to reconstruct the Response object."""
        async def new_body_iterator():
            yield body_bytes
        response.body_iterator = new_body_iterator()
        return response


# ============================================================================
# 2. PROTOCOL ADAPTERS (MCP Bridge & Server)
# ============================================================================

class FastAPIMCPAdapter:
    """Automated Bridge with strict auth propagation and context retention."""
    def __init__(self, mcp_server: "SecureMCPServer", fastapi_app: FastAPI):
        self.mcp_server = mcp_server
        self.app = fastapi_app

    def register_routes(self, allowed_tags: List[str]):
        registered_count = 0
        for route in self.app.routes:
            if not isinstance(route, APIRoute):
                continue

            if not route.tags or not any(tag in allowed_tags for tag in route.tags):
                continue

            self._project_route_to_mcp_tool(route)
            registered_count += 1

        log.info(f"[MCP Adapter] Safely registered {registered_count} FastAPI routes via Whitelist.")

    def _project_route_to_mcp_tool(self, route: APIRoute):
        tool_name = route.name or route.path.strip("/").replace("/", "_")
        path = route.path
        methods = list(route.methods)
        http_method = "POST" if "POST" in methods else ("GET" if "GET" in methods else methods[0])
        
        description = route.description or route.summary or f"Automated REST tool for {http_method} {path}"
        docstring = f"[REST Endpoint: {http_method} {path}]\n{description}"

        # Internal Loopback Dispatcher
        async def dynamic_tool_handler(ctx: Any = None, **kwargs) -> str:
            client_headers = {}
            if ctx and hasattr(ctx, "session_token"):
                client_headers["Authorization"] = f"Bearer {ctx.session_token}"

            async with httpx.AsyncClient(app=self.app, base_url="http://internal-loopback", headers=client_headers) as client:
                try:
                    if http_method == "GET":
                        response = await client.get(path, params=kwargs)
                    elif http_method in ["POST", "PUT", "PATCH"]:
                        response = await client.request(http_method, path, json=kwargs)
                    elif http_method == "DELETE":
                        response = await client.delete(path, params=kwargs)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {http_method}")

                    if response.status_code >= 400:
                        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

                    return response.text
                except Exception as e:
                    log.error(f"[MCP Adapter Error] Tool '{tool_name}' execution failed: {str(e)}")
                    raise

        dynamic_tool_handler.__name__ = tool_name
        dynamic_tool_handler.__doc__ = docstring
        original_sig = inspect.signature(route.endpoint)
        clean_params = []
        for param in original_sig.parameters.values():
            if param.default.__class__.__name__ in ["Depends", "Param"] or getattr(param.annotation, "__name__", "") == "Request":
                continue
            clean_params.append(param)
            
        dynamic_tool_handler.__signature__ = original_sig.replace(parameters=clean_params)

        self.mcp_server.tool()(dynamic_tool_handler)
        log.debug(f"[MCP Adapter] Tool mapped safely: {tool_name} -> [{http_method}] {path}")


class SecureMCPServer(MCPServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gateway = StoreGateway()

    def bind_fastapi(self, app: FastAPI, allowed_tags: List[str] = ["mcp-exposed"]):
        """Binds a FastAPI application securely using Whitelist projection."""
        adapter = FastAPIMCPAdapter(self, app)
        adapter.register_routes(allowed_tags=allowed_tags)

    async def _handle_call_tool(self, ctx, params):
        tool_name = params.name
        is_authorized = await self.gateway.authorize(
            action_id=f"invoke_tool_{tool_name}",
            action="INVOKE_TOOL",
            payload={"tool": tool_name, "params": params.arguments}
        )
        
        if not is_authorized:
            log.warning(f"[Security] Tool '{tool_name}' blocked by ToposGateway (Possible Prompt Injection).")
            return type('Result', (), {'is_error': True, 'content': [
                TextContent(type="text", text="Security Exception: Tool execution blocked by Sentinel Gateway.")
            ]})()

        result = await super()._handle_call_tool(ctx, params)
        if getattr(result, "is_error", False):
            result.content = [
                TextContent(
                    type="text", 
                    text="Internal Tool Error: The operation failed securely. Check server logs."
                )
            ]
        return result

    def sse_app(self, **kwargs) -> Any:
        app = super().sse_app(**kwargs)
        return SentinelFirewallMiddleware(app)

    def streamable_http_app(self, **kwargs) -> Any:
        app = super().streamable_http_app(**kwargs)
        return SentinelFirewallMiddleware(app)