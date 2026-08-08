# receptor.ingress.server.mcp
## @lineage: epoch.time.ingress.server.mcp
## @lineage: dphi.gov.server.mcp
## @lineage: arch.gov.server.mcp
## @lineage: kernel.topos.gov.server.mcp
## @lineage: kernel.arch.gov.server.mcp
## @lineage: arch.kernel.gov.mcp
## @lineage: arch.server.gov.mcp
## @lineage: arch.topos.server.mcp
# arch.topos.server.mcp (개선본)
import inspect
import json
from typing import Any, Optional, List
import httpx

from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.types import Scope, Receive, Send

from mcp_types import TextContent
from mcp.server.mcpserver.server import MCPServer
from watcher.plane.emitter import get_emitter

from receptor.ingress.sentinel import SpecValidator
from kernel.phase.mesh.gateway import ToposGateway

log = get_emitter("server.mcp", phase="DEFENSE")

class SentinelFirewallMiddleware:
    """ASGI Middleware integrating strict validation and Volumetric defense."""
    def __init__(self, app, max_body_size: int = 1024 * 1024 * 5):
        self.app = app
        self.max_body_size = max_body_size
        self.validator = SpecValidator()

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        headers = dict(scope.get("headers", []))
        
        # [FIX 1] Chunked Encoding 차단 (Middleware Bypass 방어)
        # Transfer-Encoding: chunked를 악용한 OOM 우회 공격을 원천 차단합니다.
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


class FastAPIMCPAdapter:
    """Automated Bridge with strict auth propagation and context retention."""
    def __init__(self, mcp_server: "SecureMCPServer", fastapi_app: FastAPI):
        self.mcp_server = mcp_server
        self.app = fastapi_app

    def register_routes(self, allowed_tags: List[str]):
        # [FIX 2] Over-projection 방어: Blacklist -> Whitelist 구조로 변경
        # 개발자가 명시적으로 'mcp-exposed' 태그를 부여한 안전한 라우트만 도구로 변환합니다.
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
            # [FIX 3] Auth Bypass 방어 (Security Context Propagation)
            # LLM 스키마에서는 Depends를 제거하더라도, 런타임에는 MCP 클라이언트의 원래 세션/인증 헤더를
            # 백엔드 루프백 호출(httpx)에 강제로 주입하여 FastAPI의 권한 검증이 동작하게 만듭니다.
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

        # LLM에게 노출되는 스키마에서만 시스템 파라미터를 숨김 처리
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
        # [NEW] Sentinel의 핵심 인가 엔진 연결
        self.gateway = ToposGateway()

    def bind_fastapi(self, app: FastAPI, allowed_tags: List[str] = ["mcp-exposed"]):
        """Binds a FastAPI application securely using Whitelist projection."""
        adapter = FastAPIMCPAdapter(self, app)
        adapter.register_routes(allowed_tags=allowed_tags)

    async def _handle_call_tool(self, ctx, params):
        # [FIX 4] Tool Poisoning / OS Command Injection 방어 (Gateway 인가)
        # LLM이 특정 도구를 호출하기 직전, 파라미터 내부에 악성 셸 명령어나
        # 인젝션 구문이 있는지 Sentinel의 ToposGateway가 평가하고 인가합니다.
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

        # 인가 성공 시에만 도구 실행
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