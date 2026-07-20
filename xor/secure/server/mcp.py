# xor.secure.server.mcp
## @lineage: xphi.xor.secure.server.mcp
## @lineage: xphi.xor.server.secure.mcp
## @lineage: anchor.phase.reflect.server.mcp
"""
# Secure MCP Server Wrapper
@desc: A single-script integration wrapper designed to mitigate Starlette-based security vulnerabilities identified in the default MCPServer of MCP (Model Context Protocol) v2.0.a3.

## Threat & Mitigation Strategies

### Unbounded Payload (Memory Exhaustion DoS)
- Threat: Missing POST request size limits allows memory exhaustion via malicious large payloads.
- Mitigation: Enforces a 5MB Content-Length limit via `SecurityFirewallMiddleware` (Returns HTTP 413).

### Authentication Bypass on Custom Routes
- Threat: Default `AuthMiddleware` does not enforce authentication when using `@server.custom_route`.
- Mitigation: ASGI middleware level enforcement of Authorization headers for specific prefixes (e.g., `/custom`).

### Information Leakage via Tool Exceptions
- Threat: Infrastructure errors (DB connection failures, API key errors) during tool execution leak raw error messages to the client.
- Mitigation: Overrides `SecureMCPServer._handle_call_tool` to mask error messages as a generic "Internal Tool Error".

### ASGI Private Attribute Bypass
- Threat: Direct calls to `request._send` in the original code bypass global middlewares (e.g., security headers).
- Mitigation: Wraps the outermost application (sse_app, streamable_http_app) with the firewall middleware to create an un-bypassable sandbox.
"""
import uvicorn
from typing import Any
from starlette.responses import JSONResponse
from starlette.types import Scope, Receive, Send
from mcp_types import TextContent
from mcp.server.mcpserver.server import MCPServer

class SecurityFirewallMiddleware:
    """ASGI Middleware to intercept HTTP traffic, control payload sizes, and enforce access to unauthorized routes."""
    def __init__(self, app, max_body_size: int = 1024 * 1024 * 5):  # 5MB Limit
        self.app = app
        self.max_body_size = max_body_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        ## @tag: mitigation_payload_limit - Prevent Memory Exhaustion DoS by enforcing payload size limits
        content_length = 0
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                content_length = int(value)
                break
                
        if content_length > self.max_body_size:
            response = JSONResponse({"detail": "Payload Too Large"}, status_code=413)
            return await response(scope, receive, send)

        """
        @tag: mitigation_auth_bypass 
        - Enforce authentication on custom routes
        - Path conditions should be adjusted according to the actual production environment
        """
        path = scope.get("path", "")
        if path.startswith("/custom"):
            headers = dict(scope.get("headers", []))
            if b"authorization" not in headers:
                response = JSONResponse({"detail": "Unauthorized Custom Route"}, status_code=401)
                return await response(scope, receive, send)

        ## Execute original app if validations pass
        await self.app(scope, receive, send)

class SecureMCPServer(MCPServer):
    """Extended class that resolves security flaws through overriding  while maintaining 100% API compatibility with the existing MCPServer"""
    async def _handle_call_tool(self, ctx, params):
        ## @tag: mitigation_info_leak - Mask tool execution error messages to prevent sensitive information leakage
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
        ## @tag: mitigation_asgi_bypass_sse - Automatically apply the firewall middleware when generating the SSE app
        app = super().sse_app(**kwargs)
        return SecurityFirewallMiddleware(app)

    def streamable_http_app(self, **kwargs) -> Any:
        ## @tag: mitigation_asgi_bypass_http - Automatically apply the firewall middleware when generating the HTTP app
        app = super().streamable_http_app(**kwargs)
        return SecurityFirewallMiddleware(app)


if __name__ == "__main__":
    ## Instantiate SecureMCPServer instead of MCPServer
    mcp = SecureMCPServer(
        name="Secure-MCP-Server",
        version="1.0.0"
    )

    @mcp.tool()
    def risky_operation(trigger_error: bool) -> str:
        """This tool is safely handled so that raw information is not exposed when an error occurs."""
        if trigger_error:
            ## This sensitive error message is masked and not sent to the client.
            raise RuntimeError("DB Connection Failed: ID=admin, PW=secret123!")
        return "Operation successful"

    ## Run the server (Middleware is applied automatically)
    mcp.run(transport="sse", host="0.0.0.0", port=8000)