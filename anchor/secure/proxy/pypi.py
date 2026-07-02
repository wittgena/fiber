# anchor.secure.proxy.pypi
## @lineage: anchor.registry.proxy.pypi
import sys
import threading
import urllib.request
from urllib.error import HTTPError, URLError
from http.server import BaseHTTPRequestHandler, HTTPServer
import anyio
from mcp.server.mcpserver.server import MCPServer
from watcher.plane.emitter import get_emitter

## @config: Initialize MCP Server and Emitter (Routing logs to stderr is CRITICAL for stdio MCP)
log = get_emitter("proxy.pypi")
mcp = MCPServer(name="mcp-pypi-proxy")

PORT = 8083
_proxy_server = None
_proxy_thread = None

class PyPIProxyHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        """
        @desc: Suppress default HTTP server logging to sys.stdout.
               This prevents JSON-RPC protocol corruption in the stdio MCP transport.
        """
        # Uncomment below to log HTTP access to stderr instead of stdout
        # log.debug(f"[Proxy Access] {self.address_string()} - {format%args}", file=sys.stderr)
        pass

    def do_GET(self):
        """
        @desc: Intercepts local requests and transparently fetches real metadata from pypi.org.
        """
        target_url = f"https://pypi.org{self.path}"
        log.info(f"[Mock Registry] Proxying request ➔ {target_url}", file=sys.stderr)
        
        try:
            req = urllib.request.Request(target_url)
            with urllib.request.urlopen(req) as response:
                self.send_response(response.status)
                for k, v in response.headers.items():
                    # Strip hop-by-hop or encoding headers that might confuse the client
                    if k.lower() not in ['transfer-encoding', 'content-encoding']:
                        self.send_header(k, v)
                self.end_headers()
                self.wfile.write(response.read())
                
        except HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
        except URLError as e:
            self.send_response(502) # Bad Gateway
            self.end_headers()
            self.wfile.write(f"Proxy Error: {e.reason}".encode())


def _start_background_proxy():
    """
    @action: Spawns the blocking HTTPServer in a daemon thread 
             to allow the async MCP event loop to run concurrently.
    """
    global _proxy_server, _proxy_thread
    if _proxy_server is not None:
        return
        
    ## @fix: Explicitly bind to IPv4 ('127.0.0.1') instead of 'localhost' 
    ## to prevent socket.connect_ex timeout issues in the Builder due to IPv6 (::1) resolution.
    _proxy_server = HTTPServer(('127.0.0.1', PORT), PyPIProxyHandler)
    _proxy_thread = threading.Thread(target=_proxy_server.serve_forever, daemon=True)
    _proxy_thread.start()
    log.info(f"🚀 [Proxy] Background Mock Registry started on 127.0.0.1:{PORT}", file=sys.stderr)


## ============================================================================
## MCP Tools Definition
## ============================================================================

## @fix: Removed 'ctx: Context' from tool arguments. 
## Pydantic cannot generate JSON schemas for internal class instances, which previously caused crashes.

@mcp.tool()
async def check_proxy_status() -> str:
    """
    @desc: Tool for the Agent to check if the internal PyPI mock proxy is actively running.
    """
    if _proxy_server and _proxy_thread and _proxy_thread.is_alive():
        return f"[STATUS] Proxy is ACTIVE and listening on port {PORT}"
    return "[STATUS] Proxy is INACTIVE or failed to start."

@mcp.tool()
async def simulate_package_fetch(package_name: str) -> str:
    """
    @desc: Tool for the Agent to query PyPI directly to verify package availability.
           Useful for verifying dependencies before triggering a build.
    """
    url = f"https://pypi.org/simple/{package_name}/"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            return f"[SUCCESS] Package '{package_name}' verified. Status: {response.status}"
    except HTTPError as e:
        return f"[ERROR] Package '{package_name}' fetch failed. HTTP Code: {e.code}"
    except URLError as e:
        return f"[ERROR] Network failure: {e.reason}"

def main():  # 👈 async 키워드 제거
    """
    @action: Ignites the background proxy and starts the main stdio MCP server loop.
    """
    ## 1. Ignite the HTTP Proxy (Daemon)
    _start_background_proxy()
    
    ## 2. Start MCP stdio communication
    log.info("🔌 [MCP] Starting stdio MCP server for PyPI Proxy...", file=sys.stderr)
    
    ## @fix: MCPServer.run() 자체적으로 anyio.run()을 호출하므로, 동기적으로 호출해야 합니다.
    mcp.run()

if __name__ == "__main__":
    main()
