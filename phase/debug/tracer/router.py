# fiber.phase.debug.tracer.router
## @lineage: fiber.phase.tracer.router
## @lineage: phase.tracer.router
import urllib.parse
from typing import Dict, Any, Optional

from xphi.watcher.tracer.scope import get_current_trace_path

class InfraRouter:
    def __init__(self, host_url: str, session_api_key: Optional[str] = None):
        self.host_url = host_url.rstrip("/")
        self.session_api_key = session_api_key
        parsed = urllib.parse.urlparse(self.host_url)
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        self.ws_url = f"{ws_scheme}://{parsed.netloc}"

    def get_http_endpoint(self, path_type: str, **kwargs) -> str:
        routes = {
            "health_check": "/sockets/health_check",
            "provision": "/api/v1/workspace/provision",
            "teardown": f"/api/v1/workspace/{kwargs.get('workspace_ref', '')}"
        }
        if path_type not in routes:
            raise KeyError(f"Topological anomaly: Unknown HTTP path_type mapping '{path_type}'")
            
        return f"{self.host_url}{routes[path_type]}"

    def get_ws_endpoint(self, path_type: str, **kwargs) -> str:
        routes = {
            "events": f"/sockets/events/{kwargs.get('conversation_id', '')}?resend_mode=since",
            "bash": "/bash-events"
        }
        if path_type not in routes:
            raise KeyError(f"Topological anomaly: Unknown WebSocket path_type mapping '{path_type}'")
            
        return f"{self.ws_url}{routes[path_type]}"

    def build_headers(self, custom_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = custom_headers or {}
        if self.session_api_key:
            headers["x-session-api-key"] = self.session_api_key
        trace_path = get_current_trace_path()
        if trace_path:
            headers["x-trace-path"] = str(trace_path)
            
        return headers