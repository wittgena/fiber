# bound.proxy.router
## @lineage: gov.sandbox.router.infra
"""
@desc: Centralized topology routing hub resolving HTTP/WS endpoints and telemetry headers.
@flow: base configuration -> parameter mapping -> synchronized network URI resolution.
"""
import urllib.parse
from typing import Dict, Any, Optional

from watcher.tracer.scope import get_current_trace_path

class InfraRouter:
    """
    @desc: Encapsulates network routing topology and credential/telemetry header generation.
    @flow: host_url registration -> endpoint lookup -> fully qualified URI output.
    """
    def __init__(self, host_url: str, session_api_key: Optional[str] = None):
        self.host_url = host_url.rstrip("/")
        self.session_api_key = session_api_key
        
        ## @step: Dynamically convert HTTP schema into WebSocket protocol equivalents
        parsed = urllib.parse.urlparse(self.host_url)
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        self.ws_url = f"{ws_scheme}://{parsed.netloc}"

    def get_http_endpoint(self, path_type: str, **kwargs) -> str:
        """@flow: path_type -> template replacement -> absolute HTTP URL"""
        ## @step: Map functional identifier keys to structural path suffixes
        routes = {
            "health_check": "/sockets/health_check",
            "provision": "/api/v1/workspace/provision",
            "teardown": f"/api/v1/workspace/{kwargs.get('workspace_ref', '')}"
        }
        
        if path_type not in routes:
            raise KeyError(f"Topological anomaly: Unknown HTTP path_type mapping '{path_type}'")
            
        return f"{self.host_url}{routes[path_type]}"

    def get_ws_endpoint(self, path_type: str, **kwargs) -> str:
        """@flow: path_type -> template replacement -> absolute WebSocket URL"""
        ## @step: Map streaming identifier keys to parameterized query string paths
        routes = {
            "events": f"/sockets/events/{kwargs.get('conversation_id', '')}?resend_mode=since",
            "bash": "/bash-events"
        }
        
        if path_type not in routes:
            raise KeyError(f"Topological anomaly: Unknown WebSocket path_type mapping '{path_type}'")
            
        return f"{self.ws_url}{routes[path_type]}"

    def build_headers(self, custom_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """@flow: static headers -> contextvar interception -> unified telemetry carrier"""
        headers = custom_headers or {}
        
        ## @step: Inject access token credential signature
        if self.session_api_key:
            headers["x-session-api-key"] = self.session_api_key
            
        ## @step: Capture local context tracing residue and propagate across network boundary
        trace_path = get_current_trace_path()
        if trace_path:
            headers["x-trace-path"] = str(trace_path)
            
        return headers