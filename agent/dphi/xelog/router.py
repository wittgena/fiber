# agent.dphi.xelog.router
## @lineage: meta.ops.xelog.router
## @lineage: phi.ops.xelog.router
from typing import Tuple, Dict
from dataclasses import dataclass
from fastapi.routing import APIRoute
from watcher.plane.emitter import get_emitter

log = get_emitter("xelog.router")

@dataclass
class E2EConfig:
    host: str = "localhost"
    port: int = 8000
    protocol: str = "http"

    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

class RouteRegistry:
    """FastAPI 라우트를 동적으로 스캔하여 하드코딩 없이 URL을 찾아내는 레지스트리"""
    def __init__(self, app):
        self.app = app
        self._routes = {
            route.name: route 
            for route in app.routes if isinstance(route, APIRoute)
        }

    def url_for(self, target_name: str, fallback: str = None) -> str:
        route = self._routes.get(target_name)
        if route:
            return route.path
            
        if fallback:
            log.warning(f"[RouteRegistry] '{target_name}' not found. Using fallback: {fallback}")
            return fallback
            
        raise ValueError(f"Route '{target_name}' not found in REST API.")

    def get_all_routes(self) -> list:
        return [(name, r.path, r.methods) for name, r in self._routes.items()]