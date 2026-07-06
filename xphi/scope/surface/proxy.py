# xphi.scope.surface.proxy
## @lineage: xphi.scope.proxy
import asyncio
from typing import Any
from xphi.scope.surface.config import SurfaceConfig
from xphi.scope.surface.sandbox import SandboxSurface
from watcher.plane.emitter import get_emitter

log = get_emitter("scope.proxy")

class ProxySurface(SandboxSurface):
    def __init__(self, config: SurfaceConfig):
        super().__init__(config)
        self.host_url = config.server_url
        self.workspace_ref = config.workspace_ref
        self.session_api_key = config.session_api_key
        self.process_name = "proxy.surface"
        
        self._engine: Optional[BaseEngine] = None
        self.engine_factory: Callable[..., BaseEngine] = getattr(config, 'engine_factory', None)
        if not self.engine_factory:
            raise ValueError("[ProxySurface] BaseEngine 생성을 위한 engine_factory가 제공되지 않았습니다.")

    def get_engine(self):
        if not self._engine:
            self._engine = self.engine_factory(
                host_url=self.host_url, 
                agent_usage="managed_context", 
                workspace_ref=self.workspace_ref,
                session_api_key=self.session_api_key
            )
        return lambda agent_usage: self._engine
        
    async def up(self):
        log.info(f"[ProxySurface] Pre-flight checking to remote server at {self.host_url}")
        engine_initializer = self.get_engine()
        engine = engine_initializer(None)
        
        try:
            health_response = await engine.health_check()
            log.info(f"[ProxySurface] Remote Server Alive: {health_response.get('status', 'OK')}")
        except Exception as e:
            log.error(f"[ProxySurface] Remote Sandbox Pre-flight connection failed: {str(e)}")
            raise ConnectionError(f"Cannot enter managed_scope. Target host unreachable: {e}")
            
        super().up()

    async def down(self):
        log.info(f"[ProxySurface] Cleaning up workspace communication resources...")
        if self._engine:
            await self._engine.close()
        log.info(f"[ProxySurface] Disconnected safely from remote server.")
        super().down()