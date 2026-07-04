# xphi.scope.manager
import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from xphi.scope.surface.config import SurfaceConfig
from xphi.scope.dsp.context import settings
from xphi.scope.surface.sandbox import SandboxSurface
from xphi.scope.surface.registry import get_surface_class

from watcher.tracer.scope import scope_trace, get_current_trace_path
from watcher.plane.emitter import get_emitter

log = get_emitter("scope.manager")

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

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log.warning(f"🚨 '{surface_type}' Surface 로드 실패: {e}. 기본 'local' 환경으로 Fallback 합니다.")
            surface_class = get_surface_class("local")
            
        self.impl = surface_class(config)

    async def up(self):
        if asyncio.iscoroutinefunction(self.impl.up):
            await self.impl.up()
        else:
            self.impl.up()

    async def down(self):
        if asyncio.iscoroutinefunction(self.impl.down):
            await self.impl.down()
        else:
            self.impl.down()
    
    def get_engine(self):
        return self.impl.get_engine()

@asynccontextmanager
async def managed_scope(**kwargs):
    surface_fields = {f for f in SurfaceConfig.__dataclass_fields__}
    surface_kwargs = {}
    dsp_kwargs = {}
    
    for k, v in kwargs.items():
        if k in surface_fields:
            surface_kwargs[k] = v
        else:
            dsp_kwargs[k] = v

    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "dphi" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    with settings.context(**dsp_kwargs):
        async with scope_trace(name=surface_name, facet=facet_type):
            log.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
            try:
                await manager.up()
                yield manager 
            except Exception as e:
                log.error(f"🚨 [managed_scope] 비즈니스 파이프라인 예외: {type(e).__name__} - {e}")
                raise
            finally:
                log.info("[managed_scope] 인프라 자원 안전 회수 시퀀스 트리거")
                await manager.down()
                log.info("[+] Context Manager closed safely.")