# topos.bound.scope.manager
## @lineage: topos.ops.scope.manager
## @lineage: ops.scope.manager
import asyncio
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Optional, Any

from topos.bound.scope.surface.config import SurfaceConfig
from topos.bound.scope.surface.registry import get_surface_class
from watcher.tracer.scope import scope_trace, get_current_trace_path
from watcher.plane.emitter import get_emitter

log = get_emitter("scope.manager")

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
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


def _instantiate_lm(model_name: str) -> Optional[Any]:
    """Dedicated factory for instantiating LM engines."""
    if not model_name:
        return None
    
    is_local = model_name.startswith("local/") or model_name in ["local-gemma-3"]
    if is_local:
        log.debug(f"[managed_scope] ⚙️ Binding Local Engine: {model_name}")
        return LocalLM(model=model_name)
    else:
        log.debug(f"[managed_scope] ⚙️ Binding Standard Engine: {model_name}")
        return DSPInstance(model=model_name)

@asynccontextmanager
async def managed_scope(**surface_kwargs):
    """
    순수하게 인프라/Surface의 라이프사이클(up/down)과 
    기본 Trace 컨텍스트만 관리하는 Base 매니저입니다.
    """
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await manager.down()
            log.info("[+] Context Manager closed safely.")