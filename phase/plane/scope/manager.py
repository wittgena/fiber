# fiber.phase.plane.scope.manager
## @lineage: fiber.phase.scope.manager
import asyncio
from contextlib import asynccontextmanager, AsyncExitStack

from fiber.phase.plane.scope.surface import get_surface_class, SurfaceConfig
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.tracer.scope import scope_trace, get_current_trace_path

log_flow = get_emitter("scope.manager")

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log_flow.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
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
async def managed_scope(**surface_kwargs):
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log_flow.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log_flow.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log_flow.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await asyncio.sleep(0.1)
            await manager.down()
            log_flow.info("[+] Context Manager closed safely.")