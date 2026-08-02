# actor.topos.scope.surface.local
## @lineage: topos.scope.surface.local
## @lineage: topos.bound.scope.surface.local
## @lineage: topos.ops.scope.surface.local
## @lineage: ops.scope.surface.local
import time
from actor.topos.scope.surface.config import BaseSurface, SurfaceConfig
from phase.bind.client.engine.local import LLMEngine
from watcher.plane.emitter import get_emitter

log = get_emitter("surface.local")

class LocalSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.engine = LLMEngine()

    def up(self):
        log.info("[*] Initializing Local Direct Surface...")
        self.engine.ensure_server()
        start_time = time.time()
        ready = False
        try:
            time.sleep(2) 
            ready = True
        except Exception as e:
            log.debug(f"[-] Wait interrupted during Local Surface init: {e}")

        if not ready:
            log.warning("[-] Local Engine might not be fully ready, proceeding anyway.")

    def down(self):
        log.info("[*] Folding Local Surface...")

    def get_engine(self):
        return lambda agent_usage: self.engine