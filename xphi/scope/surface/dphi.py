# xphi.scope.surface.dphi
from contextlib import ExitStack

from anchor.provider.dsp.local import LocalLM
from anchor.provider.dsp.instance import DSPInstance

from xphi.scope.dsp.context import settings
from xphi.scope.surface.config import BaseSurface, SurfaceConfig
from xphi.scope.thch import thch_scope

from phase.gov.proto.gate import uuid4
from watcher.plane.emitter import get_emitter

log = get_emitter("surface.dphi")

class DphiSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.lm = None
        self._stack = ExitStack()
        self.req_id = str(uuid4())[:8]

    def up(self):
        log.debug(f"[DphiSurface-{self.req_id}] 🚀 up START | model={self.config.dphi_model}")
        
        is_local_model = self.config.dphi_model.startswith("local/") or self.config.dphi_model in ["local-gemma-3"]
        
        if is_local_model:
            log.debug(f"[DphiSurface-{self.req_id}] ⚙️ Binding Local Engine: {self.config.dphi_model}")
            self.lm = LocalLM(model=self.config.dphi_model)
        else:
            log.debug(f"[DphiSurface-{self.req_id}] ⚙️ Binding Standard Engine (DSPInstance): {self.config.dphi_model}")
            self.lm = DSPInstance(model=self.config.dphi_model)

        # 🚀 수정된 부분: adapter는 managed_scope가 이미 RunContext로 분리 처리했으므로,
        # 여기서는 DphiSurface가 자체 생성한 lm만 추가로 컨텍스트에 오버라이드합니다.
        self._stack.enter_context(settings.context(lm=self.lm))

        if getattr(self.config, 'use_thch', False):
            log.debug(f"[DphiSurface-{self.req_id}] 🌌 Folding Dphi internals into ThCh Fractal...")
            self._stack.enter_context(thch_scope())
            
        log.debug(f"[DphiSurface-{self.req_id}] ✅ up END")

    def down(self):
        log.debug(f"[DphiSurface-{self.req_id}] 🛑 down START")
        self._stack.close()
        log.debug(f"[DphiSurface-{self.req_id}] 🏁 down END | Teardown complete")

    def get_engine(self):
        return lambda agent_usage: self.lm