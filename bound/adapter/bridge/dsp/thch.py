# bound.adapter.bridge.dsp.thch
## @lineage: bound.adapter.dsp.thch
import sys
import inspect
from contextlib import contextmanager
from typing import Any, Type
from pydantic import BaseModel

from xphi.xor.module.meta import Module
from xphi.xor.opt.manifold.model.cot import ChainOfThought 

from arch.xor.manifold.sign.field import InputField, OutputField
from arch.xor.manifold.sign.signature import Signature

from watcher.plane.emitter import get_emitter

log = get_emitter("scope.thch")

def _compile_to_sign(proto_cls: type[BaseModel]):
    """[런타임 형질 변환기] ProtoSignature -> Sign"""
    if hasattr(proto_cls, "__meta_compiled__"):
        return proto_cls.__meta_compiled__

    meta_fields = {}
    for field_name, field_info in proto_cls.model_fields.items():
        meta = field_info.json_schema_extra or {}
        field_type = meta.get("__meta_field_type")
        desc = meta.get("desc", "")
        if field_type == "input":
            meta_fields[field_name] = InputField(desc=desc)
        elif field_type == "output":
            meta_fields[field_name] = OutputField(desc=desc)

    sig_cls = type(proto_cls.__name__, (Signature,), {"__doc__": proto_cls.__doc__, **meta_fields})
    proto_cls.__meta_compiled__ = sig_cls
    return sig_cls

class ThCh:
    def __init__(self, signature, state_path=None, state_key=None, **kwargs):
        self.signature = signature
        self.state_path = state_path
        self.state_key = state_key
        self.kwargs = kwargs
        self._real_engine = None

    def _bootstrap(self):
        sig = _compile_to_sign(self.signature)
        self._real_engine = ChainOfThought(sig, **self.kwargs)

        if self.state_path:
            try:
                self._real_engine.load(self.state_path, prefix=self.state_key)
            except Exception as e:
                log.warning(f"[ThCh] State hydration failed, running bare: {e}")

    def __call__(self, **inputs):
        if not self._real_engine:
            self._bootstrap()
        return self._real_engine(**inputs)

@contextmanager
def folding_thch():
    try:
        import xphi.xor.opt.manifold.model.cot as cot_module
    except ImportError:
        log.warning("[ThCh] 내부 모듈(cot) 부재. 투명하게(Pass-through) 우회합니다.")
        yield
        return

    original_cot = cot_module.ChainOfThought
    class LegacyThChInterceptor(ThCh):
        pass

    try:
        cot_module.ChainOfThought = LegacyThChInterceptor
        log.info("[ThCh] 🌀 Topological Scope Opened (Legacy Mode).")
        yield
    finally:
        cot_module.ChainOfThought = original_cot
        log.info("[ThCh] Scope Closed. Restored to vacuum.")