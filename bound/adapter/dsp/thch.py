# bound.adapter.dsp.thch
## @lineage: anchor.provider.dsp.adapter.thch
import inspect
from threading import Lock
from typing import Any, Annotated, get_args, get_origin
from pydantic import BaseModel, Field

from xphi.xor.module.meta import Module
from xphi.xor.opt.manifold.model.cot import ChainOfThought 

from arch.xor.manifold.sign.field import InputField, OutputField
from arch.xor.manifold.sign.signature import Signature
from watcher.plane.emitter import get_emitter

log = get_emitter("scope.thch")

# Thread-safe 캐시 저장소
_SIGNATURE_CACHE: dict[type[BaseModel], type[Signature]] = {}
_CACHE_LOCK = Lock()

def _compile_to_sign(proto_cls: type[BaseModel]) -> type[Signature]:
    """[런타임 형질 변환기] Pydantic BaseModel -> DSPy Signature"""
    # 1. 캐시 확인 (원본 클래스 변형 방지)
    with _CACHE_LOCK:
        if proto_cls in _SIGNATURE_CACHE:
            return _SIGNATURE_CACHE[proto_cls]

    meta_fields = {}
    
    # 2. Pydantic V2 필드 및 Annotated 분석
    for field_name, field_info in proto_cls.model_fields.items():
        desc = field_info.description or ""
        field_type_hint = proto_cls.__annotations__.get(field_name)
        
        # Annotated에서 내부 메타데이터(예: "input", "output") 추출
        meta_tag = "input" # Default
        if get_origin(field_type_hint) is Annotated:
            args = get_args(field_type_hint)
            if "output" in args:
                meta_tag = "output"
            elif "input" in args:
                meta_tag = "input"

        # 필드 생성
        if meta_tag == "input":
            meta_fields[field_name] = InputField(desc=desc)
        elif meta_tag == "output":
            meta_fields[field_name] = OutputField(desc=desc)

    # 3. Signature 동적 클래스 생성
    sig_cls = type(
        proto_cls.__name__ + "Signature", 
        (Signature,), 
        {"__doc__": proto_cls.__doc__, **meta_fields}
    )
    
    with _CACHE_LOCK:
        _SIGNATURE_CACHE[proto_cls] = sig_cls
        
    return sig_cls

class ThCh:
    """지연 초기화(Lazy init)를 지원하는 체인 어댑터"""
    def __init__(self, signature: type[BaseModel], state_path: str = None, state_key: str = None, **kwargs):
        self.signature_model = signature
        self.state_path = state_path
        self.state_key = state_key
        self.kwargs = kwargs
        self._real_engine = None

    def _bootstrap(self):
        sig = _compile_to_sign(self.signature_model)
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