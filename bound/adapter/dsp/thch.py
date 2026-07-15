# bound.adapter.dsp.thch
import json
import inspect
from threading import Lock
from typing import Any, Annotated, get_args, get_origin, Union
from pydantic import BaseModel

from xphi.xor.module.meta import Module
from xphi.xor.opt.manifold.model.cot import ChainOfThought 

from arch.xor.sign.field import InputField, OutputField
from arch.xor.sign.signature import Signature
from watcher.plane.emitter import get_emitter

log = get_emitter("scope.thch")

_SIGNATURE_CACHE: dict[Any, type[Signature]] = {}
_CACHE_LOCK = Lock()

def _compile_to_sign(proto_cls: type[BaseModel]) -> type[Signature]:
    """[런타임 형질 변환기] Pydantic BaseModel -> DSPy Signature (기존)"""
    with _CACHE_LOCK:
        if proto_cls in _SIGNATURE_CACHE:
            return _SIGNATURE_CACHE[proto_cls]

    meta_fields = {}
    
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

# =====================================================================
# 2. 새로운 확장 함수 추가 (Dict 지원)
# =====================================================================
def _compile_dict_to_sign(schema: dict) -> type[Signature]:
    """[확장 형질 변환기] Dict -> DSPy Signature 변환기"""
    # Dict는 hashable하지 않으므로 JSON 문자열로 변환하여 캐시 키로 사용
    cache_key = json.dumps(schema, sort_keys=True)
    
    with _CACHE_LOCK:
        if cache_key in _SIGNATURE_CACHE:
            return _SIGNATURE_CACHE[cache_key]

    meta_fields = {}
    cls_name = schema.get("name", "DynamicDictSignature")
    docstring = schema.get("doc", "")
    
    for field_name, field_info in schema.get("fields", {}).items():
        role = field_info.get("role", "input").lower()
        desc = field_info.get("desc", "")
        
        if role == "input":
            meta_fields[field_name] = InputField(desc=desc)
        elif role == "output":
            meta_fields[field_name] = OutputField(desc=desc)
            
    sig_cls = type(
        cls_name, 
        (Signature,), 
        {"__doc__": docstring, **meta_fields}
    )
    
    with _CACHE_LOCK:
        _SIGNATURE_CACHE[cache_key] = sig_cls
        
    return sig_cls

# =====================================================================
# 3. ThCh 클래스 라우팅 개조
# =====================================================================
class ThCh:
    """지연 초기화(Lazy init)를 지원하는 체인 어댑터"""
    # signature 파라미터가 BaseModel과 Dict를 모두 허용하도록 Union 타입 힌트 적용
    def __init__(self, signature: Union[type[BaseModel], dict], state_path: str = None, state_key: str = None, **kwargs):
        self.signature_schema = signature
        self.state_path = state_path
        self.state_key = state_key
        self.kwargs = kwargs
        self._real_engine = None

    def _bootstrap(self):
        if isinstance(self.signature_schema, dict):
            sig = _compile_dict_to_sign(self.signature_schema)
        else:
            sig = _compile_to_sign(self.signature_schema)
            
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