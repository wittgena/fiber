# gov.factory.driver
## @lineage: atoa.factory.driver
## @lineage: atoa.driver.factory
import json
import os
import warnings
from typing import Any, get_args, get_origin, TYPE_CHECKING
from pydantic import BaseModel, SecretStr

from eco.llama.router.locator import get_llm_provider
if TYPE_CHECKING:
    from agent.driver.tensor import Driver
    DriverType = Driver | Any
else:
    DriverType = Any

class DriverFactory:
    """Driver 명세서를 외부 환경(JSON, ENV)으로부터 안전하게 생성(Build)하는 팩토리 클래스"""

    @staticmethod
    def from_json(json_path: str) -> DriverType:
        with open(json_path) as f:
            data = json.load(f)
        return DriverType(**data)

    @staticmethod
    def from_env(prefix: str = "LLM_") -> DriverType:
        data: dict[str, Any] = {}
        fields: dict[str, Any] = {
            name: f.annotation
            for name, f in Driver.model_fields.items()
            if not getattr(f, "exclude", False)
        }

        for key, value in os.environ.items():
            if not key.startswith(prefix):
                continue
            field_name = key[len(prefix):].lower()
            if field_name not in fields:
                continue
            
            v = DriverFactory._cast_value(value, fields[field_name])
            if v is not None:
                data[field_name] = v
                
        return DriverType(**data)

    @staticmethod
    def _unwrap_type(t: Any) -> Any:
        origin = get_origin(t)
        if origin is None:
            return t
        args = [a for a in get_args(t) if a is not type(None)]
        return args[0] if args else t

    @staticmethod
    def _cast_value(raw: str, t: Any) -> Any:
        TRUTHY = {"true", "1", "yes", "on"}
        t = DriverFactory._unwrap_type(t)
        
        if t is SecretStr: return SecretStr(raw)
        if t is bool: return raw.lower() in TRUTHY
        if t is int:
            try: return int(raw)
            except ValueError: return None
        if t is float:
            try: return float(raw)
            except ValueError: return None
            
        origin = get_origin(t)
        if (origin in (list, dict, tuple)) or (isinstance(t, type) and issubclass(t, BaseModel)):
            try: return json.loads(raw)
            except Exception: pass
            
        return raw
    

    def infer_provider(*, model: str, api_base: str | None) -> str | None:
        try:
            _model, provider, _dynamic_key, _api_base = get_llm_provider(
                model=model,
                custom_llm_provider=None,
                api_base=api_base,
                api_key=None,
            )
        except Exception:
            return None
        return provider