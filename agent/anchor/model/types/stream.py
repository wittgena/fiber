# agent.anchor.model.types.stream
## @lineage: bound.xor.model.types.stream
## @lineage: eco.model.types.stream
## @lineage: engine.model.types.stream
## @lineage: bound.model.types.stream
## @lineage: llm.types.stream
## @lineage: eco.mesh.model.types.stream
## @lineage: runtime.mesh.model.types.stream
## @lineage: mesh.model.types.stream
## @lineage: tenant.model.types.stream
import time
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, PrivateAttr

from agent.anchor.model.types.core import StreamingChoices, Usage
from arch.model.phase.gate import uuid
from arch.model.surge.model import DynamicSurgeModel

def _generate_id():
    """private helper function"""
    return "chatcmpl-" + str(uuid.uuid4())


class ModelResponseBase(DynamicSurgeModel):
    id: str = Field(default_factory=_generate_id)
    created: int = Field(default_factory=lambda: int(time.time()))
    model: Optional[str] = None
    object: str = "chat.completion.chunk"
    system_fingerprint: Optional[str] = None
    
    _hidden_params: dict = PrivateAttr(default_factory=dict)
    _response_headers: Optional[dict] = PrivateAttr(default=None)

    def model_dump(self, **kwargs):
        """Default to exclude_unset to avoid Pydantic serializer warnings for dynamic types."""
        if "exclude_unset" not in kwargs and "exclude_none" not in kwargs:
            kwargs["exclude_unset"] = True
        return super().model_dump(**kwargs)


class ModelResponseStream(ModelResponseBase):
    """
    DynamicSurgeModel 상속으로 인해 내부의 get, __contains__, __getitem__, json 등의 
    보일러플레이트 코드가 전부 불필요해졌습니다.
    """
    choices: List[StreamingChoices] = Field(default_factory=lambda: [StreamingChoices()])
    provider_specific_fields: Optional[Dict[str, Any]] = Field(default=None)

    def __init__(self, **kwargs):
        # 1. Choices 파싱 및 정규화
        choices_input = kwargs.get("choices")
        if choices_input is not None and isinstance(choices_input, list):
            new_choices = []
            for choice in choices_input:
                if isinstance(choice, StreamingChoices):
                    new_choices.append(choice)
                elif isinstance(choice, dict):
                    new_choices.append(StreamingChoices(**choice))
                elif isinstance(choice, BaseModel):
                    new_choices.append(StreamingChoices(**choice.model_dump()))
            kwargs["choices"] = new_choices
        else:
            kwargs["choices"] = [StreamingChoices()]

        # 2. Usage 파싱
        usage_input = kwargs.get("usage")
        if usage_input is not None:
            if isinstance(usage_input, dict):
                kwargs["usage"] = Usage(**usage_input)
            elif isinstance(usage_input, BaseModel):
                dump = usage_input.model_dump() if hasattr(usage_input, "model_dump") else usage_input.dict()
                kwargs["usage"] = Usage(**dump)

        # 3. 기본 속성 강제 할당
        kwargs.setdefault("id", _generate_id())
        kwargs.setdefault("created", int(time.time()))
        kwargs["object"] = "chat.completion.chunk"
        
        super().__init__(**kwargs)