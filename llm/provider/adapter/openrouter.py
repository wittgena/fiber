# fiber.llm.provider.adapter.openrouter
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from fiber.llm.provider.cost.schema import ModelCostSchema
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("provider.adapter.openrouter")

@dataclass
class OpenRouterPricing:
    prompt: float = 0.0
    completion: float = 0.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenRouterPricing':
        try:
            return cls(
                prompt=float(data.get("prompt") or 0.0),
                completion=float(data.get("completion") or 0.0)
            )
        except (ValueError, TypeError):
            raise ValueError(f"Invalid pricing format: {data}")

@dataclass
class OpenRouterArchitecture:
    input_modalities: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenRouterArchitecture':
        return cls(input_modalities=data.get("input_modalities", []))

@dataclass
class OpenRouterProviderInfo:
    max_completion_tokens: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenRouterProviderInfo':
        return cls(max_completion_tokens=data.get("max_completion_tokens"))

@dataclass
class OpenRouterModelData:
    """OpenRouter의 원본 JSON 구조를 명시적으로 담는 중간 컨테이너"""
    id: str
    context_length: Optional[int]
    pricing: OpenRouterPricing
    architecture: OpenRouterArchitecture
    top_provider: OpenRouterProviderInfo

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OpenRouterModelData':
        model_id = data.get("id")
        if not model_id:
            raise KeyError("Missing 'id' field in OpenRouter model.")
            
        return cls(
            id=model_id,
            context_length=data.get("context_length"),
            pricing=OpenRouterPricing.from_dict(data.get("pricing", {})),
            architecture=OpenRouterArchitecture.from_dict(data.get("architecture", {})),
            top_provider=OpenRouterProviderInfo.from_dict(data.get("top_provider", {}))
        )

    def to_universal_schema(self) -> ModelCostSchema:
        """OpenRouter 규격을 내부 통합 규격(ModelCostSchema)으로 변환"""
        return ModelCostSchema(
            provider_name="openrouter",
            mode="chat",  # OpenRouter는 기본적으로 text generation 기반
            max_input_tokens=self.context_length or 0,
            max_output_tokens=self.top_provider.max_completion_tokens or 0,
            input_cost_per_token=self.pricing.prompt,
            output_cost_per_token=self.pricing.completion,
            supports_vision="image" in self.architecture.input_modalities
        )