# fiber.infra.ops.finops.cost.schema
## @lineage: fiber.llm.provider.cost.schema
from typing import Dict, Optional
from dataclasses import dataclass, asdict

KEY_RENAME_MAP = {
    "litellm_provider": "provider_name"
}

@dataclass
class ModelCostSchema:
    """Schema representing the cost and capability specifications for a model."""
    provider_name: str
    mode: str
    max_input_tokens: int
    max_output_tokens: int
    input_cost_per_token: float
    output_cost_per_token: float
    input_cost_per_audio_token: Optional[float] = None
    output_cost_per_reasoning_token: Optional[float] = None
    supports_function_calling: bool = False
    supports_vision: bool = False

    @classmethod
    def from_dict(cls, data: Dict) -> 'ModelCostSchema':
        """Safely instantiates the schema, mapping keys and applying defaults."""
        mapped_data = {KEY_RENAME_MAP.get(k, k): v for k, v in data.items()}
        
        valid_keys = cls.__dataclass_fields__.keys()
        filtered_data = {k: v for k, v in mapped_data.items() if k in valid_keys}
        
        return cls(**filtered_data)
        
    def to_clean_dict(self) -> Dict:
        """Returns dictionary without None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}