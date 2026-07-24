# bound.mapper.manifest.cost
## @lineage: bound.gateway.adapter.mapper.manifest.cost
## @lineage: gateway.adapter.mapper.manifest.cost
## @lineage: eco.mapper.manifest.cost
## @lineage: adapter.mapper.manifest.cost
## @lineage: bound.adapter.mapper.manifest.cost
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, model_validator

class PricingMeta(BaseModel):
    provider: str = Field(..., description="Provider")
    model_pattern: str = Field(..., description="Model name matching pattern")
    service_tier: Optional[str] = Field(default=None, description="Billing tier")
    region: Optional[str] = Field(default=None, description="Region")

class BaseCost(BaseModel):
    unit_type: Literal["token", "character", "second", "image"] = Field(default="token")
    input_cost: float = Field(default=0.0, ge=0.0)
    output_cost: float = Field(default=0.0, ge=0.0)

class CachingCost(BaseModel):
    cache_read: Optional[float] = Field(default=None, description="Input cost upon cache hit")
    cache_write: Optional[float] = Field(default=None, description="Input cost upon cache creation (write)")
    cache_write_above_1hr: Optional[float] = Field(default=None, description="Input cost for cache creation maintained above 1 hour")

class ModalityCost(BaseModel):
    audio_input: Optional[float] = Field(default=None)
    audio_output: Optional[float] = Field(default=None)
    image_input: Optional[float] = Field(default=None)
    image_output: Optional[float] = Field(default=None)
    reasoning_output: Optional[float] = Field(default=None, description="Pricing for internal reasoning tokens")

class TieredCostRule(BaseModel):
    threshold_tokens: int = Field(..., description="Minimum number of tokens required for this rule to apply")
    input_cost: float = Field(..., ge=0.0)
    output_cost: float = Field(..., ge=0.0)
    cache_read: Optional[float] = Field(default=None)
    cache_write: Optional[float] = Field(default=None)

class CostManifest(BaseModel):
    meta: PricingMeta
    base: BaseCost
    caching: Optional[CachingCost] = None
    modality: Optional[ModalityCost] = None
    tiered_rules: List[TieredCostRule] = Field(default_factory=list)
    regional_multiplier: float = Field(default=1.0, gt=0.0)

    @model_validator(mode='after')
    def sort_tiered_rules(self) -> 'CostManifest':
        if self.tiered_rules:
            self.tiered_rules.sort(key=lambda x: x.threshold_tokens, reverse=True)
        return self