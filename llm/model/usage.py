# fiber.llm.model.usage
import copy
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional, final

from pydantic import BaseModel, Field, field_validator, model_validator

from fiber.llm.types.provider.general import ModelInfo
from fiber.llm.model.provider.registry import ModelCostRegistry 
from fiber.llm.model.cost.unit import UnitCostCalculator
from fiber.llm.model.cost.policy import CostPolicy

from xphi.arch.bound.event.next import LogEvent
from xphi.watcher.plane.emitter import flow_scope, register_interceptor

logger = logging.getLogger("model.usage")

"""Base Data Models (Metrics & Tokens)"""
class Cost(BaseModel):
    model: str
    cost: float = Field(ge=0.0, description="Cost must be non-negative")
    timestamp: float = Field(default_factory=time.time)

    @field_validator("cost")
    @classmethod
    def validate_cost(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Cost cannot be negative")
        return v

class ResponseLatency(BaseModel):
    """Metric tracking the round-trip time per completion call."""
    model: str
    latency: float = Field(ge=0.0, description="Latency must be non-negative")
    response_id: str

    @field_validator("latency")
    @classmethod
    def validate_latency(cls, v: float) -> float:
        return max(0.0, v)

class TokenUsage(BaseModel):
    """Metric tracking detailed token usage per completion call."""
    model: str = Field(default="")
    prompt_tokens: int = Field(default=0, ge=0, description="Prompt tokens must be non-negative")
    completion_tokens: int = Field(default=0, ge=0, description="Completion tokens must be non-negative")
    cache_read_tokens: int = Field(default=0, ge=0, description="Cache read tokens must be non-negative")
    cache_write_tokens: int = Field(default=0, ge=0, description="Cache write tokens must be non-negative")
    reasoning_tokens: int = Field(default=0, ge=0, description="Reasoning tokens must be non-negative")
    context_window: int = Field(default=0, ge=0, description="Context window must be non-negative")
    per_turn_token: int = Field(default=0, ge=0, description="Per turn tokens must be non-negative")
    response_id: str = Field(default="")

    def __add__(self, other: "TokenUsage") -> "TokenUsage":
        """Add two TokenUsage instances together."""
        return TokenUsage(
            model=self.model,
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cache_write_tokens=self.cache_write_tokens + other.cache_write_tokens,
            reasoning_tokens=self.reasoning_tokens + other.reasoning_tokens,
            context_window=max(self.context_window, other.context_window),
            per_turn_token=other.per_turn_token,
            response_id=self.response_id,
        )

class PromptTokensDetails(BaseModel):
    cached_tokens: int = 0
    audio_tokens: int = 0

class CompletionTokensDetails(BaseModel):
    reasoning_tokens: int = 0
    audio_tokens: int = 0

class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    
    prompt_tokens_details: Optional[PromptTokensDetails] = None
    completion_tokens_details: Optional[CompletionTokensDetails] = None
    
    cache_creation_input_tokens: Optional[int] = 0
    cache_read_input_tokens: Optional[int] = 0


# =============================================================================
# 2. Aggregated Metrics Models
# =============================================================================

class MetricsSnapshot(BaseModel):
    model_name: str = Field(default="default", description="Name of the model")
    accumulated_cost: float = Field(default=0.0, ge=0.0, description="Total accumulated cost, must be non-negative")
    max_budget_per_task: float | None = Field(default=None, description="Maximum budget per task")
    accumulated_token_usage: TokenUsage | None = Field(default=None, description="Accumulated token usage across all calls")

@final
class Metrics(MetricsSnapshot):
    costs: list[Cost] = Field(default_factory=list, description="List of individual costs")
    response_latencies: list[ResponseLatency] = Field(default_factory=list, description="List of response latencies")
    token_usages: list[TokenUsage] = Field(default_factory=list, description="List of token usage records")

    @field_validator("accumulated_cost")
    @classmethod
    def validate_accumulated_cost(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Total cost cannot be negative.")
        return v

    @model_validator(mode="after")
    def initialize_accumulated_token_usage(self) -> "Metrics":
        if self.accumulated_token_usage is None:
            self.accumulated_token_usage = TokenUsage(
                model=self.model_name,
                prompt_tokens=0,
                completion_tokens=0,
                cache_read_tokens=0,
                cache_write_tokens=0,
                reasoning_tokens=0,
                context_window=0,
                response_id="",
            )
        return self

    def get_snapshot(self) -> MetricsSnapshot:
        """Get a snapshot of the current metrics without the detailed lists."""
        return MetricsSnapshot(
            model_name=self.model_name,
            accumulated_cost=self.accumulated_cost,
            max_budget_per_task=self.max_budget_per_task,
            accumulated_token_usage=copy.deepcopy(self.accumulated_token_usage)
            if self.accumulated_token_usage
            else None,
        )

    def add_cost(self, value: float) -> None:
        if value < 0:
            raise ValueError("Added cost cannot be negative.")
        self.accumulated_cost += value
        self.costs.append(Cost(cost=value, model=self.model_name))

    def add_response_latency(self, value: float, response_id: str) -> None:
        self.response_latencies.append(
            ResponseLatency(
                latency=max(0.0, value), model=self.model_name, response_id=response_id
            )
        )

    def add_token_usage(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        cache_read_tokens: int,
        cache_write_tokens: int,
        context_window: int,
        response_id: str,
        reasoning_tokens: int = 0,
    ) -> None:
        """Add a single usage record."""
        # Token each turn for calculating context usage.
        per_turn_token = prompt_tokens + completion_tokens

        usage = TokenUsage(
            model=self.model_name,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=reasoning_tokens,
            context_window=context_window,
            per_turn_token=per_turn_token,
            response_id=response_id,
        )
        self.token_usages.append(usage)

        # Update accumulated token usage using the __add__ operator
        new_usage = TokenUsage(
            model=self.model_name,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_write_tokens=cache_write_tokens,
            reasoning_tokens=reasoning_tokens,
            context_window=context_window,
            per_turn_token=per_turn_token,
            response_id="",
        )
        if self.accumulated_token_usage is None:
            self.accumulated_token_usage = new_usage
        else:
            self.accumulated_token_usage = self.accumulated_token_usage + new_usage

    def merge(self, other: "Metrics") -> None:
        """Merge 'other' metrics into this one."""
        self.accumulated_cost += other.accumulated_cost

        # Keep the max_budget_per_task from other if it's set and this one isn't
        if self.max_budget_per_task is None and other.max_budget_per_task is not None:
            self.max_budget_per_task = other.max_budget_per_task

        self.costs += other.costs
        self.token_usages += other.token_usages
        self.response_latencies += other.response_latencies

        # Merge accumulated token usage using the __add__ operator
        if self.accumulated_token_usage is None:
            self.accumulated_token_usage = other.accumulated_token_usage
        elif other.accumulated_token_usage is not None:
            self.accumulated_token_usage = (
                self.accumulated_token_usage + other.accumulated_token_usage
            )

    def get(self) -> dict:
        """Return the metrics in a dictionary."""
        return {
            "accumulated_cost": self.accumulated_cost,
            "max_budget_per_task": self.max_budget_per_task,
            "accumulated_token_usage": self.accumulated_token_usage.model_dump()
            if self.accumulated_token_usage
            else None,
            "costs": [cost.model_dump() for cost in self.costs],
            "response_latencies": [
                latency.model_dump() for latency in self.response_latencies
            ],
            "token_usages": [usage.model_dump() for usage in self.token_usages],
        }

    def log(self) -> str:
        """Log the metrics."""
        metrics = self.get()
        logs = ""
        for key, value in metrics.items():
            logs += f"{key}: {value}\n"
        return logs

    def deep_copy(self) -> "Metrics":
        return copy.deepcopy(self)

    def diff(self, baseline: "Metrics") -> "Metrics":
        result = Metrics(model_name=self.model_name)

        # Calculate cost difference
        result.accumulated_cost = self.accumulated_cost - baseline.accumulated_cost

        # Include only costs that were added after the baseline
        if baseline.costs:
            last_baseline_timestamp = baseline.costs[-1].timestamp
            result.costs = [cost for cost in self.costs if cost.timestamp > last_baseline_timestamp]
        else:
            result.costs = self.costs.copy()

        # Include only response latencies that were added after the baseline
        result.response_latencies = self.response_latencies[len(baseline.response_latencies) :]

        # Include only token usages that were added after the baseline
        result.token_usages = self.token_usages[len(baseline.token_usages) :]

        # Calculate accumulated token usage difference
        base_usage = baseline.accumulated_token_usage
        current_usage = self.accumulated_token_usage

        if current_usage is not None and base_usage is not None:
            result.accumulated_token_usage = TokenUsage(
                model=self.model_name,
                prompt_tokens=current_usage.prompt_tokens - base_usage.prompt_tokens,
                completion_tokens=current_usage.completion_tokens - base_usage.completion_tokens,
                cache_read_tokens=current_usage.cache_read_tokens - base_usage.cache_read_tokens,
                cache_write_tokens=current_usage.cache_write_tokens - base_usage.cache_write_tokens,
                reasoning_tokens=current_usage.reasoning_tokens - base_usage.reasoning_tokens,
                context_window=current_usage.context_window,
                per_turn_token=0,
                response_id="",
            )
        elif current_usage is not None:
            result.accumulated_token_usage = current_usage
        else:
            result.accumulated_token_usage = None

        return result

    def __repr__(self) -> str:
        return f"Metrics({self.get()})"


# =============================================================================
# 3. Usage Tracking & Emitter Integration
# =============================================================================

class UsageTracker:
    def __init__(self):
        self.usage_data = defaultdict(list)

    def _flatten_usage_entry(self, usage_entry: dict[str, Any]) -> dict[str, Any]:
        result = {}
        for key, value in usage_entry.items():
            if isinstance(value, BaseModel):
                result[key] = value.model_dump()
            else:
                result[key] = value
        return result

    def _merge_usage_entries(
        self, usage_entry1: dict[str, Any] | None, usage_entry2: dict[str, Any] | None
    ) -> dict[str, Any]:
        if not usage_entry1:
            return dict(usage_entry2 or {})
        if not usage_entry2:
            return dict(usage_entry1)

        result = dict(usage_entry2)
        for k, v in usage_entry1.items():
            current_v = result.get(k)
            if isinstance(v, dict) or isinstance(current_v, dict):
                result[k] = self._merge_usage_entries(current_v, v)
            elif current_v is not None or v is not None:
                result[k] = (current_v or 0) + (v or 0)
        return result

    def add_usage(self, lm: str, usage_entry: dict[str, Any]) -> None:
        """Add a usage entry to the tracker."""
        if len(usage_entry) > 0:
            self.usage_data[lm].append(self._flatten_usage_entry(usage_entry))

    def get_total_tokens(self) -> dict[str, dict[str, Any]]:
        """Calculate total tokens from all tracked usage."""
        total_usage_by_lm = {}
        for lm, usage_entries in self.usage_data.items():
            total_usage = {}
            for usage_entry in usage_entries:
                total_usage = self._merge_usage_entries(total_usage, usage_entry)
            total_usage_by_lm[lm] = total_usage
        return total_usage_by_lm

def _usage_tracking_interceptor(event: LogEvent):
    """
    Emitter에서 발생한 이벤트를 가로채서, 
    context 내에 'usage_tracker'와 'usage_metrics'가 존재하면 사용량을 자동으로 기록합니다.
    """
    ctx = event.context or {}
    tracker = ctx.get("usage_tracker")
    usage_data = ctx.get("usage_metrics")
    
    if tracker and isinstance(tracker, UsageTracker) and usage_data:
        model_name = ctx.get("model_name", "default-model")
        tracker.add_usage(model_name, usage_data)

## 모듈이 로드될 때 인터셉터를 Emitter 시스템에 등록
register_interceptor(_usage_tracking_interceptor)

@contextmanager
def track_usage() -> Generator[UsageTracker, None, None]:
    tracker = UsageTracker()
    # flow_scope를 통해 현재 실행 스코프에 tracker를 주입
    with flow_scope(usage_tracker=tracker):
        yield tracker


# =============================================================================
# 4. Tenant Eco & Billing Services
# =============================================================================

class TenantEco:
    def __init__(self, cost_policy: Optional[CostPolicy] = None):
        self.policy = cost_policy or CostPolicy()
        self.usage_tracker = UsageTracker()

    def _fetch_model_info(self, model_name: str, provider: Optional[str] = None) -> ModelInfo:
        try:
            info = ModelCostRegistry.lookup_base_model_info(
                model=model_name,
                custom_llm_provider=provider
            )
            return info
        except Exception as e:
            logger.warning(f"[EcoService] Failed to fetch model info for {model_name}. Using free fallback. Error: {e}")
            return {
                "input_cost_per_token": 0.0,
                "output_cost_per_token": 0.0,
            }

    def _normalize_usage(self, raw_usage: Any) -> Usage:
        """Normalizes external payload Usage objects into the internal standard model."""
        if isinstance(raw_usage, Usage):
            return raw_usage
        if isinstance(raw_usage, dict):
            return Usage(**raw_usage)
        if isinstance(raw_usage, BaseModel):
            return Usage(**raw_usage.model_dump())
        
        return Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)

    async def calculate_tenant_billing(
        self, 
        tenant_id: str, 
        usage: Any, 
        model_name: str = "default-model",
        provider: Optional[str] = None
    ) -> dict:
        try:
            # 1. Normalize usage & record telemetry
            normalized_usage = self._normalize_usage(usage)
            self.usage_tracker.add_usage(model_name, normalized_usage.model_dump())

            # 2. Fetch base cost
            model_info = self._fetch_model_info(model_name, provider)
            prompt_cost, completion_cost = UnitCostCalculator.generic_cost_per_token(
                model_info=model_info,
                usage=normalized_usage
            )
            base_total_cost = prompt_cost + completion_cost

            # 3. Apply policies (Margin/Discount)
            cost_breakdown = self.policy.apply_cost_modifiers(
                base_cost=base_total_cost,
                custom_llm_provider=provider
            )

            final_cost = cost_breakdown.final_cost
            
            logger.info(
                f"[EcoServ] Tenant: {tenant_id} | Model: {model_name} ({provider}) | "
                f"Tokens: {normalized_usage.total_tokens} | "
                f"Base Cost: {base_total_cost:.6f} -> Final Cost: {final_cost:.6f}"
            )
            
            # 4. Construct the deterministic Billing Intent for the Kernel
            return {
                "status": "success",
                "billing_intent": {
                    "tenant_id": tenant_id,
                    "model_name": model_name,
                    "provider": provider,
                    "usage_metrics": normalized_usage.model_dump(),
                    "financials": {
                        "base_cost": base_total_cost,
                        "final_cost": final_cost,
                        "modifiers": [m.model_dump() for m in cost_breakdown.modifiers]
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"[EcoService] Failed to calculate billing for tenant {tenant_id}: {str(e)}")
            return {
                "status": "error", 
                "message": str(e)
            }


async def get_tenant_eco() -> TenantEco:
    policy = CostPolicy()
    return TenantEco(cost_policy=policy)