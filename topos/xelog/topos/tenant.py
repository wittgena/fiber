# topos.xelog.topos.tenant
## @lineage: topos.ops.xelog.topos.tenant
## @lineage: ops.xelog.topos.tenant
import logging
from typing import Any, Dict, Optional, Generator
from pydantic import BaseModel
from collections import defaultdict
from contextlib import contextmanager

from meta.ops.opt.runtime import runtime
from bound.resolver.model.cost import ModelCostRegistry 
from bound.resolver.legacy.types import Usage, ModelInfo
from eco.tenant.cost.policy import CostPolicy
from eco.tenant.cost.unit.calc import UnitCostCalculator

logger = logging.getLogger("topos.tenant")

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

@contextmanager
def track_usage() -> Generator[UsageTracker, None, None]:
    tracker = UsageTracker()
    with runtime.bind(usage_tracker=tracker):
        yield tracker


class TenantEco:
    """
    @desc: Calculates unit costs based on tenant API usage and applies 
    discount/margin policies (CostPolicy).
    
    [REFACTORED] Returns a deterministic 'Billing Intent' payload for WASM Kernel anchoring,
    rather than causing direct database side-effects.
    """
    def __init__(self, cost_policy: Optional[CostPolicy] = None):
        self.policy = cost_policy or CostPolicy()
        self.usage_tracker = UsageTracker()

    def _fetch_model_info(self, model_name: str, provider: Optional[str] = None) -> ModelInfo:
        """
        Retrieves actual pricing tables from ModelCostRegistry instead of using mocks.
        (Executes synchronously for speed, as it relies on an in-memory dictionary lookup).
        """
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
        """
        @desc: Stateless Cost Calculation
        Replaces the legacy `sync_tenant_usage`. Calculates pricing and policies,
        then returns a structured intent object to be sealed by the WASM Ledger.
        """
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
                f"Base Cost: ${base_total_cost:.6f} -> Final Cost: ${final_cost:.6f}"
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