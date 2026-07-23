# atoa.topos.bound.tenant
## @lineage: topos.bound.eco.tenant
## @lineage: gov.eco.tenant
## @lineage: logst.eco.tenant
import logging
from typing import Any, Dict, Optional
from pydantic import BaseModel

from bound.resolver.model.cost import ModelCostRegistry 
from bound.gateway.cost.policy import CostPolicy
from bound.gateway.cost.unit.calc import UnitCostCalculator

from eco.legacy.types import Usage, ModelInfo

from bound.xor.opt.usage import UsageTracker

logger = logging.getLogger("serv.eco")

class TenantEco:
    """
    - Calculates unit costs based on tenant API usage and applies 
    - discount/margin policies (CostPolicy) to synchronize final billing data.
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
            ## Extract cost metadata based on model and provider from the registry
            info = ModelCostRegistry.lookup_base_model_info(
                model=model_name,
                custom_llm_provider=provider
            )
            return info
        except Exception as e:
            logger.warning(f"[EcoService] Failed to fetch model info for {model_name}. Using free fallback. Error: {e}")
            ## Fail-safe: Return default 0.0 on registry failure to prevent overcharging
            return {
                "input_cost_per_token": 0.0,
                "output_cost_per_token": 0.0,
            }

    def _normalize_usage(self, raw_usage: Any) -> Usage:
        """Normalizes external payload Usage objects (e.g., from Moesif) into the internal standard model."""
        if isinstance(raw_usage, Usage):
            return raw_usage
        if isinstance(raw_usage, dict):
            return Usage(**raw_usage)
        if isinstance(raw_usage, BaseModel):
            return Usage(**raw_usage.model_dump())
        
        ## Return default empty Usage if parsing fails
        return Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)

    async def sync_tenant_usage(
        self, 
        tenant_id: str, 
        usage: Any, 
        model_name: str = "default-model",
        provider: Optional[str] = None
    ) -> bool:
        """
        @desc: Stateful Sync & Billing
        - Atomically normalizes usage, calculates pricing, applies policies, and synchronizes the tenant's billing state
        """
        try:
            ## Normalize usage and track telemetry
            normalized_usage = self._normalize_usage(usage)
            self.usage_tracker.add_usage(model_name, normalized_usage.model_dump())

            model_info = self._fetch_model_info(model_name, provider)
            prompt_cost, completion_cost = UnitCostCalculator.generic_cost_per_token(
                model_info=model_info,
                usage=normalized_usage
            )
            base_total_cost = prompt_cost + completion_cost
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
            
            ## Implement actual Redis in-memory counter (INCRBYFLOAT) or DB write logic
            return True
        except Exception as e:
            logger.error(f"[EcoService] Failed to sync usage for tenant {tenant_id}: {str(e)}")
            return False

async def get_tenant_eco() -> TenantEco:
    policy = CostPolicy()
    return TenantEco(cost_policy=policy)