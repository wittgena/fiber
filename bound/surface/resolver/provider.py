# bound.surface.resolver.provider
## @lineage: bound.resolver.provider
## @lineage: bound.surface.cost.support
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple, Union, cast
from httpx import Response
from pydantic import BaseModel
from functools import lru_cache

from anchor.registry.model.cost import model_cost
from bound.router.locator import get_llm_provider
from anchor.registry.model.config.resolver import config
from anchor.registry.model.config.constants import DEFAULT_MAX_LRU_CACHE_SIZE

from bound.surface.legacy.info import ProviderTypesSet

from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.provider")
_GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER: dict = {
    "ON_DEMAND_PRIORITY": "priority",
    "FLEX": "flex",
    "BATCH": "flex",
    "ON_DEMAND": None,
}

class ProviderResolver:
    @staticmethod
    @lru_cache(maxsize=DEFAULT_MAX_LRU_CACHE_SIZE)
    def _model_contains_known_llm_provider(model: str) -> bool:
        _provider_prefix = model.split("/")[0]
        return _provider_prefix in ProviderTypesSet

    @staticmethod
    def resolve_provider(model: Optional[str], custom_llm_provider: Optional[str] = None) -> Optional[str]:
        if custom_llm_provider is not None:
            return custom_llm_provider
        if model is None:
            return None
        try:
            _, infered_provider, _, _ = get_llm_provider(model=model)
            return infered_provider
        except Exception as e:
            log.debug(f"calculator.calc::resolve_provider() - Error inferring custom_llm_provider - {str(e)}")
            return None

    @staticmethod
    def resolve_selected_model(
        model: Optional[str],
        completion_response: Optional[Any],
        base_model: Optional[str] = None,
        custom_pricing: Optional[bool] = None,
        custom_llm_provider: Optional[str] = None,
        router_model_id: Optional[str] = None,
    ) -> Optional[str]:
        return_model: Optional[str] = None
        region_name: Optional[str] = None
        
        provider = ProviderResolver.resolve_provider(model=model, custom_llm_provider=custom_llm_provider)
        completion_response_model: Optional[str] = None
        
        if completion_response is not None:
            if isinstance(completion_response, BaseModel):
                completion_response_model = getattr(completion_response, "model", None)
            elif isinstance(completion_response, dict):
                completion_response_model = completion_response.get("model", None)
        
        hidden_params: Optional[dict] = getattr(completion_response, "_hidden_params", None)

        if custom_pricing is True:
            if router_model_id is not None and router_model_id in model_cost:
                entry = model_cost[router_model_id]
                if entry.get("input_cost_per_token") is not None or entry.get("input_cost_per_second") is not None:
                    return_model = router_model_id
                else:
                    return_model = model
            else:
                return_model = model
        elif base_model is not None:
            return_model = base_model
        elif completion_response_model is None and hidden_params is not None:
            if hidden_params.get("model", None) is not None and len(hidden_params["model"]) > 0:
                return_model = hidden_params.get("model", model)
        elif hidden_params is not None and hidden_params.get("region_name", None) is not None:
            region_name = hidden_params.get("region_name", None)

        if return_model is None and completion_response_model is not None:
            return_model = completion_response_model
        if return_model is None and model is not None:
            return_model = model

        if (
            return_model is not None
            and provider is not None
            and not ProviderResolver._model_contains_known_llm_provider(return_model)
        ):
            if region_name is not None:
                return_model = f"{provider}/{region_name}/{return_model}"
            else:
                return_model = f"{provider}/{return_model}"

        return return_model

    @staticmethod
    def resolve_response_model(completion_response: Any) -> Optional[str]:
        if completion_response is None:
            return None
        if isinstance(completion_response, BaseModel):
            return getattr(completion_response, "model", None)
        elif isinstance(completion_response, dict):
            return completion_response.get("model", None)
        return None

    @staticmethod
    def map_traffic_to_tier(traffic_type: Optional[str]) -> Optional[str]:
        if traffic_type is None:
            return None
        return _GEMINI_TRAFFIC_TYPE_TO_SERVICE_TIER.get(str(traffic_type).upper())