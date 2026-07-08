# bound.transport.response.metadata
## @lineage: bound.transport.channel.response.metadata
## @lineage: bound.channel.response.metadata
import datetime
from typing import Any, Optional, Union

from bound.channel.convert.header import process_response_headers
from bound.channel.api import get_api_base
from bound.adapter.mapper.key import adapt_payload_for_external_litellm, get_legacy_key
from bound.surface.legacy.types import EmbeddingResponse, HiddenParams, ModelResponse, TranscriptionResponse
from bound.surface.legacy.config.constants import LITELLM_DETAILED_TIMING
from bound.watcher.delegator import LogDelegator


class ResponseMetadata:
    def __init__(self, result: Any):
        self.result = result
        self._hidden_params: Union[HiddenParams, dict] = getattr(result, "_hidden_params", {}) or {}

    @property
    def supports_response_time(self) -> bool:
        """Check if response type supports timing metrics"""
        return (
            isinstance(self.result, ModelResponse)
            or isinstance(self.result, EmbeddingResponse)
            or isinstance(self.result, TranscriptionResponse)
        )

    def set_hidden_params(self, logging_obj: LogDelegator, model: Optional[str], kwargs: dict) -> None:
        """Set hidden parameters on the response"""

        model_info = kwargs.get("model_info", {}) or {}
        model_id = model_info.get("id", None)
        new_params = {
            "call_id": getattr(logging_obj, "call_id", None),
            "api_base": get_api_base(model=model or "", optional_params=kwargs),
            "model_id": model_id,
            "response_cost": logging_obj._response_cost_calculator(result=self.result, model_name=model, router_model_id=model_id),
            "additional_headers": process_response_headers(self._get_value_from_hidden_params("additional_headers") or {}),
            "model_name": model,
        }
        self._update_hidden_params(new_params)

    def _update_hidden_params(self, new_params: dict) -> None:
        """Update hidden params - dynamically maps clean keys to legacy keys"""
        adapted_params = adapt_payload_for_external_litellm(new_params)
        
        # Handle both dict and HiddenParams cases (using adapted_params instead of new_params)
        if isinstance(self._hidden_params, dict):
            self._hidden_params.update(adapted_params)
        elif isinstance(self._hidden_params, HiddenParams):
            # For HiddenParams object, set attributes individually
            for key, value in adapted_params.items():
                setattr(self._hidden_params, key, value)

    def _get_value_from_hidden_params(self, key: str) -> Optional[Any]:
        """Get value from hidden params - dynamically resolves legacy keys for reads"""
        search_key = get_legacy_key(key)
        
        if isinstance(self._hidden_params, dict):
            return self._hidden_params.get(search_key, None)
        elif isinstance(self._hidden_params, HiddenParams):
            return getattr(self._hidden_params, search_key, None)

    def set_timing_metrics(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        logging_obj: LogDelegator,
    ) -> None:
        """Set response timing metrics"""
        total_response_time_ms = (end_time - start_time).total_seconds() * 1000

        # Set total response time if supported
        if self.supports_response_time:
            self.result._response_ms = total_response_time_ms

        self._update_hidden_params({"_response_ms": total_response_time_ms,})
        llm_api_duration_ms = logging_obj.model_call_details.get("llm_api_duration_ms")
        if llm_api_duration_ms is not None:
            overhead_ms = round(total_response_time_ms - llm_api_duration_ms, 4)
            self._update_hidden_params({"overhead_time_ms": overhead_ms})

        callback_duration_ms = getattr(logging_obj, "callback_duration_ms", None)
        if callback_duration_ms is not None:
            self._update_hidden_params({"callback_duration_ms": round(callback_duration_ms, 4)})

        if (
            logging_obj.caching_details is not None
            and logging_obj.caching_details.get("cache_hit") is True
            and (
                cache_duration_ms := logging_obj.caching_details.get(
                    "cache_duration_ms"
                )
            )
            is not None
        ):
            overhead_ms = total_response_time_ms - cache_duration_ms
            self._update_hidden_params({"overhead_time_ms": overhead_ms})

        if LITELLM_DETAILED_TIMING and llm_api_duration_ms is not None:
            detailed: dict = {"timing_llm_api_ms": round(llm_api_duration_ms, 4)}

            ## message copy time from Logging.__init__()
            msg_copy_ms = getattr(logging_obj, "message_copy_duration_ms", None)
            if msg_copy_ms is not None:
                detailed["timing_message_copy_ms"] = round(msg_copy_ms, 4)

            ## pre-processing = time from request start to LLM API call start
            api_call_start = logging_obj.model_call_details.get("api_call_start_time")
            if api_call_start is not None and start_time is not None:
                pre_ms = (api_call_start - start_time).total_seconds() * 1000
                detailed["timing_pre_processing_ms"] = round(pre_ms, 4)

                ## post-processing = total - pre - llm_api
                post_ms = total_response_time_ms - pre_ms - llm_api_duration_ms
                detailed["timing_post_processing_ms"] = round(max(post_ms, 0), 4)

            self._update_hidden_params(detailed)

    def apply(self) -> None:
        """Apply metadata to the response object"""
        if hasattr(self.result, "_hidden_params"):
            self.result._hidden_params = self._hidden_params


def update_response_metadata(
    result: Any,
    logging_obj: LogDelegator,
    model: Optional[str],
    kwargs: dict,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
) -> None:
    if result is None:
        return

    metadata = ResponseMetadata(result)
    metadata.set_hidden_params(logging_obj, model, kwargs)
    metadata.set_timing_metrics(start_time, end_time, logging_obj)
    metadata.apply()