# bound.surface.legacy.cost.parser
## @lineage: bound.surface.cost.eco.parser
import time
from typing import TYPE_CHECKING, Any, List, Literal, Optional, Tuple, Union, cast
from httpx import Response
from pydantic import BaseModel

from anchor.registry.model.info import get_model_info
from bound.adapter.switch.params import ModelResponse, ModelResponseStream
from bound.surface.eco.cost.unit.calc import UnitCostCalculator
from bound.surface.legacy.param.rerank import RerankBilledUnits, RerankResponse
from bound.surface.legacy.openai.types import (
    HttpxBinaryResponseContent,
    OpenAIModerationResponse,
    OpenAIRealtimeStreamList,
    OpenAIRealtimeStreamResponseBaseObject,
    OpenAIRealtimeStreamSessionEvents,
    ResponseAPIUsage,
    ResponsesAPIResponse,
)
from bound.surface.legacy.types import (
    CallTypes, CallTypesLiteral, LiteLLMRealtimeStreamLoggingObject, Usage,
    EmbeddingResponse, ImageResponse, TextCompletionResponse, TranscriptionResponse,
    TranscriptionUsageDurationObject, TranscriptionUsageTokensObject,
    PassthroughCallTypes, PromptTokensDetailsWrapper,
)

try:
    from bound.surface.legacy.types import LiteLLMSendMessageResponse
except ImportError:
    LiteLLMSendMessageResponse = Any

from watcher.plane.emitter import get_emitter

log = get_emitter("eco.parser")

"""Type Alias"""
AnyResponseObject = Union[
    ModelResponse, EmbeddingResponse, ImageResponse, TranscriptionResponse,
    TextCompletionResponse, HttpxBinaryResponseContent, RerankResponse,
    ResponsesAPIResponse, LiteLLMRealtimeStreamLoggingObject,
    OpenAIModerationResponse, Response, Any
]

"""CallTypes Constants"""
_A2A_CALL_TYPES = frozenset({CallTypes.asend_message.value, CallTypes.send_message.value})
_VIDEO_CALL_TYPES = frozenset({
    CallTypes.create_video.value, CallTypes.acreate_video.value,
    CallTypes.video_edit.value, CallTypes.avideo_edit.value,
    CallTypes.video_remix.value, CallTypes.avideo_remix.value,
})
_SPEECH_CALL_TYPES = frozenset({CallTypes.speech.value, CallTypes.aspeech.value})
_TRANSCRIPTION_CALL_TYPES = frozenset({CallTypes.atranscription.value, CallTypes.transcription.value})
_RERANK_CALL_TYPES = frozenset({CallTypes.rerank.value, CallTypes.arerank.value})
_SEARCH_CALL_TYPES = frozenset({CallTypes.search.value, CallTypes.asearch.value})
_AREALTIME_CALL_TYPE = CallTypes.arealtime.value
_MCP_CALL_TYPE = CallTypes.call_mcp_tool.value

# [NEW] cost.unit에서 이전된 이미지 관련 CallTypes
_IMAGE_RESPONSE_CALL_TYPES = frozenset({
    CallTypes.image_generation.value,
    CallTypes.aimage_generation.value,
    PassthroughCallTypes.passthrough_image_generation.value,
    CallTypes.image_edit.value,
    CallTypes.aimage_edit.value,
})


# =============================================================================
# [NEW] UsageTransform: 사용량 페이로드 객체 변환 클래스 (cost.unit에서 이전)
# =============================================================================
class UsageTransform:
    @staticmethod
    def is_transcription_usage_object(usage_object: Any) -> bool:
        return isinstance(usage_object, TranscriptionUsageDurationObject) or isinstance(usage_object, TranscriptionUsageTokensObject)

    @staticmethod
    def transform_transcription_usage_object(
        usage_object: Union[TranscriptionUsageDurationObject, TranscriptionUsageTokensObject],
    ) -> Optional[Usage]:
        if isinstance(usage_object, TranscriptionUsageDurationObject):
            return None
        elif isinstance(usage_object, TranscriptionUsageTokensObject):
            return Usage(
                prompt_tokens=usage_object.input_tokens,
                completion_tokens=usage_object.output_tokens,
                total_tokens=usage_object.total_tokens,
                prompt_tokens_details=PromptTokensDetailsWrapper(
                    text_tokens=usage_object.input_token_details.text_tokens,
                    audio_tokens=usage_object.input_token_details.audio_tokens,
                ),
            )
        return None


class UsageTelemetryParser:
    """사용량, 응답 데이터 파싱 및 타입 검증기"""

    @staticmethod
    def has_hidden_params(obj: Any) -> bool:
        return hasattr(obj, "_hidden_params")

    @staticmethod
    def has_token_details(usage_block: Optional[Usage]) -> bool:
        if usage_block is None:
            return False
        prompt_tokens_val = getattr(usage_block, "prompt_tokens", 0) or 0
        completion_tokens_val = getattr(usage_block, "completion_tokens", 0) or 0
        prompt_details = getattr(usage_block, "prompt_tokens_details", None)

        if prompt_details is not None:
            audio_token_count = getattr(prompt_details, "audio_tokens", 0) or 0
            text_token_count = getattr(prompt_details, "text_tokens", 0) or 0
            if audio_token_count > 0 or text_token_count > 0:
                return True
        return (prompt_tokens_val > 0) or (completion_tokens_val > 0)

    @staticmethod
    def extract_usage(completion_response: Any) -> Optional[Usage]:
        usage_obj = cast(
            Union[Usage, ResponseAPIUsage, dict, BaseModel],
            (
                completion_response.get("usage")
                if isinstance(completion_response, dict)
                else getattr(completion_response, "get", lambda x: None)("usage")
            ),
        )
        if usage_obj is None:
            return None
        if isinstance(usage_obj, Usage):
            return usage_obj
        elif isinstance(usage_obj, dict):
            return Usage(**usage_obj)
        elif isinstance(usage_obj, BaseModel):
            return Usage(**usage_obj.model_dump())
        else:
            log.debug(f"Unknown usage object type: {type(usage_obj)}, usage_obj: {usage_obj}")
            return None

    @staticmethod
    def infer_call_type(call_type: Optional[CallTypesLiteral], completion_response: Any) -> Optional[CallTypesLiteral]:
        if call_type is not None:
            return call_type
        if completion_response is None:
            return None

        if isinstance(completion_response, (ModelResponse, ModelResponseStream)):
            return "completion"
        elif isinstance(completion_response, EmbeddingResponse):
            return "embedding"
        elif isinstance(completion_response, TranscriptionResponse):
            return "transcription"
        elif isinstance(completion_response, HttpxBinaryResponseContent):
            return "speech"
        elif isinstance(completion_response, RerankResponse):
            return "rerank"
        elif isinstance(completion_response, ImageResponse):
            return "image_generation"
        elif isinstance(completion_response, TextCompletionResponse):
            return "text_completion"
        elif isinstance(completion_response, LiteLLMSendMessageResponse):
            return "send_message"

        return call_type

    @staticmethod
    def is_known_usage_object(usage_obj: Any) -> bool:
        """Usage 객체가 시스템에서 알려진 타입인지 확인합니다."""
        return (
            isinstance(usage_obj, Usage)
            or isinstance(usage_obj, ResponseAPIUsage)
            or UsageTransform.is_transcription_usage_object(usage_obj)
        )

    @staticmethod
    def is_image_response(obj: Any) -> bool:
        """응답 객체가 이미지 생성 응답인지 확인합니다."""
        return isinstance(obj, ImageResponse)

    @staticmethod
    def is_realtime_stream(obj: Any) -> bool:
        """응답 객체가 실시간 스트림 로깅 객체인지 확인합니다."""
        return isinstance(obj, LiteLLMRealtimeStreamLoggingObject)

    @staticmethod
    def extract_transcription_duration(obj: Any) -> float:
        """TranscriptionResponse에서 오디오 길이를 안전하게 추출합니다."""
        _hidden = getattr(obj, "_hidden_params", {}) or {}
        return float(_hidden.get("audio_transcription_duration", getattr(obj, "duration", 0.0)))

    @staticmethod
    def extract_rerank_units(obj: Any) -> Tuple[Optional[RerankBilledUnits], Optional[int]]:
        """RerankResponse에서 과금 유닛을 안전하게 추출합니다."""
        if isinstance(obj, RerankResponse):
            meta_obj = obj.meta
            billed_units = (meta_obj.get("billed_units", {}) if meta_obj else {}) or {}
            
            rerank_billed_units = RerankBilledUnits(
                search_units=billed_units.get("search_units"), 
                total_tokens=billed_units.get("total_tokens")
            )
            completion_tokens = billed_units.get("search_units") or 1 
            return rerank_billed_units, completion_tokens
        return None, None

    @staticmethod
    def combine_usage_objects(usage_objects: List[Usage]) -> Usage:
        from bound.surface.legacy.types import CompletionTokensDetailsWrapper, PromptTokensDetailsWrapper
        combined = Usage()

        for usage in usage_objects:
            for attr in dir(usage):
                if not attr.startswith("_") and not callable(getattr(usage, attr)):
                    current_val = getattr(combined, attr, 0)
                    new_val = getattr(usage, attr, 0)
                    if new_val is not None and isinstance(new_val, (int, float)) and isinstance(current_val, (int, float)):
                        setattr(combined, attr, current_val + new_val)
                        
            if hasattr(usage, "prompt_tokens_details") and usage.prompt_tokens_details:
                if not hasattr(combined, "prompt_tokens_details") or not combined.prompt_tokens_details:
                    combined.prompt_tokens_details = PromptTokensDetailsWrapper()

                for attr in type(usage.prompt_tokens_details).model_fields:
                    if hasattr(usage.prompt_tokens_details, attr) and not attr.startswith("_") and not callable(getattr(usage.prompt_tokens_details, attr)):
                        current_val = getattr(combined.prompt_tokens_details, attr, 0) or 0
                        new_val = getattr(usage.prompt_tokens_details, attr, 0) or 0
                        if new_val is not None and isinstance(new_val, (int, float)):
                            setattr(combined.prompt_tokens_details, attr, current_val + new_val)

            if hasattr(usage, "completion_tokens_details") and usage.completion_tokens_details:
                if not hasattr(combined, "completion_tokens_details") or not combined.completion_tokens_details:
                    combined.completion_tokens_details = CompletionTokensDetailsWrapper()

                for attr in type(usage.completion_tokens_details).model_fields:
                    if not attr.startswith("_") and not callable(getattr(usage.completion_tokens_details, attr)):
                        current_val = getattr(combined.completion_tokens_details, attr, 0) or 0
                        new_val = getattr(usage.completion_tokens_details, attr, 0) or 0
                        if isinstance(new_val, (int, float)):
                            setattr(combined.completion_tokens_details, attr, current_val + new_val)

        return combined

    @staticmethod
    def _collect_usage_from_realtime_stream_results(results: OpenAIRealtimeStreamList) -> List[Usage]:
        response_done_events: List[OpenAIRealtimeStreamResponseBaseObject] = cast(
            List[OpenAIRealtimeStreamResponseBaseObject],
            [result for result in results if result["type"] == "response.done"],
        )
        usage_objects: List[Usage] = []
        return usage_objects

    @staticmethod
    def create_logging_realtime_object(usage: Usage, results: OpenAIRealtimeStreamList) -> LiteLLMRealtimeStreamLoggingObject:
        return LiteLLMRealtimeStreamLoggingObject(usage=usage, results=results)

    @staticmethod
    def process_realtime_stream(
        results: OpenAIRealtimeStreamList,
        combined_usage_object: Usage,
        custom_llm_provider: str,
        litellm_model_name: str,
        data_residency: Optional[str] = None,
    ) -> float:
        received_model = None
        potential_model_names = []
        for result in results:
            if result["type"] == "session.created":
                received_model = cast(OpenAIRealtimeStreamSessionEvents, result)["session"].get("model", None)
                potential_model_names.append(received_model)

        potential_model_names.append(litellm_model_name)
        input_cost_per_token = 0.0
        output_cost_per_token = 0.0

        for model_name in potential_model_names:
            try:
                if model_name is None:
                    continue
                
                model_info = get_model_info(model=model_name, custom_llm_provider=custom_llm_provider)
                _input_cost, _output_cost = UnitCostCalculator.generic_cost_per_token(
                    model_info=model_info,
                    usage=combined_usage_object,
                    data_residency=data_residency,
                )
            except Exception:
                continue
            
            input_cost_per_token += _input_cost
            output_cost_per_token += _output_cost
            break 
            
        return input_cost_per_token + output_cost_per_token