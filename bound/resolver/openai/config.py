# bound.resolver.openai.config
from typing import Any, AsyncIterator, Dict, Iterator, List, Optional, Union, cast
import httpx
from bound.resolver.model.config.base import BaseConfig
from bound.resolver.openai.response import BaseModelResponseIterator
from bound.resolver.openai.types import AllMessageValues
from bound.parser.response import convert_to_model_response_object
from bound.resolver.openai.base import OpenAIError
from eco.exception import BaseLLMException
from eco.tenant.switch.params import ModelResponse, ModelResponseStream
from eco.watcher.delegator import LogDelegator


class OpenAIConfig(BaseConfig):
    """OpenAI 모델의 파라미터 맵핑 및 Request/Response 변환을 담당하는 설정 클래스"""
    
    # OpenAI 표준 파라미터
    frequency_penalty: Optional[float] = None
    function_call: Optional[Union[str, dict]] = None
    functions: Optional[list] = None
    logit_bias: Optional[dict] = None
    max_completion_tokens: Optional[int] = None
    max_tokens: Optional[int] = None
    n: Optional[int] = None
    presence_penalty: Optional[float] = None
    stop: Optional[Union[str, list]] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    response_format: Optional[dict] = None

    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            if hasattr(self.__class__, key) and value is not None:
                setattr(self, key, value)

    @classmethod
    def get_config(cls):
        return super().get_config()

    def get_supported_openai_params(self, model: str) -> list:
        """
        [NEW] 전역 config에 의존하지 않고 명시적으로 지원 파라미터 목록을 반환합니다.
        필요 시 모델(예: o1, o3-mini)에 따라 분기 처리를 추가할 수 있는 확장 지점입니다.
        """
        base_params = [
            "temperature", "top_p", "max_tokens", "max_completion_tokens",
            "stream", "stream_options", "stop", "presence_penalty", "frequency_penalty",
            "logit_bias", "user", "response_format", "seed", "tools", "tool_choice",
            "parallel_tool_calls", "logprobs", "top_logprobs", "service_tier"
        ]
        
        # 모델별 예외 파라미터 처리 예시 (확장성 확보)
        if model.startswith("o1"):
            base_params.remove("temperature")  # o1 모델은 temperature 미지원
            
        return base_params

    def map_openai_params(
        self, non_default_params: dict, optional_params: dict, model: str, drop_params: bool
    ) -> dict:
        """
        [NEW] 외부 위임 없이 내부에서 파라미터 필터링 및 매핑을 직접 수행합니다.
        (_map_openai_params 등 중복 메서드를 하나로 통폐합)
        """
        supported_params = self.get_supported_openai_params(model)
        
        for param, value in non_default_params.items():
            if param in supported_params:
                optional_params[param] = value
            elif not drop_params:
                # 강제 드롭 설정이 아니라면 그대로 전달 (미래 호환성 방어)
                optional_params[param] = value

        # o1 모델 계열의 max_tokens -> max_completion_tokens 강제 트랜스폼 예시
        if model.startswith("o1") and "max_tokens" in optional_params:
            optional_params["max_completion_tokens"] = optional_params.pop("max_tokens")

        return optional_params

    def _transform_messages(self, messages: List[AllMessageValues], model: str) -> List[AllMessageValues]:
        return messages

    def get_error_class(self, error_message: str, status_code: int, headers: Union[dict, httpx.Headers]) -> BaseLLMException:
        return OpenAIError(
            status_code=status_code,
            message=error_message,
            headers=headers,
        )

    def transform_request(self, model: str, messages: List[AllMessageValues], optional_params: dict, litellm_params: dict, headers: dict) -> dict:
        messages = self._transform_messages(messages=messages, model=model)
        return {"model": model, "messages": messages, **optional_params}

    def transform_response(
        self, model: str, raw_response: httpx.Response, model_response: ModelResponse,
        logging_obj: LogDelegator, request_data: dict, messages: List[AllMessageValues],
        optional_params: dict, litellm_params: dict, encoding: Any,
        api_key: Optional[str] = None, json_mode: Optional[bool] = None,
    ) -> ModelResponse:
        
        logging_obj.post_call(original_response=raw_response.text)
        logging_obj.model_call_details["response_headers"] = raw_response.headers
        
        return cast(
            ModelResponse,
            convert_to_model_response_object(
                response_object=raw_response.json(),
                model_response_object=model_response,
                hidden_params={"headers": raw_response.headers},
                _response_headers=dict(raw_response.headers),
            ),
        )

    def validate_environment(self, headers: dict, model: str, messages: List[AllMessageValues], optional_params: dict, litellm_params: dict, api_key: Optional[str] = None, api_base: Optional[str] = None) -> dict:
        return {
            "Authorization": f"Bearer {api_key}",
            **headers,
        }

    def get_model_response_iterator(self, streaming_response: Union[Iterator[str], AsyncIterator[str], ModelResponse], sync_stream: bool, json_mode: Optional[bool] = False) -> Any:
        return OpenAIChatCompletionResponseIterator(
            streaming_response=streaming_response,
            sync_stream=sync_stream,
            json_mode=json_mode,
        )


class OpenAIChatCompletionResponseIterator(BaseModelResponseIterator):
    """OpenAI 스트림 청크 파서"""
    def chunk_parser(self, chunk: dict) -> ModelResponseStream:
        try:
            return ModelResponseStream(**chunk)
        except Exception as e:
            raise e