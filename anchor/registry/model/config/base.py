# anchor.registry.model.config.base
## @lineage: bound.surface.legacy.config.base
import types
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import httpx
from pydantic import BaseModel

from bound.adapter.switch.params import ModelResponse
from bound.surface.client.param.format import map_developer_role_to_system_role, type_to_response_format_param
from anchor.registry.model.config.constants import DEFAULT_MAX_TOKENS, RESPONSE_FORMAT_TOOL_NAME
from bound.bridge.transport.client import AsyncHTTPClient
from bound.bridge.transport.client import HTTPClient
from bound.surface.stream.wrapper import StreamWrapper
from bound.surface.legacy.openai.types import (
    AllMessageValues,
    ChatCompletionToolChoiceFunctionParam,
    ChatCompletionToolChoiceObjectParam,
    ChatCompletionToolParam,
    ChatCompletionToolParamFunctionChunk,
)

LiteLLMLoggingObj = Any

class BaseLLMException(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        headers: Optional[Union[dict, httpx.Headers]] = None,
        request: Optional[httpx.Request] = None,
        response: Optional[httpx.Response] = None,
        body: Optional[dict] = None,
    ):
        self.status_code = status_code
        self.message: str = message
        self.headers = headers
        if request:
            self.request = request
        else:
            self.request = httpx.Request(
                method="POST", url="https://docs.litellm.ai/docs"
            )
        if response:
            self.response = response
        else:
            self.response = httpx.Response(
                status_code=status_code, request=self.request
            )
        self.body = body
        super().__init__(
            self.message
        )  # Call the base class constructor with the parameters it needs


class BaseConfig(ABC):
    def __init__(self):
        pass

    @classmethod
    def get_config(cls):
        return {
            k: v
            for k, v in cls.__dict__.items()
            if not k.startswith("_")
            and not isinstance(
                v,
                (
                    types.FunctionType,
                    types.BuiltinFunctionType,
                    classmethod,
                    staticmethod,
                    property,
                ),
            )
            and v is not None
            and not callable(v)  # Filter out any callable objects including mocks
        }

    def get_json_schema_from_pydantic_object(
        self, response_format: Optional[Union[Type[BaseModel], dict]]
    ) -> Optional[dict]:
        return type_to_response_format_param(response_format=response_format)

    def is_thinking_enabled(self, non_default_params: dict) -> bool:
        return (non_default_params.get("thinking") or {}).get(
            "type"
        ) == "enabled" or non_default_params.get("reasoning_effort") is not None

    def is_max_tokens_in_request(self, non_default_params: dict) -> bool:
        """
        OpenAI spec allows max_tokens or max_completion_tokens to be specified.
        """
        return (
            "max_tokens" in non_default_params
            or "max_completion_tokens" in non_default_params
        )

    def update_optional_params_with_thinking_tokens(
        self, non_default_params: dict, optional_params: dict
    ):
        is_thinking_enabled = self.is_thinking_enabled(optional_params)
        if is_thinking_enabled and (
            "max_tokens" not in non_default_params
            and "max_completion_tokens" not in non_default_params
        ):
            thinking_token_budget = cast(dict, optional_params["thinking"]).get(
                "budget_tokens", None
            )
            if thinking_token_budget is not None:
                optional_params["max_tokens"] = (
                    thinking_token_budget + DEFAULT_MAX_TOKENS
                )

    def should_fake_stream(
        self,
        model: Optional[str],
        stream: Optional[bool],
        custom_llm_provider: Optional[str] = None,
    ) -> bool:
        return False

    def _add_tools_to_optional_params(self, optional_params: dict, tools: List) -> dict:
        if "tools" not in optional_params:
            optional_params["tools"] = tools
        else:
            optional_params["tools"] = [
                *optional_params["tools"],
                *tools,
            ]
        return optional_params

    def translate_developer_role_to_system_role(
        self,
        messages: List[AllMessageValues],
    ) -> List[AllMessageValues]:
        """
        Translate `developer` role to `system` role for non-OpenAI providers.

        Overriden by OpenAI/Azure
        """
        return map_developer_role_to_system_role(messages=messages)

    def should_retry_llm_api_inside_llm_translation_on_http_error(
        self, e: httpx.HTTPStatusError, litellm_params: dict
    ) -> bool:
        return False

    def transform_request_on_unprocessable_entity_error(
        self, e: httpx.HTTPStatusError, request_data: dict
    ) -> dict:
        return request_data

    @property
    def max_retry_on_unprocessable_entity_error(self) -> int:
        return 0

    @abstractmethod
    def get_supported_openai_params(self, model: str) -> list:
        pass

    def _add_response_format_to_tools(
        self,
        optional_params: dict,
        value: dict,
        is_response_format_supported: bool,
        enforce_tool_choice: bool = True,
    ) -> dict:
        json_schema: Optional[dict] = None
        if "response_schema" in value:
            json_schema = value["response_schema"]
        elif "json_schema" in value:
            json_schema = value["json_schema"]["schema"]

        if json_schema and not is_response_format_supported:
            _tool_choice = ChatCompletionToolChoiceObjectParam(
                type="function",
                function=ChatCompletionToolChoiceFunctionParam(
                    name=RESPONSE_FORMAT_TOOL_NAME
                ),
            )

            _tool = ChatCompletionToolParam(
                type="function",
                function=ChatCompletionToolParamFunctionChunk(
                    name=RESPONSE_FORMAT_TOOL_NAME, parameters=json_schema
                ),
            )

            optional_params.setdefault("tools", [])
            optional_params["tools"].append(_tool)
            if enforce_tool_choice:
                optional_params["tool_choice"] = _tool_choice

            optional_params["json_mode"] = True
        elif is_response_format_supported:
            optional_params["response_format"] = value
        return optional_params

    @abstractmethod
    def map_openai_params(
        self,
        non_default_params: dict,
        optional_params: dict,
        model: str,
        drop_params: bool,
    ) -> dict:
        pass

    @abstractmethod
    def validate_environment(
        self,
        headers: dict,
        model: str,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
    ) -> dict:
        pass

    def sign_request(
        self,
        headers: dict,
        optional_params: dict,
        request_data: dict,
        api_base: str,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        stream: Optional[bool] = None,
        fake_stream: Optional[bool] = None,
    ) -> Tuple[dict, Optional[bytes]]:
        return headers, None

    def get_complete_url(
        self,
        api_base: Optional[str],
        api_key: Optional[str],
        model: str,
        optional_params: dict,
        litellm_params: dict,
        stream: Optional[bool] = None,
    ) -> str:
        if api_base is None:
            raise ValueError("api_base is required")
        return api_base

    @abstractmethod
    def transform_request(
        self,
        model: str,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        headers: dict,
    ) -> dict:
        pass

    async def async_transform_request(
        self,
        model: str,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        headers: dict,
    ) -> dict:
        return self.transform_request(
            model=model,
            messages=messages,
            optional_params=optional_params,
            litellm_params=litellm_params,
            headers=headers,
        )

    @abstractmethod
    def transform_response(
        self,
        model: str,
        raw_response: httpx.Response,
        model_response: "ModelResponse",
        logging_obj: LiteLLMLoggingObj,
        request_data: dict,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        encoding: Any,
        api_key: Optional[str] = None,
        json_mode: Optional[bool] = None,
    ) -> "ModelResponse":
        pass

    @abstractmethod
    def get_error_class(
        self, error_message: str, status_code: int, headers: Union[dict, httpx.Headers]
    ) -> BaseLLMException:
        pass

    def get_model_response_iterator(
        self,
        streaming_response: Union[Iterator[str], AsyncIterator[str], "ModelResponse"],
        sync_stream: bool,
        json_mode: Optional[bool] = False,
    ) -> Any:
        pass

    async def get_async_custom_stream_wrapper(
        self,
        model: str,
        custom_llm_provider: str,
        logging_obj: LiteLLMLoggingObj,
        api_base: str,
        headers: dict,
        data: dict,
        messages: list,
        client: Optional[AsyncHTTPClient] = None,
        json_mode: Optional[bool] = None,
        signed_json_body: Optional[bytes] = None,
    ) -> "CustomStreamWrapper":
        raise NotImplementedError

    def get_sync_custom_stream_wrapper(
        self,
        model: str,
        custom_llm_provider: str,
        logging_obj: LiteLLMLoggingObj,
        api_base: str,
        headers: dict,
        data: dict,
        messages: list,
        client: Optional[Union[HTTPClient, AsyncHTTPClient]] = None,
        json_mode: Optional[bool] = None,
        signed_json_body: Optional[bytes] = None,
    ) -> "CustomStreamWrapper":
        raise NotImplementedError

    @property
    def custom_llm_provider(self) -> Optional[str]:
        return None

    @property
    def has_custom_stream_wrapper(self) -> bool:
        return False

    @property
    def supports_stream_param_in_request_body(self) -> bool:
        return True

    def post_stream_processing(self, stream: Any) -> Any:
        return stream

    def calculate_additional_costs(
        self, model: str, prompt_tokens: int, completion_tokens: int
    ) -> Optional[dict]:
        return None
