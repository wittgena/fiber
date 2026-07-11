# bound.bridge.response.stream.context
## @lineage: bound.transport.response.stream.context
## @lineage: bound.transport.stream.api.context
## @lineage: bound.surface.response.context
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import httpx
from aiohttp import ClientSession

from anchor.registry.model.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.param.legacy import GenericLiteLLMParams
from bound.surface.legacy.openai.types import (
    PromptObject,
    Reasoning,
    ResponseIncludable,
    ResponseInputParam,
    ToolChoice,
    ToolParam,
    ResponseText
)
from bound.bridge.transport.client import AsyncHTTPClient
from bound.bridge.transport.client import HTTPClient
from xphi.watcher.delegator import LogDelegator


@dataclass
class ExecutionContext:
    """네트워크 통신 및 비동기/동기 실행 환경에 대한 컨텍스트"""
    is_async: bool = False
    timeout: Optional[Union[float, httpx.Timeout]] = None
    client: Optional[Union[HTTPClient, AsyncHTTPClient]] = None
    shared_session: Optional["ClientSession"] = None
    extra_headers: Dict[str, Any] = field(default_factory=dict)
    extra_body: Dict[str, Any] = field(default_factory=dict)
    extra_query: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderContext:
    """LLM 프로바이더 및 시스템 설정 컨텍스트"""
    custom_llm_provider: str
    litellm_params: GenericLiteLLMParams
    responses_api_provider_config: Optional[BaseResponsesAPIConfig] = None
    logging_obj: Optional[LogDelegator] = None


@dataclass
class LLMPayloadContext:
    """모델 생성 요청 시 필요한 페이로드 컨텍스트 (CRUD의 경우 선택적 사용)"""
    model: Optional[str] = None
    input: Optional[Union[str, ResponseInputParam]] = None
    response_id: Optional[str] = None  # GET, DELETE, CANCEL 등에서 사용
    
    # OpenAI/LLM Optional Parameters
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_output_tokens: Optional[int] = None
    stream: Optional[bool] = None
    tools: Optional[Iterable[ToolParam]] = None
    tool_choice: Optional[ToolChoice] = None
    reasoning: Optional[Reasoning] = None
    
    # 그 외 확장 파라미터들
    include: Optional[List[ResponseIncludable]] = None
    instructions: Optional[str] = None
    prompt: Optional[PromptObject] = None
    metadata: Optional[Dict[str, Any]] = None
    parallel_tool_calls: Optional[bool] = None
    previous_response_id: Optional[str] = None
    store: Optional[bool] = None
    background: Optional[bool] = None
    text: Optional["ResponseText"] = None
    text_format: Optional[Any] = None
    truncation: Optional[Literal["auto", "disabled"]] = None
    user: Optional[str] = None
    service_tier: Optional[str] = None
    safety_identifier: Optional[str] = None


@dataclass
class ResponseAPIContext:
    """
    모든 요청 파라미터를 담는 최상위 마스터 Context 객체.
    Handler 계층에서는 오직 이 객체 하나만 인자로 받습니다.
    """
    exec: ExecutionContext
    provider: ProviderContext
    payload: LLMPayloadContext

    @property
    def is_async(self) -> bool:
        return self.exec.is_async


class ContextBuilder:
    """
    분산된 kwargs, explicit_args, common_args를 모아 
    하나의 ResponseAPIContext로 변환(조립)하는 팩토리 클래스
    """

    @classmethod
    def from_crud_context(
        cls, 
        ctx_obj: Any,  # 기존 ResponseCRUDContext 
        timeout: Optional[Union[float, httpx.Timeout]] = None
    ) -> ResponseAPIContext:
        """
        GET, DELETE, LIST 등 기존 CRUD Context 객체(또는 common_args)에서
        새로운 구조화된 Context로 변환합니다.
        """
        # Execution 인스턴스 빌드
        exec_ctx = ExecutionContext(
            is_async=ctx_obj.is_async,
            timeout=timeout or ctx_obj.explicit_args.get("timeout"),
            client=ctx_obj.kwargs.get("client"),
            shared_session=ctx_obj.kwargs.get("shared_session"),
            extra_headers=ctx_obj.explicit_args.get("extra_headers") or {},
            extra_body=ctx_obj.explicit_args.get("extra_body") or {},
            extra_query=ctx_obj.explicit_args.get("extra_query") or {},
        )

        # Provider 인스턴스 빌드
        provider_ctx = ProviderContext(
            custom_llm_provider=ctx_obj.custom_llm_provider,
            litellm_params=ctx_obj.litellm_params,
            responses_api_provider_config=ctx_obj.responses_api_provider_config,
            logging_obj=ctx_obj.log_delegator,
        )

        # Payload 인스턴스 빌드 (CRUD의 핵심은 response_id)
        payload_ctx = LLMPayloadContext(
            response_id=ctx_obj.response_id,
            # LIST에서 필요한 추가 인자 매핑
            include=ctx_obj.explicit_args.get("include"),
        )

        return ResponseAPIContext(exec=exec_ctx, provider=provider_ctx, payload=payload_ctx)

    @classmethod
    def from_explicit_args(
        cls,
        explicit_args: Dict[str, Any],
        litellm_params: GenericLiteLLMParams,
        is_async: bool = False,
        responses_api_provider_config: Optional[BaseResponsesAPIConfig] = None,
        log_delegator: Optional[LogDelegator] = None,
        client: Optional[Union[HTTPClient, AsyncHTTPClient]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> ResponseAPIContext:
        """
        responses() 함수에서 넘어오는 방대한 explicit_args 딕셔너리를 
        구조화된 Context로 변환합니다.
        """
        # Execution 인스턴스 빌드
        exec_ctx = ExecutionContext(
            is_async=is_async,
            timeout=explicit_args.get("timeout"),
            client=client,
            shared_session=shared_session,
            extra_headers=explicit_args.get("extra_headers") or {},
            extra_body=explicit_args.get("extra_body") or {},
            extra_query=explicit_args.get("extra_query") or {},
        )

        # Provider 인스턴스 빌드
        provider_ctx = ProviderContext(
            custom_llm_provider=explicit_args.get("custom_llm_provider", ""),
            litellm_params=litellm_params,
            responses_api_provider_config=responses_api_provider_config,
            logging_obj=log_delegator,
        )

        # Payload 인스턴스 빌드 (생성/컴플리션 전용 파라미터들)
        payload_ctx = LLMPayloadContext(
            model=explicit_args.get("model"),
            input=explicit_args.get("input"),
            temperature=explicit_args.get("temperature"),
            top_p=explicit_args.get("top_p"),
            max_output_tokens=explicit_args.get("max_output_tokens"),
            stream=explicit_args.get("stream"),
            tools=explicit_args.get("tools"),
            tool_choice=explicit_args.get("tool_choice"),
            reasoning=explicit_args.get("reasoning"),
            include=explicit_args.get("include"),
            instructions=explicit_args.get("instructions"),
            prompt=explicit_args.get("prompt"),
            metadata=explicit_args.get("metadata"),
            parallel_tool_calls=explicit_args.get("parallel_tool_calls"),
            previous_response_id=explicit_args.get("previous_response_id"),
            store=explicit_args.get("store"),
            background=explicit_args.get("background"),
            text=explicit_args.get("text"),
            text_format=explicit_args.get("text_format"),
            truncation=explicit_args.get("truncation"),
            user=explicit_args.get("user"),
            service_tier=explicit_args.get("service_tier"),
            safety_identifier=explicit_args.get("safety_identifier"),
        )

        return ResponseAPIContext(exec=exec_ctx, provider=provider_ctx, payload=payload_ctx)