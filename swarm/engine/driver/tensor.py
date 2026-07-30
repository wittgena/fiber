# swarm.engine.driver.tensor
from __future__ import annotations
import copy
import json
import os
import warnings
from collections.abc import Callable, Sequence, generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, get_args, get_origin, Final, cast
import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SecretStr,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.json_schema import SkipJsonSchema
from functools import cached_property
from pathlib import Path

from mesh.model.info import get_features
from mesh.model.support import supports_vision
from mesh.model.config.resolver import config

from mesh.stream.wrapper import StreamWrapper
from mesh.model.types.core import ModelResponse  # [개선] 레거시 스위치가 아닌 core 타입 사용
from tenant.token.splitter import create_pretrained_tokenizer
from tenant.token.counter import token_counter

from tenant.client.completion import completion as brane_completion
from tenant.cost.tracker.metric import Metrics
from swarm.engine.driver.observer import DriverObserver
from mesh.bound.exception.eco import (
    APIConnectionError,
    InternalServerError,
    RateLimitError,
    ServiceUnavailableError,
    Timeout as LiteLLMTimeout,
)

from mesh.bound.secure.secret.validator import serialize_secret, validate_secret
from mesh.bound.exception.types import LLMNoResponseError

from swarm.engine.driver.retry import RetryMixin
from swarm.engine.mock.mixin import MockToolCallMixin
from mesh.bound.exception.types import LLMContextWindowTooSmallError
from mesh.bound.exception.mapping import map_provider_exception
from swarm.engine.llm.response import LLMResponse
from swarm.atoa.conv.types import TokenCallbackType
from swarm.atoa.conv.message import Message
from arch.xor.xe.convset import SettingProminence, field_meta
from arch.xor.xe.depre import warn_deprecated

from swarm.engine.driver.fallback import FallbackStrategy
from swarm.engine.driver.io import DriverIO
from swarm.engine.driver.config.vendor import VendorSubstrateMixin

from swarm.engine.driver.factory.driver import DriverFactory
from swarm.engine.driver.registry import LLMProfileStore
from phi.agent.action.definition import ActionDefinition

from phase.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
log = get_emitter(__name__)

MIN_CONTEXT_WINDOW_TOKENS: Final[int] = 16384
ENV_ALLOW_SHORT_CONTEXT_WINDOWS: Final[str] = "ALLOW_SHORT_CONTEXT_WINDOWS"
DEFAULT_MAX_OUTPUT_TOKENS_CAP: Final[int] = 16384

_LLM_FALLBACK_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (
    APIConnectionError,
    RateLimitError,
    ServiceUnavailableError,
    LiteLLMTimeout,
    InternalServerError,
    LLMNoResponseError,
)

class Driver(VendorSubstrateMixin, RetryMixin, MockToolCallMixin):
    model: str = Field(
        default="claude-sonnet-4-20250514",
        description="Target topological matrix (Model ID).",
        json_schema_extra=field_meta(SettingProminence.CRITICAL),
    )
    api_key: str | SecretStr | None = Field(
        default=None,
        description="Encrypted access vector.",
        json_schema_extra=field_meta(SettingProminence.CRITICAL, label="API Key"),
    )
    base_url: str | None = Field(
        default=None,
        description="Network routing anchor.",
        json_schema_extra=field_meta(SettingProminence.CRITICAL),
    )
    api_version: str | None = Field(default=None, description="API version constraint (e.g., Azure).")
    
    num_retries: int = Field(default=5, ge=0)
    retry_multiplier: float = Field(default=8.0, ge=0)
    retry_min_wait: int = Field(default=8, ge=0)
    retry_max_wait: int = Field(default=64, ge=0)
    timeout: int | None = Field(
        default=300,
        ge=0,
        description="Maximum temporal resonance before terminating connection.",
    )

    max_message_chars: int = Field(
        default=30_000,
        ge=1,
        description="Character limit per projection vector to prevent buffer overflow.",
    )
    temperature: float | None = Field(
        default=None,
        ge=0,
        description="Topological entropy index (0.0 for deterministic bounds).",
    )
    top_p: float | None = Field(
        default=None, ge=0, le=1,
        description="Nucleus convergence boundary.",
    )
    top_k: float | None = Field(default=None, ge=0)
    max_input_tokens: int | None = Field(
        default=None, ge=1,
        description="Theoretical context horizon.",
    )
    max_output_tokens: int | None = Field(
        default=None, ge=1, 
        description="Maximum token generation limit."
    )
    model_canonical_name: str | None = Field(
        default=None,
        description="Canonical anchor for retrieving provider capabilities when using proxies.",
    )
    extra_headers: dict[str, str] | None = Field(default=None, description="Custom topological injection headers.")
    input_cost_per_token: float | None = Field(default=None, ge=0, description="Metric conversion rate for input volumes.")
    output_cost_per_token: float | None = Field(default=None, ge=0, description="Metric conversion rate for output volumes.")
    ollama_base_url: str | None = Field(default=None)
    stream: bool = Field(
        default=False,
        description="Enable continuous temporal fragmentation (streaming).",
    )
    drop_params: bool = Field(default=True)
    modify_params: bool = Field(
        default=True,
        description="Permit active structural manipulation by Brane",
    )
    disable_vision: bool | None = Field(
        default=None,
        description="Deactivate spatial processing matrices for cost optimization.",
    )
    disable_stop_word: bool | None = Field(default=False, description="Bypass stop sequence inhibitors.")
    caching_prompt: bool = Field(
        default=True,
        description="Enable temporal retention for prompt segments.",
    )
    log_completions: bool = Field(
        default=False,
        description="Record full interaction trajectories.",
    )
    log_completions_folder: str = Field(
        default=str(SELF_ROOT / "completions"),
        description="Output manifold for recording trajectories.",
    )

    custom_tokenizer: str | None = Field(default=None, description="Custom tokenizer matrix.")
    native_tool_calling: bool = Field(default=True, description="Enable native functional integration.")
    force_string_serializer: bool | None = Field(
        default=None,
        description="Flatten structure arrays into strings for primitive endpoints.",
    )
    reasoning_effort: Literal["low", "medium", "high", "xhigh", "none"] | None = Field(
        default="high",
        description="Depth limit for latent cognitive operations.",
    )
    reasoning_summary: Literal["auto", "concise", "detailed"] | None = Field(
        default=None,
        description="Density of projected reasoning traces.",
    )
    enable_encrypted_reasoning: bool = Field(
        default=True,
        description="Request secured topological traces (Responses API).",
    )
    prompt_cache_retention: str | None = Field(
        default="24h",
        description="Temporal duration for topological prompt retention.",
    )
    extended_thinking_budget: int | None = Field(
        default=200_000,
        description="Maximum volume allocation for extended latent states.",
    )
    seed: int | None = Field(default=None, description="Anchor for deterministic randomization.")
    safety_settings: list[dict[str, str]] | None = Field(
        default=None,
        deprecated=("Deprecated since v1.15.0 and scheduled for removal in v1.20.0."),
        description="Legacy constraint nodes. No longer structurally enforced.",
    )
    usage_id: str = Field(
        default="default",
        serialization_alias="usage_id",
        description="Unique identifier for spatial telemetry tracking.",
    )
    brane_extra_body: dict[str, Any] = Field(
        default_factory=dict,
        description="Injection vectors for proxy routers, vLLM constraints, or custom inference clusters.",
    )

    fallback_strategy: FallbackStrategy | None = Field(
        default=None,
        description="Alternative routing path in the event of primary endpoint collapse.",
        exclude=True,
    )

    ## @layer: Sub-System Pointers
    _io: DriverIO | None = PrivateAttr(default=None)
    _observer: DriverObserver | None = PrivateAttr(default=None)  
    
    retry_listener: SkipJsonSchema[
        Callable[[int, int, BaseException | None], None] | None
    ] = Field(default=None, exclude=True)

    ## @layer: Execution Context
    _model_info: Any = PrivateAttr(default=None)
    _tokenizer: Any = PrivateAttr(default=None)
    _is_subscription: bool = PrivateAttr(default=False)
    _provider: str | None = PrivateAttr(default=None)

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    ## Validators
    @field_validator("safety_settings", mode="before")
    @classmethod
    def _warn_safety_settings_deprecated(cls, v: list[dict[str, str]] | None) -> list[dict[str, str]] | None:
        if v is not None:
            warn_deprecated(
                "LLM.safety_settings",
                deprecated_in="1.15.0",
                removed_in="1.20.0",
                details="Safety constraints are structurally obsolete.",
            )
        return v

    @field_validator("api_key", mode="before")
    @classmethod
    def _validate_api_key(cls, v: str | SecretStr | None, info) -> SecretStr | None:
        return validate_secret(v, info)

    @model_validator(mode="before")
    @classmethod
    def _coerce_inputs(cls, data):
        if not isinstance(data, dict):
            return data
        d = dict(data)

        model_val = d.get("model")
        if not model_val:
            raise ValueError("[FATAL] Topological matrix (model) unspecified.")

        if model_val.startswith("azure") and not d.get("api_version"):
            d["api_version"] = "2024-12-01-preview"

        if model_val.startswith("brane/"):
            model_name = model_val.removeprefix("brane/")
            d["model"] = f"brane_proxy/{model_name}"
            d["base_url"] = d.get("base_url") or "https://proxy.app.brane.dev/"

        return d

    @model_validator(mode="after")
    def _init_driver_subsystems(self):
        ## @desc: Bind isolated infrastructure parameters into the global execution space
        self.inject_vendor_environment()

        observer_config = {
            "log_completions": self.log_completions,
            "log_completions_folder": self.log_completions_folder,
            "input_cost_per_token": self.input_cost_per_token,
            "output_cost_per_token": self.output_cost_per_token,
        }
        self._observer = DriverObserver(
            model_name=self.model,
            config=observer_config
        )

        if self.custom_tokenizer:
            self._tokenizer = create_pretrained_tokenizer(self.custom_tokenizer)

        if self._io is None:
            self._io = DriverIO(driver=self)
        return self

    def _retry_listener_fn(self, attempt_number: int, num_retries: int, _err: BaseException | None) -> None:
        if self.retry_listener is not None:
            self.retry_listener(attempt_number, num_retries, _err)

    @field_serializer("api_key", when_used="always")
    def _serialize_api_key(self, v: SecretStr | None, info):
        return serialize_secret(v, info)

    ## @layer: Observability Delegation
    @property
    def metrics(self) -> Metrics:
        assert self._observer is not None, "DriverObserver not initialized"
        return self._observer.metrics

    def restore_metrics(self, metrics: Metrics) -> None:
        if self._observer is not None:
            self._observer.restore_metrics(metrics)

    def reset_metrics(self) -> None:
        if self._observer is not None:
            self._observer.reset_metrics()

    @property
    def is_subscription(self) -> bool:
        return self._is_subscription

    def _handle_error(self, error: Exception, fallback_call_fn: Callable[[Driver], LLMResponse]) -> LLMResponse:
        ## @desc: Process execution ruptures and route through established fallback manifolds
        if self._observer is not None:
            self._observer.track_rupture(error)
        
        if self.fallback_strategy and self.fallback_strategy.should_fallback(error):
            result = self.fallback_strategy.try_fallback(
                primary_model=self.model,
                primary_error=error,
                primary_metrics=self.metrics,
                call_fn=fallback_call_fn,
            )
            if result is not None:
                return result
                
        mapped = map_provider_exception(error)
        if mapped is not error:
            raise mapped from error
        raise

    def completion(
        self,
        messages: list[Message],
        tools: Sequence[ActionDefinition] | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = False,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        assert self._io is not None
        return self._io.completion(
            messages=messages,
            tools=tools,
            _return_metrics=_return_metrics,
            add_security_risk_prediction=add_security_risk_prediction,
            on_token=on_token,
            **kwargs
        )

    def responses(
        self,
        messages: list[Message],
        tools: Sequence[ActionDefinition] | None = None,
        include: list[str] | None = None,
        store: bool | None = None,
        _return_metrics: bool = False,
        add_security_risk_prediction: bool = False,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> LLMResponse:
        assert self._io is not None
        return self._io.responses(
            messages=messages,
            tools=tools,
            include=include,
            store=store,
            _return_metrics=_return_metrics,
            add_security_risk_prediction=add_security_risk_prediction,
            on_token=on_token,
            **kwargs
        )

    ## @layer: Transport & Internal Operations
    def _infer_provider(self) -> str | None:
        if self._provider is not None:
            return self._provider

        provider = DriverFactory.infer_provider(model=self.model, api_base=self.base_url)
        self._provider = provider
        return provider

    def _get_api_key_value(self) -> str | None:
        api_key_value: str | None = None
        if self.api_key:
            assert isinstance(self.api_key, SecretStr)
            api_key_value = self.api_key.get_secret_value()

        if api_key_value is not None and self._infer_provider() == "bedrock":
            return None

        return api_key_value

    def _transport_call(
        self,
        *,
        messages: list[dict[str, Any]],
        enable_streaming: bool = False,
        on_token: TokenCallbackType | None = None,
        **kwargs,
    ) -> ModelResponse:
        """
        @desc: Core execution boundary for sending parameters to the LLM Gateway.
        [리팩토링] 스트리밍 시 청크를 리스트에 무겁게 축적하지 않고,
        파이프라인 Accumulator를 통해 단일 패스(Single-pass)로 완성된 응답을 추출합니다.
        """
        with self._brane_modify_params_ctx(self.modify_params):
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx.*")
                warnings.filterwarnings("ignore", message=r".*content=.*upload.*", category=DeprecationWarning)
                warnings.filterwarnings("ignore", message=r"There is no current event loop", category=DeprecationWarning)
                warnings.filterwarnings("ignore", category=UserWarning)
                warnings.filterwarnings("ignore", category=DeprecationWarning, message="Accessing the 'model_fields' attribute.*")
                
                api_key_value = self._get_api_key_value()
                
                vendor_kwargs = self.get_vendor_transport_kwargs()
                merged_kwargs = {**vendor_kwargs, **kwargs}
                merged_kwargs.pop("base_model", None)

                # 스트리밍 활성화 시 명시적 파라미터 전달
                if enable_streaming:
                    merged_kwargs["stream"] = True

                call_kwargs = {
                    "model": self.model,
                    "api_key": api_key_value,
                    "api_base": self.base_url,
                    "api_version": self.api_version,
                    "timeout": self.timeout,
                    "drop_params": self.drop_params,
                    "seed": self.seed,
                    "messages": messages,
                    **merged_kwargs,
                }
                
                call_kwargs = {
                    k: v for k, v in call_kwargs.items() 
                    if v is not None or k not in ["api_key", "api_base", "api_version"]
                }

                ret = brane_completion(**call_kwargs)

                # 🚀 파이프라인 기반의 스트림 처리 혁신 (stream_chunk_builder 폐기)
                if enable_streaming and on_token is not None:
                    assert isinstance(ret, StreamWrapper), "Streaming response must be handled by StreamWrapper Bridge."
                    
                    # 1. 스트림을 순회하며 콜백(UI/CLI 렌더링)만 호출 (메모리 축적 X)
                    for chunk in ret:
                        on_token(chunk)
                    
                    # 2. 스트림이 고갈되면, 파이프라인 Accumulator에서 완벽히 조립된 객체 즉시 추출
                    final_response = ret.ctx.accumulator.get_complete_response()

                    # 3. Usage(토큰/비용) 누락 방어 폴백 (Provider가 usage를 안 보내주는 경우)
                    if getattr(final_response.usage, "prompt_tokens", 0) == 0 and getattr(final_response.usage, "completion_tokens", 0) == 0:
                        content = final_response.choices[0].message.content or ""
                        try:
                            final_response.usage.prompt_tokens = token_counter(model=self.model, messages=messages)
                        except Exception:
                            final_response.usage.prompt_tokens = 0
                        
                        final_response.usage.completion_tokens = token_counter(model=self.model, text=content, count_response_tokens=True)
                        final_response.usage.total_tokens = final_response.usage.prompt_tokens + final_response.usage.completion_tokens

                    ret = final_response

                assert isinstance(ret, ModelResponse), f"Expected ModelResponse, got {type(ret)}"
                return ret

    @contextmanager
    def _brane_modify_params_ctx(self, flag: bool):
        old = config.modify_params
        try:
            config.modify_params = flag
            yield
        finally:
            config.modify_params = old

    ## @layer: Feature Validation & Processing
    def _model_name_for_capabilities(self) -> str:
        return self.model_canonical_name or self.model

    def _validate_context_window_size(self) -> None:
        if os.environ.get(ENV_ALLOW_SHORT_CONTEXT_WINDOWS, "").lower() in ("true", "1", "yes"):
            return

        if self.max_input_tokens is None:
            return

        if self.max_input_tokens < MIN_CONTEXT_WINDOW_TOKENS:
            raise LLMContextWindowTooSmallError(self.max_input_tokens, MIN_CONTEXT_WINDOW_TOKENS)

    def vision_is_active(self) -> bool:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return not self.disable_vision and self._supports_vision()

    def _supports_vision(self) -> bool:
        model_for_caps = self._model_name_for_capabilities()
        return (
            supports_vision(model_for_caps)
            or supports_vision(model_for_caps.split("/")[-1])
            or (self._model_info is not None and self._model_info.get("supports_vision", False))
            or False
        )

    def is_caching_prompt_active(self) -> bool:
        if not self.caching_prompt:
            return False
        return self.caching_prompt and get_features(self._model_name_for_capabilities()).supports_prompt_cache

    def uses_responses_api(self) -> bool:
        return get_features(self._model_name_for_capabilities()).supports_responses_api

    @property
    def model_info(self) -> dict | None:
        return self._model_info

    def _apply_prompt_caching(self, messages: list[Message]) -> None:
        if len(messages) > 0 and messages[0].role == "system":
            sys_content = messages[0].content
            if len(sys_content) >= 2:
                sys_content[0].cache_prompt = True
                sys_content[1].cache_prompt = False
            elif len(sys_content) == 1:
                sys_content[0].cache_prompt = True

        for message in reversed(messages):
            if message.role in ("user", "tool"):
                message.content[-1].cache_prompt = True
                break

    def format_messages_for_llm(self, messages: list[Message]) -> list[dict]:
        messages = copy.deepcopy(messages)
        if self.is_caching_prompt_active():
            self._apply_prompt_caching(messages)

        model_features = get_features(self._model_name_for_capabilities())
        cache_enabled = self.is_caching_prompt_active()
        vision_enabled = self.vision_is_active()
        function_calling_enabled = self.native_tool_calling
        force_string_serializer = (
            self.force_string_serializer
            if self.force_string_serializer is not None
            else model_features.force_string_serializer
        )
        send_reasoning_content = model_features.send_reasoning_content
        
        return [
            message.to_chat_dict(
                cache_enabled=cache_enabled,
                vision_enabled=vision_enabled,
                function_calling_enabled=function_calling_enabled,
                force_string_serializer=force_string_serializer,
                send_reasoning_content=send_reasoning_content,
            )
            for message in messages
        ]

    def format_messages_for_responses(
        self, messages: list[Message]
    ) -> tuple[str | None, list[dict[str, Any]]]:
        msgs = copy.deepcopy(messages)
        vision_active = self.vision_is_active()
        instructions: str | None = None
        input_items: list[dict[str, Any]] = []
        system_chunks: list[str] = []

        for m in msgs:
            val = m.to_responses_value(vision_enabled=vision_active)
            if isinstance(val, str):
                s = val.strip()
                if s:
                    if self.is_subscription:
                        system_chunks.append(s)
                    else:
                        instructions = s if instructions is None else f"{instructions}\n\n---\n\n{s}"
            elif val:
                input_items.extend(val)

        return instructions, input_items

    def get_token_count(self, messages: list[Message]) -> int:
        log.debug("[METRIC] Projecting spatial volume (token count) including serialized tool vectors.")
        formatted_messages = self.format_messages_for_llm(messages)
        try:
            return int(token_counter(model=self.model, messages=formatted_messages, custom_tokenizer=self._tokenizer))
        except Exception as e:
            log.error(f"[RUPTURE] Failed to calculate spatial volume for model {self.model}: {e}", exc_info=True)
            return 0