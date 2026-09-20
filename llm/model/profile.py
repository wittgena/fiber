# fiber.llm.model.profile
from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Literal, Final

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator
from pydantic.json_schema import SkipJsonSchema

from fiber.llm.model.provider.secret import validate_secret
from fiber.llm.exception.types import LLMContextWindowTooSmallError

MIN_CONTEXT_WINDOW_TOKENS: Final[int] = 16384
ENV_ALLOW_SHORT_CONTEXT_WINDOWS: Final[str] = "ALLOW_SHORT_CONTEXT_WINDOWS"

class BaseLLMProfile(BaseModel):
    model: str = Field(default="gemini/gemini-3.1-flash-lite")
    api_key: str | SecretStr | None = Field(default=None)
    base_url: str | None = Field(default=None)
    api_version: str | None = Field(default=None)
    
    # Retry 정책
    num_retries: int = Field(default=5, ge=0)
    retry_multiplier: float = Field(default=8.0, ge=0)
    retry_min_wait: int = Field(default=8, ge=0)
    retry_max_wait: int = Field(default=64, ge=0)
    retry_listener: SkipJsonSchema[
        Callable[[int, int, BaseException | None], None] | None
    ] = Field(default=None, exclude=True)

    # LLM Request Params (표준)
    timeout: int | None = Field(default=300, ge=0)
    max_message_chars: int = Field(default=30_000, ge=1)
    temperature: float | None = Field(default=None, ge=0)
    top_p: float | None = Field(default=None, ge=0, le=1)
    top_k: float | None = Field(default=None, ge=0)
    max_input_tokens: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)
    
    extra_headers: dict[str, str] | None = Field(default=None)
    extra_body: dict[str, Any] = Field(default_factory=dict)
    
    stream: bool = Field(default=False)
    drop_params: bool = Field(default=True)
    modify_params: bool = Field(default=True)
    seed: int | None = Field(default=None)

    # 기능 토글 및 커스텀 설정
    disable_vision: bool | None = Field(default=None)
    disable_stop_word: bool | None = Field(default=False)
    caching_prompt: bool = Field(default=True)
    prompt_cache_retention: str | None = Field(default="24h")
    
    native_tool_calling: bool = Field(default=True)
    force_string_serializer: bool | None = Field(default=None)
    
    reasoning_effort: Literal["low", "medium", "high", "xhigh", "none"] | None = Field(default="high")
    extended_thinking_budget: int | None = Field(default=200_000)
    enable_encrypted_reasoning: bool = Field(default=True)
    
    # Metrics
    input_cost_per_token: float | None = Field(default=None, ge=0)
    output_cost_per_token: float | None = Field(default=None, ge=0)
    usage_id: str = Field(default="default", serialization_alias="usage_id")

    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    @field_validator("api_key", mode="before")
    @classmethod
    def _validate_api_key(cls, v: str | SecretStr | None, info) -> SecretStr | None:
        return validate_secret(v, info)

    def _validate_context_window_size(self) -> None:
        if os.environ.get(ENV_ALLOW_SHORT_CONTEXT_WINDOWS, "").lower() in ("true", "1", "yes"):
            return
        if self.max_input_tokens is None:
            return
        if self.max_input_tokens < MIN_CONTEXT_WINDOW_TOKENS:
            raise LLMContextWindowTooSmallError(self.max_input_tokens, MIN_CONTEXT_WINDOW_TOKENS)

    def vision_is_active(self) -> bool:
        return not self.disable_vision
        
    def is_caching_prompt_active(self) -> bool:
        return self.caching_prompt