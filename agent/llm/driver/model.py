# agent.llm.driver.model
## @lineage: ator.driver.llm.model
from __future__ import annotations

import os
import warnings
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, Literal, Final

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

from agent.anchor.model.info import get_features, supports_vision
from agent.anchor.model.token.splitter import create_pretrained_tokenizer
from agent.anchor.model.metric import Metrics
from agent.anchor.config.cloud import VendorConfig

from agent.llm.driver.strategy.fallback import FallbackStrategy

from agent.loop.runtime.exception.types import LLMContextWindowTooSmallError
from agent.llm.driver.factory import DriverFactory
from phase.agent.topos.llm.observer import DriverObserver

from arch.xor.parser.mark.convset import SettingProminence, field_meta
from arch.xor.parser.mark.depre import warn_deprecated
from arch.model.config import config
from arch.xor.secret.validator import serialize_secret, validate_secret
from kernel.bind.resolver import find_current_self

SELF_ROOT = find_current_self()

MIN_CONTEXT_WINDOW_TOKENS: Final[int] = 16384
ENV_ALLOW_SHORT_CONTEXT_WINDOWS: Final[str] = "ALLOW_SHORT_CONTEXT_WINDOWS"

class LLMModel(BaseModel):
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
    vendor_config: VendorConfig = Field(default_factory=VendorConfig, description="Isolated vendor dimensions.")

    num_retries: int = Field(default=5, ge=0)
    retry_multiplier: float = Field(default=8.0, ge=0)
    retry_min_wait: int = Field(default=8, ge=0)
    retry_max_wait: int = Field(default=64, ge=0)
    retry_listener: SkipJsonSchema[
        Callable[[int, int, BaseException | None], None] | None
    ] = Field(default=None, exclude=True)

    # LLM Request Params
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
    safety_settings: list[dict[str, str]] | None = Field(
        default=None,
        deprecated=("Deprecated since v1.15.0 and scheduled for removal in v1.20.0."),
        description="Legacy constraint nodes. No longer structurally enforced.",
    )

    _observer: DriverObserver | None = PrivateAttr(default=None)  
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
    def _init_subsystems(self):
        self.vendor_config.inject_vendor_environment()
        
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

        return self

    @field_serializer("api_key", when_used="always")
    def _serialize_api_key(self, v: SecretStr | None, info):
        return serialize_secret(v, info)

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

    ## @layer: Feature Validation & Processing (순수 유틸리티)
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

    @contextmanager
    def _brane_modify_params_ctx(self, flag: bool):
        old = getattr(config, "modify_params", True)
        try:
            config.modify_params = flag
            yield
        finally:
            config.modify_params = old

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