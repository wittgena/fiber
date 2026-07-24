# agent.atoa.conv.chat
## @lineage: atoa.agent.conv.chat
## @lineage: atoa.context.conv.chat
## @lineage: agent.context.conv.chat
from __future__ import annotations
from typing import Any
from bound.resolver.model.info import get_features

def apply_defaults_if_absent(
    user_kwargs: dict[str, Any], defaults: dict[str, Any]
) -> dict[str, Any]:
    out = dict(user_kwargs)
    for key, value in defaults.items():
        if key not in out and value is not None:
            out[key] = value
    return out

def select_chat_options(
    llm, user_kwargs: dict[str, Any], has_tools: bool
) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "top_k": llm.top_k,
        "top_p": llm.top_p,
        "temperature": llm.temperature,
        "max_completion_tokens": llm.max_output_tokens,
    }
    out = apply_defaults_if_absent(user_kwargs, defaults)

    # Azure -> uses max_tokens instead
    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    # If user didn't set extra_headers, propagate from llm config
    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    # Reasoning-model quirks
    supports_reasoning_effort = get_features(llm.model).supports_reasoning_effort
    if supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out["reasoning_effort"] = llm.reasoning_effort

        # All reasoning models ignore temp/top_p, except Gemini
        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    # Extended thinking models
    if get_features(llm.model).supports_extended_thinking:
        if llm.extended_thinking_budget:
            budget_tokens = min(llm.extended_thinking_budget, llm.max_output_tokens - 1)
            out["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
            # Enable interleaved thinking
            # Merge default header with any user-provided headers; user wins on conflict
            existing = out.get("extra_headers") or {}
            out["extra_headers"] = {
                "anthropic-beta": "interleaved-thinking-2025-05-14",
                **existing,
            }
            out["max_tokens"] = llm.max_output_tokens
        # Anthropic models ignore temp/top_p
        out.pop("temperature", None)
        out.pop("top_p", None)

    # Tools: if not using native, strip tool_choice so we don't confuse providers
    if not has_tools:
        out.pop("tools", None)
        out.pop("tool_choice", None)

    # Send prompt_cache_retention only if model supports it
    if (
        get_features(llm.model).supports_prompt_cache_retention
        and llm.prompt_cache_retention
    ):
        out["prompt_cache_retention"] = llm.prompt_cache_retention

    # Pass through user-provided extra_body unchanged
    if llm.brane_extra_body:
        out["extra_body"] = llm.brane_extra_body

    return out


def select_responses_options(
    llm, 
    user_kwargs: dict[str, Any], 
    include: list[str] | None = None,
    store: bool | None = None,
) -> dict[str, Any]:
    """
    @desc: Responses API 호출을 위해 Driver 설정과 런타임 kwargs를 병합/정규화합니다.
    """
    defaults: dict[str, Any] = {
        "top_k": llm.top_k,
        "top_p": llm.top_p,
        "temperature": llm.temperature,
        "max_completion_tokens": llm.max_output_tokens,
        "seed": llm.seed,
    }
    
    # Responses API 전용 파라미터 우선 적용
    if include is not None:
        defaults["include"] = include
    if store is not None:
        defaults["store"] = store
        
    out = apply_defaults_if_absent(user_kwargs, defaults)

    # Azure -> uses max_tokens instead
    if llm.model.startswith("azure"):
        if "max_completion_tokens" in out:
            out["max_tokens"] = out.pop("max_completion_tokens")

    # If user didn't set extra_headers, propagate from llm config
    if llm.extra_headers is not None and "extra_headers" not in out:
        out["extra_headers"] = dict(llm.extra_headers)

    # Reasoning-model quirks
    if get_features(llm.model).supports_reasoning_effort:
        if llm.reasoning_effort is not None:
            out.setdefault("reasoning_effort", llm.reasoning_effort)
            
        if "gemini" not in llm.model.lower():
            out.pop("temperature", None)
            out.pop("top_p", None)

    # Extended thinking models
    if get_features(llm.model).supports_extended_thinking:
        if llm.extended_thinking_budget:
            budget_tokens = min(llm.extended_thinking_budget, llm.max_output_tokens - 1)
            out["thinking"] = {
                "type": "enabled",
                "budget_tokens": budget_tokens,
            }
            existing = out.get("extra_headers") or {}
            out["extra_headers"] = {
                "anthropic-beta": "interleaved-thinking-2025-05-14",
                **existing,
            }
            out["max_tokens"] = llm.max_output_tokens
            
        out.pop("temperature", None)
        out.pop("top_p", None)

    # 안전한 extra_body 병합 (런타임 extra_body와 Driver extra_body 충돌 방지)
    if llm.brane_extra_body:
        existing_extra_body = out.get("extra_body", {})
        out["extra_body"] = {**existing_extra_body, **llm.brane_extra_body}

    # 최종적으로 값이 None인 파라미터는 API Validation 에러를 유발하므로 제거
    return {k: v for k, v in out.items() if v is not None}