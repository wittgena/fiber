# fiber.llm.pipeline.interceptor.guardrail
from __future__ import annotations

import time
from typing import Any, List

from fiber.dev.console.observer.signal import UIKey, UISlot, UISlotStatus
from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.compat.secret.redact import redact_string, sanitize_payload, FastRegexRedactionEngine

from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

guardrail_log = get_emitter("interceptor.guardrail", phase="DEFENSE")

# Masking & Sanitization Group (PRE_TRANSLATE: Mutate & Pass)
class PayloadSanitizationGuardrail(DuplexChannel):
    """
    @desc: Mutation & Pass (Built-in Deep Sanitization)
    - Recursively masks built-in PII/secrets across the entire raw payload (headers, env, URLs, messages) without breaking the flow
    """
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        t0 = time.perf_counter()
        sanitized_msg = sanitize_payload(msg)
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        guardrail_log.debug(
            f"[GUARDRAIL SANITIZE] Deep-sanitized payload in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(sanitized_msg)


class CustomRegexMaskingGuardrail(DuplexChannel):
    """
    @desc: Mutation & Pass (Custom C-Regex PII Masking)
    - Uses a C-level compiled regex engine to mask custom domain PII (e.g., SSN, phone numbers) in-place before caching/translation without breaking the flow
    """
    target_slot = PipelineSlot.PRE_TRANSLATE

    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    def _mask_messages(self, messages: Any) -> int:
        masked_count = 0
        if not isinstance(messages, list):
            return masked_count

        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = msg.get("content")
            if isinstance(content, str) and content:
                content_bytes = content.encode("utf-8")
                redacted_bytes = self.engine.execute(content_bytes)
                if redacted_bytes != content_bytes:
                    msg["content"] = redacted_bytes.decode("utf-8", errors="replace")
                    masked_count += 1
        return masked_count

    async def write(self, ctx: ChannelContext, msg: Any):
        t0 = time.perf_counter()
        masked_count = 0

        if isinstance(msg, dict):
            masked_count += self._mask_messages(msg.get("messages"))
        else:
            orig_kwargs = getattr(msg, "original_kwargs", None)
            if isinstance(orig_kwargs, dict):
                masked_count += self._mask_messages(orig_kwargs.get("messages"))

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        guardrail_log.debug(
            f"[GUARDRAIL MASK] Custom C-Regex masked {masked_count} message(s) in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(msg)


# =====================================================================
# 2. Strict Blocking Group (POST_TRANSLATE: Short-Circuit on Match)
# =====================================================================
class ActiveBlockingGuardrail(DuplexChannel):
    """
    @desc: Strict Blocking (Built-in + Registry Standard Secrets)
    - Scans translated prompts against standard credential patterns and short-circuits I/O upon detection
    """
    target_slot = PipelineSlot.POST_TRANSLATE

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        t0 = time.perf_counter()
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", []) if isinstance(original_kwargs, dict) else []

        for msg in messages:
            content = msg.get("content", "") if isinstance(msg, dict) else ""
            if isinstance(content, str) and content:
                if redact_string(content) != content:
                    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
                    guardrail_log.signal(
                        f"[GUARDRAIL BLOCK] Secret or credential detected in POST_TRANSLATE ({elapsed_ms:.3f}ms)!",
                        **{
                            UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                            UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                            UIKey.LATENCY_MS.value: elapsed_ms,
                        },
                    )
                    await ctx.fire_exception_caught(
                        PermissionError("Guardrail Triggered: Sensitive credentials detected and blocked.")
                    )
                    return

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        guardrail_log.debug(
            f"[GUARDRAIL PASS] Clean prompt verified in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(processed_msg)


class CustomRegexBlockingGuardrail(DuplexChannel):
    """
    @desc: Strict Blocking (Custom Policies)
    - Uses a C-level compiled regex engine to evaluate custom domain policies (e.g., SSN, internal codes) and short-circuits upon detection
    """
    target_slot = PipelineSlot.POST_TRANSLATE

    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        t0 = time.perf_counter()
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", []) if isinstance(original_kwargs, dict) else []

        for msg in messages:
            content = msg.get("content", "") if isinstance(msg, dict) else ""
            if isinstance(content, str) and content:
                # 바이트 치환(execute) 대신 매칭 판별(contains_match)로 Zero-Copy 즉시 차단
                if self.engine.contains_match(content.encode("utf-8")):
                    elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
                    guardrail_log.signal(
                        f"[GUARDRAIL BLOCK] Custom C-Regex PII pattern detected ({elapsed_ms:.3f}ms)!",
                        **{
                            UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                            UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                            UIKey.LATENCY_MS.value: elapsed_ms,
                        },
                    )
                    await ctx.fire_exception_caught(
                        PermissionError("Guardrail Triggered: Custom PII Policy Violation.")
                    )
                    return

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
        guardrail_log.debug(
            f"[GUARDRAIL PASS] Custom C-Regex scan clean in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(processed_msg)