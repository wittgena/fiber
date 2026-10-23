# fiber.llm.pipeline.interceptor.guardrail
import time
from typing import Any, List

from fiber.dev.console.observer.signal import UIKey, UISlot, UISlotStatus
from fiber.llm.pipeline.bootstrap import PipelineSlot

from xphi.arch.xor.parser.ruleset.engine import FastRegexRedactionEngine
from xphi.arch.xor.secret.redact import redact_string, sanitize_payload
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

guardrail_log = get_emitter("interceptor.guardrail", phase="DEFENSE")

class ActiveBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        t0 = time.perf_counter()
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", []) if isinstance(original_kwargs, dict) else []

        for msg in messages:
            content = msg.get("content", "") if isinstance(msg, dict) else ""
            if isinstance(content, str) and content:
                redacted_content = redact_string(content)
                if redacted_content != content:
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
        guardrail_log.info(
            f"[GUARDRAIL PASS] Clean prompt verified in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(processed_msg)


class PayloadSanitizationGuardrail(DuplexChannel):
    """
    @desc: Mutation & Pass (Deep Sanitization)
    - Recursively masks PII/secrets across the entire raw payload (headers, env, URLs, messages) without breaking the flow
    """
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        t0 = time.perf_counter()
        sanitized_msg = sanitize_payload(msg)
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        guardrail_log.info(
            f"[GUARDRAIL SANITIZE] Deep-sanitized payload in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(sanitized_msg)


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
                content_bytes = content.encode("utf-8")
                redacted_bytes = self.engine.execute(content_bytes)

                if self.engine.mask_token in redacted_bytes:
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
        guardrail_log.info(
            f"[GUARDRAIL PASS] Custom C-Regex scan clean in {elapsed_ms:.3f}ms.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.LATENCY_MS.value: elapsed_ms,
            },
        )
        await ctx.fire_write(processed_msg)