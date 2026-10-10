# fiber.dev.trace.llm.guardrail
from typing import Any, Dict, Optional, List

from fiber.gateway.llm.pipeline import PipelineSlot

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.xor.secret.redact import redact_string, sanitize_payload, _REDACTED
from xphi.arch.bound.xor.parser.ruleset.engine import FastRegexRedactionEngine

guardrail_log = get_emitter("llm.guardrail")

class ActiveBlockingGuardrail(DuplexChannel):
    """
    @desc: Strict Blocking (Standard Secrets)
    - Detects hardcoded credentials (API keys, tokens) within messages and immediately short-circuits the pipeline
    """
    target_slot = PipelineSlot.POST_TRANSLATE
    
    async def write(self, ctx: ChannelContext, processed_msg: Any):
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", [])
        
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                # If redacted string differs from the original, a secret is present
                redacted_content = redact_string(content)
                if redacted_content != content:
                    guardrail_log.error("[GUARDRAIL BLOCK] Secret or Credential detected in prompt!")
                    # Short-circuit the pipeline immediately
                    await ctx.fire_exception_caught(
                        PermissionError("Guardrail Triggered: Sensitive credentials detected and blocked.")
                    )
                    return  
                    
        # Proceed to next slot if clean
        await ctx.fire_write(processed_msg)

class PayloadSanitizationGuardrail(DuplexChannel):
    """
    @desc: Mutation & Pass (Deep Sanitization)
    - Recursively masks PII/secrets across the entire raw payload (headers, env, URLs, messages) without breaking the flow
    """
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        guardrail_log.debug("[GUARDRAIL SCAN] Sanitizing payload before translation...")
        
        # Deep-sanitize the raw dictionary payload in a single pass
        sanitized_msg = sanitize_payload(msg)
        
        # Forward the safe, mutated payload to the next slot
        await ctx.fire_write(sanitized_msg)


class CustomRegexBlockingGuardrail(DuplexChannel):
    """
    @desc: Strict Blocking (Custom Policies)
    - Uses a C-level compiled regex engine to evaluate custom domain policies (e.g., SSN, internal codes) and short-circuits upon detection
    """
    target_slot = PipelineSlot.POST_TRANSLATE
    
    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        # Initialize the high-performance bytes regex engine
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)
        
    async def write(self, ctx: ChannelContext, processed_msg: Any):
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", [])
        
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                # Engine processes byte streams
                content_bytes = content.encode('utf-8')
                redacted_bytes = self.engine.execute(content_bytes)
                
                # A match occurred if the mask token was injected into the result
                if self.engine.mask_token in redacted_bytes:
                    guardrail_log.error("[GUARDRAIL BLOCK] Custom PII Pattern Detected!")
                    await ctx.fire_exception_caught(
                        PermissionError("Guardrail Triggered: Custom PII Policy Violation.")
                    )
                    return
                    
        await ctx.fire_write(processed_msg)