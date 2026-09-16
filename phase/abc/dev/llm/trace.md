# fiber.phase.abc.dev.llm.trace
@desc: Fiber LLM Trace & Interceptor Guide

This document defines the architectural specifications and integration patterns for injecting custom telemetry (Tracer), caching, and security (Guardrail) middleware into the Fiber LLM router (`fiber.llm.entry`).

Fiber enforces strict, Netty-style physical pipeline stability while providing developers with a Pythonic, flat-list dependency injection interface (`interceptors=[]`) known as the UX Facade.

---

## 1. Pipeline Flow & Slots (Architecture Overview)

When an array of interceptors is passed to the entry point, the UX Facade extracts the self-declared `target_slot` from each plugin and deterministically auto-routes them to designated lifecycle execution zones.

### Internal Pipeline Flow (Deterministic Execution)

```
[User Request] ➔ acompletion(..., interceptors=[Cache(), Guardrail(), Tracer()])
       │
       ▼ (Entry Facade: Auto-Routing)
       │
 ┌─────▼──────────────────────────────────────────────┐
 │ [Core: ContextBinder]  ➔ (Trace ID / Metadata injected)        │
 ├────────────────────────────────────────────────────┤
 │ [Slot: PRE_OBSERVER]   ➔ (ex: Datadog Tracer)                  │ Runs first. Emits async telemetry before any logic.
 ├────────────────────────────────────────────────────┤
 │ [Slot: PRE_TRANSLATE]  ➔ (ex: Semantic Cache)                  │ Raw dict state. Intercepts for cache hits to bypass I/O.
 ├────────────────────────────────────────────────────┤
 │ [Core: Translator]     ➔ (dict ➔ Pydantic object)              │ Enforces strict schema normalization.
 ├────────────────────────────────────────────────────┤
 │ [Slot: POST_TRANSLATE] ➔ (ex: PII Guardrail)                   │ Validated state. Enforces security policies.
 ├────────────────────────────────────────────────────┤
 │ [Core: Fuel Trap]      ➔ (Budget Exhaustion Trap)              │ Terminates socket physically on budget overrun.
 ├────────────────────────────────────────────────────┤
 │ [Core: Transport]      ➔ (Actual LLM Network I/O)              │ Edge boundary / Network transit.
 └────────────────────────────────────────────────────┘
```

### Pipeline Security
This Netty-style duplex architecture inherently defends against modern gateway vulnerabilities (such as payload injections and observability bypasses) across three critical chokepoints:

* Entry (Translator): Strict Pydantic normalization drops unverified/malformed payloads instantly, radically reducing the attack surface.

* Middle (Un-bypassable Slots): Data and error flows are decoupled. If a Guardrail (POST_TRANSLATE) ruptures the pipeline due to a policy violation, the exception deterministically flows back up to the Tracer (PRE_OBSERVER), guaranteeing zero blind spots in audit logs.

* Exit (Fuel Trap): A hard, network-level socket termination mathematically prevents malicious agents from causing infinite streaming loops and billing runaways.

---

## 2. Interceptor Specifications by Slot

Plugins operate decoupled from the core framework. Developers strictly define the operational scope by assigning the appropriate `target_slot`.

### 📍 [Slot: PRE_OBSERVER] - Asynchronous Telemetry

**Purpose:** Emits observability metrics (Datadog, LangSmith, etc.) without introducing latency to the primary business logic. Inheriting from `BaseLLMTracer` automatically binds the component to the `PRE_OBSERVER` slot.

```python
from typing import Any, Dict
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.execution import ExecutionMetadata
import logging

logger = logging.getLogger("custom.tracer")

class EnterpriseDatadogTracer(BaseLLMTracer):
    """
    Executes asynchronously via fire-and-forget.
    Must guarantee zero-latency overhead on the primary LLM I/O thread.
    """
    
    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        # Invoked immediately after ChannelContext metadata binding.
        logger.info(f"[Trace Start] ID: {meta.trace_id} | Model: {meta.base_model}")
        # datadog.increment("llm.request.start", tags=[f"model:{meta.base_model}"])

    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        # Invoked upon successful completion of the Transport phase.
        usage = getattr(response, 'usage', None)
        total_tokens = getattr(usage, 'total_tokens', 0) if usage else 0
        logger.info(f"[Trace End] ID: {meta.trace_id} | Latency: {duration_ms:.2f}ms | Tokens: {total_tokens}")

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        # Internal exceptions within this block are isolated and will not rupture the pipeline.
        logger.error(f"[Trace Error] ID: {meta.trace_id} | Failed after {duration_ms:.2f}ms | Error: {exc}")

```

### 📍 [Slot: PRE_TRANSLATE] - Semantic Caching

**Purpose:** Intercepts the raw payload (`dict` phase) prior to heavy Pydantic serialization or network I/O. Resolves cache hits by short-circuiting the pipeline.

```python
from fiber.llm.pipeline import PipelineSlot
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from fiber.llm.param import ModelResponse
import uuid

class FastRedisCache(DuplexChannel):
    # Self-routing declaration. Facade parses this attribute during bootstrap.
    target_slot = PipelineSlot.PRE_TRANSLATE 

    async def write(self, ctx: ChannelContext, msg: dict):
        # Payload remains in mutable dict format at this phase.
        prompt = msg.get("messages", [])[-1].get("content", "")
        
        # Cache hit condition simulation.
        if "hello" in prompt.lower():
            # Synthesize a normalized ModelResponse and execute early return (Short-circuit).
            # Routes the data backward through the read pipeline, bypassing the Transport Core.
            cached_response = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "cached-model"),
                choices=[{"index": 0, "message": {"role": "assistant", "content": "Hello from Cache!"}, "finish_reason": "stop"}],
                usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            )
            await ctx.fire_channel_read(cached_response)
            return 
            
        # Cache miss: Propagate execution to the subsequent handler (PayloadTranslator).
        await ctx.fire_write(msg)

```

### 📍 [Slot: POST_TRANSLATE] - Security & PII Guardrails

**Purpose:** Evaluates the immutable, validated Pydantic object against strict enterprise security policies prior to physical network egress.

```python
from fiber.llm.pipeline import PipelineSlot
from xphi.state.phase.channel import DuplexChannel, ChannelContext
import re
from typing import Any

class PIIMaskingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        # processed_msg is strictly typed (Processor Object) post-Translator execution.
        original_kwargs = getattr(processed_msg, "original_kwargs", {})
        messages = original_kwargs.get("messages", [])
        
        for msg in messages:
            content = msg.get("content", "")
            # Policy validation: SSN (Social Security Number) detection.
            if re.search(r'\d{6}-\d{7}', content):
                # Active rupture: Explicitly raise an exception to halt pipeline progression.
                # Guarantees the request never reaches the Transport / LLM Provider.
                raise PermissionError("Guardrail Triggered: PII (SSN) detected in the prompt.")
        
        await ctx.fire_write(processed_msg)

```

---

## 3. Drop-in Execution via UX Facade

Developers orchestrate components without understanding the underlying Netty structure. The `interceptors` parameter accepts a flat list and deterministically delegates placement.

```python
import asyncio
from fiber.llm.entry import acompletion

async def main():
    # 1. Instantiate modular plugins
    tracer = EnterpriseDatadogTracer()
    cache = FastRedisCache()
    guardrail = PIIMaskingGuardrail()
    
    print("🚀 Initiating LLM Call with Full Observability & Governance...")
    
    try:
        response = await acompletion(
            model="gemini-3.5-flash",
            messages=[{"role": "user", "content": "Hello, analyze this data."}],
            # UX Facade: The framework resolves order dependency autonomously based on target_slots.
            # Order of items within the list does not dictate execution sequence across different slots.
            interceptors=[tracer, guardrail, cache], 
            metadata={"kernel_auth": {"audit_hash": "audit-tx-999"}}
        )
        print(f"Response: {response.choices[0].message.content}")
        
    except Exception as e:
        print(f"Execution Blocked or Failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```