# fiber.phase.abc.spec.llm.interceptor
@desc: LLM Pipeline Interceptor & Middleware Extension Guide

This guide defines the interface contracts, slot lifecycle, and built-in utilities for building and injecting custom middleware—such as edge caches, semantic routers, DLP guardrails, and telemetry tracers—into the Fiber LLM pipeline (`fiber.llm.entry` and `/v1/chat/completions`).

---

## 1. Pipeline Topology & Slot Execution Order

Developers pass interceptors as a flat list (`interceptors=[...]`), and Fiber autonomously sorts and mounts them into three deterministic execution zones based on each class's `target_slot` attribute:

```text
[Caller] ➔ acompletion(..., interceptors=[Cache(), Router(), Guardrail(), Tracer()])
       │
       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ ZONE 1: OBSERVABILITY & IDENTITY (PipelineSlot.PRE_OBSERVER)                 │
│  ├─ Core Metadata Binding : Allocates trace_id, session_id, and system_meta  │
│  └─ BaseLLMTracer         : Non-blocking start/end/error & stream hooks      │
├──────────────────────────────────────────────────────────────────────────────┤
│ ZONE 2: MUTATION & SHORT-CIRCUIT (PipelineSlot.PRE_TRANSLATE)                │
│  ├─ PayloadSanitization   : Deep dictionary PII masking (sanitize_payload)   │
│  ├─ EdgeRedisCache        : Exact/Vector lookup -> ctx.fire_channel_read()    │
│  └─ OnnxSemanticRouter    : Intent classification -> mutates msg["model"]     │
├──────────────────────────────────────────────────────────────────────────────┤
│ CORE TRANSLATION BOUNDARY (State Traverser: Raw dict ➔ Processor Object)     │
├──────────────────────────────────────────────────────────────────────────────┤
│ ZONE 3: EGRESS SECURITY & GOVERNANCE (PipelineSlot.POST_TRANSLATE)           │
│  ├─ ActiveGuardrail       : C-level regex scan -> ctx.fire_exception_caught() │
│  ├─ Core FuelBreaker      : Real-time PTA ledger check & mid-stream cutoff   │
│  └─ Core Network I/O      : Physical HTTP/SSE transmission to LLM provider   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### ChannelContext Control Primitives

Every `DuplexChannel` interceptor receives `(self, ctx: ChannelContext, msg: Any)` in its `async def write(...)` method and controls pipeline flow using three primitives:

| Primitive | Effect on Pipeline | Primary Use Case |
| --- | --- | --- |
| `await ctx.fire_write(msg)` | Forwards the (optionally mutated) payload downstream toward the network transport. | Cache misses, model routing mutations, payload sanitization. |
| `return await ctx.fire_channel_read(ModelResponse(...))` | **Short-circuits physical I/O.** Sends a synthesized response directly back up the pipeline to the caller. | Edge caching (`EdgeRedisCache`), custom mock fulfillment. |
| `return await ctx.fire_exception_caught(PermissionError(...))` | **Aborts execution immediately.** Propagates the error safely through `on_llm_error` tracers before raising to the caller. | Secret/PII blocking guardrails (`ActiveBlockingGuardrail`). |

---

## 2. Building Interceptors by Target Slot

### 2.1. `PipelineSlot.PRE_OBSERVER` — Stream-Aware Telemetry (`BaseLLMTracer`)

Subclassing `BaseLLMTracer` automatically binds your plugin to `PipelineSlot.PRE_OBSERVER`.

* For **unary calls** (`stream=False`), `on_llm_end` receives the final `ModelResponse` and total `duration_ms`.
* For **streaming calls** (`stream=True`), `on_llm_end` is invoked as soon as the stream generator is initialized. To measure true end-to-end streaming latency and total tokens after the last chunk is consumed, append a callback to `meta.framework_flags["on_stream_complete"]`.

```python
import time
from typing import Any, Dict
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.execution import ExecutionMetadata
from xphi.watcher.plane.emitter import get_emitter

trace_log = get_emitter("custom.tracer")

class StreamAwareTelemetryTracer(BaseLLMTracer):
    async def on_llm_start(self, meta: ExecutionMetadata, kwargs: Dict[str, Any]):
        trace_log.info(f"[Start] trace_id={meta.trace_id} | model={meta.base_model}")

    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        # Stream Mode: Defer final metrics until the caller exhausts all chunks
        if hasattr(response, "__aiter__"):
            def on_stream_complete(total_tokens: int):
                start_ts = meta.framework_flags.get("start_time", time.time())
                stream_ms = (time.time() - start_ts) * 1000
                trace_log.info(
                    f"[Stream Complete] model={meta.base_model} | "
                    f"duration={stream_ms:.1f}ms | tokens={total_tokens}"
                )

            hooks = meta.framework_flags.setdefault("on_stream_complete", [])
            hooks.append(on_stream_complete)
            return

        # Unary Mode: Log immediately
        usage = getattr(response, "usage", None)
        tokens = getattr(usage, "total_tokens", 0) if usage else 0
        trace_log.info(f"[Unary Complete] model={meta.base_model} | duration={duration_ms:.1f}ms | tokens={tokens}")

    async def on_llm_error(self, meta: ExecutionMetadata, exc: Exception, duration_ms: float):
        trace_log.error(f"[Error] trace_id={meta.trace_id} | after={duration_ms:.1f}ms | err={exc}")
```

---

### 2.2. `PipelineSlot.PRE_TRANSLATE` — Edge Caching & Dynamic Routing

At `PRE_TRANSLATE`, `msg` is a **mutable raw `dict**` (`{"model": ..., "messages": [...], ...}`).

#### Pattern A: Short-Circuit Edge Cache (`TunnelFactory` + `fire_channel_read`)

Use `TunnelFactory.get_default()` to access Fiber's shared Redis connection pool and `ctx.fire_channel_read()` to fulfill hits with zero upstream token cost:

```python
import json, hashlib, uuid
from fiber.gateway.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.node.space.tunnel.factory import TunnelFactory

class EdgeRedisCache(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        messages = msg.get("messages", [])
        prompt_hash = hashlib.sha256(json.dumps(messages, sort_keys=True).encode("utf-8")).hexdigest()

        tunnel = await TunnelFactory.get_default()
        if cached_text := await tunnel.get(prompt_hash):
            fulfillment = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "edge-cache-tier"),
                choices=[{
                    "index": 0,
                    "message": {"role": "assistant", "content": cached_text},
                    "finish_reason": "stop",
                }],
                usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
            )
            # Short-circuit upstream I/O and return immediately up the pipeline
            return await ctx.fire_channel_read(fulfillment)

        await ctx.fire_write(msg)

```

#### Pattern B: Non-Blocking Semantic Router (`asyncio.to_thread` + Metadata Sync)

When dynamically mutating `msg["model"]` at `PRE_TRANSLATE`, **always synchronize `meta.base_model**` so downstream tracers and FinOps observers attribute costs to the newly routed model:

```python
import asyncio
from fiber.gateway.llm.pipeline.bootstrap import PipelineSlot
from xphi.state.phase.channel import DuplexChannel, ChannelContext

class CustomSemanticRouter(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    def __init__(self, routing_rules: dict, tool_model: str, fallback_model: str):
        super().__init__()
        self.routing_rules = routing_rules  # e.g., {"Transform": {"target": "ollama/gemma:2b", "threshold": 0.6}}
        self.tool_model = tool_model
        self.fallback_model = fallback_model

    def _apply_routing(self, ctx: ChannelContext, payload: dict, target_model: str):
        """Updates both physical request payload and logical pipeline metadata."""
        payload["model"] = target_model
        meta = payload.get("system_meta") or ctx.get_attr("system_meta")
        if meta:
            meta.base_model = target_model

    def _classify_sync(self, text: str) -> str:
        # Replace with CPU-bound ONNX session.run() (see fiber.dev.trace.llm.router.OnnxSemanticRouter)
        return "Transform" if "json" in text.lower() else "Complex_Reasoning"

    async def write(self, ctx: ChannelContext, payload: dict):
        # Route tool-calling requests directly to a tool-capable model
        if payload.get("tools"):
            self._apply_routing(ctx, payload, self.tool_model)
            return await ctx.fire_write(payload)

        # Offload CPU-bound classification to a worker thread to keep the event loop non-blocking
        prompt_text = " ".join(
            m.get("content", "") for m in payload.get("messages", []) if isinstance(m.get("content"), str)
        )
        intent = await asyncio.to_thread(self._classify_sync, prompt_text)

        rule = self.routing_rules.get(intent)
        target = rule["target"] if rule else self.fallback_model
        self._apply_routing(ctx, payload, target)

        return await ctx.fire_write(payload)
```

---

### 2.3. `PipelineSlot.POST_TRANSLATE` — Security & DLP Guardrails

At `POST_TRANSLATE`, the raw dictionary has been normalized into a `processed_msg` object carrying `processed_msg.original_kwargs`. Fiber provides built-in C-level redaction utilities (`xphi.arch.xor`) so you can choose between **Pass-Through Masking** (`PRE_TRANSLATE`) and **Strict Circuit-Breaking** (`POST_TRANSLATE`).

```python
from typing import Any, List
from fiber.gateway.llm.pipeline.bootstrap import PipelineSlot
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.arch.xor.secret.redact import redact_string, sanitize_payload
from xphi.arch.xor.parser.ruleset.engine import FastRegexRedactionEngine

# Option 1: Non-Destructive Deep Masking (PRE_TRANSLATE)
class PayloadSanitizationGuardrail(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        # Recursively masks secrets across messages, headers, and metadata without halting execution
        sanitized_msg = sanitize_payload(msg)
        await ctx.fire_write(sanitized_msg)

# Option 2: Strict Blocking via Built-in Secret Patterns (POST_TRANSLATE)
class ActiveBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        messages = getattr(processed_msg, "original_kwargs", {}).get("messages", [])
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str) and redact_string(content) != content:
                return await ctx.fire_exception_caught(
                    PermissionError("Guardrail Triggered: Sensitive credentials detected and blocked.")
                )
        await ctx.fire_write(processed_msg)

# Option 3: High-Speed Custom Byte-Regex Policy Blocking (POST_TRANSLATE)
class CustomRegexBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE

    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        messages = getattr(processed_msg, "original_kwargs", {}).get("messages", [])
        for msg in messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                redacted_bytes = self.engine.execute(content.encode("utf-8"))
                if self.engine.mask_token in redacted_bytes:
                    return await ctx.fire_exception_caught(
                        PermissionError("Guardrail Triggered: Custom PII Policy Violation.")
                    )
        await ctx.fire_write(processed_msg)
```

---

## 3. Mounting Interceptors (SDK vs. REST Gateway)

### 3.1. In-Process Python SDK (`acompletion` / `completion`)

Pass instantiated objects in any order via `interceptors=[...]`. Fiber groups them by `target_slot` and preserves list order within the same slot:

```python
from fiber.llm.entry import acompletion
from fiber.dev.trace.llm.cache import EdgeRedisCache
from fiber.dev.trace.llm.guardrail import ActiveBlockingGuardrail, CustomRegexBlockingGuardrail
from fiber.dev.trace.llm.router import OnnxSemanticRouter

router = OnnxSemanticRouter(
    routing_rules={"Transform": {"target": "ollama/gemma:2b", "threshold": 0.6}},
    tool_model="gemini/gemini-3.1-flash-lite",
    fallback_model="cerebras/qwen-3.8-27b",
    model_path="enterprise_router_quantized_int8.onnx",
)

response = await acompletion(
    model="auto",
    messages=[{"role": "user", "content": "Format this contact list into JSON."}],
    interceptors=[
        StreamAwareTelemetryTracer(),
        EdgeRedisCache(),
        router,
        ActiveBlockingGuardrail(),
        CustomRegexBlockingGuardrail(custom_patterns=[r"\b\d{3}-\d{2}-\d{4}\b"]),
    ],
)
```

### 3.2. Per-Request Injection via REST Gateway (`/v1/chat/completions`)

When running `fiber daemon`, external HTTP clients can dynamically attach registered interceptors from `fiber.dev.trace.llm` per request by passing the `interceptors` array in the JSON payload.
*(Note: Client-supplied interceptors are additive and cannot override or disable global security interceptors mounted at daemon boot time.)*

```bash
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini/gemini-3.1-flash-lite",
    "messages": [{"role": "user", "content": "Analyze customer feedback."}],
    "interceptors": [
      {"class": "cache.EdgeRedisCache", "kwargs": {}},
      {"class": "guardrail.CustomRegexBlockingGuardrail", "kwargs": {"custom_patterns": ["CONFIDENTIAL-[0-9]+"]}}
    ]
  }'

```

---

## 4. Reference Summary: Built-in Production Interceptors (`fiber.dev.trace.llm`)

| Module & Class | Target Slot | Core Capability | Constructor Kwargs |
| --- | --- | --- | --- |
| `cache.EdgeRedisCache` | `PRE_TRANSLATE` | SHA-256 exact-match Redis caching via `TunnelFactory`; zero-cost `ModelResponse` short-circuit. | None |
| `router.OnnxSemanticRouter` | `PRE_TRANSLATE` | Non-blocking `CPUExecutionProvider` INT8 ONNX classification with tool-call & fallback routing. | `routing_rules`, `tool_model`, `fallback_model`, `model_path`, `labels` |
| `guardrail.PayloadSanitizationGuardrail` | `PRE_TRANSLATE` | Recursive in-place masking (`sanitize_payload`) across raw dicts prior to translation. | None |
| `guardrail.ActiveBlockingGuardrail` | `POST_TRANSLATE` | Built-in credential/API key detection (`redact_string`) with hard pipeline abort. | None |
| `guardrail.CustomRegexBlockingGuardrail` | `POST_TRANSLATE` | C-level byte regex evaluation (`FastRegexRedactionEngine`) for custom PII/domain rules. | `custom_patterns: List[str]` |