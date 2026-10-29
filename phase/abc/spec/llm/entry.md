# fiber.phase.abc.spec.llm.entry
@desc: LLM Pipeline Entrypoint & REST Gateway Guide

## 1. Overview

`fiber.llm.entry` provides a drop-in replacement for standard OpenAI and LiteLLM SDKs (`completion` and `acompletion`). It preserves 100% interface and type-checking parity with existing application logic while unlocking Fiber's asynchronous `DuplexChannel` pipeline, mid-stream kill-switches (`StreamBreaker`), and deterministic offline fixture testing (VCR).

You can execute calls in two modes without changing your business logic:
1. **In-Process Pipeline Mode (Default):** Runs the full middleware, schema translation, and governance pipeline directly inside your Python process.
2. **Remote Gateway Forwarding (`api_base`):** Forwards requests over HTTP/SSE to a running `fiber daemon` (`/v1/chat/completions`), offloading API key management and pipeline execution to the control plane.

---

## 2. Quick Start & Core API

### 2.1. Asynchronous & Synchronous Execution

Use `acompletion` for native `asyncio` applications and `completion` for synchronous scripts. If `completion` is called inside an already running event loop (e.g., Jupyter notebooks or async frameworks), Fiber automatically applies `nest_asyncio` when available.

```python
from fiber.llm.entry import completion, acompletion

# 1. Asynchronous Unary Call (Provider prefix routing: gemini/, cohere/, cerebras/, ollama/, lm_studio/)
response = await acompletion(
    model="gemini/gemini-3.1-flash-lite",
    messages=[{"role": "user", "content": "Summarize key takeaways in 3 bullets."}],
    temperature=0.3,
)
print(response.choices[0].message.content)

# 2. Synchronous Call
sync_res = completion(
    model="ollama/gemma:2b",
    messages=[{"role": "user", "content": "Hello!"}],
)
```

### 2.2. Streaming (`stream=True`)

When `stream=True` is passed, Fiber returns a `StreamWrapper` yielding OpenAI/LiteLLM-compatible `ModelResponseStream` chunks.

```python
stream = await acompletion(
    model="cerebras/qwen-3.8-27b",
    messages=[{"role": "user", "content": "Write a quick Python sort function."}],
    stream=True,
)

async for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

### 2.3. Remote Gateway Forwarding (`api_base`)

Passing `api_base` bypasses the local in-process pipeline and streams directly from a local or remote `fiber daemon` instance. Client-side provider API keys are not required, as the daemon resolves credentials from its host environment.

```python
# Routes directly to http://127.0.0.1:8356/v1/chat/completions
response = await acompletion(
    model="cohere/command-r-08-2024",
    messages=[{"role": "user", "content": "Explain blue-green deployments."}],
    api_base="http://127.0.0.1:8356/v1",
    stream=True,
)
```

---

## 3. Return Types & Schema Compatibility

All responses map to OpenAI-compatible Pydantic models defined in `fiber.llm.response`, ensuring legacy type checks (`isinstance`) and dot-notation access remain intact.

| Return Object | Trigger Condition | Compatibility Mapping (OpenAI / LiteLLM) | Key Attributes |
| --- | --- | --- | --- |
| `ModelResponse` | `stream=False` (Default) | `openai.types.chat.ChatCompletion` | `choices[0].message.content`, `choices[0].message.tool_calls`, `usage` |
| `StreamWrapper` | `stream=True` | `openai.AsyncStream` / `ModelResponseStream` | Async iterator yielding `choices[0].delta.content` |
| `Message` | Nested in `choices` | `openai.types.chat.ChatCompletionMessage` | `role`, `content`, `tool_calls`, `function_call` |
| `Usage` | Nested in `ModelResponse` | `openai.types.CompletionUsage` | `prompt_tokens`, `completion_tokens`, `total_tokens` |

### Automatic Tool-Call & Schema Normalization

Different providers return text and function calls in incompatible structures (e.g., Gemini's `candidates.0.content.parts.0.function_call` vs. Ollama's `prompt_eval_count`). Fiber's **State Traverser** automatically normalizes these into standard OpenAI `choices[0].message.tool_calls` and `usage` fields before returning the object. Custom provider rules can be hot-swapped via `FIBER_COMPAT_RULES_PATH`.

---

## 4. Pipeline Interceptors & Governance Kwargs

Beyond standard LLM parameters (`temperature`, `max_tokens`, `tools`), `acompletion` accepts extension arguments to attach custom middleware, FinOps breakers, and VCR controls.

### 4.1. Injecting Custom Middleware (`interceptors`)

Pass a flat list of `DuplexChannel` middlewares or `BaseLLMTracer` observers via `interceptors=[...]`. Fiber automatically sorts and mounts each interceptor into its designated `PipelineSlot`:

* `PipelineSlot.PRE_TRANSLATE`: Cache short-circuiting and dynamic model routing before schema translation.
* `PipelineSlot.POST_TRANSLATE`: Secret redaction and active payload blocking prior to network I/O.
* `PipelineSlot.PRE_OBSERVER`: Asynchronous telemetry and stream-completion hooks (default slot for `BaseLLMTracer`).

```python
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.tracer import BaseLLMTracer
from fiber.llm.pipeline.bootstrap import PipelineSlot
from xphi.state.phase.channel import DuplexChannel

class SecretBlocker(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    async def write(self, ctx, processed_msg):
        for m in getattr(processed_msg, "original_kwargs", {}).get("messages", []):
            if "PRIVATE_KEY" in str(m.get("content", "")):
                raise PermissionError("Blocked: Sensitive key detected in prompt.")
        await ctx.fire_write(processed_msg)

class LatencyLogger(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        print(f"[{meta.base_model}] Completed in {duration_ms:.1f}ms")

response = await acompletion(
    model="lm_studio/qwen-3.5-9b",
    messages=[{"role": "user", "content": "Hello"}],
    interceptors=[SecretBlocker(), LatencyLogger()],
)
```

### 4.2. Mid-Stream Kill-Switch (`stream_breaker`)

To enforce strict tenant quotas and terminate runaway streams mid-flight upon budget exhaustion or `fiber account kill` revocation, attach `StreamBreaker`, `MeteringObserver`, and `NodeAuthPayload`:

```python
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.channel.breaker import StreamBreaker
from fiber.llm.pipeline.channel.observer import MeteringObserver
from fiber.infra.watcher.finops import default_finops_watcher, default_tenant_eco
from xphi.arch.model.auth import NodeAuthPayload

response = await acompletion(
    model="gemini/gemini-3.1-flash-lite",
    messages=[{"role": "user", "content": "Generate a comprehensive report."}],
    stream=True,
    stream_breaker=StreamBreaker(finops_watcher=default_finops_watcher),
    metering_observer=MeteringObserver(tenant_eco=default_tenant_eco, pta_adapter=pta_adapter),
    metadata={
        "node_auth": NodeAuthPayload(
            tenant_id="accounting_dept",
            fuel_budget=50000,
            is_enforced=True,
        )
    },
)
```

### 4.3. Offline Fixture Recording & Playback (`metadata` VCR Controls)

When running inside a `VCRInjector.use_cassette(...)` context or via legacy SDK aliasing (`vcr_setup_sequence()`), pass VCR routing hints inside `metadata`:

```python
response = await acompletion(
    model="cohere/command-r-08-2024",
    messages=[{"role": "user", "content": "Format this to JSON."}],
    metadata={
        "vcr_scenario": "json_formatting_test",
        "vcr_filename": "fixture_json_format.json",  # Optional explicit filename
        "vcr_invoker": "pytest_suite",
    },
)

```

### 4.4. Resilience & Testing Helpers (`fallbacks`, `mock_response`)

* **Model Fallbacks:** Automatically retries across alternative models or parameter sets upon upstream rate limits or API errors:
```python
response = await acompletion(
    model="gemini/gemini-3.1-flash-lite",
    messages=[...],
    fallbacks=["cerebras/qwen-3.8-27b", {"model": "ollama/gemma:2b", "temperature": 0.2}],
)

```


* **Inline Mocking:** Short-circuits network I/O for quick unit tests without recording fixtures:
```python
response = await acompletion(
    model="any/model",
    messages=[...],
    mock_response="Deterministic mock reply",
    mock_delay=0.5,  # Optional latency simulation in seconds
)

```

---

## 5. REST Gateway Usage (`/v1/chat/completions`)

Running `fiber daemon` exposes the pipeline over an OpenAI-compatible HTTP endpoint (`http://127.0.0.1:8356/v1/chat/completions`). Any language or IDE (e.g., Cursor, `continue.dev`) can invoke it using standard OpenAI client libraries or `curl`.

### 5.1. Gateway Extension Payload Parameters

In addition to standard OpenAI fields (`model`, `messages`, `stream`, `temperature`, `max_tokens`, `tools`), the REST endpoint accepts top-level extension fields for per-request VCR and interceptor control:

| JSON Field | Type | Description |
| --- | --- | --- |
| `interceptors` | `List[Object]` | Additive per-request interceptors: `[{"class": "cache.EdgeRedisCache", "kwargs": {}}]` *(Cannot override boot-time global security interceptors)* |
| `vcr` | `string` | Per-request VCR mode override: `"live"` | `"record"` | `"replay"` |
| `vcr_scenario` | `string` | Scenario identifier used to compute deterministic `trace_id` and fixture filename |
| `vcr_filename` | `string` | Explicit JSON fixture filename override |
| `vcr_speed` | `string` | Playback pacing during `"replay"`: `"max"` (~0ms I/O) | `"real"` (historical latency) |
| `vcr_chaos` | `float` | Injects artificial chaos jitter in milliseconds during `"replay"` (e.g., `500.0`) |
| `vcr_tick` | `float` | Stream micro-chunk coalescing window in milliseconds during `"record"` (default: `100.0`) |

### 5.2. HTTP Gateway Examples (`curl`)

```bash
# 1. Standard Streaming Call (SSE)
$ curl -N -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "cerebras/qwen-3.8-27b",
    "messages": [{"role": "user", "content": "Explain event loops in 2 sentences."}],
    "stream": true
  }'

# 2. Per-Request VCR Record -> Freezes live response into disk fixture
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
    "messages": [{"role": "user", "content": "Give me a 3-word motto."}],
    "vcr": "record",
    "vcr_scenario": "gateway_smoke_test"
  }'

# 3. Per-Request VCR Replay -> Replays deterministically from disk (~3ms, zero upstream cost)
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
    "messages": [{"role": "user", "content": "Give me a 3-word motto."}],
    "vcr": "replay",
    "vcr_scenario": "gateway_smoke_test",
    "vcr_speed": "max"
  }'
```