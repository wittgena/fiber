# fiber.phase.abc.spec.llm.interceptor
`2.3. Two-Track Guardrails & Declarative Secret Registry` 하단의 **`Declarative Secret Registry` 섹션**에 내장 기본 룰(`BASE_SECRET_RULES` 33종 및 `SECRET_KEY_PATTERNS` 14종)이 기본 탑재되어 있다는 설명과 함께, 사용자가 `disabled_rules`나 `patterns` 오버라이드 시 바로 참조할 수 있도록 **카테고리별 기본 Rule ID 요약 블록**을 간결하게 추가했습니다.

---

### 1. 해당 섹션 추가/수정 부분 미리보기 (`2.3` 하단)

```markdown
#### Declarative Secret Registry (`fiber.llm.compat.secret.registry`)

Out of the box, `fiber.llm.compat.secret.redact` ships with **33 built-in credential regex rules (`BASE_SECRET_RULES`)** and **14 sensitive key/URL tokens (`SECRET_KEY_PATTERNS`)**—covering major cloud providers, auth headers, and LLM vendor keys with zero configuration:

- **Cloud & Infra (`7`):** `aws_access_key_id`, `aws_kv_credential`, `gcp_api_key`, `gcp_oauth_token`, `gcp_service_account_json`, `azure_kv_secret`, `databricks_token`
- **Auth & Private Keys (`6`):** `bearer_token`, `basic_auth`, `uri_basic_auth`, `jwt_token`, `pem_private_key`, `inline_private_key_kv`
- **LLM & AI Vendors (`9`):** `llm_vendor_key` *(OpenAI/Anthropic/OpenRouter)*, `openai_legacy_key`, `openhands_key`, `groq_key`, `huggingface_token`, `together_ai_key`, `context7_key`, `claude_web_key`, `tavily_api_key`
- **SaaS & Generic KV (`11`):** `github_token`, `stripe_key`, `slack_webhook`, `slack_token`, `sentry_token`, `linear_api_key`, `jira_token`, `generic_api_key_kv`, `generic_password_kv`, `generic_config_secret_kv`, `url_query_secret`
- **Sensitive Keys (`14`):** `AUTHORIZATION`, `CERTIFICATE`, `COOKIE`, `CREDENTIAL`, `JWT`, `KEY`, `OAUTH`, `PASSPHRASE`, `PASSWORD`, `PRIVATE`, `SECRET`, `SESSION`, `SIGNATURE`, `TOKEN`

To add custom rules, override existing patterns, or disable specific built-in checks globally without modifying Python code, provide a `redact_rules.json` file in your registry directory (or set `FIBER_REDACT_RULES_PATH` / call `RedactRegistryIO.initialize_and_inject(custom_path)`).

```

---

### 2. 반영된 전체 스펙 문서 (`fiber.phase.abc.spec.llm.interceptor`)

(본문 내 `**mutable raw `dict`**`, `**always synchronize `meta.base_model`**`의 마크다운 볼드체 기호 위치 오타도 함께 바로잡았습니다.)

```markdown
# fiber.phase.abc.spec.llm.interceptor
@desc: LLM Pipeline Interceptor & Guardrail Extension Guide

This guide defines the interface contracts, slot lifecycle, and declarative security registries for building and injecting custom interceptors—such as edge caches, semantic routers, DLP guardrails, and telemetry tracers—into the Fiber LLM pipeline (`fiber.llm.entry` and `/v1/chat/completions`).

---

## 1. Pipeline Topology & Slot Execution Order

Developers pass interceptors as a flat list (`interceptors=[...]`), and Fiber autonomously sorts and mounts them into three deterministic execution zones based on each class's `target_slot` attribute:

```text
[Caller] ➔ acompletion(..., interceptors=[Cache(), Router(), MaskGuard(), BlockGuard(), Tracer()])
       │
       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ OBSERVABILITY & IDENTITY (PipelineSlot.PRE_OBSERVER)                         │
│  ├─ Core Metadata Binding : Allocates trace_id, session_id, and system_meta  │
│  └─ BaseLLMTracer         : Non-blocking start/end/error & stream hooks      │
├──────────────────────────────────────────────────────────────────────────────┤
│ MUTATION & SHORT-CIRCUIT (PipelineSlot.PRE_TRANSLATE)                        │
│  ├─ PayloadSanitization   : Deep dictionary/header/URL secret masking        │
│  ├─ CustomRegexMasking    : In-place C-regex PII masking (Mutate & Pass)     │
│  ├─ EdgeRedisCache        : Exact/Vector lookup -> ctx.fire_channel_read()   │
│  └─ OnnxSemanticRouter    : Intent classification -> mutates msg["model"]    │
├──────────────────────────────────────────────────────────────────────────────┤
│ CORE TRANSLATION (State Traverser: Raw dict ➔ Processor Object)              │
├──────────────────────────────────────────────────────────────────────────────┤
│ EGRESS SECURITY & GOVERNANCE (PipelineSlot.POST_TRANSLATE)                   │
│  ├─ Active / Custom Block : C-regex secret scan -> ctx.fire_exception_caught │
│  ├─ Core StreamBreaker    : Real-time quota check & mid-stream circuit break │
│  └─ Core Network I/O      : Physical HTTP/SSE transmission to LLM provider   │
└──────────────────────────────────────────────────────────────────────────────┘

```

### ChannelContext Control Primitives

Every `DuplexChannel` interceptor receives `(self, ctx: ChannelContext, msg: Any)` in its `async def write(...)` method and controls pipeline flow using three primitives:

| Primitive | Effect on Pipeline | Primary Use Case |
| --- | --- | --- |
| `await ctx.fire_write(msg)` | Forwards the (optionally mutated) payload downstream toward the network transport. | Cache misses, model routing, in-place PII masking (`CustomRegexMaskingGuardrail`). |
| `return await ctx.fire_channel_read(ModelResponse(...))` | **Short-circuits physical I/O.** Sends a synthesized response directly back up the pipeline to the caller. | Edge caching (`EdgeRedisCache`), custom mock fulfillment. |
| `return await ctx.fire_exception_caught(PermissionError(...))` | **Aborts execution immediately.** Propagates the error safely through `on_llm_error` tracers before raising to the caller. | Secret/policy blocking (`ActiveBlockingGuardrail`, `CustomRegexBlockingGuardrail`). |

---

## 2. Building Interceptors by Target Slot

### 2.1. `PipelineSlot.PRE_OBSERVER` — Stream-Aware Telemetry (`BaseLLMTracer`)

Subclassing `BaseLLMTracer` automatically binds your plugin to `PipelineSlot.PRE_OBSERVER`. For streaming calls (`stream=True`), append a callback to `meta.framework_flags["on_stream_complete"]` to capture true end-to-end latency and token usage after the last chunk is consumed:

```python
import time
from typing import Any
from fiber.llm.pipeline.tracer import BaseLLMTracer
from fiber.llm.execution import ExecutionMetadata
from xphi.watcher.plane.emitter import get_emitter

trace_log = get_emitter("custom.tracer")

class StreamAwareTelemetryTracer(BaseLLMTracer):
    async def on_llm_end(self, meta: ExecutionMetadata, response: Any, duration_ms: float):
        if hasattr(response, "__aiter__"):
            def on_stream_complete(total_tokens: int):
                stream_ms = (time.time() - meta.framework_flags.get("start_time", time.time())) * 1000
                trace_log.info(f"[Stream End] model={meta.base_model} | {stream_ms:.1f}ms | tokens={total_tokens}")

            meta.framework_flags.setdefault("on_stream_complete", []).append(on_stream_complete)
            return

        tokens = getattr(getattr(response, "usage", None), "total_tokens", 0)
        trace_log.info(f"[Unary End] model={meta.base_model} | {duration_ms:.1f}ms | tokens={tokens}")

```

---

### 2.2. `PipelineSlot.PRE_TRANSLATE` — Edge Caching & Dynamic Routing

At `PRE_TRANSLATE`, `msg` is a **mutable raw `dict**` (`{"model": ..., "messages": [...], ...}`).

#### Pattern A: Short-Circuit Edge Cache (`TunnelFactory` + `fire_channel_read`)

```python
import json, hashlib, uuid
from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.node.space.tunnel.factory import TunnelFactory

class EdgeRedisCache(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    async def write(self, ctx: ChannelContext, msg: dict):
        prompt_hash = hashlib.sha256(json.dumps(msg.get("messages", []), sort_keys=True).encode()).hexdigest()
        tunnel = await TunnelFactory.get_default()

        if cached_text := await tunnel.get(prompt_hash):
            return await ctx.fire_channel_read(ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "edge-cache-tier"),
                choices=[{"index": 0, "message": {"role": "assistant", "content": cached_text}, "finish_reason": "stop"}],
                usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
            ))
        await ctx.fire_write(msg)

```

#### Pattern B: Non-Blocking Semantic Router (`asyncio.to_thread` + Metadata Sync)

When mutating `msg["model"]` at `PRE_TRANSLATE`, **always synchronize `meta.base_model**` so downstream tracers and observers track the routed model:

```python
import asyncio
from fiber.llm.pipeline.bootstrap import PipelineSlot
from xphi.state.phase.channel import DuplexChannel, ChannelContext

class CustomSemanticRouter(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE

    def __init__(self, routing_rules: dict, fallback_model: str):
        super().__init__()
        self.routing_rules, self.fallback_model = routing_rules, fallback_model

    def _apply_routing(self, ctx: ChannelContext, payload: dict, target_model: str):
        payload["model"] = target_model
        if meta := (payload.get("system_meta") or ctx.get_attr("system_meta")):
            meta.base_model = target_model

    async def write(self, ctx: ChannelContext, payload: dict):
        prompt = " ".join(m.get("content", "") for m in payload.get("messages", []) if isinstance(m.get("content"), str))
        intent = await asyncio.to_thread(lambda t: "Transform" if "json" in t.lower() else "General", prompt)

        target = self.routing_rules.get(intent, {}).get("target", self.fallback_model)
        self._apply_routing(ctx, payload, target)
        await ctx.fire_write(payload)

```

---

### 2.3. Two-Track Guardrails & Declarative Secret Registry

Fiber decouples **Non-Blocking Sanitization (`PRE_TRANSLATE`)** from **Strict Circuit-Breaking (`POST_TRANSLATE`)**. This allows you to scrub soft PII (e.g., phone numbers) in-place before edge caching while still hard-blocking leaked infrastructure credentials (`sk-...`, `AKIA...`) prior to network egress.

#### Guardrail Interceptors (`fiber.llm.pipeline.interceptor.guardrail`)

```python
from typing import Any, List
from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.compat.secret.redact import redact_string, sanitize_payload, FastRegexRedactionEngine
from xphi.state.phase.channel import DuplexChannel, ChannelContext

# [Track 1A - PRE_TRANSLATE] Built-in Recursive Dict/Header/URL Sanitization (Mutate & Pass)
class PayloadSanitizationGuardrail(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    async def write(self, ctx: ChannelContext, msg: dict):
        await ctx.fire_write(sanitize_payload(msg))

# [Track 1B - PRE_TRANSLATE] Custom C-Regex In-Place PII Masking (Mutate & Pass)
class CustomRegexMaskingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx: ChannelContext, msg: dict):
        for m in msg.get("messages", []):
            if isinstance(m, dict) and isinstance(m.get("content"), str):
                m["content"] = self.engine.execute(m["content"].encode("utf-8")).decode("utf-8", errors="replace")
        await ctx.fire_write(msg)

# [Track 2A - POST_TRANSLATE] Built-in + Registry Credential Blocking (Short-Circuit)
class ActiveBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    async def write(self, ctx: ChannelContext, processed_msg: Any):
        for m in getattr(processed_msg, "original_kwargs", {}).get("messages", []):
            content = m.get("content", "")
            if isinstance(content, str) and redact_string(content) != content:
                return await ctx.fire_exception_caught(PermissionError("Blocked: Sensitive credential detected."))
        await ctx.fire_write(processed_msg)

# [Track 2B - POST_TRANSLATE] Custom C-Regex Policy Blocking (Zero-Copy Short-Circuit)
class CustomRegexBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    def __init__(self, custom_patterns: List[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx: ChannelContext, processed_msg: Any):
        for m in getattr(processed_msg, "original_kwargs", {}).get("messages", []):
            content = m.get("content", "")
            if isinstance(content, str) and self.engine.contains_match(content.encode("utf-8")):
                return await ctx.fire_exception_caught(PermissionError("Blocked: Custom policy violation."))
        await ctx.fire_write(processed_msg)

```

#### Declarative Secret Registry (`fiber.llm.compat.secret.registry`)

Out of the box, `fiber.llm.compat.secret.redact` ships with **33 built-in credential regex rules (`BASE_SECRET_RULES`)** and **14 sensitive key/URL tokens (`SECRET_KEY_PATTERNS`)**—covering major cloud providers, auth headers, and LLM vendor keys with zero configuration:

* **Cloud & Infra (`7`):** `aws_access_key_id`, `aws_kv_credential`, `gcp_api_key`, `gcp_oauth_token`, `gcp_service_account_json`, `azure_kv_secret`, `databricks_token`
* **Auth & Private Keys (`6`):** `bearer_token`, `basic_auth`, `uri_basic_auth`, `jwt_token`, `pem_private_key`, `inline_private_key_kv`
* **LLM & AI Vendors (`9`):** `llm_vendor_key` *(OpenAI/Anthropic/OpenRouter)*, `openai_legacy_key`, `openhands_key`, `groq_key`, `huggingface_token`, `together_ai_key`, `context7_key`, `claude_web_key`, `tavily_api_key`
* **SaaS & Generic KV (`11`):** `github_token`, `stripe_key`, `slack_webhook`, `slack_token`, `sentry_token`, `linear_api_key`, `jira_token`, `generic_api_key_kv`, `generic_password_kv`, `generic_config_secret_kv`, `url_query_secret`
* **Sensitive Keys (`14`):** `AUTHORIZATION`, `CERTIFICATE`, `COOKIE`, `CREDENTIAL`, `JWT`, `KEY`, `OAUTH`, `PASSPHRASE`, `PASSWORD`, `PRIVATE`, `SECRET`, `SESSION`, `SIGNATURE`, `TOKEN`

To add custom rules, override existing patterns, or disable specific built-in checks globally without modifying Python code, provide a `redact_rules.json` file in your registry directory (or set `FIBER_REDACT_RULES_PATH` / call `RedactRegistryIO.initialize_and_inject(custom_path)`).

At boot time, `RedactRegistryIO` validates each regex entry via Pydantic (safely skipping malformed expressions with a warning), removes any `disabled_rules` by exact rule ID, and injects the merged Key-Value ruleset into `fiber.llm.compat.secret.redact` via Dependency Inversion (`configure_redaction`)—automatically upgrading `ActiveBlockingGuardrail`, `PayloadSanitizationGuardrail`, and `FastRegexRedactionEngine`:

```json
{
  "patterns": {
    "fiber_internal_token": "FIBER-INTERNAL-TOKEN-[0-9]{6}",
    "corp_vault_key": "CORP-VAULT-[A-Z0-9]{16}",
    "slack_token": "xox[baprs]-[A-Za-z0-9_-]{24,}"
  },
  "disabled_rules": [
    "context7_key",
    "generic_password_kv"
  ],
  "extra_secret_keys": [
    "FIBER_VAULT_PIN",
    "INTERNAL_AUTH_HEADER"
  ]
}

```

* **`patterns` (`Dict[str, str]`):** Adds new custom regex rules or overrides existing built-in rules by matching their rule ID (e.g., `"slack_token"`).
* **`disabled_rules` (`List[str]`):** Disables specific built-in rules by exact `rule_id` from `BASE_SECRET_RULES` without risking collateral substring deletion.
* **`extra_secret_keys` (`List[str]`):** Adds case-insensitive substring tokens (minimum 3 characters) used to scrub sensitive dictionary keys and URL query parameters (`?key=value`) across `sanitize_payload`.

---

## 3. Mounting Interceptors (SDK vs. REST Gateway)

### 3.1. Chained Defense in Python SDK (`acompletion` / `completion`)

Pass instantiated interceptors in any order. Below, soft PII (`010-xxxx-xxxx`) is masked in-place at `PRE_TRANSLATE` before hitting `EdgeRedisCache`, while hard credentials (`sk-...` or registry-injected tokens) are strictly blocked at `POST_TRANSLATE`:

```python
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.interceptor.cache import EdgeRedisCache
from fiber.llm.pipeline.interceptor.guardrail import (
    ActiveBlockingGuardrail,
    CustomRegexMaskingGuardrail,
)

response = await acompletion(
    model="gemini/gemini-3.1-flash-lite",
    messages=[{"role": "user", "content": "Customer phone: 010-9999-8888. Summarize inquiry."}],
    interceptors=[
        StreamAwareTelemetryTracer(),
        CustomRegexMaskingGuardrail(custom_patterns=[r"\b010-\d{4}-\d{4}\b"]),  # PRE_TRANSLATE (Masks phone)
        EdgeRedisCache(),                                                        # PRE_TRANSLATE (Caches masked prompt)
        ActiveBlockingGuardrail(),                                               # POST_TRANSLATE (Blocks leaked keys)
    ],
)

```

### 3.2. Per-Request Injection via REST Gateway (`/v1/chat/completions`)

When running `fiber daemon`, external HTTP clients can dynamically attach registered interceptors from `fiber.llm.pipeline.interceptor` per request via the JSON payload:

```bash
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini/gemini-3.1-flash-lite",
    "messages": [{"role": "user", "content": "Contact 010-1234-5678 regarding CONFIDENTIAL-99."}],
    "interceptors": [
      {"class": "guardrail.CustomRegexMaskingGuardrail", "kwargs": {"custom_patterns": ["\\b010-\\d{4}-\\d{4}\\b"]}},
      {"class": "guardrail.CustomRegexBlockingGuardrail", "kwargs": {"custom_patterns": ["CONFIDENTIAL-[0-9]+"]}}
    ]
  }'
```

---

## 4. Reference Summary: Built-in Production Interceptors (`fiber.llm.pipeline.interceptor`)

| Module & Class | Target Slot | Core Capability | Constructor Kwargs |
| --- | --- | --- | --- |
| `cache.EdgeRedisCache` | `PRE_TRANSLATE` | SHA-256 exact-match Redis caching via `TunnelFactory`; zero-cost `ModelResponse` short-circuit. | None |
| `router.OnnxSemanticRouter` | `PRE_TRANSLATE` | Non-blocking `CPUExecutionProvider` INT8 ONNX classification with tool-call & fallback routing. | `routing_rules`, `tool_model`, `fallback_model`, `model_path`, `labels` |
| `guardrail.PayloadSanitizationGuardrail` | `PRE_TRANSLATE` | Recursive in-place masking (`sanitize_payload`) across raw dicts, headers, and URLs prior to translation. | None |
| `guardrail.CustomRegexMaskingGuardrail` | `PRE_TRANSLATE` | C-level byte regex in-place PII masking (`FastRegexRedactionEngine.execute`) without halting execution. | `custom_patterns: List[str]` |
| `guardrail.ActiveBlockingGuardrail` | `POST_TRANSLATE` | Built-in + `RedactRegistry` credential detection (`redact_string`) with hard pipeline abort. | None |
| `guardrail.CustomRegexBlockingGuardrail` | `POST_TRANSLATE` | Zero-copy C-level byte regex evaluation (`FastRegexRedactionEngine.contains_match`) for custom blocking policies. | `custom_patterns: List[str]` |
```