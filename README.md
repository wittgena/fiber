# README
**AI Runtime Harness**

Fiber is a local runtime harness that sits between AI agents and everything they call—tools and models alike. A single loopback daemon (127.0.0.1:8356) fronts your MCP servers as a Local MCP Gateway with idempotent execution and pre-flight human approval (**1**), unified under a zero-boilerplate CLI for config sync and quota governance (**3.1**). 
Through the same control plane, it governs model calls via an OpenAI-compatible endpoint and a drop-in Python SDK with mid-stream kill-switches (**2.1**), captures live streams into content-addressed fixtures for zero-cost deterministic replay (**2.2**), and normalizes heterogeneous LLM schemas declaratively (**2.3**)—achieving execution determinism, runtime safety, and quota governance without changing your agent configs or business logic.

| Section | Target Focus & Key Capabilities | Quick Jump |
| :--- | :--- | :--- |
| **1. Local MCP Gateway** | Loopback Daemon for Agents & IDEs, Pre-Flight HITL Locks, Zero-Key LLM Routing, Config Sync | **[1. Local MCP Gateway](#1-local-mcp-gateway)** |
| **2. LLM Guardrails & Replay** | Drop-in SDK, Mid-Stream Kill-Switch, Offline Record/Replay, Schema Traverser | **[2.1 LLM Guardrails](#21-llm-guardrails)** · **[2.2 VCR](#22-deterministic-replay-llm-vcr--fixtures)** · **[2.3 Schema Traverser](#23-schema-traverser--compat-registry)** |
| **3. CLI & Installation** | Connect CLI, Account & Quota Governance, Sandbox Provisioning (`uv pip`), Node Identity (`fiber init`) & Homebrew | **[3.1 Connect CLI](#31-fiber-connect-cli)** · **[3.2 Python & CLI](#32-python-environment-via-uv-pip-recommended-for-sdk--local-control-plane)** · **[3.3 Homebrew](#33-macos-homebrew-tap-for-local-daemon--mcp-gateway)** |
| **4. Planes & Benchmarks** | Multi-Plane Orchestration (`compose` / `flare`) & 200-VU Concurrency Metrics | **[4.1 Execution Planes](#41-execution-planes-compose--flare)** · **[4.2 Benchmarks](#42-pipeline-performance-benchmarks-apple-m2-32gb-ram)** |
| **5. System Validation Logs** | End-to-End Security, FSM, Guardrail & Edge Cryptographic Audit Logs | **[5. System Validation Logs](#5-system-validation-logs)** |

---

## 0. Quick Start

```bash
uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.8
```

**Track A: In-Process Python SDK**
Run the guardrail pipeline, schema traverser, and VCR directly inside your Python process:

```python
import asyncio
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.interceptor.guardrail import CustomRegexMaskingGuardrail, ActiveBlockingGuardrail

async def main():
    res = await acompletion(
        model="cerebras/qwen-3.8-27b", # or openai/gpt-4o, gemini/gemini-2.5-flash, llama_server/gemma-3-1b-it-Q4_K_M.gguf
        messages=[{"role": "user", "content": "My phone is 010-1234-5678. Say 'Received'."}],
        interceptors=[
            CustomRegexMaskingGuardrail([r"\b010-\d{4}-\d{4}\b"]), # Masks PII in-place (~0.01ms)
            ActiveBlockingGuardrail(),                             # Blocks leaked API keys
        ],
    )
    print(res.choices[0].message.content)

asyncio.run(main())
```

**Track B: Local Gateway Daemon & Console Studio (`127.0.0.1:8356`)**
Govern external AI agents (Claude Code, Cursor, Continue) with pre-flight HITL locks, real-time stream kill-switches, and a unified web control plane:

```bash
$ fiber init          # Provision workspace (~/.anchor) & local node keys
$ fiber connect copy  # Scan and sync local IDE MCP configs (.mcp.json)
$ fiber console       # Auto-spawn gateway daemon (:8356) & launch Studio UI (:8357) (or `fiber daemon` for headless)
```

## 1. Local MCP Gateway

The **Fiber Connect CLI** (`fiber`) is the single entrypoint for the local control plane. It registers your MCP servers and IDE configs, mounts them as managed workers under one host-local daemon, and—when needed—extends the same governance across remote nodes with zero boilerplate.

Most AI agents (Claude Code, Cursor, VSCode, Zed) launch MCP servers directly on your machine through point-to-point pipes—with your filesystem, databases, and credentials in reach, and nothing in between to absorb retries or demand approval. Fiber's **Local MCP Gateway** runs as a loopback daemon on the same host, absorbs your existing agent configurations and exposes them through a single stateless endpoint (`/v1/mcp-gateway/invoke`, `/tools`, or per-target `/v1/mcp-gateway/{target_id}/invoke`). Local stdio servers are spawned and supervised by the daemon itself, and remote HTTP/SSE servers can be registered behind the same gate—so every tool call passes through one control point on your machine.

**Why a Local Gateway?**
Standard MCP clients treat tool calls as fire-and-forget pipes: on network timeouts, agents blindly retry or hallucinate parameters, causing duplicate database writes or runaway API charges. Fiber’s MCP Gateway interposes a deterministic control plane between the agent and the tool:

  - **Idempotent Execution & Lineage**: Maps every invocation to a deterministic transaction handle backed by a local state ledger. Duplicate agent retries immediately return cached resolutions without re-executing the underlying tool, while upstream timeouts trigger automatic rollbacks.

  - **Pre-Flight HITL & Approval Gate (`HTTP 202`)**: On high-risk operations—detected either natively by the worker or zero-code prior to worker I/O via pre-flight regex traps (--preflight, guaranteeing Zero-I/O on persistent workers and Zero-Spawn on ephemeral sandboxes until approved)—the gateway parks the transaction via a stateless multi-round-trip yield (YIELD / HTTP 202) and awaits **human attestation** via **TOTP** (default) or **1-Click Approve/Reject buttons** (`button:<regex>`) across Slack Socket Mode, CLI, or Control Studio before resuming.

  - **Host-Local Trust Boundary**: Upstream API keys are resolved from the daemon's host environment (`.env` or OS variables) and never appear in client configs; node identity and server registry (`~/.anchor/mcp.json`) stay on the machine.

  - **Hold/Fuel Reservation**: Translates signed quota mandates into linear state transitions: each mandate is consumed exactly once to produce the next quota state. Version fencing rejects stale or replayed policies, and the same mandate can never be consumed twice or drive a balance below zero.

**LLM & MCP Routing for IDEs**
Alongside the MCP endpoint, the same local daemon serves the LLM Gateway (`/v1/chat/completions`)—the HTTP face of the pipeline described in [2.1](#21-llm-guardrails).
By configuring your IDE's (e.g., Continue.dev, Cursor) API base URL to `http://127.0.0.1:8356/v1`, you can route all model requests through Fiber's guardrails. This eliminates the need to expose upstream provider API keys in client configs, as authentication is securely resolved by the daemon's host environment (.env files or local OS variables).

**Example: continue.dev LLM Config (config.yaml)**
By setting the client provider to openai, Fiber enables IDEs to seamlessly interface with models they may not natively support. Simply append the target infrastructure prefix (e.g., `cohere/`, `cerebras/`, `llama_server/`) to the model string, and Fiber autonomously orchestrates the schema translation and OS credentials.

```yaml
models:
  # apiKey: Omitted. The daemon securely resolves KEY from its host environment.
  # Expand to Proprietary/Enterprise Models
  - name: Command R (Cohere)
    provider: openai
    model: cohere/command-r-08-2024
    apiBase: http://127.0.0.1:8356/v1

  # Ultra-Low Latency Cloud Inference
  - name: Qwen 3.8 27B (Cerebras)
    provider: openai
    model: cerebras/qwen-3.8-27b
    apiBase: http://127.0.0.1:8356/v1

  # Unified Local Execution (llama.cpp)
  - name: Gemma 3 1B (Local Engine)
    provider: openai
    model: llama_server/gemma-3-1b-it-Q4_K_M.gguf
    apiBase: http://127.0.0.1:8356/v1
```

**Agent Onboarding**
Instead of manually supervising fragmented MCP processes, Fiber automatically scans and normalizes existing agent configs (`.mcp.json`, `.cursor/mcp.json`, `.vscode/mcp.json`) alongside built-in workers.

To route your agent through Fiber, simply inject the `fiber-gateway` entry into your agent's configuration file (optionally attaching `FIBER_PREFLIGHT_PATTERN` in `env`—prefixed with `button:` for 1-Click Slack approval instead of default 6-digit TOTP—to lock third-party servers without modifying their code):

```json
{
  "mcpServers": {
    "fiber-gateway": {
      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"
    },
    "repomix": {
      "command": "npx",
      "args": ["-y", "repomix", "--mcp"],
      "env": {
        "FIBER_PREFLIGHT_PATTERN": "button:DROP|TRUNCATE|DELETE"
      }
    }
  }
}
```

**Coexistence**: Fiber's declarative mapper automatically ignores the self-referential fiber-gateway entry during scanning while mounting the remaining servers. Once verified, you can progressively remove redundant direct entries at your own pace.

**Declarative Provisioning (`fiber connect copy`)**
Rather than manually supervising background processes, `fiber connect copy` scans and registers your IDE configs (`.mcp.json`) and third-party servers into `~/.anchor/mcp.json` for automatic hot-reload by `fiber daemon` (see **[3.1](#31-fiber-connect-cli)** for advanced targeting and standalone sidecar modes):

```bash
# Auto-Scan & Sync: Registers all built-in workers and local IDE mcp.json configs
$ fiber connect copy

# Lock a 3rd-Party Stdio MCP with a 1-Click Slack Pre-Flight Guard ('button:' prefix)
$ fiber connect copy -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets|DELETE"
```

---

## 2. LLM Guardrails & Replay

### 2.1. LLM Guardrails

Serving as a drop-in replacement for standard OpenAI and LiteLLM SDKs, Fiber's duplex pipeline enforces two-track in-flight guardrails—scrubbing soft PII and sensitive headers in-place (`PRE_TRANSLATE`), while strictly blocking leaked credentials (`POST_TRANSLATE`) and severing rogue streams via `StreamBreaker` without altering your business logic. Furthermore, because the core pipeline is decoupled from parsing logic, extending support for new proprietary models is seamless when paired with Fiber's Schema Traverser **[2.3]**.

> **Unlike passive callbacks, this event-driven pipeline empowers interceptors to actively mutate payloads in-place, short-circuit physical I/O, and abort unauthorized streams in sub-millisecond time.**

> **In-Process SDK vs. Gateway Daemon:** While the Python SDK and pytest VCR run entirely in-process without a daemon, running `fiber daemon` exposes this exact guardrail pipeline as an OpenAI-compatible REST endpoint (`/v1/chat/completions`) for external agents, IDEs, and Console Studio.


**1. Two-Track Guardrails & Slot Interceptors (`PRE_TRANSLATE` vs. `POST_TRANSLATE`):**
Production guardrails are built into `fiber.llm.pipeline.interceptor.guardrail` and ready to mount directly in **Step 2**; under the hood, each interceptor implements `DuplexChannel` for its target execution slot:

```python
from fiber.llm.pipeline.tracer import BaseLLMTracer
from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.compat.secret.redact import redact_string, sanitize_payload, FastRegexRedactionEngine
from xphi.state.phase.channel import DuplexChannel

# [PRE_OBSERVER] Stream-Aware Telemetry Hook
class TelemetryTracer(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        if hasattr(response, "__aiter__"):
            meta.framework_flags.setdefault("on_stream_complete", []).append(
                lambda tokens: datadog.count("llm.tokens", tokens, tags=[f"model:{meta.base_model}"])
            )
            return
        datadog.gauge("llm.latency", duration_ms, tags=[f"model:{meta.base_model}"])

# [PRE_TRANSLATE - Track 1A] Recursive Dict, Header & URL Query Sanitization (Mutate & Pass)
class PayloadSanitizationGuardrail(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    async def write(self, ctx, msg: dict):
        await ctx.fire_write(sanitize_payload(msg))

# [PRE_TRANSLATE - Track 1B] In-Place C-Regex PII Masking (Mutate & Pass)
class CustomRegexMaskingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    def __init__(self, custom_patterns: list[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx, msg: dict):
        for m in msg.get("messages", []):
            if isinstance(m, dict) and isinstance(m.get("content"), str):
                m["content"] = self.engine.execute(m["content"].encode("utf-8")).decode("utf-8", errors="replace")
        await ctx.fire_write(msg)

# [POST_TRANSLATE - Track 2A] Strict Blocking for Built-in & Registry Secrets (Short-Circuit)
class ActiveBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    async def write(self, ctx, processed_msg):
        for msg in getattr(processed_msg, "original_kwargs", {}).get("messages", []):
            content = msg.get("content", "")
            # Scans against 33 built-in and dynamically injected credential rules
            if isinstance(content, str) and redact_string(content) != content:
                return await ctx.fire_exception_caught(
                    PermissionError("Guardrail Block: Sensitive credential detected.")
                )
        await ctx.fire_write(processed_msg)

# [POST_TRANSLATE - Track 2B] Zero-Copy C-Regex Policy Blocking (Short-Circuit)
class CustomRegexBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    def __init__(self, custom_patterns: list[str]):
        super().__init__()
        self.engine = FastRegexRedactionEngine(patterns=custom_patterns)

    async def write(self, ctx, processed_msg):
        for msg in getattr(processed_msg, "original_kwargs", {}).get("messages", []):
            content = msg.get("content", "")
            if isinstance(content, str) and self.engine.contains_match(content.encode("utf-8")):
                return await ctx.fire_exception_caught(
                    PermissionError("Guardrail Block: Custom domain policy violation.")
                )
        await ctx.fire_write(processed_msg)

```

**Built-in Guardrails & Reference Templates**: `fiber.llm.pipeline.interceptor.guardrail` provides the four two-track DLP interceptors above out of the box, alongside reference templates (`cache.EdgeRedisCache`, `router.OnnxSemanticRouter`) demonstrating how to short-circuit I/O (`ctx.fire_channel_read`) or dynamically route models in `PRE_TRANSLATE`.

**2. Execute Chained Defense via Drop-in Facade:**

```python
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.interceptor.guardrail import (
    PayloadSanitizationGuardrail,
    CustomRegexMaskingGuardrail,
    ActiveBlockingGuardrail,
)
from fiber.llm.pipeline.channel.breaker import StreamBreaker
from fiber.llm.pipeline.channel.observer import MeteringObserver
from fiber.infra.watcher.finops import default_finops_watcher, default_tenant_eco
from xphi.arch.model.auth import NodeAuthPayload

# Fiber autonomously sorts the flat list into deterministic execution slots:
# [Tracer (OBSERVER)] -> [Sanitize -> Masking (PRE_TRANSLATE)] -> [Translator] -> [Blocking -> StreamBreaker (POST_TRANSLATE)]
response = await acompletion(
    model="lm_studio/qwen-3.5-9b", # ex: cerebras/qwen-3.8-27b, gemini/gemini-3.1-flash-lite, ollama/gemma:2b
    messages=[{"role": "user", "content": "Customer phone: 010-1234-5678. Summarize inquiry."}],
    stream=True,
    interceptors=[
        TelemetryTracer(),
        PayloadSanitizationGuardrail(),                        # Scrubs sensitive dict keys & URL params
        CustomRegexMaskingGuardrail([r"\b010-\d{4}-\d{4}\b"]), # Masks soft PII in-place (~0.006ms)
        ActiveBlockingGuardrail(),                             # Hard-blocks leaked API keys (~0.014ms)
    ],
    stream_breaker=StreamBreaker(finops_watcher=default_finops_watcher),
    metering_observer=MeteringObserver(tenant_eco=default_tenant_eco, pta_adapter=pta_adapter),
    metadata={"node_auth": NodeAuthPayload(tenant_id="accounting_dept", fuel_budget=50000, is_enforced=True)}
)
```

**3. Declarative Secret Registry (`FIBER_REDACT_RULES_PATH`):**
To add custom credential patterns, override existing rules, or disable specific built-in checks globally without modifying Python code, supply an external `redact_rules.json` via `FIBER_REDACT_RULES_PATH` (`fiber.llm.compat.secret.registry`). At boot time, Fiber validates each regex via Pydantic (safely skipping malformed rules) and and injects the merged ruleset into the core redaction engine at boot:

```json
{
  "patterns": {
    "fiber_internal_token": "FIBER-INTERNAL-TOKEN-[0-9]{6}",
    "corp_vault_key": "CORP-VAULT-[A-Z0-9]{16}"
  },
  "disabled_rules": ["context7_key", "generic_password_kv"],
  "extra_secret_keys": ["FIBER_VAULT_PIN", "INTERNAL_AUTH_HEADER"]
}

```

> **Note:** Out of the box, Fiber ships with **33 built-in credential regex rules** (`BASE_SECRET_RULES`) and **14 sensitive key/URL tokens** that already cover standard cloud providers (`AKIA...`, `AIza...`), auth headers (`Bearer`, `JWT`, `PEM`), and major LLM/SaaS vendor keys (`sk-...`, `ghp_...`, `xoxb-...`) with zero configuration.
> For most deployments, mounting `ActiveBlockingGuardrail()` and `PayloadSanitizationGuardrail()` as-is provides complete baseline protection without maintaining a custom registry file.
> When you need to introduce proprietary internal token formats or selectively relax specific checks via `redact_rules.json`, refer to the **[Interceptor Spec](./phase/abc/dev/llm/interceptor.md)** for the full catalog of default rule IDs and slot extension patterns.

> **Note on Drop-in Replacement:** Fiber operates as a drop-in replacement by maintaining compatibility with OpenAI/LiteLLM entrypoints, Pydantic return objects, and local token utilities. The following specifications detail how to preserve your existing application logic while integrating active guardrails, stream circuit-breakers, and quota tracking.
> 🔗 **[Entrypoint Spec](./phase/abc/dev/llm/entry.md)**
> 🔗 **[Token Counter Spec](./phase/abc/dev/llm/token.counter.md)**
> 🔗 **[Interceptor Spec](./phase/abc/dev/llm/interceptor.md)**

---

### 2.2. Deterministic Replay (LLM VCR & Fixtures)

Fiber's Application-Layer VCR serializes LLM and tool network traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables zero-cost deterministic fast-forward replay, side-effect-free state recovery, and precise historical latency emulation without altering business logic.

> **Why Fiber VCR?**
> Standard HTTP mockers (like vcrpy) capture network-layer raw bytes, which breaks stream timing and bloats memory during LLM testing. Fiber’s Application-Layer VCR natively understands LLM semantics—autonomously coalescing micro-chunks via time-windows (e.g., 100ms) and ensuring strict duck-typing parity for SDKs like LiteLLM.
> Furthermore, because `StreamBreaker` is natively integrated into the recording lifecycle, streams terminated mid-flight by a policy kill-switch or quota limit capture only the pre-cutoff chunks—allowing you to deterministically replay and test exact circuit-breaking states offline.

Fiber provides structural approaches for injecting the local fixture engine:

**Automated Testing (Pytest Standard)**

Adopting the industry-standard VCR pattern, Fiber empowers your pytest suites to act as the single source of truth. By toggling the `VCR_MODE` environment variable, a single test script can autonomously generate its own fixtures against live APIs (Record) and subsequently execute offline in CI/CD environments (Replay)—preventing any drift between request payloads and assertions.

```python
import os, pytest
from fiber.infra.vcr.manager import VCRPlaybackConfig
from fiber.infra.vcr.injector import VCRInjector
from fiber.llm.entry import acompletion

@pytest.fixture(scope="module", autouse=True)
def inject_vcr():
    """Injects the VCR engine and safely restores original adapters upon teardown."""
    vcr_mode = os.getenv("VCR_MODE", "replay")
    config = VCRPlaybackConfig(mode=vcr_mode, speed="max", record_tick_ms=100.0)
    
    # Context manager cleanly scopes the proxy injection to a target suite directory
    with VCRInjector.use_cassette(config=config, fixture_dir="../fiber-compats/suites/router_suite"):
        yield

@pytest.mark.asyncio
async def test_semantic_routing_pipeline():
    # VCR transparently intercepts this call based on the injected context
    response = await acompletion(
        model="auto",
        messages=[{"role": "user", "content": "Format this to JSON."}],
        metadata={
            "vcr_scenario": "soft_rule_intent",
            "vcr_filename": "fixture_soft_rule_intent.json" # Explicit naming for clarity
        }
    )
    assert response is not None


```

**Transparent Integration for Legacy Codebases**

For applications heavily coupled to third-party SDKs (e.g., LiteLLM), establishing offline tests often requires complex refactoring. Fiber solves this through transparent runtime routing.

By declaring explicit aliases at the boot sequence, Fiber intercepts legacy imports and routes traffic directly to its offline fixture engine (VCR). This enables deterministic playback and time-window stream coalescing without altering your business logic. Fiber also preserves exact duck-typing parity, so legacy stream consumers require zero changes.

```python
import asyncio

# Transparently route legacy SDK imports to Fiber's gateway before business logic runs
from fiber.dev.bridge.vcr import vcr_setup_sequence, inspect_fixture
VCR_MODE, RESOLVED_FIXTURE_DIR = vcr_setup_sequence()

"""[Legacy Business Logic] Unmodified Boundary"""
import litellm 

async def main():
    scenario_name = "tech_debt_migration"
    
    # Fiber processes this standard call. The `metadata` acts as a bridge, 
    # guiding the underlying engine to manage deterministic fixture routing.
    response = await litellm.acompletion(
        model="cohere/command-r-08-2024", # Any model supported by the Traverser works here
        messages=[{"role": "user", "content": "Explain migration strategies."}],
        stream=True,
        metadata={
            "vcr_scenario": scenario_name,
            "vcr_invoker": "legacy_app"
        }
    )
    
    async for chunk in response:
        # Standard legacy parsing remains flawless
        if hasattr(chunk, "choices") and chunk.choices:
            print(chunk.choices[0].delta.content or "", end="", flush=True)

    # Validate generated fixture offline
    if VCR_MODE == "record":
        inspect_fixture(scenario_name, RESOLVED_FIXTURE_DIR)

if __name__ == "__main__":
    asyncio.run(main())

```

**Per-Request HTTP Gateway VCR**

When running `fiber daemon`, the REST Gateway (`/v1/chat/completions`) keeps the VCR proxy mounted in zero-overhead `live` passthrough mode. Any HTTP client (Node.js, Go, Rust, or `curl`) can dynamically trigger fixture recording or offline playback per request—without restarting the daemon—by passing `vcr` parameters in the JSON payload:

```bash
# Dynamic Record: Executes live LLM call and freezes the response into a local fixture
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
    "messages": [{"role": "user", "content": "Give me a 3-word motto."}],
    "vcr": "record",
    "vcr_scenario": "gateway_smoke_test"
  }'

# Dynamic Replay: Replays deterministically from disk (~3ms) with zero upstream I/O
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

**Pytest Record & Replay**

Once recorded (`VCR_MODE=record`), suites execute entirely offline (`VCR_MODE=replay` by default)—running at maximum velocity for CI/CD or with real-time latency and chaos jitter emulation:

```bash
# 1. Record live API streams into coalesced local fixtures (100ms window)
$ VCR_MODE=record pytest -v fiber/llm/pipeline/interceptor/tests/router.py

# 2. Replay offline at maximum velocity (~0ms I/O) or emulate real latency + 500ms chaos jitter
$ pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py
$ VCR_SPEED=real VCR_CHAOS=500.0 pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# 3. Verify live HTTP Gateway roundtrip (Unary, SSE Stream, and Dynamic Record -> Replay)
$ python -m fiber.dev.ex.tester.gateway
```

---

### 2.3. Schema Traverser & Compat Registry

Applying consistent guardrails and deterministic replay across a fragmented LLM ecosystem requires unified payload normalization. Fiber eliminates brittle `if/elif` parsing logic through its **Rule-based Traverser**—using dot-notation and **Path Fallbacks** to seamlessly navigate mixed topologies (Dicts, Lists, Pydantic Objects) across request parameters, sync responses, and async stream chunks through a unified schema.

**Zero-Config Defaults & Provider Extension:**
Built-in rules already normalize standard providers (`openai`, `gemini`, `cohere`, `cerebras`, `ollama`, `llama_server`) out of the box with zero configuration. When integrating a proprietary internal model or overriding existing extraction paths without modifying Python code, supply an external JSON file via the `FIBER_COMPAT_RULES_PATH` environment variable.

Fiber loads this registry exactly once at boot-time. It performs strict Pydantic schema validation (Fail-Fast) and deep-merges the rules into the base registry. Invalid formats are safely ignored with a warning (Partial Update).

```json
{
  "provider_param_rules": {
    "my_custom_llm": {
      "message_schema": "openai_array",
      "supported": ["temperature", "max_tokens", "stream"],
      "mapping": {
        "max_tokens": "max_output_tokens"
      }
    }
  },
  "extraction_rules": {
    "ollama": {
      "text": ["choices.0.delta.content", "message.content", "response", "choices.0.message.content"],
      "finish_reason": ["done_reason", "choices.0.finish_reason"],
      "is_finished_cond": {"path": "done", "value": true},
      "usage": [
        "usage",
        {"prompt_tokens": "prompt_eval_count", "completion_tokens": "eval_count"}
      ]
    },
    "gemini": {
      "text": [
        "choices.0.delta.content",
        "content.parts.0.text",
        "candidates.0.content.parts.0.text",
        "choices.0.message.content"
      ],
      "fallback_tool_name": "content.parts.0.function_call.name",
      "fallback_tool_args": "content.parts.0.function_call.args"
    }
  }
}
```

**Autonomous Rule Synthesis & Validation**: Because rules are purely declarative, even lightweight LLMs can one-shot synthesize new provider bindings from raw API payloads using existing rules as a reference—forming a closed, hallucination-free self-healing loop when paired with `fiber compat fixture --vcr replay` for instant offline verification.

---

## 3. CLI & Installation

### 3.1. Fiber Connect CLI

| Command | Role | Function & Lifecycle | Example |
| --- | --- | --- | --- |
| **`init`** | **[Bootstrap]** | Initializes workspace topology (`~/.anchor/bound.json`) and local node keys. | `fiber init` |
| **`console`** | **[Web Studio]** | Launches the Console Studio web UI (`MCP Gateway`, `Guardrail`, `Metering`, `VCR`, `Setting`) for HITL approvals and live telemetry. | `fiber console` |
| **`sign`** | **[Identity & TOTP]** | Inspects local node key status and manages AES-GCM encrypted Admin TOTP secrets in SQLite. | `fiber sign enroll -u fiber` |
| **`connect copy`** | **[Config Sync]** | Registers built-in workers, IDE configs (`mcp.json`), or third-party servers (with `-p` HITL guards) into `~/.anchor/mcp.json` for automatic hot-reload by `fiber daemon`. | `fiber connect copy -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets\|DELETE"` |
| **`connect`** | **[Standalone Sidecar]** | Runs MCP workers or configs directly in the foreground without writing to `~/.anchor/mcp.json` (automatically skips targets already running in the daemon). | `fiber connect -e worker.deployer -p "DROP"` |
| **`daemon`** | **[Local Gateway]** | Ignites the local gateway daemon (`127.0.0.1:8356`), automatically booting and hot-reloading all MCP servers registered in `~/.anchor/mcp.json`. | `fiber daemon` |
| **`compat`** | **[Compat & VCR]** | Records raw provider payloads (`record`) and verifies declarative extraction rules offline (`replay`) for schema parity. | `fiber compat fixture -m gemini/gemini-3.1-flash-lite --vcr record` |
| **`account`** | **[Account & Quota]** | Manages tenant quotas, binds optional tenant identities, issues signed execution mandates, and triggers real-time stream kill-switches. | `fiber account --tenant accounting_dept --spiffe fiber jwt --usd 50 --op PROVISION --sync` |

**MCP Server Provisioning & Sidecar (`fiber connect`)**
`fiber connect copy` hot-reloads updated configs into `fiber daemon` (inferring transport, concurrency, and `-p` HITL guards); omit `copy` to run any worker as a standalone foreground sidecar:

```bash
# Built-in Worker: Lock 'finlib' with a 6-digit TOTP Pre-Flight Guard (Default)
$ fiber connect copy -t worker.eco.finlib -e finlib -p "normalize_order|pow\(|exp\("

# Remote HTTP/SSE MCP: Register remote server with a 1-Click Pre-Flight Guard ('button:' prefix)
$ fiber connect copy -t net-context7-remote-01 -e https://mcp.context7.com/mcp -p "button:security|auth|secret"

# Standalone Mode (without daemon): Omit 'copy' to run a worker directly in the foreground
$ fiber connect -t prod-db -e "npx -y @modelcontextprotocol/server-postgres" -p "DROP|DELETE"
```

**Node Identity & Admin TOTP (`fiber sign`)**
Inspects local node keys and enrolls administrator HITL TOTP credentials:

```bash
# Inspect local node keys, Node PubKey, and Audit
$ fiber sign status

# Enroll an administrator TOTP secret (renders terminal QR code)
$ fiber sign enroll -u fiber
```

**Account & Quota Governance (`fiber account`)**
Commits signed quota mandates to the linear state chain and broadcasts real-time `REVOKE` kill-switches (optional `--spiffe` bindings log a non-blocking warning if unenrolled):

```bash
# Run full lifecycle walkthrough (Provision -> TopUp -> Stale Replay Check -> Revoke)
$ fiber account demo

# List all tracked local accounts and their bound SPIFFE identities from the registry
$ fiber account list

# Provision $50.0 (50,000,000 Fuel), bind an optional identity ID, and commit as a linear state transition
$ fiber account --tenant accounting_dept --spiffe fiber jwt --usd 50.0 --op PROVISION --ver 1 --sync

# Trigger a Pub/Sub REVOKE kill-switch to sever active streams
$ fiber account --tenant accounting_dept kill --ver 99

```

**Console Studio**
Console Studio is a unified web console for the local daemon (`127.0.0.1:8356`) featuring **MCP Gateway, Guardrail, Metering, VCR, and Setting** tabs—allowing you to approve parked HITL transactions, toggle secret rules, monitor real-time token velocity and fuel budgets, and inspect VCR fixtures. Launch with `fiber console` (or `python -m fiber.dev.console.app`).

---

### 3.2. Python Environment via `uv pip` (Recommended for SDK & Local Control Plane)

**Prerequisites**

* **Python**: `>= 3.12`
* **Redis** *(Optional)*: Only required for distributed modes (`fiber daemon`), pub/sub event routing, and stateful governance (e.g., Stream Breaker). Standalone pipeline execution runs entirely in-memory without Redis.

Best suited for full-stack integration—including the drop-in Python SDK (`from fiber.llm.entry import acompletion`), CLI orchestration, and local daemon deployment. We recommend using `uv pip` for deterministic dependency resolution:

```bash
## 1. Create a dedicated sandbox environment & install Fiber
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.8
# OR: uv pip install -e /path/to/local/self/fiber

## Bootstrap workspace & Provision node identity
fiber init

## (Optional) Start the local gateway daemon for MCP/LLM HTTP routing & Console
fiber daemon
```

### 3.3. macOS Homebrew Tap *(For Local Daemon & MCP Gateway)*

Best suited for running Fiber as a standalone background service (`fiber daemon`) to govern local AI agents via the **MCP Gateway** without managing Python virtual environments.

> **Note (Experimental):** The Homebrew packaging pipeline is currently undergoing stabilization across macOS environments (e.g., Xcode Command Line Tools & build metadata bindings). If you encounter environment-specific build issues, please fall back to the `uv pip` installation above.

```bash
## Install Fiber and its isolated runtime in a single command
brew install wittgena/fiber/fiber

## Initialize local node identity & start the background daemon service
fiber init
brew services start wittgena/fiber/fiber
```

## 4. Execution Planes & Pipeline Benchmarks

Beyond the host loopback daemon, Fiber extends its runtime governance across **Docker Compose** for containerized CI parity and **Cloudflare Edge V8 Isolates** for zero-trust sandbox execution.

### 4.1. Execution Planes (`compose` & `flare`)

Fiber dynamically synthesizes target infrastructure blueprints (`docker-compose.yml`, `wrangler.toml`), executes plane-specific verification suites, and tears down ephemeral workspaces automatically (or preserves crash dumps via `--keep-workspace`):

```bash
# Container Plane: Replicates the GitHub Actions CI topology locally (--rebuild for no-cache)
$ fiber e2e infra.plane.compose --mode dev

# Edge Sandbox Plane: Provisions Dual V8 Isolates and seals execution into a Merkle root
$ fiber e2e infra.plane.flare --mode dev     # or --mode deploy for *.workers.dev
```

| Execution Plane | Entrypoint & Substrate | Architectural Role & Security Boundary |
| --- | --- | --- |
| **Host / CI Plane** | `fiber daemon` (Native OS + Redis) | Runs the primary loopback control plane (`127.0.0.1:8356`) across local environments and GitHub Actions PR gates with automated process reaping. |
| **Container Plane** | `infra.plane.compose` (Docker Compose) | Synthesizes a network-coupled (`service:redis`) container topology in `VCR_MODE=replay` to cross-validate gateway, FinOps, and WASM suites locally. |
| **Edge V8 Plane** | `infra.plane.flare` (Cloudflare Workers) | Binds a WASM Gateway (`gateway`/`phase`/`fsm.wasm`) to a native Python V8 isolate, intercepting CPU exhaustion (`Error 1102`) and cryptographic state verification. |

---

### 4.2. Pipeline Performance Benchmarks (Apple M2, 32GB RAM)

These metrics measure the **pure internal overhead** of the Fiber LLM Pipeline. To eliminate network variance and isolate framework performance, the suite was executed in deterministic **VCR Replay** mode.

**Run the benchmark with VCR driving 200 concurrent streams locally:**

```bash
fiber e2e gateway.inter.concurrency --vcr replay --concurrency 200
```

| Performance Dimension | Metric (M2, 32GB) | Architectural Significance |
| --- | --- | --- |
| **Unary Throughput** | **~1,180 req/s** | Sustains over 1,000 control-plane transactions per second in a single Python process. |
| **Stream Interleaving** | **~2,200 chunks/s** | High-density multiplexing of asynchronous SSE byte-streams without blocking the event loop. |
| **Routing Overhead** | **p50: <1.0ms / p95: <1.5ms** | Sub-millisecond framework latency added to Time-To-First-Token under concurrent bursts. |
| **Memory Footprint** | **<1 MB (Unary) / ~25 MB (Streams)** | Minimal GC pressure; buffers 200 active parallel streams within strict memory bounds. |
| **Context Isolation** | **Zero Cross-Talk** | Strict coroutine context isolation across parallel streams without state bleeding. |

---

## 5. System Validation Logs

Fiber enforces execution determinism and runtime safety through automated CI gates (on every PR and release tag) alongside targeted end-to-end validation suites across each subsystem:

* **[mcp.guard.log](./phase/abc/log/gateway/worker/guard.20261022.log):** Validates zero-code HITL governance for MCP servers, confirming that safe tool calls pass through instantly while high-risk payloads pause for Slack approval (via 6-digit TOTP or 1-Click buttons)—spawning third-party stdio sandboxes only after human verification.
* **[ex.switch.log](./phase/abc/log/vcr/ex.switch.llama_server.20260929.log):** Validates the legacy migration, confirming that module aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.
* **[fsm.tester.log](./phase/abc/log/fsm/tester.20261027.log):** Validates the declarative agent state machine, confirming that standard AWS Step Functions (ASL) workflows compile into compact execution graphs, split and reclaim budgets across parallel branches without leakage, pause in-place for pre-flight human approval, and resume or re-branch from any checkpoint without re-running prior steps.
* **[llm.compat.log](./phase/abc/log/gateway/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming sub-millisecond two-track DLP guardrails (`PRE_TRANSLATE` PII masking and `POST_TRANSLATE` credential blocking), strict Stream Breaker terminations on mid-flight policy revocation or quota exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
- **[infra.plane.flare.log](./phase/abc/log/infra/plane/flare.20261031.log):** Validates remote sandbox execution on Cloudflare Edge V8 isolates, confirming zero-leakage host isolation, bit-exact compute determinism, and tamper-proof state verification via Merkle roots.