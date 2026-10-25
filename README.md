# fiber.README
@lineage: README

The zero-point that aligns Fiber’s core with its surface rests on four self-imposed questions:

* *Can you onboard a new LLM's async streams and tool calls onto your existing codebase using only external JSON rules—without touching a line of Python or redeploying packages?*  
  It eliminates brittle `if/elif` parsers and repeated production redeployments every time a provider shifts its parameters or streaming chunk schema.

* *While hundreds of LLM streams are pouring in, can you physically sever a single stream mid-flight (`HTTP 402`) within 1ms at the exact token chunk its budget runs out?*  
  Post-request billing is too late to stop runaway agent loops or streaming bursts from draining API budgets and triggering OOM crashes in real time.

* *Can you coalesce thousands of raw SSE micro-chunks into 10ms or 100ms time-windows on record, and replay them chunk-for-chunk at either 0ms (`max`) or exact historical pacing (`real`)?*  
  It replaces bloated packet dumps and flaky live API calls with a single deterministic fixture—covering instant CI assertions, real-time latency tests, and mid-stream kill-switch cutoffs.

* *Without touching third-party MCP server code, can you freeze high-risk tool calls for Slack approval before the OS process even spawns (`Zero-Spawn`)—and guarantee single execution across ten agent retries?*  
  It stops untrusted tools from blindly spawning local processes and prevents timeout-induced agent retries from corrupting or wiping out production state.

---

Operating as a drop-in asynchronous pipeline with real-time FinOps circuit breakers and mid-stream kill-switches (**1.1**), Fiber captures network traffic into content-addressed fixtures for zero-cost deterministic I/O replay and latency emulation (**1.2**), while autonomously normalizing heterogeneous LLM schemas on the fly (**1.3**)—achieving execution determinism and strict cost governance without altering your business logic.

| Section | Target Focus & Key Capabilities | Quick Jump |
| :--- | :--- | :--- |
| **1. LLM Pipeline & VCR** | Drop-in SDK, Mid-Stream Kill-Switch, Offline Record/Replay, Schema Traverser | **[1.1 Pipeline](#11-the-drop-in-llm-pipeline)** · **[1.2 VCR](#12-fixture-testing-llm-recordreplay)** · **[1.3 Schema Traverser](#13-schema-traverser--compat-registry)** |
| **2. Installation** | Sandbox provisioning (`uv pip`), Node Identity (`sign init`) & Homebrew | **[2.1 Python & CLI](#21-python-environment-via-uv-pip-recommended-for-sdk--local-control-plane)** · **[2.2 Homebrew](#22-macos-homebrew-tap-for-local-daemon--mcp-gateway)** |
| **3. IDE & MCP Gateway** | Zero-Key IDE Routing (`continue.dev`), Pre-Flight HITL Locks, Config Sync & FinOps CLI | **[3.1 MCP & IDE Config](#31-mcp-gateway)** · **[3.2 Connect CLI](#32-fiber-connect-cli)** |
| **4–5. Benchmarks & Logs** | 200-VU Concurrency Metrics & Cryptographic Validation Logs | **[4. Benchmarks](#4-pipeline-performance-benchmarks-apple-m2-32gb-ram)** · **[5. System Logs](#5-system-validation-logs)** |

---

## 1. LLM Pipeline & Fixture Testing

### 1.1. The Drop-In LLM Pipeline

Serving as a drop-in replacement for standard OpenAI and LiteLLM SDKs, Fiber's duplex pipeline achieves execution transparency without altering a single line of your business logic. Furthermore, because the core pipeline is decoupled from parsing logic, extending support for cutting-edge proprietary models becomes instantly achievable when paired with Fiber's Schema Traverser **[1.3]**.

> **Unlike passive callbacks, this event-driven pipeline empowers middleware to actively short-circuit I/O, physically intercept streams, and tunnel deep infrastructural states.**

> **Remote Gateway Mode:** Beyond the Python SDK, running `fiber daemon` exposes this exact pipeline as an OpenAI-compatible REST endpoint (`/v1/chat/completions`), allowing external agents to offload execution over HTTP.

**1. Define Middleware by Target Slot:**

<details>
<summary><b>View Middleware Implementation Snippets (Telemetry, Cache, Router, Guardrail)</b></summary>

```python
from fiber.llm.pipeline.tracer import BaseLLMTracer
from fiber.llm.pipeline.bootstrap import PipelineSlot
from xphi.state.phase.channel import DuplexChannel

# PRE_OBSERVER: Asynchronous Telemetry with Stream-Aware Lifecycle
class TelemetryTracer(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        # Stream Mode: Defer telemetry until the inner pipeline exhausts the chunks
        if hasattr(response, "__aiter__"):
            def on_stream_complete(total_tokens: int):
                # Calculate the true end-to-end latency when the last chunk arrives
                real_duration_ms = (time.time() - meta.framework_flags["start_time"]) * 1000
                datadog.gauge("llm.latency", real_duration_ms, tags=[f"model:{meta.base_model}", "type:stream"])
                datadog.count("llm.tokens", total_tokens, tags=[f"model:{meta.base_model}"])
            
            # Piggyback the callback onto the framework's hook lifecycle
            hooks = meta.framework_flags.setdefault("on_stream_complete", [])
            hooks.append(on_stream_complete)
            return

        datadog.gauge("llm.latency", duration_ms, tags=[f"model:{meta.base_model}", "type:singular"])

# PRE_TRANSLATE: Edge Caching for Instant Short-circuit
class EdgeRedisCache(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    async def write(self, ctx, msg: dict):
        # note: Replace this exact-match hash with an Embedding to upgrade to a VectorDB Semantic Cache.
        prompt_hash = hashlib.sha256(str(msg.get("messages", [])).encode()).hexdigest()
        if cached_response := await redis_client.get(prompt_hash):
            return await ctx.fire_channel_read(cached_response)
            
        await ctx.fire_write(msg)

# PRE_TRANSLATE: Active Payload Mutation (Dynamic Semantic Routing)
class SemanticRouter(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    
    def __init__(self, routing_rules: dict, default_model: str):
        # Inject rules externally to decouple business logic from infrastructure
        self.routing_rules = routing_rules
        self.default_model = default_model

    def _apply_routing(self, ctx, msg: dict, target_model: str):
        """Syncs the physical payload with the logical pipeline metadata."""
        msg["model"] = target_model
        
        # Must update system_meta so downstream tracers/observers track the correct model
        meta = msg.get("system_meta") or ctx.get_attr("system_meta")
        if meta:
            meta.base_model = target_model

    async def write(self, ctx, msg: dict):
        # Evaluate intent (e.g., via lightweight ONNX CPU model) without blocking the event loop
        intent = await asyncio.to_thread(self._predict_intent, msg.get("messages", []))
        
        # Actively mutate the payload and sync state based on semantic intent
        if rule := self.routing_rules.get(intent):
            self._apply_routing(ctx, msg, rule["target"])
        else:
            self._apply_routing(ctx, msg, msg.get("model", self.default_model))
            
        await ctx.fire_write(msg)

# POST_TRANSLATE: Strict Blocking for Standard Secrets - API Keys, Tokens
class ActiveBlockingGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    async def write(self, ctx, processed_msg):
        messages = getattr(processed_msg, "original_kwargs", {}).get("messages", [])
        for msg in messages:
            content = msg.get("content", "")
            # redact_string checks against built-in credential regex patterns
            if isinstance(content, str) and redact_string(content) != content:
                raise PermissionError("Guardrail Block: Sensitive credential detected.") # Active pipeline break
        await ctx.fire_write(processed_msg)
```
</details><br>

**Out-of-the-box Guardrails**: The snippets above are conceptual examples. Fiber includes production-grade samples in `fiber.dev.pipeline.interceptor`—including INT8 Quantized ONNX Routers, Active Secret Blocking, and Custom C-Level Regex Injection—ready for enterprise deployment.

**2. Execute via Drop-in Facade:**

```python
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.channel.fuel import FuelBreaker, BillingObserver
from fiber.infra.watcher.finops import default_finops_watcher, default_tenant_eco
from xphi.arch.model.auth import NodeAuthPayload

# The framework autonomously restructures the flat list into the strict pipeline:
# [Cache] ➔ [Translator] ➔ [Active Guardrail] ➔ [FuelBreaker] ➔ [Tracer] ➔ [Network I/O]
response = await acompletion(
    model="lm_studio/qwen-3.5-9b", # ex: cerebras/qwen-3.8-27b, gemini/gemini-3.1-flash-lite, ollama/gemma:2b
    messages=[{"role": "user", "content": "Analyze this data."}],
    stream=True,
    interceptors=[TelemetryTracer(), ActiveBlockingGuardrail(), EdgeRedisCache(), SemanticRouter()],
    # Real-time FinOps Governance: Gracefully terminates active streams mid-flight upon budget exhaustion or policy revocation
    fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
    billing_observer=BillingObserver(tenant_eco=default_tenant_eco, pta_adapter=pta_adapter),
    metadata={"node_auth": NodeAuthPayload(tenant_id="accounting_dept", fuel_budget=50000, is_enforced=True)}
)
```

> **Note on Drop-in Replacement:** Fiber operates as a drop-in replacement by maintaining compatibility with OpenAI/LiteLLM entrypoints, Pydantic return objects, and local token utilities. The following specifications detail how to preserve your existing application logic while integrating asynchronous telemetry and cost tracking.
> 🔗 **[Entrypoint Spec](./phase/abc/dev/llm/entry.md)** 
> 🔗 **[Token Utilities Spec](./phase/abc/dev/llm/token.md)** 
> 🔗 **[Cost Tracker Spec](./phase/abc/dev/llm/cost.tracker.md)**

---

### 1.2. Fixture Testing (LLM Record/Replay)

The Offline Fixture Engine serializes LLM and tool network traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables zero-cost deterministic fast-forward replay, side-effect-free state recovery, and precise historical latency emulation without altering business logic.

> **Why Fiber VCR?**
> Standard HTTP mockers (like vcrpy) capture network-layer raw bytes, which breaks stream timing and bloats memory during LLM testing. Fiber’s Application-Layer VCR natively understands LLM semantics—autonomously coalescing micro-chunks via time-windows (e.g., 100ms) and ensuring strict duck-typing parity for SDKs like LiteLLM. 
> Furthermore, because `FuelBreaker` is natively integrated into the recording lifecycle, streams terminated mid-flight by a budget kill-switch capture only the pre-cutoff chunks—allowing you to deterministically replay and test exact quota-exhaustion states offline.

Fiber provides three approaches for injecting the local fixture engine:

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

<details>
<summary><b>View Legacy SDK Aliasing & Duck-Typing Example (litellm)</b></summary>

By declaring explicit aliases at the boot sequence, Fiber intercepts legacy imports and routes traffic directly to its offline fixture engine (VCR). This enables deterministic playback and time-window stream coalescing without altering your business logic. Fiber also maintains duck-typing parity, returning exact mock objects to satisfy strict legacy type checks.

```python
import asyncio

# Transparently route legacy SDK imports to Fiber's gateway before business logic runs
from fiber.dev.bridge.vcr import vcr_setup_sequence, inspect_fixture
VCR_MODE, RESOLVED_FIXTURE_DIR = vcr_setup_sequence()

"""[Legacy Business Logic] Unmodified Boundary"""
import litellm 
from litellm.types.utils import ModelResponseStream

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
        # Duck-typing parity: Legacy type-checks continue to pass
        assert isinstance(chunk, ModelResponseStream)
        
        # Standard legacy parsing remains flawless
        if hasattr(chunk, "choices") and chunk.choices:
            print(chunk.choices[0].delta.content or "", end="", flush=True)

    # Validate generated fixture offline
    if VCR_MODE == "record":
        inspect_fixture(scenario_name, RESOLVED_FIXTURE_DIR)

if __name__ == "__main__":
    asyncio.run(main())
```
</details><br>

**Per-Request HTTP Gateway VCR (Language-Agnostic)**

When running `fiber daemon`, the REST Gateway (`/v1/chat/completions`) keeps the VCR proxy mounted in zero-overhead `live` passthrough mode. Any HTTP client (Node.js, Go, Rust, or `curl`) can dynamically trigger fixture recording or offline playback per request—without restarting the daemon—by passing `vcr` parameters in the JSON payload:

<details>
<summary><b>View Per-Request HTTP Record & Replay Snippets (curl)</b></summary>

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

</details><br>

**Pytest Record & Replay**

Fiber standardizes the testing lifecycle around `pytest` for CI/CD suites and `gateway.tester` for live HTTP daemon verification—eliminating the need for separate recording scripts.


* **Step 1: Record (Capture & Coalesce Network I/O)**
Executes live API calls across your parameterized test matrix (`llama_server`, `cohere`, `gemini`, `cerebras`). The engine autonomously coalesces streaming micro-chunks within a 100ms time-window and persists normalized payloads into human-readable JSON fixtures (`fiber-compats/suites/router_suite/`).

```bash
# Pytest: Generate suites via live API calls
$ VCR_MODE=record pytest -v fiber/llm/pipeline/interceptor/tests/router.py
```

* **Step 2: Replay (Deterministic CI/CD & Gateway Verification)**
Executes the exact same test suite entirely offline (`VCR_MODE=replay` by default). You can run at maximum velocity for CI/CD pipelines, emulate historical latencies (`VCR_SPEED=real`), inject chaos jitter (`VCR_CHAOS=500.0`), or validate the live HTTP Gateway roundtrip.

```bash
# Pytest (Default Replay): Run the multi-provider suite offline at maximum velocity (~0ms I/O)
$ pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# Pytest (Chaos & Latency Emulation): Validate timeout resilience with real-time pacing + 500ms jitter
$ VCR_SPEED=real VCR_CHAOS=500.0 pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# HTTP Gateway Suite: Verify Unary, SSE Stream, and Dynamic Record ➔ Replay roundtrip against the daemon
$ python -m fiber.dev.gateway.tester
```


---

### 1.3. Schema Traverser & Compat Registry

The LLM ecosystem is fragmented. Fiber eliminates brittle `if/elif` parsing logic through its **Rule-based Traverser**. Using dot-notation and **Path Fallbacks**, it seamlessly navigates mixed topologies (Dicts, Lists, Pydantic Objects) across request parameters, sync responses, and async stream chunks through a unified schema.

**Provider Extension:**
To integrate a new provider or override existing parsing logic, you no longer need to modify Python code. Simply map their request parameters and response extraction paths in an external JSON file and inject it via the `FIBER_COMPAT_RULES_PATH` environment variable.

Fiber loads this registry exactly once at boot-time. It performs strict Pydantic schema validation (Fail-Fast) and deep-merges the rules into the base registry. Invalid formats are safely ignored with a warning (Partial Update).

<details>
<summary><b>View Declarative Rule Registry Example (FIBER_COMPAT_RULES_PATH=/etc/fiber/compat_rules.json)</b></summary>

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
</details><br>

**Autonomous Rule Synthesis & Validation**: Because rules are purely declarative, even lightweight LLMs can one-shot synthesize new provider bindings from raw API payloads using existing rules as a reference—forming a closed, hallucination-free self-healing loop when paired with `fiber compat fixture --vcr replay` for instant offline verification.

---

## 2. Installation & Infra Provisioning

**Prerequisites**
- **Python**: `>= 3.12`
- **Redis** *(Optional)*: Only required for distributed modes (`fiber daemon`), pub/sub event routing, and real-time stateful governance (e.g., Active Fuel Breaker). Standalone pipeline execution runs entirely in-memory without Redis.

### 2.1. Python Environment via `uv pip` (Recommended for SDK & Local Control Plane)
Best suited for full-stack integration—including the drop-in Python SDK (`from fiber.llm.entry import acompletion`), CLI orchestration, and local daemon deployment. We recommend using `uv pip` for deterministic dependency resolution:

```bash
## 1. Create a dedicated sandbox environment & install Fiber
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.7
# OR: uv pip install -e /path/to/local/self/fiber

## 2. Bootstrap workspace topology (~/.anchor/bound.json)
python -m xphi.node.space.bind.around

## 3. Provision zero-env node identity (~/.ssh/fiber_ed25519 & local_cipher.key)
fiber sign init
fiber sign status
```

### 2.2. macOS Homebrew Tap *(For Local Daemon & MCP Gateway)*

Best suited for running Fiber as a standalone background service (`fiber daemon`) to govern local AI agents via the **MCP Gateway** without managing Python virtual environments.

> **Note (Experimental):** The Homebrew packaging pipeline is currently undergoing stabilization across macOS environments (e.g., Xcode Command Line Tools & build metadata bindings). If you encounter environment-specific build issues, please fall back to the `uv pip` installation above.

```bash
## Install Fiber and its isolated runtime in a single command
brew install wittgena/fiber/fiber

## Initialize local node identity & start the background daemon service
fiber sign init
brew services start wittgena/fiber/fiber
```

---

## 3. MCP Gateway & Fiber Connect

The **Fiber Connect CLI** (`fiber`) is the unified deployment and orchestration entrypoint. It dynamically configures node profiles, mounts autonomous MCP workers onto the distributed Tunnel Bus, and delegates execution across local and remote boundaries with zero boilerplate.

### 3.1. MCP Gateway

Connecting AI agents (Claude Code, Cursor, VSCode, Zed) directly to raw MCP servers via point-to-point pipes exposes host systems to retry-induced state corruption, unapproved tool execution, and credential leaks. Fiber resolves this by absorbing your existing agent configurations onto the Tunnel Bus and exposing them through a stateless **MCP Gateway** (`/v1/mcp-gateway/invoke`, `/tools`, or per-target `/v1/mcp-gateway/{target_id}/invoke`).

**Why MCP Gateway?**
Standard MCP clients treat tool calls as fire-and-forget pipes: on network timeouts, agents blindly retry or hallucinate parameters, causing duplicate database writes or runaway API charges. Fiber’s MCP Gateway interposes a deterministic control plane between the agent and the tool:

  - **Idempotent Execution & Lineage**: Maps every invocation to a deterministic transaction handle backed by a Phase Transition Anchor (PTA). Duplicate agent retries immediately return cached resolutions without re-executing the underlying tool, while upstream timeouts trigger automatic rollbacks.

  - **Pre-Flight HITL & Cryptographic Gate (`HTTP 202` / `SEP-2322` Aligned)**: On high-risk operations—detected either natively by the worker or zero-code prior to worker I/O via pre-flight regex traps (--preflight, guaranteeing Zero-I/O on persistent workers and Zero-Spawn on ephemeral sandboxes until approved)—the gateway parks the transaction via a stateless multi-round-trip yield (YIELD / HTTP 202) and awaits **Ed25519 attestation** via **RFC-6238 TOTP** (default) or **1-Click Approve/Reject buttons** (`button:<regex>`) across Slack Socket Mode, CLI, or Control Studio before resuming.

  - **FinOps Circuit Breaker & PTA Escrow**: Translates external, EdDSA-signed billing mandates into linear PTA ledger states. It enforces strict transaction idempotency (preventing double-spending and overdrafts), rejects stale policy replays, and broadcasts sub-second mid-stream circuit breakers across the control plane to halt runaway costs instantly.

**LLM & MCP Routing for IDEs**
Alongside the MCP endpoint, the daemon simultaneously serves the LLM Gateway (`/v1/chat/completions`). By configuring your IDE's (e.g., Continue.dev, Cursor) API base URL to `http://127.0.0.1:8356/v1`, you can route all model requests through Fiber's guardrails. This eliminates the need to expose upstream provider API keys in client configs, as authentication is securely resolved by the daemon's host environment (.env files or local OS variables).

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
Rather than manually running separate background processes, `fiber connect copy` registers your built-in workers, IDE configs (`.mcp.json`), and third-party servers into Fiber's central configuration (`~/.anchor/mcp.json`). If `fiber daemon` is running, it automatically detects the updated config and hot-reloads the workers—inferring transport (`stdio` vs. `network`), concurrency modes (`multiplex`, `linear`, `ephemeral`), and pre-flight HITL guards (`-p`):

```bash
# Auto-Scan & Sync: Registers all built-in workers and local IDE mcp.json configs
$ fiber connect copy

# Built-in Worker: Lock 'finlib' with a 6-digit TOTP Pre-Flight Guard (Default)
$ fiber connect copy -t worker.eco.finlib -e finlib -p "normalize_order|pow\(|exp\("

# 3rd-Party Stdio MCP: Lock with a 1-Click Slack Button ('button:' prefix)
$ fiber connect copy -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets|DELETE"

# Remote HTTP/SSE MCP: Register remote server with a 1-Click Pre-Flight Guard
$ fiber connect copy -t net-context7-remote-01 -e https://mcp.context7.com/mcp -p "button:security|auth|secret"

# Standalone Mode (without daemon): Omit 'copy' to run a worker directly in the foreground
$ fiber connect -t prod-db -e "npx -y @modelcontextprotocol/server-postgres" -p "DROP|DELETE"
```

### 3.2. Fiber Connect CLI

| Command | Role | Architectural Function & Lifecycle | Example |
| :--- | :--- | :--- | :--- |
| **`sign`** | **[Identity & TOTP]** | Provisions Zero-Env node keys (`~/.ssh/fiber_ed25519` & `local_cipher.key`) and manages AES-GCM encrypted Admin TOTP secrets in SQLite. | `fiber sign enroll -u fiber` |
| **`connect copy`** | **[Config Sync]** | Registers built-in workers, IDE configs (`mcp.json`), or third-party servers (with `-p` HITL guards) into `~/.anchor/mcp.json` for automatic hot-reload by `fiber daemon`. | `fiber connect copy -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets\|DELETE"` |
| **`connect`** | **[Standalone Sidecar]** | Runs MCP workers or configs directly in the foreground without writing to `~/.anchor/mcp.json` (automatically skips targets already running in the daemon). | `fiber connect -e worker.deployer -p "DROP"` |
| **`daemon`** | **[Production Host]** | Ignites the control-plane kernel (`rest_edge`, `rpc_worker`, `mcp_worker`), automatically booting and hot-reloading all MCP servers registered in `~/.anchor/mcp.json`. | `fiber daemon -s all -f .env` |
| **`compat`** | **[Compat & VCR]** | Records raw provider payloads (`record`) and verifies declarative extraction rules offline (`replay`) for schema parity. | `fiber compat fixture -m gemini/gemini-3.1-flash-lite --vcr record` |
| **`account`** | **[FinOps & Quota]** | Manages tenant budgets, binds optional SPIFFE identities, issues cryptographic billing tokens, and triggers real-time stream kill-switches. | `fiber account --tenant accounting_dept --spiffe fiber jwt --usd 50 --op PROVISION --sync` |

**Zero-Exposure Node Identity & Admin TOTP (`fiber sign`)**
Manages the node's local cryptographic keychain and human-in-the-loop (HITL) TOTP credentials:

```bash
# Inspect local node keys (~/.ssh/fiber_ed25519 & local_cipher.key), Node PubKey, and Audit
$ fiber sign status

# Enroll an administrator TOTP secret (renders terminal QR code for Google Authenticator / Authy)
$ fiber sign enroll -u fiber

# Verify AES-GCM decryption parity and test 6-digit TOTP attestation
$ fiber sign verify -u fiber
```

**FinOps Account & Quota Governance (`fiber account`)**
Provides a unified CLI to govern tenant quotas and billing lifecycles. It seamlessly translates stateless billing tokens into enforceable runtime quotas—handling idempotent top-ups, versioned policy enforcement, and instant circuit-breaker broadcasting while abstracting the underlying **Phase Transition Anchor (PTA)** ledger complexities.
You can optionally bind a SPIFFE ID (`--spiffe`) to an account; if the identity is not yet enrolled in the local TOTP database, Fiber logs a non-blocking warning and proceeds without interrupting the operation:

```bash
# Run full lifecycle walkthrough (Provision -> TopUp -> Stale Replay Check -> Revoke)
$ fiber account demo

# List all tracked local accounts and their bound SPIFFE identities from the registry
$ fiber account list

# Provision $50.0 (50,000 Fuel) via EdDSA JWT, bind a SPIFFE ID, and seal to the linear PTA ledger
$ fiber account --tenant accounting_dept --spiffe fiber jwt --usd 50.0 --op PROVISION --ver 1 --sync

# Trigger a real-time Pub/Sub REVOKE kill-switch to sever active streams mid-flight
$ fiber account --tenant accounting_dept kill --ver 99
```

---

## 4. Pipeline Performance Benchmarks (Apple M2, 32GB RAM)

These metrics measure the **pure internal overhead** of the Fiber LLM Pipeline. To eliminate network variance and isolate the framework's performance, the suite was executed in deterministic **VCR Replay** mode.

**Run the benchmark with VCR driving 200 concurrent streams locally:**

```bash
fiber e2e inter.concurrency --vcr replay --concurrency 200
```

| Performance Dimension | Metric (M2, 32GB) | Architectural Significance |
| --- | --- | --- |
| **Unary Throughput** | **~1,180 req/s** | Handles over 1,000 enterprise control-plane pipeline transactions per second in a single Python process. |
| **Stream Interleaving** | **~2,200 chunks/s** | High-density multiplexing of asynchronous byte-streams (SSE) without blocking the main event loop. |
| **Routing Overhead** | **p50: <1.0ms / p95: <1.5ms** | Negligible framework-level latency added to the Time-To-First-Token under heavy concurrent bursts. |
| **Memory Footprint** | **<1 MB (Unary) / ~25 MB (Streams)** | Near-zero GC pressure; safely buffers 200 highly active parallel streams within strict memory boundaries. |
| **Context Isolation** | **Guaranteed Isolation (Zero Cross-talk)** | Flawless asynchronous coroutine interleaving with deterministic state safety across parallel contexts. |

**Measuring Actual LLM Performance?**
If you want to benchmark actual LLM inference metrics (like TTFT and Chunks/sec) against a live provider or local server, run the suite in `record` or live mode and specify your target model.

```bash
# Example: Stress-testing a local Gemma-3 model with 200 concurrent VUs
$ fiber e2e inter.concurrency --vcr record --model llama_server/gemma-3-1b-it-Q4_K_M.gguf --concurrency 200
```

---

## 5. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* **[ex.switch.log](./phase/abc/log/vcr/ex.switch.llama_server.20260929.log):** Validates the legacy migration, confirming that module aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.
* **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming strict Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* **[gateway.wasm.log](./phase/abc/log/inter/wasm/gateway.wasm.20261002.log):** Validates Firewall and FSM stability, confirming that the Pest AST parser strictly enforces URN grammar while rejecting payload injections, and that all branching and linear PTA state transitions execute deterministically with millisecond latency.
* **[inter.finops.log](./phase/abc/log/inter/finops.20261019.log):** Validates the FinOps control plane, confirming DPoP-authenticated MCP multiplexing, cryptographic mandate translation (with strict idempotency and replay protection), automated ledger reconciliation, and sub-second kill-switch intervention that cleanly severs active streams mid-flight.
* **[mcp.guard.log](./phase/abc/log/worker/guard.20261022.log):** Validates zero-code HITL governance for MCP servers, confirming that safe tool calls pass through instantly while high-risk payloads pause for Slack approval (via 6-digit TOTP or 1-Click buttons)—spawning third-party stdio sandboxes only after human verification.