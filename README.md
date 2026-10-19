# README
**Deterministic LLM Pipeline**

Fiber is an asynchronous execution pipeline designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities (such as OOM and API billing runaways) while providing a deterministic runtime environment that unifies heterogeneous LLM integrations and enables idempotent offline testing.

This document provides a practical guide on how to integrate and deploy Fiber across its operational pillars:

Operating as a drop-in asynchronous pipeline with real-time quota enforcement and mid-stream kill-switches (**1.1**), Fiber serializes network traffic into local JSON fixtures for idempotent offline testing and precise latency emulation (**1.2**), while autonomously normalizing heterogeneous LLM schemas on the fly (**1.3**)—achieving execution determinism and strict cost governance without altering your business logic.

Additionally, this guide covers **[2] Installation**, **[3] CLI Deployment (connect, daemon, compat)**, and **[4] System Validation Logs** to help you quickly provision and validate your infrastructure.

---

## 1. LLM Pipeline & Fixture Testing

### 1.1. The Drop-In LLM Pipeline

Serving as a drop-in replacement for standard OpenAI and LiteLLM SDKs, Fiber's duplex pipeline achieves execution transparency without altering a single line of your business logic. Furthermore, because the core pipeline is decoupled from parsing logic, extending support for cutting-edge proprietary models becomes instantly achievable when paired with Fiber's State Traverser **[1.3]**.

> **Unlike passive callbacks, this event-driven pipeline empowers middleware to actively short-circuit I/O, physically intercept streams, and tunnel deep infrastructural states.**

**1. Define Middleware by Target Slot:**

```python
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.gateway.llm.pipeline.bootstrap import PipelineSlot
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

**Out-of-the-box Guardrails**: The snippets above are conceptual examples. Fiber includes production-grade samples in `fiber.dev.trace.llm`—including INT8 Quantized ONNX Routers, Active Secret Blocking, and Custom C-Level Regex Injection—ready for enterprise deployment.

**2. Execute via Drop-in Facade:**

```python
from fiber.llm.entry import acompletion
from fiber.gateway.llm.pipeline.channel.fuel import FuelBreaker, BillingObserver
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

The Offline Fixture Engine serializes LLM network traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables zero-cost, deterministic offline testing and precise historical latency emulation without altering business logic.

> **Why Fiber VCR?**
Standard HTTP mockers (like vcrpy) capture network-layer raw bytes, which breaks stream timing and bloats memory during LLM testing. Fiber’s Application-Layer VCR natively understands LLM semantics—autonomously coalescing micro-chunks via time-windows (e.g., 100ms) and ensuring strict duck-typing parity for SDKs like LiteLLM. 
Furthermore, because `FuelBreaker` is natively integrated into the recording lifecycle, streams terminated mid-flight by a budget kill-switch capture only the pre-cutoff chunks—allowing you to deterministically replay and test exact quota-exhaustion states offline.

Fiber provides three approaches for injecting the local fixture engine:

**Automated Testing (Pytest Standard)**

Adopting the industry-standard VCR pattern, Fiber empowers your pytest suites to act as the single source of truth. By toggling the VCR_MODE environment variable, a single test script can autonomously generate its own fixtures against live APIs (Record) and subsequently execute offline in CI/CD environments (Replay)—preventing any drift between request payloads and assertions.

```python
import os, pytest
from fiber.dev.trace.vcr.manager import VCRPlaybackConfig
from fiber.dev.trace.vcr.injector import VCRInjector
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

**The Record & Replay Flow**

Fiber unifies the operational workflow across all integration methods (Pytest, Native, Legacy) using environmental toggles.

* **Step 1: Record (Capture & Coalesce Network I/O)**
Executes live API calls to the target LLM. The engine autonomously coalesces micro-chunks using a time-window (e.g., 100ms) and persists the normalized payloads and network metrics into strict, human-readable local fixtures.

```bash
# Pytest: Generate suites via live API calls
$ VCR_MODE=record pytest -v fiber/dev/trace/llm/tests/router.py

# CLI: Record standard executions with 100ms tick optimization
$ python -m fiber.dev.ex.recorder --vcr record --vcr-tick 100.0
```

* **Step 2: Replay (Deterministic Playback & Chaos Injection)**
Streams the recorded fixture fully offline with zero network I/O. You can emulate exact historical latencies (`--vcr-speed real`), run at maximum velocity for CI/CD pipelines (`--vcr-speed max`), or inject artificial latency jitter (`--vcr-chaos`) to validate your application's timeout resilience.

```bash
# Pytest: Run suites entirely offline with maximum velocity (default replay mode)
$ pytest -v fiber/dev/trace/llm/tests/router.py

# CLI: Replay in real-time with a 500ms artificial chaos jitter
$ python -m fiber.dev.ex.recorder --vcr replay --vcr-speed real --vcr-chaos 500.0
```

---

### 1.3. State Traverser & Compat Registry

The LLM ecosystem is fragmented. Fiber eliminates brittle `if/elif` parsing logic through its **Rule-based Traverser**. Using dot-notation and **Path Fallbacks**, it seamlessly navigates mixed topologies (Dicts, Lists, Pydantic Objects) across request parameters, sync responses, and async stream chunks through a unified schema.

**Provider Extension:**
To integrate a new provider or override existing parsing logic, you no longer need to modify Python code. Simply map their request parameters and response extraction paths in an external JSON file and inject it via the `FIBER_COMPAT_RULES_PATH` environment variable.

Fiber loads this registry exactly once at boot-time. It performs strict Pydantic schema validation (Fail-Fast) and deep-merges the rules into the base registry. Invalid formats are safely ignored with a warning (Partial Update).

**Ex: Export env - FIBER_COMPAT_RULES_PATH=/etc/fiber/compat_rules.json**

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

## 2. Installation & Infra Provisioning

**Prerequisites**
- **Python**: `>= 3.12`
- **Redis** *(Optional)*: Only required for distributed modes (`fiber daemon`), pub/sub event routing, and real-time stateful governance (e.g., Active Fuel Breaker). Standalone pipeline execution and VCR testing run entirely in-memory without Redis.

### 2.1. Python Environment via `uv pip` (Recommended for SDK & VCR Testing)
Best suited for application-level integration (`from fiber.llm.entry import acompletion`), offline VCR fixture testing, and running `pytest` suites. We recommend using `uv pip` for strict dependency resolution:

```bash
## Create a dedicated sandbox environment
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

## Install via remote git reference OR local source
uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.7
# OR: uv pip install /path/to/local/self/fiber

## Verify anchor (Anchors to ~/.anchor/bound.json)
fiber --help
```

### 2.2. macOS Homebrew Tap *(For Local Daemon & MCP Gateway)*

Best suited for running Fiber as a standalone background service (`fiber daemon`) to govern local AI agents via the **MCP Gateway** without managing Python virtual environments.

> **Note (Experimental):** The Homebrew packaging pipeline is currently undergoing stabilization across macOS environments (e.g., Xcode Command Line Tools & build metadata bindings). If you encounter environment-specific build issues, please fall back to the `uv pip` installation above.

```bash
## Install Fiber and its isolated runtime in a single command
brew install wittgena/fiber/fiber

## Verify CLI or start the background daemon service
fiber --help
brew services start wittgena/fiber/fiber
```

---

## 3. MCP Gateway & Fiber Connect

The **Fiber Connect CLI** (`fiber`) is the unified deployment and orchestration entrypoint. It dynamically configures node profiles, mounts autonomous MCP workers onto the distributed Tunnel Bus, and delegates execution across local and remote boundaries with zero boilerplate.

### 3.1. MCP Gateway

Connecting AI agents (Claude Code, Cursor, VSCode, Zed) directly to raw MCP servers via point-to-point pipes exposes host systems to retry-induced state corruption, unapproved tool execution, and credential leaks. Fiber resolves this by absorbing your existing agent configurations onto the Tunnel Bus and exposing them through a stateless **MCP Gateway** (`/v1/mcp-gateway/invoke`, `/tools`, or per-target `/v1/mcp-gateway/{target_id}/invoke`).

**Why MCP Gateway?**
Standard MCP clients treat tool calls as fire-and-forget pipes: on network timeouts, agents blindly retry or hallucinate parameters, causing duplicate database writes or runaway API charges. Fiber’s MCP Gateway interposes a deterministic control plane between the agent and the tool:

  - **Idempotent Execution:** Maps every invocation to a deterministic transaction handle. Duplicate agent retries immediately return cached resolutions without re-executing the underlying tool, while upstream timeouts trigger automatic rollbacks.
  
  - **Human-in-the-Loop Authorization:** On high-risk operations (e.g., destructive mutations), the gateway parks the execution sandbox (`HTTP 202`) and awaits explicit user approval (TOTP/CLI) before resuming from the exact suspended state.

  - **Credential Isolation & Stream Sanitization:** Injects external API keys (`Bearer` tokens) strictly at the egress boundary so secrets never enter the agent's context, while stripping non-JSON `stdout` noise before it corrupts the agent's parser.

  - **FinOps Governance**: Translates external, EdDSA-signed billing tokens into synchronized internal ledger states. It enforces strict transaction idempotency (preventing double-spending), rejects stale policy replays, and broadcasts sub-second kill-switches across the control plane to halt runaway costs instantly.

**Agent Onboarding**
Instead of manually supervising fragmented MCP processes, Fiber automatically scans and normalizes existing agent configs (`.mcp.json`, `.cursor/mcp.json`, `.vscode/mcp.json`) alongside built-in workers.

To route your agent through Fiber, simply inject the `fiber-gateway` entry into your agent's configuration file:

```json
{
  "mcpServers": {
    "fiber-gateway": {
      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"
    },
    "repomix": {
      "command": "npx",
      "args": ["-y", "repomix", "--mcp"]
    }
  }
}
```

**Coexistence**: Fiber's declarative mapper automatically ignores the self-referential fiber-gateway entry during scanning while mounting the remaining servers. Once verified, you can progressively remove redundant direct entries at your own pace.

**Sidecar Provisioning (fiber connect)**
While fiber daemon autonomously manages workers in production, `fiber connect` acts as an on-demand sidecar to mount specific config files or individual workers, automatically inferring transport (`stdio` vs. `network`) and concurrency modes (`multiplex`, `linear`, `ephemeral`):

```bash
# Auto-Scan or Explicit Config: Mounts all servers defined in mcp.json
$ fiber connect -c .mcp.json

# Built-in Worker: Mounts worker with auto-inferred target & mode
$ fiber connect -e worker.docs

# Remote Commercial MCP: HTTP/SSE multiplexing with Bearer auth injection
$ fiber connect -t ctx7-remote -e https://mcp.context7.com/mcp -f .env
```

### 3.2. Fiber Connect CLI

| Command | Role | Architectural Function & Lifecycle | Example |
| :--- | :--- | :--- | :--- |
| **`connect`** | **[Egress Sidecar]** | Mounts built-in workers, agent configs (mcp.json), or remote HTTP/SSE endpoints as governed nodes on the Tunnel Bus. | `fiber connect -e worker.docs` |
| **`daemon`** | **[Production Host]** | Ignites the control-plane kernel (`rest_edge`, `rpc_worker`, `mcp_worker`). Use `-s` to apply topology presets (`core`, `all`). | `fiber daemon -s all -f .env` |
| **`compat`** | **[Compat & VCR]** | Records raw provider payloads (record) and verifies declarative extraction rules offline (replay) for schema parity. | `fiber compat fixture -m gemini/gemini-3.1-flash-lite --vcr record` |
| **`account`** | **[FinOps & Quota]** | Manages tenant budgets, issues cryptographic billing tokens, synchronizes usage ledgers, and triggers real-time stream kill-switches. | `fiber account jwt --usd 50 --op PROVISION --sync` |

**FinOps Account & Quota Governance (`fiber account`)**
Provides a unified CLI to govern tenant quotas and billing lifecycles. It seamlessly translates stateless billing tokens into enforceable runtime quotas—handling idempotent top-ups, versioned policy enforcement, and instant kill-switch broadcasting without exposing underlying ledger complexities:

```bash
# Run full lifecycle walkthrough (Provision -> TopUp -> Stale Replay Check -> Revoke)
$ fiber account demo

# Provision $50.0 (50,000 Fuel) via EdDSA JWT and synchronize the PTA ledger
$ fiber account --tenant accounting_dept jwt --usd 50.0 --op PROVISION --ver 1 --sync

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

* **[llm.vcr.log](./phase/abc/log/vcr/e2e.vcr.20260920.log):** Validates the VCR engine's core orchestration, confirming offline network emulation, deterministic Trace ID assignment via context tunneling, and precise time-window (100ms) chunk coalescing for playback optimization.
* **[ex.switch.log](./phase/abc/log/vcr/ex.switch.llama_server.20260929.log):** Validates the legacy migration, confirming that module aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.
* **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming strict Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* **[gateway.wasm.log](./phase/abc/log/inter/wasm/gateway.wasm.20261002.log):** Validates Firewall and FSM stability, confirming that the Pest AST parser strictly enforces URN grammar while rejecting payload injections, and that all non-linear FSM transitions execute deterministically with millisecond latency.
* **[inter.finops.log](./phase/abc/log/inter/finops.20261019.log):** Validates the FinOps control plane, confirming DPoP-authenticated MCP multiplexing, cryptographic mandate translation (with strict idempotency and replay protection), automated ledger reconciliation, and sub-second kill-switch intervention that cleanly severs active streams mid-flight.