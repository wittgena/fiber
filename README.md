# README
**Zero Trust Gateway for LLMs & Agentic AI**

Fiber is a proxy gateway designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities inherent in modern stateless protocols (like MCP)—such as memory leaks (OOM), confused deputy attacks, and API billing runaways.

This document provides a practical guide on how to integrate and deploy Fiber across its core operational pillars:

* **[1.1] The Drop-In LLM Pipeline:** Replace standard OpenAI/LiteLLM SDKs with a strict, Netty-driven asynchronous core to transparently orchestrate execution traces and enforce hard budget limits.
* **[1.2] LLM Record & Replay:** How to serialize network traffic into local JSON fixtures for idempotent offline testing and precise latency profiling.
* **[1.3] Secure MCP Gateway:** How to safely connect legacy REST systems to AI agents using zero-trust execution sandboxes—without altering existing code.
* **[1.4] Universal State Traverser**: Instantly support any new LLM provider without writing custom parsers. Just map their JSON topology `{"text": "message.content"}` and let the Traverser autonomously normalize streams, responses, and tool-calls.

Additionally, this guide covers **[2] Installation** and **[3] CLI Deployment (connect, daemon, e2e)** to help you quickly provision your infrastructure.

---

## 1. Core Pillars in Action

### 1.1. The Drop-In LLM Pipeline (Netty-Driven Routing & Trace)

Fiber fundamentally reimagines LLM routing by marrying a **developer-friendly Python facade** with a **strict, Netty-style asynchronous pipeline** under the hood. 

Serving as a flawless **drop-in replacement** for standard OpenAI and LiteLLM SDKs, this architecture achieves unprecedented execution transparency without altering a single line of your business logic. It effortlessly orchestrates deep execution tracing, network recording, and budget controls. 

Furthermore, because the core pipeline is completely decoupled from parsing logic, extending support for cutting-edge proprietary models becomes instantly achievable when paired with Fiber's Universal State Traverser **[1.4]**.

* **Fuel Breaker:** Strictly prevents unexpected billing spikes by physically terminating the TCP connection if a streaming response exceeds its predefined token budget.
* **Declarative Tool Recovery:** Dynamically detects and normalizes malformed tool calls from heterogeneous LLMs (e.g., Gemini) into the strict OpenAI standard format.
* **Slot-based Middleware (UX Facade):** Safely inject custom plugins (e.g., Datadog Tracers, Semantic Caches, PII Guardrails) using a simple flat list (`interceptors=[]`). The Entry Facade autonomously routes them to designated lifecycle slots without blocking the main I/O or risking core pipeline corruption.

**1. Define Middleware by Target Slot:**

```python
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from xphi.state.phase.channel import DuplexChannel

# PRE_OBSERVER: Fire-and-forget telemetry
class DatadogTracer(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        datadog.gauge("llm.latency", duration_ms, tags=[f"model:{meta.base_model}"])

# PRE_TRANSLATE: Intercept raw dict payload for instant Semantic Caching
class SemanticCache(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    async def write(self, ctx, msg: dict):
        if "USE_CACHE" in str(msg):
            return await ctx.fire_channel_read(mock_response) # Short-circuit physical I/O
        await ctx.fire_write(msg)

# POST_TRANSLATE: Enforce Security Policies on strict Pydantic objects
class PIIGuardrail(DuplexChannel):
    target_slot = PipelineSlot.POST_TRANSLATE
    async def write(self, ctx, processed_msg):
        if "SECRET-SSN" in str(getattr(processed_msg, "original_kwargs", {})):
            raise PermissionError("Guardrail Block: PII detected.") # Active pipeline rupture
        await ctx.fire_write(processed_msg)
```

**2. Execute via Drop-in Facade (Order-Agnostic Injection):**

```python
from fiber.llm.entry import acompletion

# The framework autonomously restructures the flat list into the strict pipeline:
# [Cache] ➔ [Translator] ➔ [PII Guardrail] ➔ [Tracer] ➔ [Network I/O]
response = await acompletion(
    model="gemini-3.5-flash",
    messages=[{"role": "user", "content": "Analyze this data."}],
    interceptors=[DatadogTracer(), PIIGuardrail(), SemanticCache()],
    metadata={"kernel_auth": {"audit_hash": "audit_12345"}} 
)
```

---

### 1.2. LLM Record & Replay

The VCR utility serializes LLM network traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables deterministic offline testing and precise historical latency emulation without altering business logic.

Fiber provides two approaches for VCR integration:

**Native Integration**

If using Fiber's SDK, the VCR can be injected globally. It wraps the AdapterRegistry, making standard acompletion calls recordable. The architecture enforces deterministic Trace ID generation and metadata tunneling to guarantee idempotent replay matching.

```python
import os, asyncio
from fiber.llm.entry import acompletion

# Decoupled VCR architecture: Storage & Interceptor
from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig, VCRIdentityRule
from fiber.dev.trace.llm.vcr.proxy import VCRInjector
from xphi.arch.bound.event.next import next_trace_id

# Inject VCR globally with Time-Window Coalescing
vcr_mode = os.environ.get("VCR_MODE", "live").lower()
config = VCRPlaybackConfig(mode=vcr_mode, speed="real", record_tick_ms=100.0)
VCRInjector.apply(config=config, fixture_dir="./fixtures")

async def main():
    scenario = "native_demo"
    messages = [{"role": "user", "content": "Count from 1 to 5."}]
    
    # Generate deterministic Trace ID to ensure Record & Replay target the exact same fixture
    seed = VCRIdentityRule.generate_seed(scenario_name=scenario, messages=messages, invoker="readme.app")
    trace_id = next_trace_id(seed) if vcr_mode in ("record", "replay") else next_trace_id()

    # Execute with explicit context tunneling
    response = await acompletion(
        model="gemini/gemini-3.1-flash-lite",
        messages=messages,
        stream=True,
        trace_id=trace_id,
        metadata={
            "vcr_scenario": scenario,
            "vcr_invoker": "readme.app"
        }
    )
    
    async for chunk in response:
        print(chunk.choices[0].delta.content or "", end="", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
```

**Transparent Integration for Legacy Codebases**

For existing applications heavily coupled to third-party SDKs (e.g., LiteLLM), migrating to a new gateway or establishing offline tests can be challenging. Fiber provides a Transparent Integration Path via standard sys.modules aliasing. This creates a safe, drop-in sandbox that grants your legacy codebase immediate access to the VCR engine and time-window stream coalescing—without requiring a massive refactoring of your business logic.

Crucially, the application code remains completely undisturbed. By simply including standard metadata in your existing API calls, Fiber's adapter gracefully routes the payload to the VCR engine. It ensures duck-typing parity, returning perfect mock objects during offline replays so that strict legacy type checks never fail.

```python
import os, sys, asyncio

"""Integration Bridge - Executes before legacy business logic loads"""
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()

if VCR_MODE in ("record", "replay"):
    import fiber.llm.entry as litellm_entry
    import fiber.llm.param as fiber_param
    
    # Transparently route legacy SDK imports to Fiber's gateway
    sys.modules["litellm"] = litellm_entry
    sys.modules["litellm.types.utils"] = fiber_param
    
    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    
    config = VCRPlaybackConfig(mode=VCR_MODE, speed="real", record_tick_ms=100.0)
    VCRInjector.apply(config=config, fixture_dir="./fixtures")

"""Legacy Business Logic (Unmodified)"""
import litellm 
from litellm.types.utils import ModelResponseStream

async def main():
    # Fiber gracefully processes this standard call. The `metadata` acts as a bridge, 
    # guiding the VCR engine to manage deterministic fixture files for testing.
    response = await litellm.acompletion(
        model="gemini/gemini-3.1-flash-lite",
        messages=[{"role": "user", "content": "Explain migration."}],
        stream=True,
        metadata={
            "vcr_scenario": "tech_debt_migration",
            "vcr_invoker": "legacy_app"
        }
    )
    
    async for chunk in response:
        assert isinstance(chunk, ModelResponseStream)
        
        # Standard legacy parsing continues to work flawlessly
        if hasattr(chunk, "choices") and chunk.choices:
            print(chunk.choices[0].delta.content or "", end="", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
```

---

**The Record & Replay Flow**

* **Step 1: Record (Capture & Coalesce Network I/O)**
Executes live API calls to the target LLM. The engine autonomously coalesces micro-chunks using a time-window (e.g., 100ms) and persists the normalized payloads and network metrics into strict, human-readable local fixtures.

```bash
# Record with 100ms chunk coalescing to optimize future playback
python -m fiber.dev.ex.recorder --vcr record --vcr-tick 100.0

```

* **Step 2: Replay (Offline Emulation & Chaos Injection)**
Streams the cached response fully offline with zero network I/O. You can emulate exact historical latencies (`--vcr-speed real`), run at maximum velocity for CI/CD pipelines (`--vcr-speed max`), or inject artificial latency jitter (`--vcr-chaos`) to validate your application's timeout resilience.

```bash
# Replay in real-time with a 500ms artificial chaos jitter
python -m fiber.dev.ex.recorder --vcr replay --vcr-speed real --vcr-chaos 500.0
```

---

### 1.3. Secure MCP Gateway

As agent protocols shift to stateless architectures, they push heavy complexities onto the client. The gateway's **Transition Bridge** absorbs this burden by decoupling HTTP ingress from physical execution. The gateway fundamentally relies on a symbiotic pair: an **Edge Node** (`rest_edge`) for HTTP ingress, and a **Compute Node** (`rpc_worker`) for state and auth management.

Instead of exposing host systems to unvalidated raw REST payloads, it translates intents into deterministic events routed via **Tri-Track Concurrency**:

* **`Ephemeral` Mode:** Instantiates single-use, fault-isolated sandboxes per request, ensuring zero memory leaks.
* **`Linear` Mode:** Routes CPU-heavy workloads sequentially to eliminate cold starts.
* **`Multiplex` Mode:** Unleashes extreme lock-free concurrency for thousands of I/O-bound operations.

```bash
## Boot the Core Gateway (Autostarts both 'rest_edge' and 'rpc_worker' by default)
fiber daemon

## Optional: Enable autonomic extensions (e.g., dynamic_pricing, risk_vault) via -s preset
# fiber daemon -s eco
# fiber daemon -s full

## Wrap and boot your legacy server script as an mcp server
fiber connect --target oracle-01 --mode multiplex --exec "python legacy_server.py"
```

---

### 1.4. Universal State Traverser

The LLM ecosystem is highly fragmented. Local inference servers and new providers often introduce proprietary JSON schemas for streaming chunks and tool calls. Fiber eliminates the need for messy `if/elif` parsing blocks through its `StateTraverser` engine.

Powered by dot-notation, the traverser safely navigates mixed topologies (Dicts, Lists, Pydantic Objects), silently absorbing missing keys or index errors without crashing the pipeline.

**Extending Fiber for a New Provider:**
Integrating a non-OpenAI-compliant provider requires zero custom parsing logic. Simply append their JSON topology to the internal declarative rulesets, and Fiber will autonomously normalize streams, responses, and tool calls into strict OpenAI standards.

```python
# Map Stream Chunks (e.g., fiber/llm/router/stream/parser/chunk.py)
# Safely resolve deeply nested lists and objects using dot-notation:
STREAM_EXTRACTION_RULES["ollama"] = {
    "text": "message.content",
    "finish_reason": "done_reason",
    "is_finished_cond": {"path": "done", "value": True},
    "usage": {
        "prompt_tokens": "prompt_eval_count",
        "completion_tokens": "eval_count"
    }
}

# Map State & Tool-Call Recovery (e.g., fiber/gateway/llm/mapper/traverser.py)
# Reconstruct complex tool calls from proprietary schemas without imperative code:
STATE_EXTRACTION_RULES["gemini"] = {
    "fallback_tool_name": "content.parts.0.function_call.name",
    "fallback_tool_args": "content.parts.0.function_call.args"
}

# Register routing aliases
PROVIDER_RULE_ALIAS["llama_server"] = "openai"
```

Once mapped, Fiber's core engine seamlessly parses the proprietary format. This guarantees that your business logic, metric tracers, and VCR coalescing engine support the new model flawlessly on day one.

---

## 2. Installation & Infra Provisioning

**Prerequisites**
* **Python**: `>= 3.12` (Required for strict asynchronous pipelines and dot-notation traversal)
* **Redis**: Required as the core message broker (Tunnel) for asynchronous event streaming, pub/sub routing, and distributed state management.
* *Note: Infrastructure routing (Host/Port) is safely managed via `xphi/arch/contract/config/env.py` using collision-safe prefixes (e.g., `XPHI_REDIS_HOST`).*

Fiber utilizes an integrated installation pipeline where `fiber` and its core dependency `xphi` are tightly coupled. We recommend using `uv pip` for strict dependency resolution.

```bash
## 1. Create a dedicated sandbox environment
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

## 2. Install via local source OR remote git reference
uv pip install /path/to/local/self/fiber
# OR: uv pip install git+https://github.com/wittgena/fiber.git@v1.1.2

## 3. Verify anchor (Anchors to ~/.anchor/bound.json)
fiber --help

```

---

## 3. Fiber CLI Tool

The `fiber` CLI is a **Deployment Entrypoint**, dynamically assigning the appropriate node profile and delegating execution. It transparently forwards unknown arguments directly to the target module to ensure zero-friction scalability.

| Mode | Description | Example |
| --- | --- | --- |
| **`connect`** | **[Egress Sidecar]** Transforms any legacy MCP server into an autonomous node, securely connecting standard I/O to the distributed network. | `fiber connect -m multiplex -t oracle -e "python legacy_server.py"` |
| **`daemon`** | **[Production Host]** Boots core gateway daemons (Edge + RPC) by default. Use `-s` to apply presets (`eco`, `full`). | `fiber daemon -s eco` |
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e llm.trace --model gemini/gemini-3.1-flash-lite` |

---

## 4. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[phase.wasm.log](./phase/abc/log/dphi/phase.wasm.20260923.logw):** Validates deterministic execution across Ephemeral sandboxes, confirming precise Resource Exhaustion Traps (OOM / CPU Time Limits), Distributed Execution Determinism recovery, Tripartite Parity recovery, and Cryptographic Proof generation (3bb93907...).
* 🔗 **[plane.flare.log](./phase/abc/log/plane/flare.20260917.log):** Validates V8 isolation sandboxing within Cloudflare Edge microservices, confirming absolute containment against host filesystem/socket breaches and ensuring Parity/FP determinism across distributed JS-Python workers.
* 🔗 **[dphi.clearing.log](./phase/abc/log/dphi/clearing.20260916.log):** Validates the WASM-based Clearing FSM and transaction pipeline, confirming deterministic edge defenses against invalid EIP-712 signatures, zero balances, and corrupted calldata via chaos injection.
* 🔗 **[edge.sandbox.log](./phase/abc/log/gateway/sandbox.20260911.log):** Validates the Edge Gateway's absolute perimeter defenses, confirming cryptographic Tamper-Resistance (Fail-Fast) of the origin state, zero-trust ingress signature validation, and Sentinel Chaos WAF resilience.
* 🔗 **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming physical Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* 🔗 **[llm.vcr.log](./phase/abc/log/vcr/e2e.vcr.20260920.log):** Validates the VCR engine's core orchestration, confirming zero-network offline emulation, deterministic Trace ID assignment via context tunneling, and precise time-window (100ms) chunk coalescing for extreme playback optimization.
* 🔗 **[ex.switch.log](./phase/abc/log/vcr/ex.switch.20260920.log):** Validates the zero-code legacy migration, confirming that `sys.modules` aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.