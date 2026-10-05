# fiber.README
**Deterministic LLM Gateway**

Fiber is a proxy gateway designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities (such as OOM and API billing runaways) while providing a deterministic execution environment that unifies heterogeneous LLM integrations and enables idempotent offline testing.

This document provides a practical guide on how to integrate and deploy Fiber across its operational pillars:

Serialize network traffic into local JSON fixtures for idempotent offline testing and precise latency emulation (**1.2**). Fiber operates as a drop-in asynchronous pipeline (**1.1**) that autonomously normalizes heterogeneous LLM schemas on the fly (**1.3**)—achieving execution determinism without altering your business logic.

Additionally, this guide covers **[2] Installation**, **[3] CLI Deployment (connect, daemon, e2e)**, and **[4] System Validation Logs** to help you quickly provision and validate your infrastructure.

---

## 1. LLM VCR & Pipeline

### 1.1. The Drop-In LLM Pipeline

Fiber reimagines LLM routing by Python facade with a strict, netty style asynchronous pipeline under the hood. 

Serving as a drop-in replacement for standard OpenAI and LiteLLM SDKs, this architecture achieves execution transparency without altering a single line of your business logic. Furthermore, because the core pipeline is decoupled from parsing logic, extending support for cutting-edge proprietary models becomes instantly achievable when paired with Fiber's State Traverser **[1.3]**.

> **Unlike passive callbacks, this event-driven pipeline empowers middleware to actively short-circuit I/O, physically intercept streams, and tunnel deep infrastructural states.**

**1. Define Middleware by Target Slot:**

```python
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from xphi.state.phase.channel import DuplexChannel

# PRE_OBSERVER: Asynchronous Telemetry with Stream-Aware Lifecycle
class DatadogTracer(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        # Stream Mode: Defer telemetry until the inner pipeline exhausts the chunks
        if hasattr(response, "__aiter__"):
            def on_stream_complete(total_tokens: int):
                # Calculate the true end-to-end latency when the last chunk arrives
                real_duration_ms = (time.time() - meta.framework_flags["start_time"]) * 1000
                datadog.gauge("llm.latency", real_duration_ms, tags=[f"model:{meta.base_model}", "type:stream"])
                datadog.count("llm.tokens", total_tokens, tags=[f"model:{meta.base_model}"])
            
            # Piggyback the callback onto the framework's hook lifecycle
            hooks = meta.framework_flags.setdefault("on_stream_complete_hooks", [])
            hooks.append(on_stream_complete)
            return

        # Singular Mode: Instant physical I/O completion
        datadog.gauge("llm.latency", duration_ms, tags=[f"model:{meta.base_model}", "type:singular"])

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
            raise PermissionError("Guardrail Block: PII detected.") # Active pipeline break
        await ctx.fire_write(processed_msg)
```

**2. Execute via Drop-in Facade:**

```python
from fiber.llm.entry import acompletion

# The framework autonomously restructures the flat list into the strict pipeline:
# [Cache] ➔ [Translator] ➔ [PII Guardrail] ➔ [Tracer] ➔ [Network I/O]
response = await acompletion(
    model="gemini/gemini-3.1-flash-lite", # ex: ollama/gemma:2b, llama_server/gemma-3-1b-it-Q4_K_M.gguf
    messages=[{"role": "user", "content": "Analyze this data."}],
    interceptors=[DatadogTracer(), PIIGuardrail(), SemanticCache()],
    metadata={"kernel_auth": {"audit_hash": "audit_12345"}} 
)
```

> **Note on Drop-in Replacement:** Fiber operates as a drop-in replacement by maintaining compatibility with OpenAI/LiteLLM entrypoints, Pydantic return objects, and local token utilities. The following specifications detail how to preserve your existing application logic while integrating asynchronous telemetry and cost tracking.
> 🔗 **[Entrypoint Spec](./phase/abc/dev/llm/entry.md)** 
> 🔗 **[Token Utilities Spec](./phase/abc/dev/llm/token.md)** 
> 🔗 **[Cost Tracker Spec](./phase/abc/dev/llm/cost.tracker.md)**

---

### 1.2. LLM VCR

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

For applications heavily coupled to third-party SDKs (e.g., LiteLLM), establishing offline tests often requires complex refactoring. Fiber solves this through transparent runtime routing.

By declaring explicit aliases at the boot sequence, Fiber intercepts legacy imports and routes traffic directly to its VCR engine. This enables deterministic playback and time-window stream coalescing without altering your business logic. Fiber also maintains duck-typing parity, returning exact mock objects to satisfy strict legacy type checks.

```python
import os
import sys
import asyncio

"""Boot Sequence: Establishing the Isolation Layer"""
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()

if VCR_MODE in ("record", "replay"):
    from fiber.phase.cli.sandbox import create_security_sandbox
    from fiber.dev.ex.space.bind.redirector import PhaseAirlock
    import fiber.llm.entry as llm_entry
    import fiber.llm.response as llm_response
    
    # Enforce strict PEP-578 security boundaries
    create_security_sandbox(vcr_mode=VCR_MODE)
    
    # Transparently route legacy SDK imports to Fiber's gateway
    PhaseAirlock.alias({
        "litellm": llm_entry.__name__,
        "litellm.types.utils": llm_response.__name__
    })
    
    # Mount the VCR engine for deterministic testing and traffic coalescing
    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    
    config = VCRPlaybackConfig(mode=VCR_MODE, speed="real", record_tick_ms=100.0)
    VCRInjector.apply(config=config, fixture_dir="./fixtures")

"""Legacy Business Logic (Unmodified Boundary)"""
import litellm 
from litellm.types.utils import ModelResponseStream

async def main():
    # Fiber processes this standard call. The `metadata` acts as a bridge, 
    # guiding the underlying engine to manage deterministic fixture routing.
    response = await litellm.acompletion(
        model="gemini/gemini-3.1-flash-lite",
        messages=[{"role": "user", "content": "Explain migration strategies."}],
        stream=True,
        metadata={
            "vcr_scenario": "tech_debt_migration",
            "vcr_invoker": "legacy_app"
        }
    )
    
    async for chunk in response:
        # Duck-typing parity: Legacy type-checks continue to pass
        assert isinstance(chunk, ModelResponseStream)
        
        # Standard legacy parsing remains flawless
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

### 1.3. State Traverser & Compat Registry

The LLM ecosystem is fragmented. Fiber eliminates brittle `if/elif` parsing logic through its **Rule-based Traverser**. Using dot-notation and **Path Fallbacks**, it seamlessly navigates mixed topologies (Dicts, Lists, Pydantic Objects) across request parameters, sync responses, and async stream chunks.

**Provider Extension:**
To integrate a new provider or override existing parsing logic, you no longer need to modify Python code. Simply map their schema in an external JSON file and inject it via the `FIBER_COMPAT_RULES_PATH` environment variable.

Fiber loads this registry exactly once at boot-time. It performs strict Pydantic schema validation (Fail-Fast) and deep-merges the rules. Invalid formats are safely ignored with a warning (Partial Update).

**Ex: Export env - FIBER_COMPAT_RULES_PATH=/etc/fiber/compat_rules.json**

```json
{
  "provider_param_rules": {
    "my_custom_llm": {
      "supported": ["temperature", "max_tokens", "stream"],
      "tool_format": "standard"
    }
  },
  "stream_extraction_rules": {
    "ollama": {
      "text": ["message.content", "choices.0.delta.content"],
      "usage": [
        "usage", 
        {"prompt_tokens": "prompt_eval_count", "completion_tokens": "eval_count"}
      ],
      "is_finished_cond": {"path": "done", "value": true}
    }
  },
  "state_extraction_rules": {
    "gemini": {
      "sync_content_paths": ["candidates.0.content.parts.0.text", "choices.0.message.content"],
      "fallback_tool_name": "content.parts.0.function_call.name"
    }
  }
}
```

**Community Validation**: To guarantee these custom rules work flawlessly, you can instantly validate them offline using `fiber compat fixture --vcr-mode test`. The community actively maintains these raw network fixtures in the `fiber-compats` repository, ensuring perfect parsing parity across the ever-evolving LLM landscape.

---

## 2. Installation & Infra Provisioning

**Prerequisites**
* **Python**: `>= 3.12`
* **Redis**: Required as the core message broker for asynchronous event streaming, pub/sub routing.

We recommend using `uv pip` for strict dependency resolution.

```bash
## 1. Create a dedicated sandbox environment
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

## 2. Install via local source OR remote git reference
uv pip install /path/to/local/self/fiber
# OR: uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.1

## 3. Verify anchor (Anchors to ~/.anchor/bound.json)
fiber --help
```

---

## 3. Fiber CLI Tool

The `fiber` CLI is a **Deployment Entrypoint**, dynamically assigning the appropriate node profile and delegating execution. It transparently forwards unknown arguments directly to the target module to ensure low friction scalability.

| Mode | Description | Example |
| --- | --- | --- |
| **`connect`** | **[Egress Sidecar]** Transforms any legacy MCP server into an autonomous node, securely connecting standard I/O to the distributed network. | `fiber connect -m multiplex -t oracle -e "python legacy_server.py"` |
| **`daemon`** | **[Production Host]** Boots core gateway daemons (Edge + RPC) by default. Use `-s` to apply presets (`eco`, `full`). | `fiber daemon -s eco` |
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e llm.trace --model ollama/gemma:2b` |
| **`compat`** | **[Compat & VCR]** Manages local extraction rules and generates/verifies VCR fixtures to guarantee LLM compatibility offline | `fiber compat fixture --model gemini/gemini-3.1-flash-lite --vcr-mode record` |
---

## 4. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* **[llm.vcr.log](./phase/abc/log/vcr/e2e.vcr.20260920.log):** Validates the VCR engine's core orchestration, confirming offline network emulation, deterministic Trace ID assignment via context tunneling, and precise time-window (100ms) chunk coalescing for playback optimization.
* **[ex.switch.log](./phase/abc/log/vcr/ex.switch.llama_server.20260929.log):** Validates the legacy migration, confirming that module aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.
* **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming strict Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* **[gateway.wasm.log](./phase/abc/log/wasm/gateway.wasm.20261002.log):** Validates Firewall and FSM stability, confirming that the Pest AST parser strictly enforces URN grammar while rejecting payload injections, and that all non-linear FSM transitions execute deterministically with millisecond latency.