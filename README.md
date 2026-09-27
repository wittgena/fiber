# fiber.README
**Zero Trust Gateway for LLMs and Agents**

Fiber is a proxy gateway designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities (such as OOM and API billing runaways) while providing a deterministic execution environment that unifies heterogeneous LLM integrations and enables idempotent offline testing.

This document provides a practical guide on how to integrate and deploy Fiber across its core operational pillars:

* **[1] LLM VCR & Pipeline:** Serialize network traffic into local JSON fixtures for idempotent offline testing and precise latency emulation (**1.2**). Fiber operates as a drop-in asynchronous pipeline (**1.1**) that autonomously normalizes heterogeneous LLM schemas on the fly (**1.3**)—achieving execution determinism without altering your business logic.
* **[2] MCP Gateway:** Decouple external network ingress (Edge) from worker(mcp server) execution. Securely route high-concurrency I/O workloads, enforce cryptographic identity (DPoP), and gracefully handle Human-in-the-Loop (HITL) transaction suspensions using non-blocking asynchronous state management.

Additionally, this guide covers **[3] Installation**, **[4] CLI Deployment (connect, daemon, e2e)**, and **[5] System Validation Logs**—focusing on verifiable execution proofs rather than raw speed benchmarks—to help you quickly provision and validate your infrastructure.

---

## 1. LLM VCR & Pipeline

### 1.1. The Drop-In LLM Pipeline

Fiber reimagines LLM routing by Python facade with a strict, netty style asynchronous pipeline under the hood. 

Serving as a drop-in replacement for standard OpenAI and LiteLLM SDKs, this architecture achieves execution transparency without altering a single line of your business logic. Furthermore, because the core pipeline is decoupled from parsing logic, extending support for cutting-edge proprietary models becomes instantly achievable when paired with Fiber's Universal State Traverser **[1.3]**.

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
            raise PermissionError("Guardrail Block: PII detected.") # Active pipeline break
        await ctx.fire_write(processed_msg)
```

**2. Execute via Drop-in Facade:**

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

For applications heavily coupled to third-party SDKs (e.g., LiteLLM), establishing offline tests often requires complex refactoring. Fiber eliminates this friction via PhaseAirlock runtime routing.

By declaring explicit aliases at the boot sequence, PhaseAirlock intercepts legacy imports and routes traffic to Fiber's VCR engine. This grants your existing codebase immediate access to deterministic playback and time-window stream coalescing—without altering a single line of business logic. Fiber ensures duck-typing parity, returning exact mock objects so that strict legacy type checks continue to function.

To guarantee offline determinism, Fiber simultaneously injects a PEP-578 Security Sandbox at the CPython boundary. This low-level audit hook physically intercepts OS-level operations, instantly blocking unexpected external network connections (socket.connect) or subprocess executions from upstream dependencies during replay.

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
    import fiber.llm.param as llm_param
    
    # Enforce strict PEP-578 security boundaries
    create_security_sandbox(vcr_mode=VCR_MODE)
    
    # Transparently route legacy SDK imports to Fiber's gateway
    PhaseAirlock.alias({
        "litellm": llm_entry.__name__,
        "litellm.types.utils": llm_param.__name__
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

### 1.3. Universal State Traverser

The LLM ecosystem is highly fragmented. Local inference servers and new providers often introduce proprietary JSON schemas for streaming chunks and tool calls. Fiber eliminates the need for `if/elif` parsing blocks through its `StateTraverser` engine.

Powered by dot-notation, the traverser safely navigates mixed topologies (Dicts, Lists, Pydantic Objects), absorbing missing keys or index errors without crashing the pipeline.

**Extending Fiber for a New Provider:**
Integrating a non-OpenAI-compliant provider doesn't requires custom parsing logic. Simply append their JSON topology to the internal declarative rulesets, and Fiber will autonomously normalize streams, responses, and tool calls into strict OpenAI standards.

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

---

## 2. MCP Gateway

Fiber decouples external HTTP ingress (Edge) from worker(mcp server) execution to securely manage state without burdening the client. Executions are routed via **Tri-Track Concurrency**:

* **`Ephemeral`**: Single-use, fault-isolated sandboxes per request (prevents OOM).
* **`Linear`**: Sequential execution for CPU-heavy workloads.
* **`Multiplex`**: High-concurrency for I/O-bound operations using non-blocking asynchronous routing.

### 2.1. The Edge Control Plane (`serv.edge`)
The Core Gateway acts as the network perimeter, handling distributed state and security autonomously:
* **Stateful Suspend & Resume:** Parks transactions awaiting human input (`YIELD`) and automatically injects a `RESUME` intent to the worker upon receiving the OTP.
* **Edge-Level Security:** Blocks replay attacks and validates agent identity via DPoP (Demonstrating Proof-of-Possession).

### 2.2. The Execution Worker (`AsyncWorkerProtocol`)
By inheriting `AsyncWorkerProtocol`, your standard scripts become robust MCP servers. The protocol abstracts away complex non-blocking asynchronous operations—such as prompting users for input or delegating RPC calls—so you can focus entirely on business logic.

Below is a worker (`deployer.py`) handling a Human-in-the-Loop (HITL) database migration, demonstrating how easily you can pause execution for user confirmation and verify cryptographic attestations:

```python
# fiber.dev.ex.worker.deplyer
import asyncio
from cryptography.hazmat.primitives.asymmetric import ed25519
from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol

class ExecutionDeployer(AsyncWorkerProtocol):
    def __init__(self):
        super().__init__(agent_name="execution.deployer")
        self.validator_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(...)
        self.pending_prompts = {}

    async def _route_request_async(self, req):
        req_id = req.get("id") or req.get("payload", {}).get("id")
        if req.get("action") == "RESUME" or ("method" not in req and "result" in req):
            if req_id in self.pending_prompts:
                self.pending_prompts.pop(req_id).set_result(req.get("payload", req))
            return
            
        await super()._route_request_async(req)

    async def handle_tools_call(self, req_id, tool_name, args, meta):
        if tool_name == "execute_db_migration":
            sql = args.get("sql_script", "").upper()
            if "DROP" in sql:
                # Park transaction via Elicitation (Yields execution back to Edge)
                self.pending_prompts[req_id] = asyncio.get_running_loop().create_future()
                await self.send_request(req_id, "elicitation/createMessage", {"message": "Enter TOTP:"})
                otp_res = await asyncio.wait_for(self.pending_prompts[req_id], timeout=300.0)
                
                # Delegate attestation to backend Validator via Connector RPC
                del_id = f"delegate_{req_id}"
                self.pending_prompts[del_id] = asyncio.get_running_loop().create_future()
                await self.send_request(del_id, "rpc_delegate", {
                    "target_method": "validate.attest", 
                    "data": {"otp_code": otp_res.get("value")}
                })
                delegate_res = await asyncio.wait_for(self.pending_prompts[del_id], timeout=20.0)
                self.validator_pub_key.verify(
                    signature=bytes.fromhex(delegate_res["result"]["signature"]), 
                    data=payload_hash
                )

            # Execute upon successful verification
            await self.send_response(req_id, {"content": "Migration executed."})

if __name__ == "__main__":
    asyncio.run(ExecutionDeployer().serve_forever_async())
```

---

## 3. Installation & Infra Provisioning

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
# OR: uv pip install git+https://github.com/wittgena/fiber.git@v1.1.2

## 3. Verify anchor (Anchors to ~/.anchor/bound.json)
fiber --help
```

---

## 4. Fiber CLI Tool

The `fiber` CLI is a **Deployment Entrypoint**, dynamically assigning the appropriate node profile and delegating execution. It transparently forwards unknown arguments directly to the target module to ensure low friction scalability.

| Mode | Description | Example |
| --- | --- | --- |
| **`connect`** | **[Egress Sidecar]** Transforms any legacy MCP server into an autonomous node, securely connecting standard I/O to the distributed network. | `fiber connect -m multiplex -t oracle -e "python legacy_server.py"` |
| **`daemon`** | **[Production Host]** Boots core gateway daemons (Edge + RPC) by default. Use `-s` to apply presets (`eco`, `full`). | `fiber daemon -s eco` |
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e llm.trace --model gemini/gemini-3.1-flash-lite` |

---

## 5. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[llm.vcr.log](./phase/abc/log/vcr/e2e.vcr.20260920.log):** Validates the VCR engine's core orchestration, confirming offline network emulation, deterministic Trace ID assignment via context tunneling, and precise time-window (100ms) chunk coalescing for playback optimization.
* 🔗 **[ex.switch.log](./phase/abc/log/vcr/ex.switch.20260920.log):** Validates the zero-code legacy migration, confirming that module aliasing seamlessly intercepts legacy SDK calls, normalizes heterogeneous streams, and achieves duck-typing parity during real-time VCR playback.
* 🔗 **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming strict Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* 🔗 **[gateway.worker.log](./phase/abc/log/gateway/worker.log.20260912.log):** Validates the Zero-Trust MCP Gateway's core routing pipeline, confirming native async multiplexing, high-throughput linear queue stress resilience, precise error isolation, and autonomic dynamic pricing with strict x402 billing defense.
* 🔗 **[phase.wasm.log](./phase/abc/log/dphi/phase.wasm.20260923.logw):** Validates deterministic execution across Ephemeral sandboxes, confirming precise Resource Exhaustion Traps (OOM / CPU Time Limits), and Execution Receits generation.