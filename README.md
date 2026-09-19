# fiber.README

@desc: AI Agent Gateway

Fiber is a proxy gateway designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities inherent in modern stateless protocols (like MCP)—such as memory leaks (OOM), confused deputy attacks, and API billing runaways.

This document provides a practical guide on how to integrate and deploy Fiber across its core operational pillars:

* **[1.1] LLM Routing, Trace:** How to use Fiber as a drop-in replacement for standard LLM SDKs to route traffic, enforce network-level budget limits, and manage execution traces.
* **[1.2] LLM VCR (Record & Replay Engine):** How to serialize network traffic into local JSON fixtures for idempotent offline testing and precise latency profiling.
* **[1.3] Secure Agentic Bridge (MCP Gateway):** How to safely connect legacy REST systems to AI agents using zero-trust execution sandboxes—without altering existing code.
* **[1.4] Audit Log & Compliance Engine:** How to record verifiable Merkle-proof receipts for agent actions.

Additionally, this guide covers **[2] Installation** and **[3] CLI Deployment (connect, daemon, e2e)** to help you quickly provision your infrastructure.

---

## 1. Core Pillars in Action

### 1.1. LLM Routing, Trace

The fiber.llm.entry module is a high-performance LLM router that provides a drop-in replacement for the OpenAI SDK and LiteLLM. Beyond simple routing, it utilizes a strict, Netty-style asynchronous pipeline to seamlessly integrate execution tracing, network recording, and budget controls without compromising the developer-friendly UX.

* **Fuel Breaker:** Strictly prevents unexpected billing spikes by physically terminating the TCP connection if a streaming response exceeds its predefined token budget.
* **Declarative Tool Recovery:** Dynamically detects and normalizes malformed tool calls from heterogeneous LLMs (e.g., Gemini) into the strict OpenAI standard format.
* **Slot-based Middleware (UX Facade):** Safely inject custom plugins (e.g., Datadog Tracers, Semantic Caches, PII Guardrails) using a simple flat list (`interceptors=[]`). The Entry Facade autonomously routes them to designated lifecycle slots without blocking the main I/O or risking core pipeline corruption.

**1. Define Middleware by Target Slot:**

```python
from fiber.dev.trace.llm.interceptor import BaseLLMTracer
from fiber.llm.pipeline import PipelineSlot
from xphi.state.phase.channel import DuplexChannel

# [1] PRE_OBSERVER: Fire-and-forget telemetry (Zero-latency)
class DatadogTracer(BaseLLMTracer):
    async def on_llm_end(self, meta, response, duration_ms):
        datadog.gauge("llm.latency", duration_ms, tags=[f"model:{meta.base_model}"])

# [2] PRE_TRANSLATE: Intercept raw dict payload for instant Semantic Caching
class SemanticCache(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE
    async def write(self, ctx, msg: dict):
        if "USE_CACHE" in str(msg):
            return await ctx.fire_channel_read(mock_response) # Short-circuit physical I/O
        await ctx.fire_write(msg)

# [3] POST_TRANSLATE: Enforce Security Policies on strict Pydantic objects
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
# [Cache] ➔ [Translator Core] ➔ [PII Guardrail] ➔ [Tracer] ➔ [Network I/O]
response = await acompletion(
    model="gemini-3.5-flash",
    messages=[{"role": "user", "content": "Analyze this data."}],
    interceptors=[DatadogTracer(), PIIGuardrail(), SemanticCache()], # Clean injection
    metadata={"kernel_auth": {"audit_hash": "audit_12345"}} 
)
```

---

### 1.2. LLM VCR (Record & Replay Engine)

The VCR utility serializes LLM network traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables deterministic offline testing and precise historical latency emulation without altering business logic.

Fiber provides two approaches for VCR integration:

**[1] Native Integration**

If using Fiber's SDK, the VCR can be injected globally. It wraps the `AdapterRegistry`, making standard `acompletion` calls recordable.

```python
import os, asyncio
from fiber.llm.entry import acompletion
from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig

# 1. Inject VCR globally
config = VCRPlaybackConfig(mode=os.environ.get("VCR_MODE", "live"), speed="real")
VCRInjector.apply(config=config, fixture_dir="./fixtures")

async def main():
    # 2. Execute. Explicit `trace_id` binds the execution to a specific fixture.
    response = await acompletion(
        model="gemini/gemini-3.1-flash-lite",
        messages=[{"role": "user", "content": "Count from 1 to 5."}],
        stream=True,
        metadata={"trace_id": "native_demo"}
    )
    async for chunk in response:
        print(chunk.choices[0].delta.content or "", end="", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
```

**[2] Zero-Code Integration (Module Aliasing & State Mapping)**

For legacy codebases tightly coupled to third-party SDKs (e.g., LiteLLM), Fiber uses **Zero-Code Integration**. By aliasing `sys.modules` at runtime, Fiber proxies legacy I/O calls.

Crucially, the adapter layer autonomously normalizes heterogeneous stream payloads (like Gemini's native JSON) into strict OpenAI-compliant formats. This ensures rigid legacy parsers do not fail, while offering a 1-line declarative migration path via `StateTraverseRule`.

```python
import os, sys, asyncio

# 1. Alias module namespaces before business logic loads
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()
if VCR_MODE in ("record", "replay"):
    import fiber.llm.entry as litellm_entry
    sys.modules["litellm"] = litellm_entry
    
    from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig
    VCRInjector.apply(config=VCRPlaybackConfig(mode=VCR_MODE), fixture_dir="./fixtures")

# 2. Legacy Business Logic
import litellm 
from fiber.llm.mapper.traverser import StateTraverseRule

async def main():
    response = await litellm.acompletion(
        model="gemini/gemini-3.1-flash-lite",
        messages=[{"role": "user", "content": "Explain migration."}],
        stream=True,
        metadata={"trace_id": "tech_debt_migration"} 
    )
    
    async for chunk in response:
        # [Method A] Legacy Approach: Rigid, schema-bound defensive parsing
        legacy_content = ""
        try:
            choices = getattr(chunk, "choices", None) or (chunk.get("choices", []) if isinstance(chunk, dict) else [])
            if choices:
                delta = getattr(choices[0], "delta", None) or (choices[0].get("delta", {}) if isinstance(choices[0], dict) else {})
                legacy_content = getattr(delta, "content", None) or (delta.get("content", "") if isinstance(delta, dict) else "")
        except Exception:
            pass
            
        # [Method B] Fiber Approach: Declarative multi-topology traversal
        # Reliably extracts data whether the chunk is an Object/Dict or OpenAI/Gemini schema
        fiber_content = StateTraverseRule.extract_stream_content(chunk, default="")
        
        # Behavioral Equivalency Guaranteed: Adapter normalization ensures 100% parity
        assert legacy_content == fiber_content  
        print(fiber_content, end="", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
```

**[3] The Record & Replay Flow**

* **Step 1: Record (Capture Network I/O)**
Executes live API calls, persisting stream timing (`delta_ms`) and normalized payloads to local fixtures.

```bash
VCR_MODE=record python -m fiber.dev.ex.switch
```

* **Step 2: Replay (Offline Emulation)**
Streams the cached response using exact historical latency. Add `--vcr-chaos` (in CLI) or `chaos_latency_ms` (in config) to inject artificial jitter for timeout resilience testing.

```bash
VCR_MODE=replay python -m fiber.dev.ex.switch
```

*(Note: In `live` mode, the VCR wrapper and aliases are bypassed entirely, introducing zero overhead.)*

### 1.3. Secure Agentic Bridge (MCP Gateway)

As agent protocols shift to stateless architectures, they push heavy complexities onto the client. The gateway's **Transition Bridge** absorbs this burden by decoupling HTTP ingress from physical execution.

Instead of exposing host systems to unvalidated raw REST payloads, it translates intents into deterministic events routed via **Tri-Track Concurrency**:

* **`Ephemeral` Mode:** Instantiates single-use, fault-isolated sandboxes per request, ensuring zero memory leaks.
* **`Linear` Mode:** Routes CPU-heavy workloads sequentially to eliminate cold starts.
* **`Multiplex` Mode:** Unleashes extreme lock-free concurrency for thousands of I/O-bound operations.

```bash
## 1. Boot the Gateway Daemon
fiber daemon -s rest_edge

## 2. Wrap and boot your legacy script as an autonomous worker 
fiber connect --target oracle-01 --mode multiplex --exec "python legacy_agent.py"

```

---

### 1.4. Audit Log & Compliance Engine

In an agentic economy, standard logging is insufficient; actions must be cryptographically verifiable. Fiber exposes public endpoints that act as a decentralized notary for agent telemetry and critical events, enabling absolute legal and technical accountability.

* **Telemetry Audit:** Protected by a rigorous zero-trust pipeline. Payloads must pass strict Pydantic schema typing and the `OtlpExtractionEngine` before being cryptographically sealed into the unalterable Core Ledger.
* **Audit Trails:** Powered by a KMS-backed `SecretAuditor` (integrating with Azure Key Vault, AWS KMS, etc.), the gateway automatically intercepts and encrypts sensitive PII/financial data in memory. It issues an `AuditReceipt` containing a **Merkle Membership Proof**, proving to auditors that an action occurred exactly as claimed without exposing raw data.

**Example: Generating an Immutable Audit Proof (Client-Side Fail-Fast)**

```python
from fiber.dphi.eco.client.sdk import DphiPublicClient, StrictPayloadFactory

client = DphiPublicClient(base_url="http://127.0.0.1:8000")

# 1. Generate a schema-validated audit payload
# The factory ensures strict zero-trust schema compliance before transmission
audit_payload = StrictPayloadFactory.create_audit_payload(
    message="Executed financial trade: 500 AAPL @ Market",
    actor="agent-node-007",
    action="trade_execution",
    require_proof=True
)

# 2. Provide the cryptographic payment receipt (X402 Capability Token)
# Required for executing state-mutating actions in the decentralized economy
x402_receipt = "x402_cap_AgEEZGF0Y..."

# 3. Record the event and receive an immutable Merkle Proof
receipt = await client.record_audit_event(
    request=audit_payload,
    payment_receipt=x402_receipt
)

print("Notarized Hash:", receipt["hash"])
print("Merkle Proof:", receipt["membership_proof"])

```

---

## 2. Installation & Infra Provisioning

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
| **`connect`** | **[Egress Sidecar]** Transforms any legacy MCP server into an autonomous node, securely connecting standard I/O to the distributed network. | `fiber connect -m multiplex -t oracle -e "python app.py"` |
| **`daemon`** | **[Production Host]** Provisions a subordinate node (K8s/Docker) and applies topology profiles. | `fiber daemon -s rest_edge` |
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e llm.trace --model gemini/gemini-3.1-flash-lite` |

> **Note on X402 Protocol Scaling:**
> Fiber naturally scales from Standalone (local testing) to Enterprise (internal API chargebacks) to Commercial (external metered billing and routing) using the exact same CLI commands. No architectural teardowns are required.

---

## 4. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[dphi.wasm.log](./phase/abc/log/dphi/wasm.entry.20260916.log):** Validates deterministic execution across Ephemeral sandboxes, confirming precise Resource Exhaustion Traps (OOM / CPU Time Limits), Distributed Execution Determinism recovery, Tripartite Parity recovery, and Cryptographic Proof generation (3bb93907...).
* 🔗 **[plane.flare.log](./phase/abc/log/plane/flare.20260917.log):** Validates V8 isolation sandboxing within Cloudflare Edge microservices, confirming absolute containment against host filesystem/socket breaches and ensuring Parity/FP determinism across distributed JS-Python workers.
* 🔗 **[dphi.clearing.log](./phase/abc/log/dphi/clearing.20260916.log):** Validates the WASM-based Clearing FSM and transaction pipeline, confirming deterministic edge defenses against invalid EIP-712 signatures, zero balances, and corrupted calldata via chaos injection.
* 🔗 **[edge.sandbox.log](./phase/abc/log/gateway/sandbox.20260911.log):** Validates the Edge Gateway's absolute perimeter defenses, confirming cryptographic Tamper-Resistance (Fail-Fast) of the origin state, zero-trust ingress signature validation, and Sentinel Chaos WAF resilience.
* 🔗 **[llm.compat.log](./phase/abc/log/llm/compat.20260918.log):** Validates the LLM governance pipeline, confirming physical Fuel Breaker terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.
* 🔗 **[llm.vcr.log](./phase/abc/log/llm/vcr.replay.20260917.log):** Validates the VCR (Record & Replay) network interceptor, confirming zero-latency offline execution, precise latency breakdown (Network I/O vs Framework Overhead), and absolute Tracer shielding during idempotent fallbacks.
* 🔗 **[ex.switch.log](./phase/abc/log/ex/switch.20260919.log):** Validates the Sandbox zero-code integration, confirming runtime module aliasing, seamless multi-topology stream normalization via the State Mapper, and precise historical TTFB emulation during VCR playback.
