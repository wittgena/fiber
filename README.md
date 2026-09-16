# fiber.README

@desc: Fiber Project - Agent Deterministic Infrastructure

Fiber is a cryptographic proxy designed to secure and scale autonomous AI agents. It protects host systems from severe vulnerabilities inherent in modern stateless protocols (like MCP)—such as memory leaks (OOM), confused deputy attacks, and API billing runaways.

This document provides a practical guide on how to integrate and deploy Fiber across its three core operational pillars:

* **[1.1] Cryptographic Audit & Compliance:** How to record mathematically provable, Merkle-proof receipts for every agent action.
* **[1.2] Secure Agentic Bridge (MCP Sidecar):** How to safely connect legacy REST systems to AI agents using zero-trust execution sandboxes—without altering existing code.
* **[1.3] LLM Governance & Dynamic Pipeline:** How to use Fiber as a drop-in replacement for standard LLM SDKs to enforce hard network-level budget traps and normalize tool calls across models (OpenAI, Gemini, Anthropic).

Additionally, this guide covers **[2] Installation** and **[3] CLI Deployment (connect, daemon, e2e)** to help you quickly provision your infrastructure.

---

## 1. Core Pillars in Action

### 1.1. Cryptographic Compliance & Audit Engine

In an agentic economy, standard logging is insufficient; actions must be mathematically provable. Fiber exposes public endpoints that act as a decentralized notary for agent telemetry and critical events, enabling absolute legal and technical accountability.

* **Deterministic Telemetry:** Protected by a rigorous zero-trust pipeline. Payloads must pass strict Pydantic schema typing and the `OtlpExtractionEngine` before being cryptographically sealed into the unalterable Core Ledger.
* **Audit Trails:** Powered by a KMS-backed `SecretAuditor` (integrating with Azure Key Vault, AWS KMS, etc.), the gateway automatically intercepts and encrypts sensitive PII/financial data in memory. It issues an `AuditReceipt` containing a **Merkle Membership Proof**, proving to auditors that an action occurred exactly as claimed without exposing raw data.

**Example: Generating an Immutable Audit Proof (Client-Side Fail-Fast)**

```python
from fiber.dphi.eco.client.sdk import DphiPublicClient, StrictPayloadFactory

client = DphiPublicClient(base_url="http://127.0.0.1:8000")

# 1. Generate a mathematically provable payload
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

### 1.2. Secure Agentic Bridge (MCP Gateway)

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

### 1.3. LLM Governance & Dynamic Pipeline

Beyond MCP protocol management, the `fiber.llm.entry` module is a high-performance LLM router providing a **Drop-in Replacement** for the OpenAI SDK and LiteLLM. It enforces strict, Netty-style pipeline governance while maintaining a Pythonic, developer-friendly UX Facade.

* **Physical Fuel Trap:** Mathematically prevents billing runaways by physically terminating the TCP connection if a streaming response exhausts its predefined token budget.
* **Declarative Tool Recovery:** Dynamically detects and normalizes malformed tool calls from heterogeneous LLMs (e.g., Gemini) into the strict OpenAI standard format.
* **Slot-based Middleware (UX Facade):** Safely inject custom plugins (e.g., Datadog Tracers, Semantic Caches, PII Guardrails) using a simple flat list (`interceptors=[]`). The Entry Facade autonomously routes them to deterministic lifecycle slots without blocking the main I/O or risking core pipeline corruption.

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

* Upcoming Milestone: 3-Point Record & Replay: Captures exact pipeline states across Entry, Translator, and Exit slots. This will enable zero-cost, deterministic CI/CD and local time-travel debugging by replaying LLM interactions without physical network calls.

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
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e llm.compat --model gemini/gemini-3.1-flash-lite` |

> **Note on X402 Economic Scaling:** 
> Fiber naturally scales from `Standalone` (local testing) to `Enterprise` (internal chargebacks) to `Commercial` (real financial settlement on external networks) using the exact same CLI commands. No architectural teardowns are required.

---

## 4. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[dphi.wasm.log](./phase/abc/log/dphi/wasm.entry.20260916.log):** Validates deterministic execution across Ephemeral sandboxes, confirming precise Resource Exhaustion Traps (OOM / CPU Time Limits), Distributed Execution Determinism recovery, Tripartite Parity recovery, and Cryptographic Proof generation (3bb93907...)
* 🔗 **[workflow.flare.log](./phase/abc/log/plane/e2e.plane.flare.20260911.log):** Validates V8 isolation Sandboxing within Cloudflare Edge microservices, confirming absolute containment against host filesystem/socket breaches and ensuring Parity/FP determinism across distributed JS-Python workers.
* 🔗 **[dphi.clearing.log](./phase/abc/log/dphi/clearing.20260916.log):** Validates the WASM-based Clearing FSM and transaction pipeline, confirming deterministic edge defenses against invalid EIP-712 signatures, zero balances, and corrupted calldata via chaos injection.
* 🔗 **[edge.sandbox.log](./phase/abc/log/edge/sandbox.20260911.log):** Validates the Edge Gateway's absolute perimeter defenses, confirming cryptographic Tamper-Resistance (Fail-Fast) of the origin state, zero-trust ingress signature validation, and Sentinel Chaos WAF resilience
* 🔗 **[llm.compat.log](./phase/abc/log/edge/llm/compat.20260913.log):** Validates the LLM governance pipeline, confirming physical Fuel Trap terminations on streaming budget exhaustion, dynamic tier-based fallback routing, deterministic recovery of heterogeneous tool calls via the InterLLM adapter, and zero-overhead plug-and-play tracer injection for custom observability.