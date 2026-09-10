# fiber.README

@desc: Fiber Project - Agent Deterministic Infrastructure

While autonomous AI agents offer unprecedented capabilities, modern stateless agent protocols (like MCP) routinely expose host systems to severe vulnerabilities—from uncontained memory leaks (OOM) and Confused Deputy attacks to unpredictable billing runaways.

**Fiber** is a zero-trust cryptographic metering proxy that definitively resolves these structural bottlenecks. Designed for enterprise-grade AI deployment, Fiber anchors your agentic architecture on three core pillars:

1. **Cryptographic Audit & Compliance:** Generates mathematically provable, zero-knowledge Merkle proofs for every agent action, enabling safe B2B adoption and strict liability tracking.
2. **Secure Agentic Bridge (MCP Sidecar):** Sublimates legacy systems into autonomous agents with zero code changes, enforced by hardware-level isolation.
3. **LLM Governance & Fuel Traps:** Provides a low-friction, drop-in replacement for existing LLM SDKs (e.g., LiteLLM, OpenAI) that physically terminates connections to prevent API billing runaways.

---

## 1. Core Pillars in Action

### 1.1. Cryptographic Compliance & Audit Engine

In an agentic economy, standard logging is insufficient; actions must be mathematically provable. Fiber exposes public endpoints that act as a decentralized notary for agent telemetry and critical events, enabling absolute legal and technical accountability.

* **Deterministic Telemetry:** Protected by a "Double-Barrier" zero-trust system. Payloads must pass rigorous Pydantic schema typing and the `StrictOtlpExtractionEngine` before being cryptographically sealed into the unalterable Core Ledger.
* **Zero-Knowledge Audit Trails:** Powered by a KMS-backed `SecretAuditor` (integrating with Azure Key Vault, AWS KMS, etc.), the gateway automatically intercepts and encrypts sensitive PII/financial data in memory. It issues an `AuditReceipt` containing a **Merkle Membership Proof**, proving to auditors that an action occurred exactly as claimed without exposing raw data.

**Example: Generating an Immutable Audit Proof (Client-Side Fail-Fast)**

```python
from fiber.dphi.eco.client.sdk import DphiPublicClient, StrictPayloadFactory

client = DphiPublicClient(base_url="http://127.0.0.1:8000")

# 1. Generate a strictly compliant payload
payload = StrictPayloadFactory.create_audit_payload(
    message="Financial trade executed for user secret@corp.com",
    actor="bot-007",
    action="financial_trade",
    require_proof=True
)

# 2. Record the event and receive a Cryptographic Merkle Proof
receipt = await client.record_audit_event(
    request=payload,
    billing_ref="<enterprise_billing_id>"
)
print("Notarized Hash:", receipt["hash"])
print("Merkle Proof:", receipt["membership_proof"])

```

### 1.2. Secure Agentic Bridge (MCP Gateway)

As agent protocols shift to stateless architectures, they push heavy complexities onto the client. The gateway's **Transition Bridge** absorbs this burden by decoupling HTTP ingress from physical execution.

Instead of exposing host systems to chaotic raw REST payloads, it translates intents into deterministic events routed via **Tri-Track Concurrency**:

* **`Ephemeral` Mode:** Instantiates single-use, fault-isolated sandboxes per request, ensuring zero memory leaks.
* **`Linear` Mode:** Routes CPU-heavy workloads sequentially to eliminate cold starts.
* **`Multiplex` Mode:** Unleashes extreme lock-free concurrency for thousands of I/O-bound operations.

```bash
## 1. Boot the Gateway Daemon
fiber daemon -s rest_edge

## 2. Wrap and boot your legacy script as an autonomous worker 
fiber connect --target oracle-01 --mode multiplex --exec "python legacy_agent.py"

```

### 1.3. LLM Governance & Dynamic Pipeline

Beyond MCP protocol management, the `fiber.llm.entry` module is a high-performance LLM router that provides a **Drop-in Replacement** for the OpenAI SDK and LiteLLM.

* **Fuel Trap:** Physically terminates the connection at the hypervisor level if a streaming response exhausts its token budget, preventing billing runaways.
* **Declarative Tool Recovery:** Dynamically detects and strictly normalizes malformed tool calls from heterogeneous LLMs (like Gemini) into the OpenAI standard format.

```python
## Just change your import path. No architecture rewrite needed.
from fiber.llm.entry import acompletion

response = await acompletion(
    model="gemini-3.5-flash",
    messages=[{"role": "user", "content": "Analyze this data."}],
    fallbacks=["gpt-4o-mini"], 
    metadata={"post_call_rules": [async_pii_filter_function]} # Dynamic Guardrails
)
```

---

## 2. Installation & Topology Alignment

Fiber utilizes a self-bifurcating installation pipeline where `fiber` and its core dependency `xphi` are tightly coupled. We recommend using `uv pip` for strict dependency resolution.

```bash
## 1. Create a dedicated sandbox environment
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

## 2. Install via local source OR remote git reference
uv pip install /path/to/local/self/fiber
# OR: uv pip install git+https://github.com/wittgena/fiber.git@v1.1.2

## 3. Verify Topology (Anchors to ~/.anchor/bound.json)
fiber --help

```

---

## 3. Fiber CLI Tool

The `fiber` CLI is a **Topological Router**, dynamically assigning the appropriate node profile and delegating execution. It transparently forwards unknown arguments directly to the target module to ensure zero-friction scalability.

### 3.1. Ecosystem Operational Modes

| Mode | Description | Example |
| --- | --- | --- |
| **`connect`** | **[Egress Sidecar]** Sublimates any legacy MCP server into a DPHI autonomous node wrapping standard I/O to the distributed FSM bus. | `fiber connect -m multiplex -t oracle -e "python app.py"` |
| **`daemon`** | **[Production Host]** Provisions a subordinate node (K8s/Docker) and applies topology profiles. | `fiber daemon -s rest_edge` |
| **`e2e`** | **[Test Orchestrator]** Forwards suite-specific arguments to internal test pipelines. | `fiber e2e bridge.llm.compat --model gemini` |

### 3.2. Egress Sidecar (The `connect` Mode)

The `fiber connect` command is the ecosystem's most potent adoption vector. It enables you to integrate existing Web2 servers into the Agent-to-Agent economy with **absolutely zero code modifications**.

* **Zero-Trust NAT Traversal:** Operates purely via outbound subscription. Organizations can safely expose internal DBs to global AI agents while remaining concealed behind strict corporate VPCs (Zero inbound firewall configurations).
* **Enterprise-Grade Access Control:** The Edge Gateway handles complex DPoP cryptography, Identity verification, and Idempotency Shields automatically before a request ever hits your legacy application.

---

## 4. System Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[workflow.wasm.log](./phase/abc/log/workflow.wasm.20260825.log):** Validates deterministic execution across Ephemeral sandboxes, confirming precise Fuel traps (OOM/Timeout blocks), Tripartite Parity recovery, and Cryptographic Proof generation (3bb93907...)
* 🔗 **[workflow.flare.log](./phase/abc/log/workflow.flare.20260827.log):** Validates V8 Hologram isolation within Cloudflare Edge microservices, confirming absolute containment against host filesystem/socket breaches and ensuring Parity/FP determinism across distributed JS-Python workers.
* 🔗 **[workflow.settlement.log](./phase/abc/log/workflow.settlement.20260825.log):** Validates REVM pre-validation of smart contract state transitions and rollbacks.
* 🔗 **[e2e.edge.sandbox.log](./phase/abc/log/edge/e2e.edge.sandbox.20260911.log):** Validates the Edge Gateway's absolute perimeter defenses, confirming cryptographic Tamper-Resistance (Fail-Fast) of the origin state, zero-trust ingress signature validation, and Sentinel Chaos WAF resilience