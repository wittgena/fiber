# fiber.README
@desc: Fiber Project - Agent Deterministic Infrastructure

While autonomous AI agents offer unprecedented capabilities, modern stateless agent protocols (like MCP) routinely expose host systems to severe vulnerabilities—from uncontained memory leaks (OOM) and Confused Deputy attacks to unpredictable billing runaways.

**Fiber** is a zero-trust cryptographic metering proxy that definitively resolves these structural bottlenecks. Functioning primarily as a **Secure MCP Bridge**, Fiber replaces fragile software assumptions with hardware-level isolation, deterministic state enforcement, and absolute budget control. Additionally, it provides a zero-friction drop-in replacement for existing LLM SDKs (e.g., LiteLLM, OpenAI).

---

## 1. Secure MCP Bridge & LLM Gateway

### 1.1. MCP Gateway (The Stateless Complexity Anchor)

As agent protocols (e.g., MCP 2.0) shift to stateless architectures, they push heavy complexities—race conditions, cryptographic replay attacks, and idempotency—onto the client. The gateway's **Transition Bridge** absorbs this burden.

It centralizes DPoP signature validation and tracks inbound intents through a strict Finite State Machine context. Instead of exposing host systems to chaotic raw REST payloads, it translates intents into deterministic `LogicStream` events routed via **Tri-Track Concurrency**:

* **`Ephemeral` Mode:** Instantiates single-use, fault-isolated sandboxes per request, ensuring zero memory leaks and safe Human-in-the-Loop interaction.
* **`Linear` Mode:** Routes CPU-heavy workloads into a pre-warmed daemon with strict sequential queueing, eliminating cold starts.
* **`Multiplex` Mode:** Unleashes extreme lock-free concurrency within a single async daemon to handle thousands of I/O-bound operations (e.g., Oracle data fetches) in parallel.

### 1.2. Edge Gateway (REST API)

For decentralized agents, point your Base URL to the DPHI Gateway and inject the X402 payment proof.

```http
POST /v1/chat/completions HTTP/1.1
Authorization: Bearer <provider_key_if_any>
X-X402-Receipt: <x402_signed_receipt>

```

### 1.3. LLM Compatibility & Zero-Friction Migration

Beyond MCP protocol management, the `fiber.llm.entry` module is a high-performance LLM router that provides a **Drop-in Replacement for the OpenAI SDK and LiteLLM**. It transparently embeds DPHI’s core features without requiring rewrites to your agent architecture.

Return objects follow standard Pydantic models (e.g., `openai.types.chat.ChatCompletion`). Simply change your import path:

```python
# Instead of: from openai import AsyncOpenAI / litellm import acompletion
from fiber.llm.entry import acompletion

response = await acompletion(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Analyze this data."}],
    stream=True,
    # Standard OpenAI kwargs are fully supported (temperature, tool_calls, etc.)
)

```

### 1.4. Dynamic Pipeline Control

* **Fuel Trap:** Physically terminates the connection at the hypervisor level if a streaming response exhausts its token budget, preventing billing runaways.
* **Declarative Tool Recovery:** Dynamically detects and strictly normalizes malformed tool calls from heterogeneous LLMs (like Gemini) into the OpenAI standard format.

```python
response = completion(
    model="gemini-3.5-flash",
    messages=[...],
    fallbacks=["gpt-4o-mini"], # Auto-retry on RateLimit or API errors
    mock_response="Simulated Response", # Bypasses network for rapid testing
    metadata={"post_call_rules": [async_pii_filter_function]} # Dynamic Guardrails
)

```

---

## 2. Fiber CLI Tool

The `fiber` CLI is the single entry point for bootstrapping the ecosystem. It functions as a **Topological Router**, dynamically assigning the appropriate node profile and delegating execution.

### 2.1. Execution & Local Usage

```bash
# Global execution
fiber [OPTIONS] COMMAND [ARGS]...

# Local / Development execution
python -m fiber.phase.cli.main [OPTIONS] COMMAND [ARGS]...

```

### 2.2. E2E Testing & Dynamic Argument Forwarding

Instead of hardcoding parameters, the CLI transparently forwards unknown arguments directly to the target module's standard `main(args)` entrypoint. This ensures zero-friction scalability as new domains and parameters are added.

**Example:**

```bash
# Run the LLM Compatibility suite with suite-specific arguments
fiber e2e bridge.llm.compat --model gemini/gemini-3.1-flash-lite --proxy

```

> *Note: In the example above, `--model` and `--proxy` are completely unknown to the root `fiber` CLI. They are gracefully passed down to the `bridge.llm.compat` suite's internal `argparse`.*

### 2.3. Ecosystem Operational Modes

Beyond testing, the CLI routes the system into specific operational contexts, automatically segregating topologies (e.g., `EDGE` vs. `COMPUTE`) based on the requested workload:

| Mode | Description | Example |
| --- | --- | --- |
| **`daemon`** | **[Production Host]** Provisions a subordinate node (K8s/Docker). Analyzes requested daemons and dynamically applies topology profiles (e.g., bypassing heavy WASM pools if only acting as an `EDGE` proxy). | `fiber daemon -s rest_edge,gateway_edge` |
| **`trace`** | **[Experimental / Chaos Sandbox]** Ignites a specialized hypervisor (`tracer_controller`) to inject structural anomalies (e.g., OOM traps, Byzantine faults) into isolated containers to observe kernel resilience. | `fiber trace -t oom_tracer -c fault.yml` |
| **`deploy`** | **[Deployment Manager]** Manages multi-node orchestration and cluster scaling logic. | `fiber deploy -t master` |
| **`shell`** | **[Client Observatory]** Launches an interactive God-Mode console. Connects directly to the asynchronous message tunnel without booting a full local kernel reactor. | `fiber shell --env-file .env` |
| **`connect`** | **[Egress Sidecar / A2A Bridge]** Sublimates any legacy MCP server into a DPHI autonomous node. Acts as a topology-adaptive proxy (Ephemeral, Linear, or Multiplex) wrapping standard I/O to the distributed FSM bus. | `fiber connect --mode multiplex -t oracle -e "python agent.py"` |

### 2.4. Egress Sidecar & A2A Sublimation (The `connect` Mode)

The `fiber connect` command is the ecosystem's most potent adoption vector. It enables you to integrate existing Web2 servers into the Agent-to-Agent economy with **absolutely zero code modifications**.

* **Zero-Trust NAT Traversal:** Operates purely via outbound subscription (pull-based). Organizations can safely expose internal DBs or ERPs to global AI agents while remaining concealed behind strict corporate VPCs, requiring **zero inbound firewall configurations**.
* **Topology-Adaptive Execution:** By simply appending a `--mode` flag, legacy scripts instantly inherit the Tri-Track concurrency (Ephemeral/Linear/Multiplex) without rewriting internal business logic.
* **Instant X402 Monetization:** The Edge Gateway handles complex X402 stablecoin netting, DPoP cryptography, and Idempotency Shields automatically.
* **The Pathway to WASM:** By isolating physical execution within this Sidecar boundary, Fiber establishes a seamless migration path for providers to eventually swap legacy subprocesses with fully autonomous, instruction-metered WASM smart contracts.

---

## 3. Architecture & Sandbox Constraints

Fiber utilizes a universal, runtime-agnostic compute architecture (DPHI).

* **In-Memory Netting:** Processes micro-transactions off-chain to eliminate database row-locking and network gas fees.
* **3-Tier Sandbox Execution:**

1. **Tier 1 (V8 Isolate):** Handles external I/O and protocol translation.
2. **Tier 2 (Pyodide):** Constrained, deterministic execution for AI inference/logic.
3. **Tier 3 (Native WASM):** Instruction-level metering and exact state updates.

---

## 4. System Certification & Validation Logs

The infrastructure guarantees execution determinism and security through end-to-end integration tests upon every build.

* 🔗 **[workflow.wasm.log](./phase/abc/log/workflow.wasm.20260825.log):** Validates strict WASM memory boundaries, PRNG sequences, and hypervisor halts on fuel exhaustion.
* 🔗 **[workflow.flare.log](./phase/abc/log/workflow.flare.20260827.log):** Validates dual V8 isolates blocking unauthorized filesystem/socket access and Sybil attacks.
* 🔗 **[workflow.settlement.log](./phase/abc/log/workflow.settlement.20260825.log):** Validates REVM pre-validation of smart contract state transitions and rollbacks.
* 🔗 **[phase.e2e.edge.log](./phase/abc/log/edge/phase.e2e.edge.20260830.log):** Validates API perimeter X402 payment routing and EIP-712 signature ingress defenses.