# README
@desc: Fiber Project - Agent Deterministic Infrastructure

The Fiber project is a **cryptographic metering proxy architecture** that logically separates execution from settlement processes. Through deterministic state transitions and an isolated sandbox environment, it securely proxies external LLMs and computing resources while processing off-chain micro-transactions via an in-memory UTXO model.

This repository contains the Gateway and Sandbox modules, offering a drop-in replacement for external AI systems alongside computational budget control and cryptographic integrity.

---

## 1. DPHI Gateway Overview

🔗 **[Read the Full Document: Gateway Overview](./phase/abc/dphi/overview.md)**

A universal compute and metering proxy architecture agnostic to specific runtimes or settlement layers.

* **Execution & Ledger Agnosticism (BYOC & BYOS):** Acts as a transparent proxy that does not enforce a specific execution runtime. After sealing a session, it asynchronously routes proof data (`AuditReceipt`) to external ledgers (RDBMS, Vaults, DA, EVM) via agnostic egress adapters.
* **In-Memory Netting:** Leverages an off-chain UTXO model to process micro-transactions entirely in-memory, mitigating database row-locking bottlenecks and external network gas fees.
* **Core Components:**
* `edge.llm`: A router that maps LLM requests to computational intents, translating token usage into internal `Fuel` units and enforcing budget limits.
* `dvm.wasm`: A native WASM isolated environment that ensures state consistency through precise instruction-level metering.

---

## 2. Fiber CLI Tool

The `fiber` CLI is the single entry point for bootstrapping the DPHI ecosystem. Rather than acting as a heavy, monolithic controller, it functions as a **Topological Router**. It delegates execution, argument parsing, and lifecycle management directly to the target modules while dynamically assigning the appropriate node profile to prevent resource overkill.

### 2.1. Execution & Local Usage

If the `fiber` package is installed globally (e.g., via `pip install -e .`), you can use the `fiber` command directly. For local development or environments without a global installation, you can execute the module directly using Python's standard `-m` flag:

```bash
# Global execution
fiber [OPTIONS] COMMAND [ARGS]...

# Local / Development execution
python -m fiber.cli.main [OPTIONS] COMMAND [ARGS]...

```

### 2.2. E2E Testing & Dynamic Argument Forwarding (Core Feature)

The most powerful aspect of the `fiber` CLI is its transparent test orchestration. The `fiber e2e` command dynamically loads distributed integration suites (`defin`, `eco`, `edge`, `flare`, `wasm.entry`, `llm.compat`).

Instead of hardcoding every possible test parameter into the root CLI, `fiber` captures unknown arguments and transparently **forwards them to the target module's standard `main(args)` entrypoint**. This ensures zero-friction scalability as new domains and parameters are added.

**Example:**

```bash
# Run the LLM Compatibility suite with suite-specific arguments
fiber e2e llm.compat --model gemini/gemini-3.1-flash-lite --proxy

```

> *Note: In the example above, `--model` and `--proxy` are completely unknown to the root `fiber` CLI. They are gracefully passed down to the `llm.compat` suite's internal `argparse`.*

### 2.3. Ecosystem Operational Modes

Beyond testing, the CLI routes the system into specific operational contexts, automatically segregating topologies (e.g., `EDGE` vs. `COMPUTE`) based on the requested workload:

| Mode | Description | Example |
| --- | --- | --- |
| **`daemon`** | **[Production Host]** Provisions a subordinate node (K8s/Docker). It performs **Topological Segregation** by analyzing the requested daemons. For example, requesting only `gateway_edge` dynamically sets the `EDGE` profile, safely bypassing heavy WASM worker pools to prevent resource overkill. | `fiber daemon -s rest_edge,gateway_edge` |
| **`trace`** | **[Experimental / Chaos Sandbox]** An experimental structure dedicated to **Chaos Engineering and Deep Diagnostics**. It ignites a specialized hypervisor (`tracer_controller`) to inject structural anomalies (e.g., OOM traps, Byzantine faults) into isolated containers and observe the kernel's resilience. Supports custom YAML manifests via `--config`. | `fiber trace -t oom_tracer -c fault.yml` |
| **`shell`** | **[Client Observatory]** Launches an interactive God-Mode console. It connects directly to the asynchronous message tunnel (Redis PubSub/Stream) to monitor cluster capacity or inject out-of-band signals without booting a full local kernel reactor. | `fiber shell --env-file .env.local` |

---

## 3. LLM Compatibility & Edge Gateway

🔗 **[Read the Full Documents: Compat Entry](./phase/abc/llm/compat/entry.md) | [Edge Gateway](./phase/abc/llm/edge.md) | [Token Utilities**](./phase/abc/llm/compat/token.md)

The `fiber.llm` module is a high-performance LLM router and gateway that provides a **Drop-in Replacement for the OpenAI SDK and LiteLLM**. It transparently embeds DPHI’s core features—Fuel metering, fault-tolerance, and state normalizations—without requiring rewrites to your existing agent architectures.

### 3.1. Zero-Friction Migration (Drop-in Replacement)

You can integrate your existing LLM pipelines into the DPHI ecosystem simply by changing the import path. All return objects follow the standard Pydantic models (e.g., `openai.types.chat.ChatCompletion`), ensuring full compatibility with your existing type hints and downstream logic.

```python
# Instead of: from openai import AsyncOpenAI
# Instead of: from litellm import acompletion
from fiber.llm.entry import acompletion

response = await acompletion(
    model="gpt-4o",
    messages=[{"role": "user", "content": "Analyze this data."}],
    stream=True,
    # Standard OpenAI kwargs are fully supported (temperature, tool_calls, etc.)
)
```

### 3.2. Advanced Pipeline & Dynamic Control

Beyond basic compatibility, `fiber.llm` exposes a powerful asynchronous channel pipeline via the `metadata` and `kwargs` fields.

* **Fuel Trap & Authorization (`kernel_auth`):** Enforces a strict token budget. If a streaming response exhausts the allocated `fuel_budget`, the connection is immediately terminated (Kinetic Trap) to prevent budget overruns.
* **Declarative Tool Recovery:** Heterogeneous LLMs (e.g., Gemini) often leak or malform function call formats. The internal `StateMapper` dynamically detects and recovers these deviations, strictly normalizing them into the OpenAI `tool_calls` format.
* **Model Fallbacks & Mocking:**
```python
response = completion(
    model="gemini-3.5-flash",
    messages=[...],
    fallbacks=["gpt-4o-mini"], # Auto-retry on RateLimit or API errors
    mock_response="Simulated Response", # Bypasses network for rapid testing
)
```

* **Dynamic Guardrails:** Inject custom validation rules per request without altering global configurations.
```python
metadata={"post_call_rules": [async_pii_filter_function]}
```

### 3.3. Edge Gateway & Zero-Trust Integration

For decentralized agents, the FastAPI-based REST Gateway (`edge.llm`) provides a standard HTTP interface (`/v1/chat/completions`). Clients simply point their Base URL to the DPHI Gateway and inject the L402 payment proof.

```http
POST /v1/chat/completions HTTP/1.1
Authorization: Bearer <provider_key_if_any>
X-X402-Receipt: <l402_macaroon_proof>

```

The gateway extracts the receipt, invokes the `AUTHORIZE_INTENT` via the WASM Kernel, and delegates the approved Fuel budget to the underlying pipeline. Invalid or depleted receipts immediately trigger an `HTTP 402 Payment Required` to initiate a transparent retry.

### 3.4. Native Token Utilities

The `fiber.llm.model.token` package provides exact equivalents to LiteLLM’s token management utilities, augmented with universal encoding support.

* **Precise Token Counting:** Automatically switches between `tiktoken` and `HuggingFace Tokenizer` based on the model, accurately calculating tokens for images (via Vision tile math) and non-standard JSON schemas (e.g., Anthropic `tool_use`).

```python
from fiber.llm.model.token.counter import token_counter, get_modified_max_tokens
safe_limit = get_modified_max_tokens(model="gpt-4o", messages=msgs, user_max_tokens=4000)
```

* **Safe Context Trimming:** `trim_messages()` safely evicts older messages while strictly preserving `system` prompts and critical `tool_result` contexts.
* **Token-Safe Splitter:** A native `TokenSplitter` slices documents by Token IDs rather than string length, completely preventing multi-byte character corruption across chunk boundaries during RAG workloads.

---

## 4. DPHI Sandbox Architecture

🔗 **[Read the Full Document: Sandbox Architecture](./phase/abc/dphi/milestone/sandbox.md)**

Defines the core sandbox engine principles for executing deterministic state transitions and isolated computations.

* **3-Tier Execution Layers:**
* **Tier 1 (General I/O Isolate):** A V8 Isolate-based gateway handling external network I/O and protocol translation (Non-deterministic).
* **Tier 2 (Constrained Pyodide):** An I/O-constrained Python runtime ensuring deterministic execution for business logic like AI agent inference and data transformation.
* **Tier 3 (Native WASM):** A deterministic native WASM execution layer for core system modules. Responsible for UTXO state updates, precision metering, and receipt issuance.


* **Ephemeral Runtime & Lock-Free UTXO:** Reduces idle daemon overhead by creating and destroying sandboxes on a per-request basis. The UTXO tree structure removes database locking bottlenecks, supporting concurrent scaling.

---

## 5. System Certification & Security Validation

🔗 **[Execution Logs: View Pipeline Reports](./phase/abc/log/)**

The DPHI infrastructure validates its cryptographic integrity, execution determinism, and perimeter security through end-to-end integration tests upon every build.

### 5.1. Core WASM Engine & Instruction-Level Determinism

🔗 **[View Log: workflow.wasm.log](./phase/abc/log/workflow.wasm.20260825.log)**

The core execution engine compiles native Rust modules, leveraging **AOT compilation caching** and **pre-warmed pools** to minimize cold starts.

* **Constrained Sandbox:** Executes untrusted Python logic inside a strict WASM memory boundary, maintaining consistent PRNG sequences and floating-point computations across distributed nodes.
* **Resource Limit Enforcement:** Hardware limits are strictly governed per tier. The hypervisor intercepts and safely halts execution upon fuel exhaustion, memory spikes, or excessive call-stack depths (infinite loops).
* **State Recovery & Sealing:** Generates cryptographic parity triplets. If network fragmentation occurs, missing topology frames are recovered via built-in XOR Parity before sealing the state into a canonical `AuditReceipt`.

### 5.2. Edge V8 Isolate & Ingress Defense

🔗 **[View Log: workflow.flare.log](./phase/abc/log/workflow.flare.20260827.log)**

Orchestrates dual V8 isolates within a simulated Cloudflare Worker environment to enforce boundaries before intents enter the core.

* **Environment Isolation:** Detects and blocks unauthorized host filesystem inspection, raw socket bindings, subprocess execution, and thread spawning at the edge.
* **Consensus & Fault Tolerance:** Enforces a 2-of-3 threshold consensus to reject invalid signatures (Sybil attacks) and recover state frames.
* **Side-Channel Mitigation:** Reduces timer resolution to mitigate Spectre-style attacks and strictly clears memory between execution contexts.

### 5.3. EVM Simulated Settlement & Ledger Validation

🔗 **[View Log: workflow.settlement.log](./phase/abc/log/workflow.settlement.20260825.log)**

Embeds **REVM (Rust EVM)** within the WASM sandbox to deterministically simulate off-chain deferred settlements for Web3 smart contracts.

* **State Pre-validation:** Simulates smart contract state transitions (e.g., ERC-20 transfers) against an injected state snapshot prior to L1 broadcasting.
* **Rollback Handling:** Halts execution and triggers rollbacks when encountering insufficient balances, malformed calldata, or invalid opcode injections.
* **L2 Readiness:** Validated transactions emit exact storage slot mutation proofs alongside an L2 Rollup Hash.

### 5.4. End-to-End DePIN Orchestration & Fault Injection

🔗 **[View Log: phase.e2e.defin.log](./phase/abc/log/defin/phase.e2e.defin.20260830.log)**

The infrastructure utilizes an event-driven **Finite State Machine (FSM)** to decouple computational intent from infrastructure execution across decentralized workflows.

* **Compute Lifecycle:** Verifies caller identities via EIP-712 signatures, mints Genesis UTXOs, orchestrates parallel WASM executions, and seals final micro-settlements into L1 proofs.
* **FSM-Driven Validation:** Halts invalid transaction workflows before execution begins, rejecting zero-balance requests and unverified callers.
* **Fault Injection Resilience:** The state machine intercepts in-flight anomalies—such as forced allowance zeroing or corrupted calldata—triggering safe halts to maintain network stability.

### 5.5. API Gateway & L402 Ingress Validation

🔗 **[View Log: phase.e2e.edge.log](./phase/abc/log/edge/phase.e2e.edge.20260830.log)**

The FastAPI-based REST Gateway serves as the entry point, ensuring compute resources are allocated only to authenticated and funded requests.

* **L402 Payment & Tier Routing:** Handles incoming requests through a quoting phase, mapping intents to execution tiers and clearing charges via Web3 wallets and Rollup Adapters.
* **Signature Verification:** Ingress traffic is strictly inspected. If a payload's cryptographic signature is invalid, the edge immediately terminates the workflow.
* **Rate Limiting & WAF:** Deflects unauthorized or unfunded request spikes at the API perimeter to preserve internal execution capacity for valid intents.