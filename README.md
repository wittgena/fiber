# fiber.README
@lineage: README

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

## 2. LLM Compatibility & Edge Gateway

🔗 **[Read the Full Document: LLM Compat Entry](./phase/abc/llm/compat/entry.md)**
🔗 **[Read the Full Document: LLM Edge Gateway](./phase/abc/llm/edge.md)**

An LLM gateway powered by an internal asynchronous channel pipeline. It provides an **interface compatible with the OpenAI SDK and LiteLLM**, allowing clients to integrate with DPHI by simply updating their Base URL and headers.

* **Drop-in Replacement:** Fully supports Pydantic-compatible response objects, SSE streaming, and Tool Calling. It includes a declarative state translator to recover and normalize malformed function calls from heterogeneous LLMs.
* **Authorization & Fuel Budget Control:** Intercepts requests to validate cryptographic proofs (`X-X402-Receipt`). If the allocated Fuel budget is exceeded during an ongoing stream, the gateway enforces a hard timeout and immediately terminates the connection to prevent budget overruns.
* **Advanced Pipeline Features:**
* Dynamic model fallbacks and mock/bypass capabilities for timeout simulations.
* Centralized prompt registry injection (`PromptTransformer`).
* Dynamic injection of custom guardrails (e.g., PII filtering) per request and comprehensive observability.

---

## 3. DPHI Sandbox Architecture

🔗 **[Read the Full Document: Sandbox Architecture](./phase/abc/dphi/milestone/sandbox.md)**

Defines the core sandbox engine principles for executing deterministic state transitions and isolated computations.

* **3-Tier Execution Layers:**
* **Tier 1 (General I/O Isolate):** A V8 Isolate-based gateway handling external network I/O and protocol translation (Non-deterministic).
* **Tier 2 (Constrained Pyodide):** An I/O-constrained Python runtime ensuring deterministic execution for business logic like AI agent inference and data transformation.
* **Tier 3 (Native WASM):** A deterministic native WASM execution layer for core system modules. Responsible for UTXO state updates, precision metering, and receipt issuance.


* **Ephemeral Runtime & Lock-Free UTXO:** Reduces idle daemon overhead by creating and destroying sandboxes on a per-request basis. The UTXO tree structure removes database locking bottlenecks, supporting concurrent scaling.

---

## 4. System Certification & Security Validation

🔗 **[Execution Logs: View Pipeline Reports](./phase/abc/log/)**

The DPHI infrastructure validates its cryptographic integrity, execution determinism, and perimeter security across the compute stack through a rigorous CI/CD test executed upon every build.

### 4.1. Core WASM Engine & Instruction-Level Determinism

🔗 **[View Log: workflow.wasm.log](./phase/abc/log/workflow.wasm.20260825.log)**

The core execution engine compiles native Rust modules (`dphi.wasm`, `dvm.wasm`) and runs them across distributed `TaskWasm` daemons. The engine utilizes **AOT (Ahead-of-Time) compilation caching** and **pre-warmed instance pools** to minimize sandbox instantiation latency.

* **Constrained Python Sandbox:** Executes untrusted Python logic (e.g., JSON parsing, math-heavy algorithms) inside a strict WASM memory boundary. It ensures deterministic execution, maintaining consistent PRNG sequences and floating-point computations across distributed nodes.
* **Resource Limit Enforcement:** Hardware limits are strictly governed per tier (`SYSTEM`: 256MB / 2B Fuel; `STANDARD`: 64MB / 10M Fuel). The hypervisor intercepts resource anomalies, including:
* **Fuel Exhaustion:** Triggers execution halt upon budget depletion (`wasm trap: all fuel consumed`).
* **Memory Limits:** Catches payload memory spikes exceeding the allocated boundary (`MemoryError`).
* **CPU & Call-Stack Limits:** Prevents infinite loops (via execution timeouts) and excessive call depths (`RecursionError`).


* **State Recovery & Parity:** Generates a cryptographic triplet (`topos_id`, `nexus_id`, `phase_id`) for state transitions. If network fragmentation occurs, the engine recovers missing topology frames via built-in XOR Parity.
* **Ledger Sealing:** Concludes the execution lifecycle by computing a root fingerprint and sealing the state into a verified canonical hash (`AuditReceipt`).

### 4.2. Edge V8 Isolate & Ingress Defense

🔗 **[View Log: workflow.flare.log](./phase/abc/log/workflow.flare.20260827.log)**

The Edge Ingress layer orchestrates dual V8 isolates (JS Router and Native Python Engine) within a simulated Cloudflare Worker environment to enforce execution boundaries before intents enter the core.

* **Environment Isolation:** Detects and blocks unauthorized access attempts at the edge. The isolate triggers permission errors against host filesystem inspection (e.g., `/etc/passwd`), low-level socket bindings, subprocess execution (`OSError: emscripten does not support processes`), and thread spawning.
* **Consensus & Fault Tolerance:** Dynamically generates Topos Anchor IDs and verifies Tripartite Parity triplets. The state machine enforces a 2-of-3 threshold consensus to reject invalid signatures and recover state frames.
* **Side-Channel Mitigation:** Reduces timer resolution to mitigate Spectre-style side-channel attacks and clears memory between execution contexts. It continuously validates connection teardowns to prevent worker hangs.

### 4.3. EVM Simulated Settlement & Ledger Validation

🔗 **[View Log: workflow.settlement.log](./phase/abc/log/workflow.settlement.20260825.log)**

DPHI embeds **REVM (Rust EVM)** within the WASM sandbox to deterministically simulate and validate off-chain deferred settlements for Web3 smart contracts.

* **Deterministic Simulation:** Simulates smart contract state transitions (e.g., ERC-20 `transferFrom` via signature `0x23b872dd`) against an injected state snapshot. This enables pre-validation of storage mutations prior to L1 broadcasting.
* **State Rollback Handling:**
* Halts execution and triggers rollbacks (`REVM Reverted`) when encountering insufficient account balances, depleted allowances, or expired mandates.
* Detects malformed calldata and invalid opcode injections (`0xdeadbeef`), halting the VM to preserve integrity.


* **L2 Rollup Readiness:** Validated transactions emit exact storage slot mutation proofs alongside an L2 Rollup Hash for settlement readiness.

### 4.4. End-to-End DePIN Orchestration & Fault Injection

🔗 **[View Log: phase.e2e.defin.log](./phase/abc/log/defin/phase.e2e.defin.20260830.log)**

The infrastructure utilizes an event-driven **Finite State Machine (FSM)** to decouple computational intent from infrastructure execution across decentralized physical infrastructure (DePIN) workflows.

* **Compute & Settlement Lifecycle:** Verifies caller identities via **EIP-712 signatures**, mints Genesis UTXOs (`100,000,000 Fuel`), orchestrates parallel WASM executions, and seals final micro-settlements (e.g., `$0.050001 USDC`) into L1 proofs.
* **FSM-Driven Request Validation:** Halts invalid transaction workflows before execution begins, rejecting zero-balance requests and unverified callers at the ingress boundary.
* **Fault Injection Resilience:** The test suite injects anomalies during active execution—such as forcing storage allowances to zero or corrupting transaction calldata in-flight. The FSM intercepts these anomalies, triggers safe halts, and maintains network stability.

### 4.5. API Gateway & L402 Ingress Validation

🔗 **[View Log: phase.e2e.edge.log](./phase/abc/log/edge/phase.e2e.edge.20260830.log)**

The FastAPI/Uvicorn-based REST Gateway serves as the entry point to the DPHI network, ensuring compute resources are allocated only to authenticated and funded requests.

* **L402 Payment & Tier Routing:** Handles incoming REST requests through a quoting phase (`/v1/public/agent/quote`), maps requests to execution tiers (`STANDARD` / `SYSTEM`), and clears charges via Web3 wallet and Rollup Adapters (`/v1/ext/wallet/pay/x402`).
* **Signature Verification:** Ingress traffic is inspected by a `VerifiedHttpClient`. If a payload’s signature does not match or is rejected by the WASM broker, the Edge returns `401 Unauthorized` and terminates the workflow.
* **Rate Limiting & WAF:** Deflects unauthorized or unfunded request spikes at the API perimeter with `HTTP 402 Payment Required` responses, preserving internal execution capacity for valid intents.