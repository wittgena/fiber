# README
@desc: Fiber Project - Agent Deterministic Infrastructure

The Fiber project is a **cryptographic metering proxy architecture** that physically and logically separates execution from settlement processes. Through deterministic state transitions and a Zero-Trust sandbox, it securely proxies external LLMs and computing resources while processing gas-free, real-time micro-transactions via an in-memory UTXO model.

This repository contains the Gateway and Sandbox modules, offering a 100% drop-in replacement for external AI systems alongside strict computational budget control and cryptographic integrity.

---

## 1. DPHI Gateway Overview
🔗 **[Read the Full Document: Gateway Overview](./phase/abc/dphi/overview.md)**

A universal cryptographic compute and metering proxy architecture agnostic to specific runtimes or settlement layers.

* **Execution & Ledger Agnosticism (BYOC & BYOS):** Acts as a transparent proxy that does not enforce a specific execution runtime. After sealing the session, it asynchronously routes proof data (`AuditReceipt`) to external ledgers (RDBMS, Vaults, DA, EVM) via Agnostic Egress Adapters.
* **Zero-Gas In-Memory Netting:** Leverages an off-chain UTXO model to process micro-transactions entirely in-memory, completely eliminating database row-locking and external network gas fees.
* **Core Components:**
  * `edge.llm`: A Zero-Trust router that ties LLM requests to computational intents, translating token usage into internal `Fuel` units and enforcing kill-switches.
  * `dvm.wasm`: A native WASM isolated environment guaranteeing a 0.000% state divergence rate through precise instruction-level metering.

---

## 2. LLM Compatibility & Edge Gateway
🔗 **[Read the Full Document: LLM Compat Entry](./phase/abc/llm/compat/entry.md)**
🔗 **[Read the Full Document: LLM Edge Gateway](./phase/abc/llm/edge.md)**

A Zero-Trust LLM gateway powered by an internal asynchronous channel pipeline. It provides a **100% compatible interface with the OpenAI SDK and LiteLLM**, allowing clients to integrate with DPHI simply by updating their Base URL and headers.

* **Complete Drop-in Replacement:** Fully supports Pydantic-compatible response objects, SSE streaming, and Tool Calling. It even includes a declarative state translator to automatically recover and normalize malformed function calls from heterogeneous LLMs.
* **Kernel Authorization & Fuel Budget (Kinetic Trap):** Intercepts requests to validate cryptographic proofs (`X-X402-Receipt`). If the allocated Fuel budget is exceeded during an ongoing stream, a "Kinetic Trap" physically severs the connection to prevent budget overruns.
* **Advanced Pipeline Features:**
  * Dynamic model fallbacks and mock/bypass for timeout simulations.
  * Centralized prompt registry injection (`PromptTransformer`).
  * Dynamic injection of custom guardrails (e.g., PII filtering) per request and comprehensive telemetry/observability.

---

## 3. DPHI Sandbox Architecture
🔗 **[Read the Full Document: Sandbox Architecture](./phase/abc/dphi/milestone/sandbox.md)**

Defines the core sandbox engine principles for executing deterministic state transitions and Zero-Trust computations.

* **3-Tier Execution Layers:**
  * **Tier 1 (General I/O Isolate):** V8 Isolate-based gateway handling external network I/O and protocol translation (Non-deterministic).
  * **Tier 2 (Constrained Pyodide):** A strictly constrained Python runtime with blocked I/O, ensuring deterministic execution for business logic like AI agent inference and data transformation.
  * **Tier 3 (Pure Direct WASM):** A fully deterministic native WASM execution layer for core system modules. Responsible for UTXO state updates, precision metering, and cryptographic receipt issuance.
* **Ephemeral Runtime & Lock-Free UTXO:** Eliminates idle daemon overhead by creating and destroying sandboxes on a per-request basis. The UTXO tree structure removes database locking bottlenecks, allowing for linear concurrency scaling.

---

## 4. System Certification & Security Validation
🔗 **[Execution Logs: View Pipeline Reports](./phase/abc/log/)**

The DPHI infrastructure mathematically proves its cryptographic integrity, execution determinism, and perimeter security across every layer of the compute stack. This is validated through a rigorous proprietary CI/CD test harness executed upon every build.

### 4.1. Core WASM Engine & Instruction-Level Determinism
🔗 **[View Log: workflow.wasm.log](./phase/abc/log/workflow.wasm.20260825.log)**

The core execution engine compiles native Rust modules (`dphi.wasm`, `dvm.wasm`) and runs them across distributed `TaskWasm` daemons. Designed for ultra-low latency, the engine utilizes **AOT (Ahead-of-Time) compilation caching** and **pre-warmed instance pools** (e.g., 11 concurrent VMs) to reduce sandbox instantiation wait times to absolute zero (`0.00ms`).

* **Constrained Python Legacy Jail:** Safely executes untrusted Python logic (e.g., JSON parsing, math-heavy algorithms) entirely inside a strict WASM memory boundary. It guarantees 100% determinism, ensuring that PRNG sequences and floating-point computations achieve bit-level parity up to 15 decimal places across distributed nodes.
* **Multi-Vector Cgroup Resource Traps:** Hardware limits are strictly governed per tier (`SYSTEM`: 256MB / 2B Fuel; `STANDARD`: 64MB / 10M Fuel). The hypervisor natively intercepts and neutralizes multi-vector resource attacks in O(1) time complexity, including:
  * **Fuel Exhaustion:** Triggers an immediate Kinetic Trap (`wasm trap: all fuel consumed`).
  * **Memory Leaks:** Catches payload memory spikes exceeding the 64MB boundary (`MemoryError`).
  * **CPU Deadlocks & Call-Stack Overflows:** Defends against intentional infinite loops (5.0s Remote Execution Timeout) and malicious depth calls (`RecursionError`).
* **Self-Healing State Engine & XOR Parity:** Generates a cryptographic triplet (`topos_id`, `nexus_id`, `phase_id`) for every state transition. If network fragmentation occurs, the engine automatically recovers missing topology frames via built-in XOR Parity recovery. 
* **Canonical Ledger Sealing:** Concludes every execution lifecycle by computing a tamper-proof root fingerprint and sealing the ecosystem state into a verified, unforgeable canonical hash (`AuditReceipt`).

### 4.2. Edge V8 Isolate & Cloudflare Perimeter Defense
🔗 **[View Log: workflow.flare.log](./phase/abc/log/workflow.flare.20260827.log)**

The Edge Ingress layer orchestrates dual V8 holograms (JS Router and Native Python Engine) within a simulated Cloudflare Worker environment to enforce OS-level Zero-Trust boundaries before compute intents enter the core.

* **OS-Level Kernel Isolation:** Actively detects and blocks low-level breach attempts at the edge. The isolate triggers immediate permission errors against host filesystem inspection (e.g., `/etc/passwd`), low-level socket bindings, subprocess execution (`OSError: emscripten does not support processes`), and thread spawning.
* **Causality & Byzantine Fault Tolerance:** Dynamically generates Topos Anchor IDs and verifies Tripartite Parity triplets. The state machine enforces a 2-of-3 threshold consensus, successfully quarantines rogue cryptographic signatures, and recovers lost state frames via XOR parity.
* **Hardware Attack Mitigation & Physical Boundaries:** Employs "Time Freezing" to neutralize Spectre-style side-channel attacks and eliminates warm-start memory bleeding between execution contexts. It continuously validates physical connection teardown (Error 1102 simulation) to prevent rogue worker hangs.

### 4.3. EVM Shadow Settlement & Ledger Validation
🔗 **[View Log: workflow.settlement.log](./phase/abc/log/workflow.settlement.20260825.log)**

Beyond general-purpose workloads, DPHI embeds **REVM (Rust EVM)** within the WASM sandbox to deterministically simulate and validate off-chain deferred settlements for Web3 smart contracts.

* **Deterministic Shadow Execution:** Simulates smart contract state transitions (e.g., ERC-20 `transferFrom` via signature `0x23b872dd`) against an injected state snapshot. This enables zero-gas pre-validation of storage mutations prior to L1 broadcasting.
* **Cryptographic Reversion Defense:** 
  * Accurately halts execution and triggers deterministic rollbacks (`REVM Reverted`) when encountering insufficient account balances, depleted allowances, or expired mandates.
  * Detects malicious calldata corruption and invalid opcode injections (`0xdeadbeef`), instantly halting the VM to preserve system integrity.
* **L2 Rollup Readiness:** Successfully validated transactions systematically emit exact storage slot mutation proofs alongside an L2 Rollup Hash, ensuring cryptographic settlement readiness.

### 4.4. End-to-End DePIN Orchestration & Chaos Engineering
🔗 **[View Log: phase.e2e.defin.log](./phase/abc/log/defin/phase.e2e.defin.20260830.log)**

The infrastructure utilizes an event-driven **Finite State Machine (FSM) Bridge** to decouple computational intent from infrastructure execution across decentralized physical infrastructure (DePIN) workflows.

* **Full-Cycle Compute & Settlement:** Verifies caller identities via **EIP-712 signatures**, mints Genesis UTXOs (`100,000,000 Fuel`), orchestrates parallel WASM executions across multi-agent clusters, and seals final micro-debts (e.g., `$0.050001 USDC`) into L1 settlement proofs.
* **FSM-Driven Perimeter Defense:** Natively halts invalid transaction workflows before execution begins, dropping zero-balance requests and rejecting unverified callers at the ingress boundary.
* **Chaos Engineering Resilience:** The test suite injects Byzantine faults during active execution—such as forcing storage snapshot allowances to zero and corrupting transaction calldata packets in-flight. The FSM intercepts these anomalies, triggers graceful halts, and maintains overall network stability.

### 4.5. Zero-Trust API Gateway & L402 Ingress Validation
🔗 **[View Log: phase.e2e.edge.log](./phase/abc/log/edge/phase.e2e.edge.20260830.log)**

The FastAPI/Uvicorn-based REST Gateway serves as the strict, cryptographic front door to the DPHI network, guaranteeing that compute resources are never expended on unauthorized or unfunded requests.

* **L402 Payment & Tier Routing:** Dynamically handles incoming REST requests through a dry-run quoting phase (`/v1/public/agent/quote`), maps requests to isolated execution tiers (`STANDARD` / `SYSTEM`), and clears charges via native Web3 wallet and Rollup Adapters (`/v1/ext/wallet/pay/x402`).
* **Cryptographic Attestation & MitM Defense:** All ingress traffic is inspected by a `VerifiedHttpClient`. If a payload’s cryptographic signature does not match its contents or an intent signature is rejected by the WASM broker, the Edge immediately returns `401 Unauthorized` and terminates the workflow.
* **Sentinel WAF & DDoS Protection:** Deflects unauthorized or unfunded request spikes at the API perimeter with strict `HTTP 402 Payment Required` responses, ensuring internal execution capacity is preserved exclusively for funded, authenticated intents.