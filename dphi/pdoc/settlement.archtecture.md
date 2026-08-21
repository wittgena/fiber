# dphi.pdoc.settlement.architecture 
@desc: DPHI Universal Cryptographic Metering & Settlement Architecture

---

## 1. Abstract

This document defines the architecture of the **DPHI Settlement Engine**, a deterministic, ultra-high-frequency cryptographic metering and netting gateway designed for usage-based AI economies and enterprise micro-billing.

The core philosophy of this architecture is the **Absolute Decoupling of both Execution and Settlement**. DPHI functions as a "Universal Cryptographic Metering Proxy." It sits transparently in front of any compute environment, intercepts API traffic, dynamically nets micro-costs in-memory, and emits mathematically unforgeable receipts. 

**DPHI dictates neither what you execute nor where you settle.** Enterprises can route traffic to their existing proprietary LLMs or SaaS backends (BYOC) and drop the resulting cryptographic billing proofs into any storage medium or ledger (BYOS)—from traditional relational databases to public Web3 networks—with zero architectural friction.

---

## 2. Core Architectural Principles

### 2.1. Bring Your Own Compute (BYOC) - Execution Agnosticism
DPHI acts as a transparent reverse proxy. It does not force workloads into a specific runtime. Enterprises keep their existing AI models, load balancers, and backend servers. DPHI strictly handles the perimeter authorization, high-frequency metering, and cryptographically signed billing, passing only validated traffic to the core business logic.

### 2.2. Bring Your Own Settlement (BYOS) - Ledger Agnosticism
Once a billing session is netted and sealed into a `canonical_hash` (AuditReceipt), the Engine's responsibility ends. Pluggable Egress Adapters route this mathematical proof to any target: internal RDBMS (PostgreSQL), compliance vaults, or Web3 Smart Contracts (EVM), completely decoupling high-frequency billing from storage latency.

### 2.3. Zero-Gas In-Memory Netting
All micro-transactions (e.g., token-by-token LLM API billing down to $0.0001) are computed strictly in-memory using an off-chain UTXO (Unspent Transaction Output) model. This eliminates traditional database row-locking bottlenecks and payment gateway minimum fees (`100,000+ TPS`).

---

## 3. Core Engine Components

### 3.1. The UTXO Billing Adapter (`kernel.dphi.adapter.utxo`)
Manages concurrent state transitions without centralized locks.
* **State Pointers:** Tracks unspent balances (Fuel/Credit) across thousands of parallel API consumers.
* **Micro-Billing Validation:** Evaluates requests strictly mathematically, verifying Ed25519 signatures and UTXO pointers in `< 150 µs`.
* **Overdraft & Enterprise Credit:** Supports deferred billing where negative (overdraft) UTXOs are authorized based on enterprise whitelist policies, bounded securely by the client's cryptographic signature.

### 3.2. Dynamic Metering Engine (NEW)
Translates arbitrary compute usage into UTXO cost deductions in real-time.
* **Header & Payload Parsing:** Intercepts responses from the external compute backend to extract usage metrics (e.g., `x-llm-input-tokens: 500`, `x-llm-output-tokens: 150`).
* **Multi-Dimensional Pricing Rules:** Applies customizable, multi-factor cost algorithms (e.g., `(Input * A) + (Output * B) = Total Fuel Deducted`) dynamically before sealing the transaction.

### 3.3. Merkle Rollup Compressor
* **Function:** Aggregates tens of thousands of individual micro-billing hashes into a single binary tree, computing a singular **Canonical Root Hash** (`canonical_hash`).
* **Utility:** Provides an immutable, `O(log N)` verifiable proof of usage (AuditReceipt) for resolving enterprise billing disputes or satisfying compliance audits.

---

## 4. The Proxy Mode Lifecycle (State Transition Workflow)

When deployed as a Standalone Metering Gateway in front of existing enterprise infrastructure, the lifecycle operates as follows:

1. **Ingress & Pre-Auth (The Gatekeeper):**
   * Client submits an API request signed via Ed25519.
   * DPHI Gateway intercepts, validates the signature, and confirms the client holds a valid UTXO pointer (sufficient credit/allowance).

2. **Transparent Forwarding (Target Compute):**
   * Upon validation, DPHI strips the cryptographic envelope and forwards the raw request to the enterprise's existing backend (e.g., Proprietary LLM cluster, RAG server).
   * DPHI awaits the response, holding the state pointer in memory.

3. **Post-Metering & Dynamic Deduction:**
   * The enterprise backend returns the response and usage metadata to DPHI.
   * The **Dynamic Metering Engine** parses the usage, calculates the exact micro-cost, and executes a UTXO split/deduction in-memory.

4. **State Collapse & Merkle Compression:**
   * At session end (or fixed intervals), the UTXO Adapter merges all remaining change/debt for the client.
   * The Engine computes the Merkle Root of all transactions and issues a multi-sig sealed **`AuditReceipt`**.

5. **Agnostic Egress (Customer Routing):**
   * DPHI returns the actual API response to the client alongside the `AuditReceipt`.
   * Simultaneously, DPHI hands the Rollup Data to the configured Egress Adapter for backend storage.

---

## 5. Agnostic Egress & Adapter Matrix

| Adapter Type | Target Infrastructure | Primary Use Case |
| :--- | :--- | :--- |
| **`adapter.egress.rdbms`** | PostgreSQL, Oracle | **Web2 SaaS Billing:** Drops the Rollup Hash into a relational row for traditional monthly invoicing. |
| **`adapter.egress.vault`** | Enterprise Private Vaults | **EU AI Act Compliance:** Proofs are kept private, accessed only during legal audits to prove AI system traceability and non-repudiation. |
| **`adapter.egress.da`** | Celestia, EigenDA | **Web3 / DePIN:** Requires public Data Availability (DA) for decentralized networks without consensus overhead. |
| **`adapter.egress.evm`** | Ethereum, Base, Arbitrum | **On-Chain Clearing:** Formats the net debt receipt into standard EVM `calldata` (e.g., `transferFrom`) for trustless smart contract settlement. |

---

## 6. Target Performance Metrics

* **Proxy Pass-through Latency:** `< 500 µs` overhead added to standard API requests.
* **Cryptographic Netting Latency:** `< 1 ms` per state entanglement and UTXO update.
* **Throughput:** `100,000+` state transitions per second (TPS) per single node. Completely immune to traditional database row-locking latency.

---
**End of Specification**