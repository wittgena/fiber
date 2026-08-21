# dphi.receptor.pdoc.public.edge
@desc: DPHI Edge Technical Specification

## 1. Architectural Overview

DPHI Edge is a Zero-Trust compute gateway architected to structurally eliminate the vulnerabilities of legacy AI proxies (e.g., supply chain attacks, Remote Code Execution, and centralized API key theft). Moving beyond simple traffic routing, DPHI enforces strict execution control within a WASM-based isolated environment and issues cryptographic Proofs-of-Action, guaranteeing strictly "Verifiable Compute."

---

## 2. Core Technical Baseline & Differentiators

The DPHI architecture explicitly removes trust assumptions from host operating systems and databases. Instead, it operates on a foundation of mathematical proofs and deterministic execution, driven by four core technical pillars.

### 2.1. Deterministic WASM Sandbox

Instead of relying on host OS Python runtimes or vulnerable Docker containers, all computations are isolated using a native WASM runtime (`dvm.wasm`).

* **Physical Resource Control:** Introduces `max_fuel` to precisely meter executed instructions. Malicious infinite loops or Out-Of-Memory (OOM) attacks are instantly terminated at the kernel level.
* **Deterministic State Transitions:** Operations occur in an I/O-segregated memory space. Identical inputs invariably produce identical outputs and state fingerprints, structurally preventing state divergence across distributed nodes.

### 2.2. Cryptographic Notarization

DPHI replaces standard database logging with mathematical proofs issued by the `dphi.wasm` kernel.

* **Kernel Sealing:** The `dphi.wasm` engine functions as an isolated cryptographic calculator, decoupled from network I/O. It computes root fingerprints by hashing and signing the canonicalized bytes of incoming payloads.
* **Non-Repudiation:** Execution traces and metrics are returned as an `AuditReceipt` containing multi-signature consensus (`NotarySwarm`) and ZK/Merkle Proofs. This makes retroactive tampering mathematically impossible.

### 2.3. L402-based Off-Chain Clearing

DPHI eliminates centralized API key databases—a primary target for attackers—enabling verifiable micro-transactions.

* **Fuel Netting:** Utilizing the `X-X402-Receipt` header, compute costs are netted in off-chain memory per request, enabling zero-gas micro-cent transactions.
* **Deferred Pull Settlement:** Finalized compute debt is collected asynchronously via L1/L2 smart contracts (`transferFrom`) from the external agent's wallet, effectively decoupling compute finality from settlement finality.

### 2.4. Asynchronous I/O Decoupling

To prevent heavy cryptographic operations from bottlenecking high-throughput traffic, the processing pipeline is strictly decoupled.

* **Synchronous Proofs, Asynchronous Storage:** Upon payload ingestion, the system synchronously returns a Root Fingerprint via `dphi.wasm` (`200 OK`). The heavy lifting of ledger recording and data rollups is delegated to background tasks via `Distributed Pub/Sub`.

---

## 3. Public API Specification (`receptor.edge.public`)

**API Selection Rationale:**
While the Public API is extensible for future workloads, the current endpoints are prioritized to satisfy the four fundamental prerequisites of an autonomous AI ecosystem: secure compute, observability, accountability, and trust distribution. They form the minimal verifiable interface for external agents to safely execute code, prove outcomes, and monitor systems.

All endpoints mandate the use of `VerifiedHttpClient` to cross-verify cryptographic signatures between client and server, neutralizing Man-in-the-Middle (MitM) vectors.

### 3.1. Fetch Trusted Signer Keys (`GET /v1/public/keys`)

Distributes the initial Trust Anchor required for Zero-Trust communication.

* **Necessity:** Clients must securely acquire the server's public keys without MitM interference to validate the integrity of subsequent server responses.
* **Mechanism:** Returns a list of active node public keys, pre-signed offline by a hardcoded Master Root Key. Clients call this endpoint once (Lazy Loading) to establish a cryptographic baseline.

### 3.2. Single Agent Intent Execution (`POST /v1/public/agent/execute`)

Isolates and executes untrusted workloads from external agents.

* **Necessity:** To safely run third-party agent code without risking the host system, while precisely metering resource usage for billing.
* **Mechanism:** Receives a payment proof (`X-X402`) and the payload, routing it to the internal WASM sandbox for execution within the `max_fuel` limit.
* **Output:** Returns an immutable **`AuditReceipt`** containing the execution result, consumed Fuel, and multi-signature consensus data.

### 3.3. OTLP Telemetry Ingress (`POST /v1/public/telemetry/logs`)

Collects observability data while guaranteeing payload integrity.

* **Necessity:** Provides a drop-in interface for AIOps monitoring that prevents post-hoc log tampering without requiring modifications to existing pipelines (e.g., Datadog).
* **Mechanism:** Ingests standard OTLP log structures, extracts key metrics, and synchronously returns a **content hash and cryptographic fingerprint** in the response headers via the `dphi.wasm` kernel.

### 3.4. Audit Event Recording (`POST /v1/public/audit/event`)

Performs encrypted logging for legal evidence and accountability in highly regulated environments.

* **Necessity:** Actions involving sensitive data (PII) require strict non-repudiation mechanisms to clarify liability during security incidents.
* **Mechanism:** Sanitizes and encrypts sensitive data within the event payload before recording it to the ledger. Returns a state fingerprint, and upon request (`verbose=true`), attaches a **Merkle Proof** to mathematically verify ledger inclusion.