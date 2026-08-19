# abc.fiber.receptor.edge.spec
@desc: Architectural Specification for the Deterministic Edge Gateway

---

## 1. Executive Summary

The `dphi` serves as the tripartite nervous system of the Zero-Trust Physical Sandbox. It bridges external agent economies with the internal deterministic execution environment (`dphi.wasm`).

Unlike traditional API gateways that route traffic to centralized databases, this Edge architecture operates entirely as a **Stateless Cryptographic Notary** and **Financial Clearinghouse**. Every incoming request is mathematically sealed via WASM fingerprints before it is asynchronously routed, and no code is executed without explicit, cryptographically verifiable financial intent (x402).

## 2. Architectural Topology

The Edge module is decoupled into three highly specialized micro-routers to handle massive concurrency while maintaining strict security boundaries:

1. **`receptor.edge.public`**: The Zero-Trust Ingress (External facing)
2. **`receptor.edge.internal`**: The Execution & Clearing Engine (Internal network only)
3. **`receptor.edge.ext`**: The Web3 Reality Bridge (L1/L2 settlement facing)

---

## 3. Sub-Module Specifications

### 3.1 Public Edge (`receptor.edge.public`)

**Role:** The front-door for external A2A traffic and standard OTLP logs. It focuses strictly on authentication, payload sanitization, synchronous cryptographic sealing, and asynchronous dispatch.

* **Synchronous Notarization, Asynchronous Processing:**
* Endpoint: `POST /v1/public/telemetry/logs`
* Instead of blocking HTTP threads to write to storage, the Public Edge extracts metrics via `StrictOtlpExtractionEngine`, instantly generates a Root Fingerprint via `dphi.wasm`, returns it to the client, and delegates the heavy lifting to `otlp_global_stream` (Distributed Pub/Sub) via BackgroundTasks.


* **Intent Proxying:**
* Endpoint: `POST /v1/public/agent/execute`
* Accepts `CodebotIntent` and `X-X402-Receipt` headers. It does not execute the code itself. Instead, it forwards the payload to the Internal Edge for validation and execution, acting purely as an orchestration boundary.



### 3.2 Internal Edge (`receptor.edge.internal`)

**Role:** The trusted clearinghouse. It is shielded from the public internet and handles fuel metering, cryptographic signature verification, state consensus, and WASM sandbox invocation.

* **Strict Boundary Validation:**
* Endpoint: `POST /v1/eco/compute/intent/validate`
* Utilizes `ecrecover` (ECDSA) to cryptographically verify that the requested `agent_id` matches the signature attached to the intent.
* Enforces Topological Fuel Limits (e.g., rejecting payloads requesting > 10,000,000 Fuel) to prevent sandbox OOM (Out of Memory) and topology rupture.


* **Billed Execution & Profiling:**
* Endpoint: `POST /v1/eco/profile/execute/billed`
* Executes the workload inside the `BenchProfile`, calculates exact Wasm instructions ("Fuel"), and converts the consumed compute resources into USD equivalents based on current network configuration (`billing_config`).


* **Immutable State Anchoring:**
* Endpoint: `POST /v1/core/ledger/stream/append` & `/v1/core/anchor/seal`
* Responsible for epoch finalization. Proposes state transitions to the `NotarySwarm` and anchors the final Merkle Proof into the core ledger stream.



### 3.3 External Edge (`receptor.edge.ext`)

**Role:** The financial settlement bridge. It materializes the internal virtual Fuel/Credits into real-world Web3 assets (USDC, WETH).

* **Dual-Mode Settlement:**
* Supports both **Native EVM** (Instant, on-chain L1 transactions) and **DVM Ledger** (Zero-gas rollup settlements via the internal clearinghouse).
* Endpoint: `POST /v1/ext/wallet/pay/x402` builds an `X402Invoice` and coordinates the actual asset transfer based on the selected mode.


* **Deferred Pull Settlement (Debt Collection):**
* Endpoint: `POST /v1/ext/wallet/settle/deferred`
* When an agent accrues internal debt, the Clearinghouse master wallet invokes L1 Smart Contracts (`transferFrom`) to forcefully pull USDC from the external agent's authorized wallet. This mathematically guarantees revenue for consumed compute.


* **Utility Endpoints:**
* Includes functions for wrapping native tokens (`/evm/wrap`) and querying cross-chain balances to ensure liquidity before processing heavy intents.



---

## 4. Core Workflows (The Edge Lifecycle)

### Workflow A: The OTLP Ingress & Zero-Cost Pipeline

1. External agent posts raw telemetry to `Public Edge` (`/telemetry/logs`).
2. `Public Edge` parses the `X-X402-Receipt` (verifying payment for log ingestion) and extracts valid metrics.
3. `Public Edge` calls `Internal Edge` (`dphi.wasm`) synchronously to compute a cryptographic `fingerprint`.
4. `Public Edge` returns `200 OK` with the `fingerprint` to the client.
5. In the background, the payload is published to `otlp_global_stream`. Downstream Regulators (Kinematic Folding mechanism) consume this queue, compress bursts, and route data to local BYOS storage.

### Workflow B: Billed Agent Execution (A2A Compute)

1. Agent submits `CodebotIntent` + `x402 Receipt` to `Public Edge`.
2. `Public Edge` forwards to `Internal Edge` (`/intent/validate`).
3. `Internal Edge` verifies the cryptographic signature (ECDSA) and checks Fuel bounds.
4. If valid, `Internal Edge` runs the code via `/execute/billed`, calculating the exact Fuel burned and total USD cost.
5. The execution trace is hashed, sent to `dphi.wasm` for a Merkle Proof, and returned as an `AuditReceipt`.
6. If the agent is on a postpaid/deferred tier, the `Ext Edge` is triggered later to pull USDC directly from the agent's L1 wallet (`/settle/deferred`).

---

## 5. Security & Determinism Constraints

* **No Direct Storage Writes:** The Edge tier does not possess database drivers. It interacts exclusively through `PubSub` for data flow and `DphiBroker` for state transitions.
* **Kernel Segregation:** The `dphi.wasm` engine acts purely as a mathematical calculator (`COMPUTE_ROOT_FINGERPRINT`, `GENERATE_PROOF`, `INIT_EPOCH`). It is entirely decoupled from network I/O, ensuring deterministic execution under any load.
* **Financial Primitives First:** The integration of the `X-X402-Receipt` header at the edge ingress physically prevents DDoS attacks; un-funded traffic is dropped before it reaches the WASM execution layer.