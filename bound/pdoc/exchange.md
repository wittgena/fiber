# abc.fiber.receptor.edge.exchange
@desc: A2A Clearinghouse Flow & Cross-VM Deterministic Settlement Specification

## 1. Architectural Baseline

The system executes cross-domain intent matching (Compute vs. Capital) and state transitions within an isolated, deterministic off-chain WebAssembly (WASM) sandbox (`dphi.wasm`). External ledgers (e.g., EVM, SVM environments) function exclusively as data ingestion sources and execution sinks. They are entirely decoupled from the core matching and execution logic, acting merely as final settlement layers for the Agent-to-Agent (A2A) economy.

---

## 2. Technical Exchange & Clearing Pipeline

### Phase 1: Ingress & Cross-VM State Shadowing

* **Endpoint:** `POST /v1/eco/exchange/order/ingress`
* **Input:** Cryptographically signed Agent Intents (e.g., Request for Compute, Resource Bid/Ask) and optional x402 headers.
* **Execution:**
1. The Internal Edge gateway validates cryptographic signatures and topological fuel boundaries (`Membrane Strict Block`).
2. The orchestrator dispatches an `init_epoch` execution intent to the WASM broker.
3. The `ShadowAdapter` queries external L1/L2 adapters (via `Web3Adapter`) to fetch instantaneous account states (balances, nonces) without triggering any on-chain transactions.


* **Output:** Session assignment and Epoch configuration (`nexus_id`, `phase_id`, `topo_id`).

### Phase 2: Deterministic Matching & Fuel Netting

* **Execution:**
1. The matching engine dynamically binds Execution State A (Agent Requesting Compute/Data) and Execution State B (Sandbox providing Fuel/Execution) within isolated memory.
2. Computes the exact resource consumption variables and verifies residual financial imbalance ($imbalance = 0$).
3. The WASM tasker enforces strict physical resource bounds via `Cgroup` (Memory limits, Max Fuel budgets).


* **Output:** Validated clearing batch state and exact USD-metered execution costs.

### Phase 3: Multi-Sig Consensus (Epoch Collapse & Pre-Finalization)

* **Endpoint:** `POST /v1/eco/exchange/clearing/receipt/generate`
* **Execution:**
1. Dispatches the `seal_epoch` execution intent to the `DphiBroker`.
2. A distributed `NotarySwarm` executes the exact deterministic WASM binary independently across nodes to prevent state divergence.
3. Generates a matching root fingerprint and executes an $N$-of-$N$ multi-signature attestation on the final state.


* **Output:** Cryptographic Clearing Receipt containing the canonical `commit_hash`, execution trace (`vm_trace_hash`), and attestation signatures.

### Phase 4: Asynchronous Projection & External Claim (L1/L2 Settlement)

* **Execution:**
1. Renders the finalized matching batch into a serialized payload (`Batch ID`, `State Root`, `Attestations`, `Accrued Debt`).
2. Emits the payload to the External Edge (`receptor.edge.ext`).
3. If immediate settlement is required, the `RollupAdapter` or `EthWalletAdapter` executes a `transferFrom` (Deferred Pull Settlement) on the external ledger to physically collect the USDC/WETH debt from the agent's external wallet.


* **Output:** True finality on external ledgers (EVM/SVM), strictly executing the pre-determined cryptographic claims.

---

## 3. Flow Differences & Resulting Technical Phenomena

| Architectural Dimension | Conventional Model (DEX / Bridge Relays / Cloud APIs) | Zero-Trust Sandbox Clearinghouse (`dphi.wasm`) |
| --- | --- | --- |
| **Execution Path** | Synchronous, direct on-chain execution per intent. | Off-chain WASM memory execution with batched programmatic netting. |
| **Cross-VM Interop** | Lock-and-Mint (Synthetic assets) or Asynchronous Message Relaying. | Synchronous multi-VM memory shadowing via `ShadowAdapter`. |
| **Finality Mechanism** | Ledger-dependent block time confirmation. | Cryptographic pre-finalization via multi-sig `NotarySwarm`. |

### 3.1. Phenomenon of Off-Chain Netting (vs. Direct On-Chain Execution)

* **Operational Mechanism:** Agent intents and compute costs are aggregated and mathematically balanced ($imbalance = 0$) within off-chain memory prior to any external ledger interaction.
* **Resulting Consequences:**
* **Zero Intermediate Gas Consumption:** The execution and matching phases incur zero ledger transaction fees, enabling micro-transactions (e.g., paying $0.0001 for a single LLM inference or WASM tick).
* **Elimination of Mempool Front-Running:** Because intents never enter public mempools prior to clearing, sandwich attacks and transaction reordering vectors are structurally neutralized.
* **Throughput Decoupling:** System transaction throughput is constrained solely by local CPU/WASM execution speeds rather than external block generation intervals.



### 3.2. Phenomenon of Cross-VM State Shadowing (vs. Lock-and-Mint / Relays)

* **Operational Mechanism:** The engine projects the states of disparate virtual machines (e.g., EVM for payments, Sandbox WASM for compute, SVM for data availability) into a unified local memory space simultaneously.
* **Resulting Consequences:**
* **Abolition of Synthetic Assets:** Eliminates the necessity for wrapped tokens, removing the associated bridge-pool Total Value Locked (TVL) honeypot vectors.
* **Prevention of Cross-Chain Execution Desynchronization:** Eliminates partial execution states where a transaction succeeds on one network while reverting on another due to execution speed differentials (e.g., EVM 12s vs. SVM 400ms). State validity and sufficient funding are computed simultaneously before external publishing.



### 3.3. Phenomenon of Pre-Finalization via Multi-Sig Attestation (vs. Ledger-Dependent Finality)

* **Operational Mechanism:** State transitions achieve absolute finality the moment the `NotarySwarm` signs the canonical hash off-chain. External ledgers act merely as passive verification sinks.
* **Resulting Consequences:**
* **Demotion of External Ledgers to Courts of Appeal:** External ledgers do not process trade or compute logic; they execute binary state updates and asset transfers only upon cryptographic proof validation.
* **Deferred Asynchronous Claiming (Netting Claims):** Agents and the Sandbox are not bound to atomic, immediate on-chain settlement windows. Cryptographic receipts function as standalone transferable assets, enabling deferred batched settlement (e.g., End-of-day debt collection via `transferFrom`) to minimize overall network fee overhead.