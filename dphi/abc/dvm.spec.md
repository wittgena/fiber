# dphi.abc.dvm.spec
@lineage: dphi.abc.dvm
@desc: `dvm.spec` : Multi-VM Hypervisor Architecture Specification

## 1. Overview

The **DVM (Decentralized Virtual Machine)** is not a traditional blockchain node. It is an **Enterprise-Grade Multi-VM Hypervisor OS**. It abstracts underlying execution engines (e.g., Ethereum's EVM, Cosmos's CosmWasm) into lightweight, stateless WebAssembly (`.wasm`) plugins. Orchestrated by a Python-based host, it completely decouples state management, transaction routing, and cryptographic verification from the core virtual machines, enabling infinite horizontal scalability and seamless cross-ecosystem interoperability.

---

## 2. The 5-Phase Execution Pipeline (Main Flow)

The DVM guarantees rigorous determinism and cryptographic integrity by strictly adhering to a 5-Phase pipeline for every transaction or intent.

### Phase 1: Intent Resolution & Routing

* **Function:** Analyzes the raw user intent to determine the target execution environment.
* **Mechanism:** Routes the payload to either the EVM engine (`dvm.wasm`) or the CosmWasm engine (`cw20_base.wasm`), normalizing inputs into VM-specific structures (e.g., translating CosmWasm's `env, info, msg` JSON vs. EVM's `calldata, value`).

### Phase 2: Shadow State Projection

* **Function:** Constructs a lightweight, isolated local database for the transaction.
* **Mechanism:** Fetches real-time base state (balance, nonce, storage slots, bytecode) from target mainnets (e.g., via Alchemy RPC) and projects it into local memory. It supports dynamic state overrides for complex DeFi routing (e.g., Uniswap exactInputSingle) without altering the actual on-chain state.

### Phase 3: Phronetic VM Simulation

* **Function:** Just-In-Time (JIT) stateless sandboxed execution.
* **Mechanism:** The host instantiates the target `.wasm` engine, pushes the shadow state into its memory, executes the transaction, and pops the resulting State Diff (Residue) and Trace. The VM is immediately destroyed after execution, preventing memory leaks.

### Phase 4: Trace & Integrity Verification

* **Function:** Validates the execution trace against expected scenario rules.
* **Mechanism:** The `TraceVerifier` engine analyzes the JSON trace independently of the VM. It guarantees cryptographic integrity by asserting specific boundary conditions—such as validating an intentional `REVERT`, catching ERC-4337 `AA90` violations, or verifying cross-VM inversion calls.

### Phase 5: Cryptographic Proof Sealing

* **Function:** Finalizes the off-chain execution for on-chain settlement.
* **Mechanism:** Validated state diffs and execution traces are hashed. A Decentralized Notary Swarm (multiple independent nodes) cryptographically signs the hash, producing a verifiable `Receipt ID` ready for ZK-Rollup or Coprocessor settlement layers.

---

## 3. Core Features

* **Stateless JIT Sandboxing:** Execution engines carry no internal database (No MPT or IAVL trees). They are pure computational functions. State is pushed dynamically by the Host OS and popped out as JSON state diffs.
* **Zero-Modification Cross-VM Hooks (Inversion):** EVM and CosmWasm can communicate synchronously. An EVM contract can trigger a Host-Escape Hatch (`invoke_native_vm`), allowing the Python Host to spin up a CosmWasm sandbox, execute logic, and return the result to the EVM—all without modifying the core Rust source code of `revm` or `cosmwasm-std`.
* **OS-Level Resource Isolation:** Gas metering is no longer a heavy burden on the VM. CPU and memory limits are strictly enforced at the OS level via `Cgroup` (Wasmtime Fuel). A malicious infinite loop simply halts the isolated sandbox without affecting the Host OS or other transactions.

---

## 4. Key Paradigm Shifts: DVM vs. Traditional Blockchains

The DVM introduces a fundamental "Inversion of Control" compared to traditional Monolithic or Hybrid blockchain nodes (e.g., standard Cosmos SDK + EVM chains).

| Feature | Traditional Blockchain Nodes | DVM Hypervisor Architecture |
| --- | --- | --- |
| **Locus of Control** | The Monolithic Node (Go/Rust) embeds VMs as static libraries. | The Host OS (Python) acts as the hypervisor orchestrating VMs as standalone plugins. |
| **State Management** | Deep Coupling. EVM and Wasm state trees (MPT, IAVL) require hardcoded, complex translation layers. | Total Decoupling. The Host translates all states into a unified JSON format. VMs are completely stateless. |
| **Engine Upgrades** | Requires global network **Hard-forks** to update EVM versions or add new VMs. | Supports **Hot-Swapping**. Simply replace the `.wasm` file on disk with zero downtime. |
| **Execution Model** | Synchronous, sequential execution bottlenecked by internal gas metering. | Asynchronous, parallel JIT execution isolated by OS-level Cgroups. |
| **Interoperability** | Siloed. VMs cannot easily communicate within the same transaction without massive protocol upgrades. | Native Cross-VM Bridge. The Host acts as a universal router between any VM types (`EVM ↔ Host ↔ CosmWasm`). |
