# fiber.phase.abc.spec.fsm.runner

@desc: Core Engine Architecture, 2KB Claim-Check IR, and M:N PTA State Transition Specification

**Fiber FSM/DAG Runner** is a deterministic, WASM-verified, and ledger-anchored multiverse execution engine for autonomous AI agents. Built on top of **Phase Transition Anchors (PTA)**, **Content-Addressable Storage (CAS)**, and the **Canonical FSM WASM Kernel**, it compiles standard and shorthand AWS Step Functions (ASL) definitions into ultra-compact (`<= 2KB`) topological blobs, statically prevents barrier deadlocks prior to execution, and governs parallel hypothesis branching with strict FinOps fuel conservation.

> **Authoring & Integration Guide:** For ASL syntax rules, Polyglot URI schemes (`fn://`, `py://`, `http(s)://`, `exec://`, `mcp://`), SDK recipes, and progressive framework onboarding, refer to **[`fiber.phase.abc.spec.fsm.guide`](/phase/abc/spec/fsm/guide.md)**.

---

## 1. Architectural Overview

Traditional agent graph frameworks serialize massive, mutable state snapshots into databases—leading to out-of-memory (OOM) crashes, unreplayable side effects, barrier deadlocks, and runaway API costs during parallel exploration.

Fiber solves this by physically decoupling **Graph Topology & Static Verification (WASM Kernel)**, **Heavy I/O Payloads (`CAS`)**, and **Execution Lineage & Budget (`PTA AnchorStore`)** across five tightly integrated architectural layers backed by a zero-I/O **Shared Contract** foundation:

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ 5. Orchestration Layer (GraphTraverser)                                      │
│    • Unified Multiverse Scheduler (`run`, `traverse_bundle`, `resume`)       │
│    • Automatic 1:N Fan-Out Detection, Barrier Sync, and N:1 Arbiter Collapse │
│    • Fault-Tolerant `Catch` (`fb`) Fallback Routing & Fuel Reclamation       │
└──────────────────┬────────────────────────────────────────┬──────────────────┘
                   │ State & Anchor Transitions             │ I/O Actuation
                   ▼                                        ▼
┌─────────────────────────────────────────┐  ┌─────────────────────────────────┐
│ 3. State & PTA Runtime Layer            │  │ 4. Execution Bridge Layer       │
│    (BranchController)                   │  │    (Fiber / Polyglot Bridge)    │
│    • Unified M:N PTA Commit Engine      │  │    • Drop-in LLM Completion     │
│      (0:1 Spawn, 1:1 Step, 1:N Fork,    │  │      + Real-time `StreamBreaker`│
│       N:1 Collapse & Merge)             │  │    • Stateless `MCP Gateway`    │
│    • O(1) Cursor Rehydration (`tx:idx`) │  │    • Polyglot `OPERSIG` Dispatch│
│    • Dual-WASM Guard & `DagJoinBarrier` │  │      (`fn`, `py`, `http`, `exec`│
│    • Read-Only Proposer CAS Cache Sync  │  │       + Unified `202` HITL Park)│
└──────────────────┬──────────────────────┘  └──────────────┬──────────────────┘
                   │ 2KB Blob & Step Guard                  │ Shared Contract
                   ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ 2. Compiler & Shared Contract Layer (FSMCompiler, StateProjector & Contract) │
│    • Single-Source URI Parser (`ParsedResource`), `.$` & `Projection` Slicer │
│    • Automatic `Choice` Variable Inference & Off-Store `TaskIOSpec`(`blob:`) │
│    • Compact 2KB JCS Projection (`CompactKey`) & Unified `NodeType`          │
└─────────────────────────────────────┬────────────────────────────────────────┘
                                      │ FFI & WASM Adapter Protocol
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ 1. Canonical WASM Kernel Layer (FSM & Phase Verification VM)                 │
│    • Auto-Linker: Bottom-Up SESE Rolling Fold for `>2KB` Graphs (`SYMLINK`)  │
│    • Static Sandbox: Pre-Mount `DagJoinBarrier` Deadlock Linter (`fb` check) │
│    • Runtime Step Guard: `StreamBreaker`, `ROUTER` Eval, and Cross-VM Bridge │
└──────────────────────────────────────────────────────────────────────────────┘

```

### Layer & Component Responsibilities

| Architectural Layer | Core Component | Core Responsibility |
| --- | --- | --- |
| **1. WASM Kernel Layer** | `FSM & Phase VM` | Executes AOT-cached WASM compilation and runtime guards. Automatically partitions `>2KB` graphs into `branch:<hash>#entry` subgraphs, rejects parallel branches lacking `Catch` (`fb`) fallbacks, and validates runtime transitions. |
| **WASM Adapter Bridge** | `FsmAdapter` | Constructs standardized `FSM_COMPILE`, `FSM_STEP`, `FORK`, `MERGE`, and `HITL_PARK` dispatch payloads between the host runtime and the WASM broker. |
| **2. Contract & Compiler** | `FSMCompiler` & `Contract` | Centralizes zero-I/O URI scheme classification (`parse_resource_uri`), dynamic `.$` resolution, and `Projection` slicing (`apply_projection`). Strips `Parameters` into off-ledger `TaskIOSpec` CAS blobs (`blob:<hash>`) and delegates 2KB linking/linting to the WASM kernel. |
| **3. State & PTA Runtime** | `BranchController` | Drives state transitions via a unified M-inputs-to-N-outputs PTA engine, synchronizes `DagJoinBarrier` nodes, maintains dual-key (`16-char` / `64-char`) CAS caches for read-only proposer parity, and rehydrates cursors in $O(1)$ time. |
| **4. Execution Bridge** | `FiberExecutionBridge` & `PolyglotExecutionBridge` | Connects FSM nodes to Fiber's LLM Pipeline (`acompletion`, `StreamBreaker`, `VCR`) and Stateless MCP Gateway (`mcp://`), while optionally extending `NodeType.OPERSIG` to in-process callables (`fn://`, `py://`), direct REST APIs (`http(s)://`), and local binaries (`exec://`) with unified `202` HITL parking. |
| **5. Orchestration Layer** | `GraphTraverser` | Orchestrates the end-to-end multiverse lifecycle (`run`, `traverse_bundle`, `resume`), routes faulted/exhausted branches via `Catch` (`fb`) edges, manages HITL-parked siblings, and guarantees 100% fuel reclamation. |

---

## 2. Core Data Contracts & 2KB Claim-Check Pattern

To guarantee sub-millisecond state transitions and prevent ledger bloat, every on-ledger state anchor (`PhaseAnchorOutput`) and graph definition is strictly bounded to **`2,048 Bytes` (`MAX_BLOB_BYTES`)**.

### 2.1. Compile-Time Stripping, Static Linting & Auto-Linking (`CompiledBundle`)

When compiling an ASL state machine, the Compiler Layer and WASM Kernel execute a three-stage pipeline:

1. **Off-Store `TaskIOSpec` Stripping (`blob:<sha256_16>`):** Strips `resource_uri`, `result_path`, `call_params` (prompts, SQL queries, tool schemas), `vcr` config, and `projection` (`offload_path`, `extract_keys`) rules into `AnchorStore`.
2. **Pre-Mount Static Deadlock Sandbox (`E_BARRIER_DEADLOCK_RISK`):** The WASM kernel analyzes forward in-degrees across the topology. If a `1:N` parallel fork converges at a `DagJoinBarrier` (in-degree >= 2) and any parallel `Task` node omits a `Catch` (`fb`) fallback edge, compilation is rejected prior to ledger inscription.
3. **Bottom-Up SESE Auto-Linker (`SYMLINK` Call-Stack):** If the projected topology exceeds `2,048 Bytes`, the kernel iteratively folds trailing Single-Entry Single-Exit (SESE) subgraphs into independent `<= 2KB` `branch:<sub_hash>` bundles, replaces terminal `END` tokens with `RETURN`, and rewrites parent edges into `"branch:<sub_hash>#<entry_node>"` `SYMLINK` pointers.

```json
{
  "_m": {"ep": "Plan_Intent_LLM", "tr": "turn_01", "v": "1.0"},
  "g": {
    "Plan_Intent_LLM": {
      "t": "projector",
      "a": {"io_ref": "blob:8f3a19c4e2b109d2", "allow_parallel": true},
      "nx": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"]
    },
    "Fast_Readonly_MCP": {
      "t": "opersig",
      "a": {"io_ref": "blob:3c91a0b2f4e8120a"},
      "fb": "Evaluate_And_Join",
      "nx": "Evaluate_And_Join"
    },
    "Evaluate_And_Join": {
      "t": "router",
      "rl": [
        {"if": "$.strategy_override==fallback_reflect", "nx": "Reflect_Fallback_LLM"},
        {"if": "$._score>=0.99", "nx": "Reflect_Fallback_LLM"}
      ],
      "dn": "Final_Act"
    }
  }
}

```

### 2.2. Runtime State Ticket (`BranchCursor` $\leftrightarrow$ `PhaseAnchorOutput`)

Each active branch is represented in memory by a `BranchCursor` and physically backed on the ledger by an unspent PTA output (`tx_hash:output_index`):

* **`pointer` (`PtaPointer`):** Cryptographic UTXO handle signed by the node identity, passed to external actuators as `checkpoint_handle` to guarantee execution idempotency.
* **`fuel` (`int`):** Strict integer budget allocated to the branch.
* **`meta_ctx` (`Dict[str, Any]`):** Lightweight routing variables lifted via `Projection.extract_keys`. Heavy runtime outputs (`offload_data`) are automatically inscribed into CAS and referenced via `_last_ptr: "blob:<sha256_16>"`.
* **`call_stack` (`List[str]`):** Stack of `"parent_blob#resume_node"` frames enabling cycle-free `SYMLINK` subgraph jumps and `RETURN` transitions across auto-linked `branch:<hash>` blobs.
* **`tick` (`int`):** Monotonic execution counter persisted on-ledger (`executable_payload["tick"]`) and restored across $O(1)$ checkpoint resumptions to guarantee deterministic VCR scenario keys.

---

## 3. Unified M:N PTA Transition Semantics

All state movements in `BranchController` execute through a single cryptographic commit pipeline, enforcing strict fuel conservation across the multiverse:

| Operation | PTA Cardinality | PTA Phase Status | WASM Guard & AnchorStore Mechanics |
| --- | --- | --- | --- |
| **`mount_bundle_and_spawn`** | `0 Inputs -> 1 Output` | `GENESIS` | Inscribes `blob:` and auto-linked `branch:` objects into `AnchorStore` (synchronizing short/full hash caches for read-only proposer parity), initializes `DagJoinBarrier` via DFS cycle/in-degree analysis, and mints the root `BranchCursor` with `initial_fuel`. |
| **`step`** | `1 Input -> 1 Output` | `ACTIVE` / `SEALED` | Verifies transition via dual WASM guards (logging `wasm.reject` on violation), spends current PTA, deducts `step_cost`, offloads heavy output to CAS (`_last_ptr`), and mints next PTA. |
| **`fork`** | `1 Input -> N Outputs` | `FORKED` | Deducts parent `step_cost`, inscribes parent `offload_data` into CAS, spends parent PTA, and atomically slices remaining fuel (`parent.fuel - step_cost`) across $N$ child branches (`stream_id/b0..bN-1`) weighted by `fuel_ratios` or equal split. |
| **`collapse_and_merge`** | `N Inputs -> 1 Output` | `MERGED` | Spends all $N$ arriving branch PTAs at a `DagJoinBarrier` (or exit sweep), selects the winning state via the configured Arbiter, and reclaims $\sum \text{fuel}_i$ into a single stream. |
| **Fallback Transition (`Catch`)** | `1 Input -> 1 Output` | `ACTIVE` / `SEALED` | Triggered when a node raises an execution fault or `Exhausted fuel` error. Drains remaining fuel to `0` on exhaustion, records `_fallback_error`, and routes the cursor to its `Catch` (`fb`) target so sibling branches never deadlock at a barrier. |

---

## 4. Engine Verification Suites

| Verification Target | CLI Entrypoint | Validated Subsystems |
| --- | --- | --- |
| **FSM Multiverse Lifecycle Suite** | `python -m dev.tester.fsm` | Shorthand ASL compilation, pre-mount deadlock linter, `1:N` fork, `HTTP 202` zero-spend parking, $O(1)$ PTA rehydration, time-travel re-branching, `>2KB` Auto-Linker (45 nodes $\rightarrow$ 5 sub-branches), and runaway `Catch` recovery. |
| **Unit Contract & Polyglot Bridge Suite** | `python -m dev.e2e.fsm.contract` | `contract` URI matrix (10 schemes), `.$` nested JSONPath resolution, `Projection` CAS slicing, atomic M:N `BranchController` transitions (asymmetric `[0.5, 0.3, 0.2]` fork & proposer CAS cache), and live `PolyglotExecutionBridge` dispatch (`fn://`, `py://`, `https://`, `exec://`, `mcp://`). |
| **Isolated WASM Kernel & Broker Suite** | `python -m dev.e2e.infra.wasm.fsm` | Distributed WASM Broker $\leftrightarrow$ Kernel execution across `compile`, `multiverse`, and `incident` (03:00 AM outage) test suites with canonical proof sealing. |