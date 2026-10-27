# fiber.phase.abc.experimental.fsm.runner

**Fiber FSM/DAG Runner** is a deterministic, ledger-anchored state machine and multiverse execution engine for autonomous AI agents. Built on top of **Phase Transition Anchors (PTA)** and **Content-Addressable Storage (CAS)**, it compiles standard AWS Step Functions (ASL) definitions into ultra-compact (`<= 2KB`) topological blobs, governs parallel hypothesis branching with strict FinOps fuel conservation, and supports zero-cost mid-flow resumption and time-travel re-branching.

---

## 1. Architectural Overview

Traditional agent graph frameworks serialize massive, mutable state snapshots into databases—leading to out-of-memory (OOM) crashes, unreplayable side effects, and runaway API costs during parallel exploration.

Fiber solves this by physically decoupling **Graph Topology (Space)**, **Heavy I/O Payloads (CAS)**, and **Execution Lineage & Budget (PTA Ledger)** across four lean modules:

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ 4. ExGraphTraverser (fiber.phase.fsm.traverser)                              │
│    • Unified Multiverse Scheduler (`run`, `traverse_bundle`, `resume`)       │
│    • Automatic 1:N Fan-Out Detection, Barrier Sync, and N:1 Arbiter Collapse │
│    • Context-managed Dangling PTA Reclamation (`async with`)                 │
└──────────────────┬────────────────────────────────────────┬──────────────────┘
                   │ State & Ledger Transitions             │ I/O Actuation
                   ▼                                        ▼
┌─────────────────────────────────────────┐  ┌─────────────────────────────────┐
│ 2. ExBranchController (fsm.runtime)     │  │ 3. FiberExecutionBridge (bridge)│
│    • Unified M:N PTA Commit Engine      │  │    • Drop-in `acompletion` LLM  │
│      (0:1 Spawn, 1:1 Step, 1:N Fork,    │  │      + Real-time `FuelBreaker`  │
│       N:1 Collapse & Merge)             │  │    • Application-Layer `VCR`    │
│    • O(1) Cursor Rehydration (`tx:idx`) │  │    • Stateless `MCP Gateway`    │
│    • WASM Guard, `SYMLINK` & Barrier    │  │      + Pre-Flight HITL (`202`)  │
└──────────────────┬──────────────────────┘  └──────────────┬──────────────────┘
                   │ 2KB Blob & CAS Resolution              │ `io_ref` Lookup
                   ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ 1. ExFSMCompiler & ExStateProjector (fiber.phase.fsm.compiler)               │
│    • ASL Parser + `__fiber_extensions` (`VCR`, `PTA_Mint`) Strip & Split     │
│    • Off-Ledger `TaskIOSpec` CAS Inscription (`blob:<hash>`)                 │
│    • Compact 2KB JCS Projection (`CompactKey`) & Unified `NodeType`          │
└──────────────────────────────────────────────────────────────────────────────┘

```

### Module Responsibilities

| Module | Primary Class | Core Responsibility |
| --- | --- | --- |
| **`fiber.phase.fsm.compiler`** | `ExFSMCompiler` | Parses ASL JSON, strips heavy `Parameters` and `__fiber_extensions` into off-ledger `TaskIOSpec` CAS blobs (`blob:<hash>`), and projects the pure topology into a `<= 2KB` canonical blob (`branch:<hash>`). |
| **`fiber.phase.fsm.runtime`** | `ExBranchController` | Drives state transitions via a unified M-inputs-to-N-outputs PTA engine, verifies step transitions against the WASM broker (`CORE` / `SYMLINK` / `ANCHOR`), synchronizes DAG join barriers (`DagJoinBarrier`), and rehydrates cursors in $O(1)$ time. |
| **`fiber.phase.fsm.bridge`** | `FiberExecutionBridge` | Connects FSM nodes to Fiber's Drop-in LLM Pipeline (`acompletion`, `FuelBreaker`, `BillingObserver`, `VCR`) and Stateless MCP Gateway (`/v1/mcp-gateway/{target}/invoke` with `HTTP 202` Pre-Flight HITL). |
| **`fiber.phase.fsm.traverser`** | `ExGraphTraverser` | Orchestrates the end-to-end multiverse lifecycle (`run` and `resume`), manages HITL-parked and barrier-waiting sibling cursors, and guarantees 100% fuel reclamation upon completion or exit. |

---

## 2. Core Data Contracts & 2KB Claim-Check Pattern

To guarantee millisecond state transitions and prevent ledger bloat, every on-ledger state anchor (`PhaseAnchorOutput`) and graph definition is strictly bounded to **`2,048 Bytes` (`MAX_BLOB_BYTES`)**.

### 2.1. Compile-Time Stripping & Modular Subgraphs (`ExCompiledBundle`)

When compiling an ASL state machine, `ExFSMCompiler` separates static execution payloads from routing topology:

1. **Off-Ledger `TaskIOSpec` (`blob:<sha256_16>`):** Stores `resource_uri`, `result_path`, `call_params` (prompts, tool schemas), `vcr` config, and `pta_mint` projection rules in `PhaseStore`.
2. **On-Ledger Compact Graph (`branch:<sha256_16>`):** Retains only 1–2 character `CompactKey` fields (`_m`, `ep`, `tr`, `v`, `g`, `t`, `a`, `nx`, `fb`, `rl`, `if`, `dn`) and an `io_ref` pointer to the CAS blob.
3. **Infinite Graph Modularity (`SYMLINK` Call-Stack):** Graphs exceeding `2KB` are modularized into sub-branches (`branch:<hash>#entry`). During execution, `ExBranchController` pushes the caller's resume target (`parent_blob#fallback_or_next`) onto `call_stack` and seamlessly pops it upon hitting a `RETURN` token.

```json
{
  "_m": {"ep": "Plan_Intent_LLM", "tr": "turn_01", "v": "1.0"},
  "g": {
    "Plan_Intent_LLM": {
      "t": "projector",
      "a": {"io_ref": "blob:8f3a19c4e2b109d2", "allow_parallel": true},
      "nx": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"]
    },
    "Evaluate_And_Join": {
      "t": "router",
      "rl": [{"if": "$.strategy_override==fallback_reflect", "nx": "Reflect_Fallback_LLM"}],
      "dn": "Final_Act"
    }
  }
}

```

### 2.2. Runtime State Ticket (`BranchCursor` $\leftrightarrow$ `PhaseAnchorOutput`)

Each active branch is represented in memory by a `BranchCursor` and physically backed on the ledger by an unspent PTA output (`tx_hash:output_index`):

* **`pointer` (`PtaPointer`):** Cryptographic UTXO handle signed via `NodeSigner` (`to_signed_input`), preventing double-spending and duplicate tool execution.
* **`fuel` (`int`):** Strict integer budget allocated to the branch.
* **`meta_ctx` (`Dict`):** Ultra-lightweight routing variables. Heavy runtime outputs (`offload_data`) are automatically inscribed into CAS and referenced via `_last_ptr: "blob:<sha256_16>"`.
* **`call_stack` (`List[str]`):** Stack of `"parent_blob#resume_node"` frames enabling cycle-free `SYMLINK` subgraph calls and `RETURN` transitions.
* **`tick` (`int`):** Monotonic execution counter persisted on-ledger (`executable_payload["tick"]`) and restored across $O(1)$ checkpoint resumptions to guarantee deterministic VCR scenario keys.

---

## 3. Unified M:N PTA Transition Semantics

All state movements in `ExBranchController` execute through a single cryptographic commit pipeline (`_commit_pta_transition`), enforcing strict fuel conservation across the multiverse:

| Operation | PTA Cardinality | PTA Phase Status | WASM Guard & Ledger Mechanics |
| --- | --- | --- | --- |
| **`mount_bundle_and_spawn`** | `0 Inputs -> 1 Output` | `GENESIS` | Inscribes CAS blobs, initializes `DagJoinBarrier` via DFS cycle/in-degree analysis, and mints the root `BranchCursor` with `initial_fuel`. |
| **`step`** | `1 Input -> 1 Output` | `ACTIVE` / `SEALED` | Verifies transition via WASM broker (`CORE` / `SYMLINK` / `ANCHOR`; logs `wasm.reject` on failure), spends current PTA, deducts `step_cost`, offloads heavy output to CAS, and mints next PTA. |
| **`fork`** | `1 Input -> N Outputs` | `FORKED` | Deducts parent `step_cost`, inscribes parent `offload_data` into CAS (`_last_ptr`), spends parent PTA, and atomically slices remaining fuel (`parent.fuel - step_cost`) across $N$ child branches (`stream_id/b0..bN-1`) via `_slice_fuel`. |
| **`collapse_and_merge`** | `N Inputs -> 1 Output` | `MERGED` | Spends all $N$ arriving branch PTAs at a `DagJoinBarrier` (or exit sweep), selects winner via `ArbiterFn`, and reclaims $\sum \text{fuel}_i$ into a single stream. |

---

## 4. Key Execution Scenarios & Lifecycle Flows

### Scenario 1: ASL Compilation & 1:N Multiverse Fan-Out

When a node defines multiple targets (`"Next": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"]`) or dynamically injects `_fork_branches` into `meta_ctx`, `ExGraphTraverser` deducts the parent node's `step_cost` and triggers a `1:N` atomic fork.

```text
[Plan_Intent_LLM (1000 Fuel)]
       │ (Spends 20 Fuel -> 980 Remaining; Offloads LLM output to CAS)
       ▼
  runtime.fork() ──► Atomic Fuel Slicing [490, 490]
       ├──► Branch b0 (run_101/b0): Fast_Readonly_MCP  (490 Fuel)
       └──► Branch b1 (run_101/b1): Admin_Mutation_MCP (490 Fuel)

```

### Scenario 2: Pre-Flight HITL Parking (`HTTP 202`) & Barrier Synchronization

When Branch `b1` invokes a sensitive MCP tool (e.g., `DELETE` query), the Fiber MCP Gateway intercepts the request prior to worker execution and returns **`HTTP 202 (Pre-Flight HITL Lock)`**.

1. **Zero-Spend Parking:** `FiberExecutionBridge` signals `_hitl_yield: True`. `ExGraphTraverser` parks `b1` in place **without spending its PTA or deducting fuel** (`490 Fuel` and `Tick=2` preserved).
2. **Sibling Barrier Preservation:** Meanwhile, Branch `b0` finishes (`-5 Fuel` $\rightarrow$ `485 Fuel`) and arrives at the diamond join node (`Evaluate_And_Join`). Because `b1` is parked, `ExGraphTraverser` drains `b0` from the `DagJoinBarrier` (`drain_waiting()`) and returns both `[parked_b1, waiting_b0]` in `TraversalResult.parked_cursors`.

### Scenario 3: $O(1)$ Cursor Rehydration & Mid-Flow Resumption

Once an administrator approves the HITL request (via Slack 1-Click button or `fiber sign verify` 6-digit TOTP), execution resumes directly from the mid-graph PTA pointers (`tx_hash:idx`) or stream heads (with optional `resume_meta` injection)—**skipping `Plan_Intent_LLM` and `Fast_Readonly_MCP` entirely**:

```python
# Rehydrate & resume directly from unspent PTA pointer strings ('tx_hash:idx')
res2 = await traverser.resume(
    targets=[waiting_b0.pointer.to_key(), parked_b1.pointer.to_key()],
    base_stream_id="run_101",
)
# b0 immediately re-enters the barrier at 'Evaluate_And_Join';
# b1 executes Admin_Mutation_MCP (-15 Fuel -> 475), arrives at 'Evaluate_And_Join',
# triggers N:1 collapse_and_merge with b0 (485 + 475 = 960 Reclaimed Fuel), and reaches END.

```

### Scenario 4: Time-Travel Re-Branching from a Checkpoint

Even if a parent cursor has already reached `END_TOKEN` (`is_terminated`), `runtime.fork()` safely supports **Time-Travel Forking**. Developers can take any historical or completed `BranchCursor` (or PTA pointer) and fork new experimental hypotheses with overridden metadata or target nodes:

```python
# Fork 2 new hypotheses directly from an existing checkpoint cursor (960 Fuel -> [480, 480])
exp_cursors = await traverser.runtime.fork(
    parent=res2.final_cursor,
    branch_targets=[
        ("Reflect_Fallback_LLM", {"strategy_override": "fallback_reflect", "_score": 0.99}),
        ("Final_Act", {"strategy_override": "direct_exit", "_score": 0.50}),
    ],
    fuel_ratios=[0.5, 0.5],
)

# Traverse both hypotheses; b1 waits at Join Node 'Final_Act' until b0 finishes 'Reflect_Fallback_LLM' (-20 Fuel),
# then merges at 'Final_Act' (460 + 480 = 940 Reclaimed Fuel) under winning strategy 'fallback_reflect'.
res3 = await traverser.resume(targets=exp_cursors, base_stream_id="run_101_exp")

```

---

## 5. Quickstart & Verification

### 5.1. Programmatic SDK Usage (Production Mode with Live Bridge)

By default, `ExGraphTraverser` automatically binds `FiberExecutionBridge` to route LLM nodes through `fiber.llm.entry.acompletion` and tool nodes through the local MCP Gateway:

```python
import asyncio
from fiber.phase.fsm.traverser import ExGraphTraverser

async def main(asl_definition: dict):
    async with ExGraphTraverser(
        tenant_id="accounting_dept",
        mcp_gateway_url="http://127.0.0.1:8356/v1/mcp-gateway",
    ) as traverser:
        result = await traverser.run(
            asl_schema=asl_definition,
            base_stream_id="agent_session_01",
            initial_fuel=5000,
        )
        print(f"Status: {result.final_cursor.curr_node} | Fuel Left: {result.reclaimed_fuel}")
        print(f"Trace : {' -> '.join(result.visited_history)}")

asyncio.run(main(my_asl_json))
```

### 5.2. Running the Standalone E2E Verification Suite

To verify 2KB ASL compilation, 1:N fan-out, `HTTP 202` HITL parking, $O(1)$ cursor restoration, barrier collapse, and time-travel re-branching offline:

```bash
python -m dev.e2e.fsm.tester
```

**Verified E2E Execution Log:**

```text
======================================================================================
 [FIBER FSM/DAG MULTIVERSE TESTER] Starting E2E Lifecycle Verification
======================================================================================
 LOG | KERNEL | INFO  |  anchor.mempool: [Mempool:SEALER] Background Consumer [sealer_80786_d767d9] attached.
 LOG | KERNEL | INFO  |  anchor.store: [Ledger] Acquired physical lock. Operating as SEALER. WASM Kernel mounted.
 LOG | ANCHOR | INFO  |  anchor.oracle: [AnchorOracle] Mounted Anchor DB via PhaseStore. Canonical WASM Broker attached.
 LOG | KERNEL | INFO  |  adapter.pta: [Crypto] Node identity loaded. PubKey: b77bda9b...

--------------------------------------------------------------------------------------
 [Phase 1: Compile] Stripping heavy Task I/O into CAS & projecting <=2KB Topology Blob
--------------------------------------------------------------------------------------
  • Root Graph Hash : branch:552a3360678c2c34 (On-Ledger Compact IR)
  • Root Graph Size : 678B / 2048B Limit (33.1% used)
  • Offloaded Blobs : 5 CAS objects (4 TaskIOSpecs + 1 Root Blob)

--------------------------------------------------------------------------------------
 [Phase 2: Initial Run & HITL Park] Genesis(1000 Fuel) -> Plan(-20) -> 1:2 Fork[490, 490]
--------------------------------------------------------------------------------------
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: 2c5c0fcc
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 2c5c0fcc... | Hash: 47fc2298
  [Actuator] LLM Node 'Plan_Intent_LLM' on 'run_101' | Tick=1 | Spending 20 Fuel (1000 -> 980) | Offloading response to CAS
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: 4bd863af
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 4bd863af... | Hash: 2f05f4f7
  [Actuator] 🛠️  Safe MCP Node 'Fast_Readonly_MCP' on 'run_101/b0' | Tick=2 | Spending 5 Fuel (490 -> 485) | Score=0.70
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101/b0 | Commit: 786a5e39
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 786a5e39... | Hash: 620b2857
  [Actuator] MCP Pre-Flight HITL Trap (HTTP 202) at 'Admin_Mutation_MCP' on 'run_101/b1'! Regex matched 'DELETE'. Parking cursor WITHOUT spending PTA (2f05f4f7..:1) or Fuel (490).

  [Phase 2 Summary: Multiverse Paused Safely]
  • Detected Join Nodes : ['Evaluate_And_Join', 'Final_Act']
  • Visited Trace       : run_101:Plan_Intent_LLM -> run_101/b0:Fast_Readonly_MCP(FORK) -> run_101/b1:Admin_Mutation_MCP(FORK) -> run_101/b0:Evaluate_And_Join -> run_101/b1:Admin_Mutation_MCP(HITL_PARKED)
  • Parked Cursors      : ['run_101/b1', 'run_101/b0']
  • Branch b0 (Sibling) : Node='Evaluate_And_Join' (Waiting at Join Barrier) | PTA=620b2857..:0 | Fuel=485 (490 - 5) | Tick=3
  • Branch b1 (Locked)  : Node='Admin_Mutation_MCP' (Zero-Spend HITL Parked)  | PTA=2f05f4f7..:1 | Fuel=490 (490 - 0) | Tick=2

--------------------------------------------------------------------------------------
 [Phase 3: HITL Approval & Mid-Flow Resume] O(1) Restore from PTA Pointers (Skipping Prior Nodes)
--------------------------------------------------------------------------------------
  • O(1) Rehydrated b1  : stream='run_101/b1' at node='Admin_Mutation_MCP' | PTA=2f05f4f7..:1 | Preserved Tick=2 | Preserved Fuel=490
  • Admin Action        : Approved TOTP/Slack Ticket 'hitl_ticket_drop_99'. Resuming branches...

  [Actuator] MCP HITL Approved ('hitl_ticket_drop_99')! Executing 'Admin_Mutation_MCP' on 'run_101/b1' | Tick=2 | Spending 15 Fuel (490 -> 475) | Score=0.95
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101/b1 | Commit: dccdf65e
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: dccdf65e... | Hash: 5a4da66b
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: 24b82d3b
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 24b82d3b... | Hash: 76d4e469
  [Actuator] Control Node 'Evaluate_And_Join' (router) on 'run_101' | Tick=4 | Cost=0 Fuel (Remaining=960)
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: 550f8aa6
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 550f8aa6... | Hash: e2f2bd89
  [Actuator] Control Node 'Final_Act' (act) on 'run_101' | Tick=5 | Cost=0 Fuel (Remaining=960)
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: 401ef659
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 401ef659... | Hash: dd4a88db

  [Phase 3 Summary: Diamond Barrier Merged & Fuel Reclaimed]
  • Resumed Trace       : run_101/b0:Evaluate_And_Join(RESUMED) -> run_101/b1:Admin_Mutation_MCP(RESUMED) -> run_101/b1:Evaluate_And_Join -> run_101:Evaluate_And_Join(MERGED) -> run_101:Final_Act -> run_101:END
  • Arbiter Winner      : Selected b1 (_score=0.95 > b0 _score=0.70) at 'Evaluate_And_Join'
  • Final Node          : END | Join Collapses: 1 | Final PTA=dd4a88db..:0
  • Reclaimed Fuel      : 960 (485 from b0 + 475 from b1 = 960 Total Conserved)

--------------------------------------------------------------------------------------
 [Phase 4: Time-Travel Re-Branching] Forking 2 New Hypotheses from Completed Checkpoint (960 Fuel)
--------------------------------------------------------------------------------------
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101 | Commit: caf417df
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: caf417df... | Hash: 96b7ee82
  • Forked 2 Experimental Branches from Sealed PTA dd4a88db..:0 (960 Fuel -> [480, 480]):
    - run_101/b0: Target='Reflect_Fallback_LLM' | PTA=96b7ee82..:0 | Fuel=480 | Override='fallback_reflect' (_score=0.99)
    - run_101/b1: Target='Final_Act' | PTA=96b7ee82..:1 | Fuel=480 | Override='direct_exit' (_score=0.5)

  [Actuator] LLM Node 'Reflect_Fallback_LLM' on 'run_101/b0' | Tick=7 | Spending 20 Fuel (480 -> 460) | Offloading response to CAS
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101/b0 | Commit: 7d237b25
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 7d237b25... | Hash: c1e93840
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101_exp | Commit: 29f8a9d2
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 29f8a9d2... | Hash: 289a23b4
  [Actuator] Control Node 'Final_Act' (act) on 'run_101_exp' | Tick=9 | Cost=0 Fuel (Remaining=940)
 LOG | KERNEL | INFO  |  anchor.store: [Ledger:LEADER] Epoch Sealed. Stream: run_101_exp | Commit: 6f6fa2dc
 LOG | KERNEL | INFO  |  adapter.pta: [PtaAdapter:778AD0] Tx Sealed. Receipt: 6f6fa2dc... | Hash: 6bcf9b67

  [Phase 4 Summary: Time-Travel Hypotheses Converged]
  • Time-Travel Trace   : run_101/b0:Reflect_Fallback_LLM(RESUMED) -> run_101/b1:Final_Act(RESUMED) -> run_101/b0:Final_Act -> run_101_exp:Final_Act(MERGED) -> run_101_exp:END
  • Final Exp State     : Stream='run_101_exp' | Node='END' | Reclaimed Fuel=940 (460 from b0 + 480 from b1 = 940)
  • Winning Strategy    : 'fallback_reflect' (_score=0.99)

======================================================================================
 ✅ ALL MULTIVERSE FSM/DAG & PTA LEDGER ASSERTIONS PASSED
======================================================================================
```