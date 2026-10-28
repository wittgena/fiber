# fiber.phase.abc.experimental.fsm.runner

**Fiber FSM/DAG Runner** is a deterministic, WASM-verified, and ledger-anchored multiverse execution engine for autonomous AI agents. Built on top of **Phase Transition Anchors (PTA)**, **Content-Addressable Storage (CAS)**, and the **Canonical FSM WASM Kernel (`fsm.wasm`)**, it compiles standard and shorthand AWS Step Functions (ASL) definitions into ultra-compact (`<= 2KB`) topological blobs, statically prevents barrier deadlocks prior to execution, governs parallel hypothesis branching with strict FinOps fuel conservation, and supports zero-cost mid-flow resumption and time-travel re-branching.

---

## 1. Architectural Overview

Traditional agent graph frameworks serialize massive, mutable state snapshots into databases—leading to out-of-memory (OOM) crashes, unreplayable side effects, barrier deadlocks, and runaway API costs during parallel exploration.

Fiber solves this by physically decoupling **Graph Topology & Static Verification (`fsm.wasm` + Space)**, **Heavy I/O Payloads (`CAS`)**, and **Execution Lineage & Budget (`PTA Ledger`)** across five tightly integrated layers:

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│ 5. ExGraphTraverser (fiber.phase.fsm.traverser)                               │
│    • Unified Multiverse Scheduler (`run`, `traverse_bundle`, `resume`)        │
│    • Automatic 1:N Fan-Out Detection, Barrier Sync, and N:1 Arbiter Collapse │
│    • Fault-Tolerant `Catch` (`fb`) Fallback Routing & Fuel Reclamation       │
└──────────────────┬────────────────────────────────────────┬──────────────────┘
                   │ State & Ledger Transitions             │ I/O Actuation
                   ▼                                        ▼
┌─────────────────────────────────────────┐  ┌─────────────────────────────────┐
│ 3. ExBranchController (fsm.runtime)     │  │ 4. FiberExecutionBridge (bridge)│
│    • Unified M:N PTA Commit Engine       │  │    • Drop-in `acompletion` LLM  │
│      (0:1 Spawn, 1:1 Step, 1:N Fork,    │  │      + Real-time `StreamBreaker`│
│       N:1 Collapse & Merge)             │  │    • Application-Layer `VCR`    │
│    • O(1) Cursor Rehydration (`tx:idx`) │  │    • Stateless `MCP Gateway`    │
│    • Dual-WASM Guard & `DagJoinBarrier` │  │      + Pre-Flight HITL (`202`)  │
└──────────────────┬──────────────────────┘  └──────────────┬──────────────────┘
                   │ 2KB Blob & Step Guard                  │ `io_ref` Lookup
                   ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ 2. ExFSMCompiler & ExStateProjector (fiber.phase.fsm.compiler)                │
│    • Shorthand & Standard ASL Parser + `__fiber_extensions` (`Projection`)    │
│    • Automatic `Choice` Variable Inference & Off-Ledger `TaskIOSpec`(`blob:`)│
│    • Compact 2KB JCS Projection (`CompactKey`) & Unified `NodeType`           │
└─────────────────────────────────────┬────────────────────────────────────────┘
                                      │ FFI (`FSMInterpreter` & `FsmAdapter`)
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ 1. Canonical FSM WASM Kernel (`theoria.fsm` / `fsm.wasm` + `phase.wasm`)     │
│    • Auto-Linker: Bottom-Up SESE Rolling Fold for `>2KB` Graphs (`SYMLINK`)  │
│    • Static Sandbox: Pre-Mount `DagJoinBarrier` Deadlock Linter (`fb` check) │
│    • Runtime Step Guard: `StreamBreaker`, `ROUTER` Eval, and Cross-VM Bridge │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Module Responsibilities

| Module | Primary Class / Binary | Core Responsibility |
| --- | --- | --- |
| **`theoria.fsm` (`fsm.wasm`)** | `execute_router` (`FSMInterpreter`) | Executes AOT-cached WASM compilation and runtime guards. Automatically partitions `>2KB` graphs into `branch:<hash>#entry` subgraphs, rejects parallel branches lacking `Catch` (`fb`) fallbacks, and validates runtime transitions. |
| **`xphi.arch.bound.adapter.fsm`** | `FsmAdapter` | Constructs standardized `FSM_COMPILE`, `FSM_STEP`, `FORK`, `MERGE`, and `HITL_PARK` payloads for `FSMInterpreter` and `DphiBroker`. |
| **`fiber.phase.fsm.compiler`** | `ExFSMCompiler` | Parses ASL JSON (supporting shorthand URIs and `if` expressions), auto-infers `Choice` variables into `Projection.extract_keys`, strips `Parameters` into off-ledger `TaskIOSpec` CAS blobs (`blob:<hash>`), and delegates 2KB linking/linting to `fsm.wasm`. |
| **`fiber.phase.fsm.runtime`** | `ExBranchController` | Drives state transitions via a unified M-inputs-to-N-outputs PTA engine, verifies step transitions against both `phase.wasm` and `fsm.wasm`, synchronizes `DagJoinBarrier` nodes (including fallback edges), and rehydrates cursors in $O(1)$ time. |
| **`fiber.phase.fsm.bridge`** | `FiberExecutionBridge` | Connects FSM nodes to Fiber's Drop-in LLM Pipeline (`acompletion`, `StreamBreaker`, `BillingObserver`, `VCR`) and Stateless MCP Gateway (`/v1/mcp-gateway/{target}/invoke` with `HTTP 202` Pre-Flight HITL and `checkpoint_handle`). |
| **`fiber.phase.fsm.traverser`** | `ExGraphTraverser` | Orchestrates the end-to-end multiverse lifecycle (`run`, `traverse_bundle`, `resume`), routes faulted/exhausted branches via `Catch` (`fb`) edges, manages HITL-parked siblings, and guarantees 100% fuel reclamation. |

---

## 2. Core Data Contracts & 2KB Claim-Check Pattern

To guarantee sub-millisecond state transitions and prevent ledger bloat, every on-ledger state anchor (`PhaseAnchorOutput`) and graph definition is strictly bounded to **`2,048 Bytes` (`MAX_BLOB_BYTES`)**.

### 2.1. Compile-Time Stripping, Static Linting & Auto-Linking (`ExCompiledBundle`)

When compiling an ASL state machine, `ExFSMCompiler` and `fsm.wasm` execute a three-stage pipeline:

1. **Off-Ledger `TaskIOSpec` Stripping (`blob:<sha256_16>`):** Strips `resource_uri`, `result_path`, `call_params` (prompts, SQL queries, tool schemas), `vcr` config, and `projection` (`offload_path`, `extract_keys`) rules into `PhaseStore`.
2. **Pre-Mount Static Deadlock Sandbox (`E_BARRIER_DEADLOCK_RISK`):** `fsm.wasm` analyzes forward in-degrees across the topology. If a `1:N` parallel fork converges at a `DagJoinBarrier` (in-degree >= 2) and any parallel `Task` node omits a `Catch` (`fb`) fallback edge, compilation is rejected prior to ledger inscription.
3. **Bottom-Up SESE Auto-Linker (`SYMLINK` Call-Stack):** If the projected topology exceeds `2,048 Bytes`, `fsm.wasm` iteratively folds trailing subgraphs into independent `<= 2KB` `branch:<sub_hash>` bundles, replaces terminal `END` tokens with `RETURN`, and rewrites parent edges into `"branch:<sub_hash>#<entry_node>"` `SYMLINK` pointers.

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

* **`pointer` (`PtaPointer`):** Cryptographic UTXO handle signed via `NodeSigner` (`to_signed_input`), passed to external actuators as `checkpoint_handle` to guarantee idempotency.
* **`fuel` (`int`):** Strict integer budget allocated to the branch.
* **`meta_ctx` (`Dict[str, Any]`):** Lightweight routing variables lifted via `Projection.extract_keys`. Heavy runtime outputs (`offload_data`) are automatically inscribed into CAS and referenced via `_last_ptr: "blob:<sha256_16>"`.
* **`call_stack` (`List[str]`):** Stack of `"parent_blob#resume_node"` frames enabling cycle-free `SYMLINK` subgraph jumps and `RETURN` transitions across auto-linked `branch:<hash>` blobs.
* **`tick` (`int`):** Monotonic execution counter persisted on-ledger (`executable_payload["tick"]`) and restored across $O(1)$ checkpoint resumptions to guarantee deterministic VCR scenario keys.

---

## 3. Fiber ASL Authoring Specification

Fiber ASL is 100% backward-compatible with standard AWS Step Functions JSON while introducing concise syntactic sugar designed for both human engineers and LLM workflow generators.

### 3.1. Structural Keywords (`[FIXED]` System-Bound vs. `[FREE]` User-Defined)

| Field / Location | Binding Type | Specification & Engine Contract |
| --- | --- | --- |
| **`"Version"`, `"StartAt"`, `"States"**` | `[FIXED]` | Required root-level keys. `"StartAt"` must match a valid node key inside `"States"`. |
| **State (Node) IDs** | `[FREE]` | Any custom identifier (e.g., `"Plan_Intent_LLM"`, `"Step_01"`). Must **not** use reserved terminal tokens (`"END"`, `"RETURN"`) or reserved prefixes (`"blob:"`, `"branch:"`). |
| **`"Type"`** | `[FIXED]` | Allowed values:<br>

<br>• `"Task"`: Executes an LLM or MCP actuator via `"Resource"`.<br>

<br>• `"Choice"`: Compiles to `NodeType.ROUTER` (`"t": "router"`).<br>

<br>• `"Succeed"` / `"Fail"`: Compiles to terminal `NodeType.ACT` (`"nx": "END"`). |
| **`"Next"`** | `[MIXED]` | • `str`: Standard `1:1` transition (or `"branch:<hash>#<entry>"` for `SYMLINK` call).<br>

<br>• `List[str]` (`len >= 2`): Triggers an atomic `1:N` multiverse fork (`runtime.fork`). |
| **`"Catch"`** | `[MIXED]` | Mandatory on parallel branch `"Task"` nodes converging at a `DagJoinBarrier`.<br>

<br>• **Shorthand:** `"Catch": "Evaluate_And_Join"`<br>

<br>• **Standard ASL:** `"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Evaluate_And_Join"}]` |

### 3.2. `"Resource"` URI Routing Rules

`ExFSMCompiler._parse_resource_uri()` and `FiberExecutionBridge` route `"Task"` nodes based on the URI scheme or embedded category keyword:

| Shorthand URI (Recommended) | Legacy Full URI Format | Compiled `NodeType` | Bridge & Kernel Execution Behavior |
| --- | --- | --- | --- |
| **`"llm://<model>"`**<br>

<br>*(e.g., `"llm://gpt-4o"`)* | `"fiber://.../interllm_proxy"` | `PROJECTOR`<br>

<br>(`"projector"`) | Automatically binds `Parameters["model"] = "<model>"` (if omitted) and invokes `fiber.llm.entry.acompletion()`. |
| **`"reflect://<model>"`**<br>

<br>*(e.g., `"reflect://gpt-4o-mini"`)* | `"fiber://.../reflect_llm"` | `REFLECT`<br>

<br>(`"reflect"`) | Automatically binds `Parameters["model"] = "<model>"` and invokes `acompletion()` for self-reflection/verification. |
| **`"tension://<model>"`** | `"fiber://.../tension_eval"` | `TENSION`<br>

<br>(`"tension"`) | Invokes `acompletion()` and triggers the Cross-VM Escape Hatch (`fsm.wasm` $\rightarrow$ `phase.wasm` `evaluate_tension`). |
| **`"mcp://<target_id>"`**<br>

<br>*(e.g., `"mcp://prod_db_admin"`)* | `"fiber://mcp_sidecar/<target_id>"` | `OPERSIG`<br>

<br>(`"opersig"`) | Extracts `<target_id>` after the last `'/'` and sends `POST {mcp_gateway_url}/<target_id>/invoke`. |
| **`"pending://<target_id>"`** | `"fiber://.../pending_gate"` | `PENDING`<br>

<br>(`"pending"`) | Routes through the MCP Gateway for asynchronous external wait gates. |
| **`"act://<name>"`** | `"fiber://.../act"` | `ACT`<br>

<br>(`"act"`) | Pure control/no-op node (`0` external I/O, `0` Fuel cost). |

### 3.3. `"Parameters"`, Dynamic Binding (`.$`), and `"__fiber_extensions"`

All `"Parameters"` are stripped from the on-ledger graph at compile time and inscribed into CAS (`blob:<hash>`).

* **Dynamic Context Resolution (`".$"` Suffix):** Any parameter key ending with `".$"` (e.g., `"intent_filter.$": "$.intent_type"`) is dynamically resolved at runtime from `cursor.meta_ctx["intent_type"]` prior to invoking the LLM or MCP tool.
* **`"__fiber_extensions"` Block:** Stripped prior to actuator invocation. Supports:
* **`"Projection"` (`ProjectionConfig`):**
* `"offload_path"` (`Optional[str]`): JSON path within the heavy actuator response to offload into CAS (`blob:<hash>`). Defaults to offloading the entire response dictionary.
* `"extract_keys"` (`List[str]`): JSON paths to lift from the actuator response into the lightweight on-ledger `cursor.meta_ctx`. **Compiler Auto-Inference:** Any variable referenced in downstream `"Choice"` nodes plus `"$._score"` are automatically scanned and injected into every Task's `extract_keys` even if `"Projection"` is omitted.


* **`"VCR"` (`VCRConfig`):** `{"capture_mode": "socket_stream" | "rpc_payload", "coalescing_window_ms": int}` for deterministic replay.
* **Execution Attributes:** `"pressure"` (`0.0 ~ 1.0`), `"max_failures"` (`int`), `"allow_parallel"` (`bool`), and `"step_cost"` (`int`).



### 3.4. `"Choice"` (`ROUTER`) Expressions & Reserved `meta_ctx` Keys

`"Choice"` nodes evaluate rules top-to-bottom (`rl`) and fall back to `"Default"` (`dn`) if no rule matches. Both `runtime.py` and `fsm.wasm` evaluate numeric (`float`), string, and boolean comparisons across 6 operators (`>=`, `<=`, `!=`, `==`, `>`, `<`).

| Operator Type | Shorthand `"if"` Syntax (Recommended) | Standard ASL `"Choices"` Syntax |
| --- | --- | --- |
| **Numeric Comparison** | `{"if": "$._score >= 0.95", "Next": "Node_A"}` | `{"Variable": "$._score", "NumericGreaterThanEquals": 0.95, "Next": "Node_A"}` |
| **String Equality** | `{"if": "$.mode == 'fallback'", "Next": "Node_B"}` | `{"Variable": "$.mode", "StringEquals": "fallback", "Next": "Node_B"}` |
| **Inequality** | `{"if": "$.status != error", "Next": "Node_C"}` | `{"Variable": "$.status", "StringNotEquals": "error", "Next": "Node_C"}` |
| **Boolean Check** | `{"if": "$.is_verified == true", "Next": "Node_D"}` | `{"Variable": "$.is_verified", "BooleanEquals": true, "Next": "Node_D"}` |

#### Reserved Runtime `cursor.meta_ctx` System Keys

| Key | Type | Engine Binding & Behavior |
| --- | --- | --- |
| **`_score`** | `float` | Evaluated by `ExGraphTraverser._default_arbiter` (and `fsm.wasm` `MERGE`) to select the winning branch at a `DagJoinBarrier` (tie-breaker: remaining `fuel`). |
| **`_hitl_yield`** | `bool` | Set to `True` upon receiving `HTTP 202` from the MCP Gateway. Parks the cursor in place with **zero PTA/fuel spend**. Automatically cleared by `traverser.resume()`. |
| **`_hitl_ticket`** | `str` | Ticket ID attached during `HTTP 202` Pre-Flight HITL locking. |
| **`_fork_ratios`** | `List[float]` | Optional weight list (e.g., `[0.4, 0.4, 0.2]`) popped during `1:N` fan-out to slice remaining parent fuel asymmetrically across child branches. |
| **`_fork_branches`** | `List[Tuple[str, dict]]` | Optional dynamic fan-out targets injected at runtime by an LLM planner node. |
| **`_last_ptr`** | `str` | Automatically populated with the CAS pointer (`"blob:<sha256_16>"`) of the most recent offloaded payload. |
| **`_fallback_error`** | `str` | Automatically populated with the fault/exhaustion reason when a branch is routed via its `"Catch"` (`fb`) fallback edge. |

### 3.5. Reference ASL Definition

```python
SAMPLE_MULTIVERSE_ASL = {
    "Version": "1.0",
    "StartAt": "Plan_Intent_LLM",
    "States": {
        "Plan_Intent_LLM": {
            "Type": "Task",
            "Resource": "llm://gpt-4o",
            "Parameters": {
                "prompt": "Analyze user request and dispatch parallel verification strategies.",
                "__fiber_extensions": {
                    "VCR": {"capture_mode": "socket_stream", "coalescing_window_ms": 100},
                    "Projection": {
                        "offload_path": "$.raw_stream_payload",
                        "extract_keys": ["$.intent_type"],
                    },
                    "allow_parallel": True,
                },
            },
            "Next": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"],
        },
        "Fast_Readonly_MCP": {
            "Type": "Task",
            "Resource": "mcp://readonly_db_search",
            "Parameters": {
                "query": "SELECT count(*) FROM audit_logs",
                "intent_filter.$": "$.intent_type",
            },
            "Next": "Evaluate_And_Join",
            "Catch": "Evaluate_And_Join",
        },
        "Admin_Mutation_MCP": {
            "Type": "Task",
            "Resource": "mcp://prod_db_admin",
            "Parameters": {
                "sql": "DELETE FROM stale_sessions WHERE expired = true",
            },
            "Next": "Evaluate_And_Join",
            "Catch": "Evaluate_And_Join",
        },
        "Evaluate_And_Join": {
            "Type": "Choice",
            "Choices": [
                {"if": "$.strategy_override == fallback_reflect", "Next": "Reflect_Fallback_LLM"},
                {"if": "$._score >= 0.99", "Next": "Reflect_Fallback_LLM"},
            ],
            "Default": "Final_Act",
        },
        "Reflect_Fallback_LLM": {
            "Type": "Task",
            "Resource": "reflect://gpt-4o-mini",
            "Parameters": {"prompt": "Verify fallback safety."},
            "Next": "Final_Act",
        },
        "Final_Act": {
            "Type": "Succeed",
        },
    },
}

```

---

## 4. Unified M:N PTA Transition Semantics

All state movements in `ExBranchController` execute through a single cryptographic commit pipeline (`_commit_pta_transition`), enforcing strict fuel conservation across the multiverse:

| Operation | PTA Cardinality | PTA Phase Status | WASM Guard & Ledger Mechanics |
| --- | --- | --- | --- |
| **`mount_bundle_and_spawn`** | `0 Inputs -> 1 Output` | `GENESIS` | Inscribes `blob:` and auto-linked `branch:` objects into `PhaseStore`, initializes `DagJoinBarrier` via DFS cycle/in-degree analysis (including `fb` edges), and mints the root `BranchCursor` with `initial_fuel`. |
| **`step`** | `1 Input -> 1 Output` | `ACTIVE` / `SEALED` | Verifies transition via dual WASM guards (`phase.wasm` & `fsm.wasm`; logs `wasm.reject` on failure), spends current PTA, deducts `step_cost`, offloads heavy output to CAS (`_last_ptr`), and mints next PTA. |
| **`fork`** | `1 Input -> N Outputs` | `FORKED` | Deducts parent `step_cost`, inscribes parent `offload_data` into CAS, spends parent PTA, and atomically slices remaining fuel (`parent.fuel - step_cost`) across $N$ child branches (`stream_id/b0..bN-1`) via `_slice_fuel` (weighted by `fuel_ratios` or equal split). |
| **`collapse_and_merge`** | `N Inputs -> 1 Output` | `MERGED` | Spends all $N$ arriving branch PTAs at a `DagJoinBarrier` (or exit sweep), selects winner via `ArbiterFn`, and reclaims $\sum \text{fuel}_i$ into a single stream. |
| **`_transition_via_fallback`** | `1 Input -> 1 Output` | `ACTIVE` / `SEALED` | Triggered by `ExGraphTraverser` when a node raises an execution fault or `Exhausted fuel` error. Drains remaining fuel to `0` on exhaustion, records `_fallback_error`, and routes the cursor to its `Catch` (`fb`) target so sibling branches never deadlock at a barrier. |

---

## 5. Key Execution Scenarios & Lifecycle Flows

### Scenario 1: ASL Compilation & 1:N Multiverse Fan-Out

When a node defines multiple targets (`"Next": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"]`) or dynamically injects `_fork_branches` (and optional `_fork_ratios`) into `meta_ctx`, `ExGraphTraverser` deducts the parent node's `step_cost` and triggers a `1:N` atomic fork.

```text
[Plan_Intent_LLM (1000 Fuel)]
       │ (Spends 20 Fuel -> 980 Remaining; Offloads LLM output to CAS)
       ▼
  runtime.fork() ──► Atomic Fuel Slicing [490, 490]
       ├──► Branch b0 (run_101/b0): Fast_Readonly_MCP  (490 Fuel)
       └──► Branch b1 (run_101/b1): Admin_Mutation_MCP (490 Fuel)

```

### Scenario 2: Pre-Flight HITL Parking (`HTTP 202`) & Barrier Synchronization

When Branch `b1` invokes a sensitive MCP tool (e.g., `DELETE` query on `mcp://prod_db_admin`), the Fiber MCP Gateway intercepts the request prior to worker execution and returns **`HTTP 202 (Pre-Flight HITL Lock)`**.

1. **Zero-Spend Parking:** `FiberExecutionBridge` signals `_hitl_yield: True`. `ExGraphTraverser` parks `b1` in place **without spending its PTA or deducting fuel** (`490 Fuel` and `Tick=2` preserved).
2. **Sibling Barrier Preservation:** Meanwhile, Branch `b0` finishes (`-5 Fuel` $\rightarrow$ `485 Fuel`) and arrives at the diamond join node (`Evaluate_And_Join`). Because `b1` is parked, `ExGraphTraverser` drains `b0` from the `DagJoinBarrier` (`drain_waiting()`) and returns both `[parked_b1, waiting_b0]` in `TraversalResult.parked_cursors`.

### Scenario 3: $O(1)$ Cursor Rehydration & Mid-Flow Resumption

Once an administrator approves the HITL request, execution resumes directly from the mid-graph PTA pointers (`tx_hash:idx`) or stream heads—**skipping `Plan_Intent_LLM` and `Fast_Readonly_MCP` entirely**:

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

res3 = await traverser.resume(targets=exp_cursors, base_stream_id="run_101_exp")

```

### Scenario 5: Runaway Fault Isolation via `Catch` (`fb`) Fallback

If a parallel branch attempts to spend more fuel than its allocated slice (or encounters a tool timeout/exception), `ExBranchController.step()` raises a fuel exhaustion error. `ExGraphTraverser` catches the fault, drains the runaway branch's fuel to `0`, and transitions it along its `Catch` (`fb`) edge to the `DagJoinBarrier`. Surviving sibling branches merge cleanly and reclaim all remaining fuel without barrier starvation.

---

## 6. Quickstart & Verification CLI

### 6.1. Programmatic SDK Usage (Production Mode with Live Bridge)

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

asyncio.run(main(SAMPLE_MULTIVERSE_ASL))
```

### 6.2. Verification Commands

| Verification Target | Command | Validated Subsystems |
| --- | --- | --- |
| **FSM Lifecycle & PTA Ledger Suite** | `python -m dev.e2e.fsm.tester` | Shorthand ASL compilation, pre-mount deadlock linter, `1:N` fork, `HTTP 202` zero-spend parking, $O(1)$ PTA rehydration, time-travel re-branching, `>2KB` Auto-Linker (45 nodes $\rightarrow$ 5 sub-branches), and runaway `Catch` recovery. |
| **Isolated WASM Kernel & Broker Suite** | `python -m dev.e2e.inter.wasm.fsm` | Distributed `DphiBroker` $\leftrightarrow$ `TaskWasm` $\leftrightarrow$ `FSMInterpreter` (`fsm.wasm`) execution across `compile`, `multiverse`, and `incident` (03:00 AM outage) test suites with canonical proof sealing via `phase.wasm`. |