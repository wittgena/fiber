# fiber.phase.abc.spec.fsm.guide

@desc: Fiber ASL Authoring Specification, Polyglot Actuator Cookbook, and Progressive Onboarding Guide

This guide provides the practical authoring specification and integration recipes for building deterministic workflows on the **Fiber FSM/DAG Runner** (specified in **[`fiber.phase.abc.spec.fsm.runner`](/phase/abc/spec/fsm/runner.md)**). It covers ASL syntax rules, connecting heterogeneous actuators (MCP servers, existing REST APIs, Python functions, and CLI binaries), controlling multiverse lifecycles, and progressively offloading high-risk subgraphs from existing agent frameworks.

---

## 1. Fiber ASL Authoring Specification

Fiber ASL is 100% backward-compatible with standard AWS Step Functions JSON while introducing concise syntactic sugar designed for both human engineers and LLM workflow generators.

### 1.1. Structural Keywords (`[FIXED]` System-Bound vs. `[FREE]` User-Defined)

| Field / Location | Binding Type | Specification & Engine Contract |
| --- | --- | --- |
| **`"Version"`, `"StartAt"`, `"States"**` | `[FIXED]` | Required root-level keys. `"StartAt"` must match a valid node key inside `"States"`. |
| **State (Node) IDs** | `[FREE]` | Any custom identifier (e.g., `"Plan_Intent_LLM"`, `"Step_01"`). Must **not** use reserved terminal tokens (`"END"`, `"RETURN"`) or reserved prefixes (`"blob:"`, `"branch:"`). |
| **`"Type"`** | `[FIXED]` | Allowed values:<br>

<br>• `"Task"`: Executes an LLM, MCP, or Polyglot actuator via `"Resource"`.<br>

<br>• `"Choice"`: Compiles to `NodeType.ROUTER` (`"t": "router"`).<br>

<br>• `"Succeed"` / `"Fail"`: Compiles to terminal `NodeType.ACT` (`"nx": "END"`). |
| **`"Next"`** | `[MIXED]` | • `str`: Standard `1:1` transition (or `"branch:<hash>#<entry>"` for `SYMLINK` call).<br>

<br>• `List[str]` (`len >= 2`): Triggers an atomic `1:N` multiverse fork. |
| **`"Catch"`** | `[MIXED]` | Mandatory on parallel branch `"Task"` nodes converging at a `DagJoinBarrier`.<br>

<br>• **Shorthand:** `"Catch": "Evaluate_And_Join"`<br>

<br>• **Standard ASL:** `"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Evaluate_And_Join"}]` |

### 1.2. `"Choice"` (`ROUTER`) Expressions & Reserved `meta_ctx` Keys

`"Choice"` nodes evaluate rules top-to-bottom (`rl`) and fall back to `"Default"` (`dn`) if no rule matches. Both the Host Runtime and the WASM Kernel evaluate numeric (`float`), string, and boolean comparisons across 6 operators (`>=`, `<=`, `!=`, `==`, `>`, `<`).

| Operator Type | Shorthand `"if"` Syntax (Recommended) | Standard ASL `"Choices"` Syntax |
| --- | --- | --- |
| **Numeric Comparison** | `{"if": "$._score >= 0.95", "Next": "Node_A"}` | `{"Variable": "$._score", "NumericGreaterThanEquals": 0.95, "Next": "Node_A"}` |
| **String Equality** | `{"if": "$.mode == 'fallback'", "Next": "Node_B"}` | `{"Variable": "$.mode", "StringEquals": "fallback", "Next": "Node_B"}` |
| **Inequality** | `{"if": "$.status != error", "Next": "Node_C"}` | `{"Variable": "$.status", "StringNotEquals": "error", "Next": "Node_C"}` |
| **Boolean Check** | `{"if": "$.is_verified == true", "Next": "Node_D"}` | `{"Variable": "$.is_verified", "BooleanEquals": true, "Next": "Node_D"}` |

#### Reserved Runtime `cursor.meta_ctx` System Keys

| Key | Type | Engine Binding & Behavior |
| --- | --- | --- |
| **`_score`** | `float` | Evaluated by the Default Arbiter (and WASM `MERGE` guard) to select the winning branch at a `DagJoinBarrier` (tie-breaker: remaining `fuel`). |
| **`_hitl_yield`** | `bool` | Set to `True` upon receiving `HTTP 202` (MCP/REST), exit code `202` (`exec://`), or `{"_hitl_yield": True}` (`fn://`). Parks the cursor in place with **zero PTA/fuel spend**. Cleared upon `resume()`. |
| **`_hitl_ticket`** | `str` | Ticket ID attached during `202` Pre-Flight HITL locking. |
| **`_fork_ratios`** | `List[float]` | Optional weight list (e.g., `[0.5, 0.3, 0.2]`) consumed during `1:N` fan-out to slice remaining parent fuel asymmetrically across child branches. |
| **`_fork_branches`** | `List[Tuple[str, dict]]` | Optional dynamic fan-out targets injected at runtime by an LLM planner node. |
| **`_last_ptr`** | `str` | Automatically populated with the CAS pointer (`"blob:<sha256_16>"`) of the most recent offloaded payload. |
| **`_fallback_error`** | `str` | Automatically populated with the fault/exhaustion reason when a branch is routed via its `"Catch"` (`fb`) fallback edge. |

---

## 2. Polyglot Resource & Actuator Cookbook (`parse_resource_uri`)

Fiber does not force teams to rewrite existing microservices, Python libraries, or CLI binaries into MCP servers. The shared URI parser (`ParsedResource`) maps 10 scheme patterns into canonical `NodeType` slots while dispatching each transport natively via `FiberExecutionBridge` or `PolyglotExecutionBridge`:

| Shorthand URI (Recommended) | Legacy / Extended Format | Compiled `NodeType` | Bridge & Kernel Execution Behavior |
| --- | --- | --- | --- |
| **`"llm://<model>"`**<br>

<br>*(e.g., `"llm://gpt-4o"`)* | `"fiber://.../interllm_proxy"` | `PROJECTOR`<br>

<br>(`"projector"`) | Automatically binds `Parameters["model"] = "<model>"` (if omitted) and invokes the Fiber LLM completion pipeline. |
| **`"reflect://<model>"`**<br>

<br>*(e.g., `"reflect://gpt-4o-mini"`)* | `"fiber://.../reflect_llm"` | `REFLECT`<br>

<br>(`"reflect"`) | Automatically binds `Parameters["model"] = "<model>"` and invokes the LLM pipeline for self-reflection/verification. |
| **`"tension://<model>"`** | `"fiber://.../tension_eval"` | `TENSION`<br>

<br>(`"tension"`) | Invokes the LLM pipeline and triggers the Cross-VM Escape Hatch (FSM VM $\rightarrow$ Phase VM tension evaluation). |
| **`"mcp://<target_id>"`**<br>

<br>*(e.g., `"mcp://prod_db_admin"`)* | `"fiber://mcp_sidecar/<target_id>"` | `OPERSIG`<br>

<br>(`"opersig"`) | Dispatches `POST {mcp_gateway_url}/<target_id>/invoke` with `checkpoint_handle` and native `HTTP 202` Pre-Flight HITL locking. |
| **`"fn://<handler_name>"`**<br>

<br>**`"py://<module>:<callable>"`** | Polyglot In-Process Callable | `OPERSIG`<br>

<br>(`"opersig"`) | Invokes registered sync/async functions (`fn://`) or dynamically imported callables (`py://`) with automatic signature adaptation (`**kwargs`, `1-arg`, or `(args, meta, ckpt)`). |
| **`"http(s)://<endpoint>"`** | Polyglot Direct REST / Webhook | `OPERSIG`<br>

<br>(`"opersig"`) | Sends direct HTTP requests with `X-Fiber-Checkpoint` and `X-Fiber-Tenant` headers. Parks cursor with `0` Fuel spend on `HTTP 202`. |
| **`"exec://<cmd_and_args>"`** | Polyglot Subprocess / Binary | `OPERSIG`<br>

<br>(`"opersig"`) | Spawns async subprocesses passing a JSON envelope over `stdin`. Parks cursor with `0` Fuel spend on process exit code `202`. |
| **`"pending://<target_id>"`** | `"fiber://.../pending_gate"` | `PENDING`<br>

<br>(`"pending"`) | Routes through the MCP Gateway for asynchronous external wait gates. |
| **`"act://<name>"`** | `"noop://<name>"` | `ACT`<br>

<br>(`"act"`) | Pure control/no-op node (`0` external I/O, `0` Fuel cost). |

### Protocol-Specific Integration Contracts (`PolyglotExecutionBridge`)

1. **In-Process Python Functions (`fn://<name>` & `py://<module>:<func>`):**
* **Zero-Wrapper Signature Adaptation:** `PolyglotExecutionBridge` inspects the callable signature at runtime. If `Parameters` keys match the function's named arguments (e.g., `def calc(a=1, b=2)`), they are unpacked as `**kwargs`. Otherwise, the bridge passes `(resolved_args)`, `(resolved_args, meta_ctx)`, or `(resolved_args, meta_ctx, ckpt_handle)` according to parameter arity.
* **In-Process HITL Yield:** Returning `{"_hitl_yield": True, "ticket_id": "..."}` from any Python function immediately parks the branch cursor with `0` Fuel spend.


2. **Existing Enterprise REST APIs (`http://` & `https://`):**
* **Idempotency Headers:** Every request automatically carries `X-Fiber-Checkpoint: <tx_hash:idx>`, `X-Fiber-Tenant: <tenant_id>`, and `X-Fiber-Stream: <stream_id>`.
* **Native `HTTP 202` Parking:** If an existing internal API returns status `202 Accepted` (with a JSON body containing `"ticket_id"` or header `X-Fiber-HITL-Ticket`), the branch parks in-place with `0` Fuel spend just like a native MCP server.


3. **Local Binaries & Shell Scripts (`exec://<cmd>`):**
* **Stdio Envelope:** When `allow_exec_subprocess=True` is enabled on the bridge (or when mounted behind `fiber connect`), the process receives `{"checkpoint_handle": ..., "stream_id": ..., "node": ..., "tick": ..., "arguments": ..., "meta": ...}` via `stdin`.
* **Exit Code `202` Parking:** Exiting with code `202` parks the cursor with `0` Fuel spend; exiting with `0` parses `stdout` (JSON or text) into the `Projection` slicer.



---

## 3. Memory Diet (`Projection`) & Deterministic Replay (`VCR`)

All `"Parameters"` are stripped from the on-ledger graph at compile time and inscribed into CAS (`blob:<hash>`).

* **Dynamic Context Resolution (`".$"` Suffix):** Any parameter key ending with `".$"` (e.g., `"intent_filter.$": "$.intent_type"` or nested `"sku.$": "$.items.1.sku"`) is dynamically resolved at runtime from `cursor.meta_ctx` via `resolve_dynamic_params()` prior to actuator invocation.
* **`"__fiber_extensions"` Block:** Stripped prior to actuator invocation. Supports:
* **`"Projection"` (`ProjectionConfig` via `apply_projection()`):**
* `"offload_path"` (`Optional[str]`): JSON path within the heavy actuator response to slice and offload into CAS (`blob:<hash>`). Defaults to offloading the full response dictionary.
* `"extract_keys"` (`List[str]`): JSON paths lifted from the actuator response into the lightweight on-ledger `cursor.meta_ctx`. **Compiler Auto-Inference:** Any variable referenced in downstream `"Choice"` nodes plus `"$._score"` are automatically scanned and injected into every Task's `extract_keys` even if `"Projection"` is omitted.


* **`"VCR"` (`VCRConfig`):** `{"capture_mode": "socket_stream" | "rpc_payload", "coalescing_window_ms": int}` for zero-cost deterministic CI/CD replay.
* **Execution Attributes:** `"pressure"` (`0.0 ~ 1.0`), `"max_failures"` (`int`), `"allow_parallel"` (`bool`), and `"step_cost"` (`int`).



### Reference Multiverse ASL Definition

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

## 4. Lifecycle Execution Scenarios & SDK Recipes

### Scenario 1: ASL Compilation & 1:N Multiverse Fan-Out

When a node defines multiple targets (`"Next": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"]`) or dynamically injects `_fork_branches` (and optional `_fork_ratios`) into `meta_ctx`, `GraphTraverser` deducts the parent node's `step_cost` and triggers a `1:N` atomic fork.

```text
[Plan_Intent_LLM (1000 Fuel)]
       │ (Spends 20 Fuel -> 980 Remaining; Offloads LLM output to CAS)
       ▼
  runtime.fork() ──► Atomic Fuel Slicing [490, 490]
       ├──► Branch b0 (run_101/b0): Fast_Readonly_MCP  (490 Fuel)
       └──► Branch b1 (run_101/b1): Admin_Mutation_MCP (490 Fuel)

```

### Scenario 2: Pre-Flight HITL Parking (`202`) & Barrier Synchronization

When Branch `b1` invokes a sensitive tool (e.g., `DELETE` query on `mcp://prod_db_admin`, a gated REST call `https://...`, or a privileged CLI script `exec://...`), the gateway or target returns **`202 (Pre-Flight HITL Lock)`**.

1. **Zero-Spend Parking:** The Execution Bridge signals `_hitl_yield: True`. `GraphTraverser` parks `b1` in place **without spending its PTA or deducting fuel** (`490 Fuel` and `Tick=2` preserved).
2. **Sibling Barrier Preservation:** Meanwhile, Branch `b0` finishes (`-5 Fuel` $\rightarrow$ `485 Fuel`) and arrives at the diamond join node (`Evaluate_And_Join`). Because `b1` is parked, `GraphTraverser` safely extracts `b0` from the `DagJoinBarrier` and returns both `[parked_b1, waiting_b0]` in `TraversalResult.parked_cursors`.

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

If a parallel branch attempts to spend more fuel than its allocated slice (or encounters a tool timeout/exception), `BranchController.step()` raises a fuel exhaustion error. `GraphTraverser` catches the fault, drains the runaway branch's fuel to `0`, and transitions it along its `Catch` (`fb`) edge to the `DagJoinBarrier`. Surviving sibling branches merge cleanly and reclaim all remaining fuel without barrier starvation.

---

## 5. Programmatic SDK & Progressive Framework Onboarding

### 5.1. Canonical & Polyglot SDK Initialization

By default, `GraphTraverser` binds `FiberExecutionBridge` for standard `llm://` and `mcp://` execution. To mix in-process Python functions (`fn://`, `py://`), existing REST endpoints (`https://`), or local binaries (`exec://`), mount `PolyglotExecutionBridge`:

```python
import asyncio
from fiber.phase.fsm import GraphTraverser
from fiber.phase.fsm.bridge.polyglot import PolyglotExecutionBridge

async def main(asl_definition: dict):
    async with GraphTraverser(
        tenant_id="accounting_dept",
        mcp_gateway_url="http://127.0.0.1:8356/v1/mcp-gateway",
    ) as traverser:
        # Optional: Mount PolyglotExecutionBridge to enable fn://, py://, https://, and exec://
        bridge = PolyglotExecutionBridge(
            runtime=traverser.runtime,
            tenant_id="accounting_dept",
            allow_exec_subprocess=True,
        )
        bridge.register_fn("custom_audit", lambda args: {"_score": 0.98, "verified": True})
        traverser.node_executor = bridge

        result = await traverser.run(
            asl_schema=asl_definition,
            base_stream_id="agent_session_01",
            initial_fuel=5000,
        )
        print(f"Status: {result.final_cursor.curr_node} | Fuel Left: {result.reclaimed_fuel}")
        print(f"Trace : {' -> '.join(result.visited_history)}")

asyncio.run(main(SAMPLE_MULTIVERSE_ASL))

```

### 5.2. Progressive Onboarding from Existing Frameworks (Embrace & Offload)

Rather than rewriting an entire application built on LangGraph, CrewAI, LlamaIndex, or native vendor SDKs (Claude Agent SDK, OpenAI Agents SDK), teams can adopt Fiber's FSM engine incrementally across three entrypoints:

1. **Stage A — Zero-Touch I/O Governance (Gateway Only):** Keep existing framework orchestration untouched and point `OPENAI_BASE_URL` and MCP tool endpoints to `[http://127.0.0.1:8356/v1](http://127.0.0.1:8356/v1)`. Enforces two-track DLP guardrails, `StreamBreaker`, and MCP Pre-Flight HITL locks (`HTTP 202`) immediately.
2. **Stage B — High-Risk Subgraph Co-Processor (Selective Off-Ramp):** When an existing agent hits checkpointer DB bloat, parallel branch runaway, or complex HITL approval requirements, offload **only that critical sub-workflow** to `GraphTraverser.run(sub_asl)` inside a single framework node while reusing existing Python tool functions via `fn://<name>` or `py://<module>:<func>`.
3. **Stage C — Declarative ASL Artifact Consolidation:** Replace brittle framework-specific state graphs with immutable `<= 2KB` compiled ASL bundles (`branch:<hash>`), enabling static deadlock verification (`E_BARRIER_DEADLOCK_RISK`) and zero-cost CI/CD VCR replay across the entire organization.