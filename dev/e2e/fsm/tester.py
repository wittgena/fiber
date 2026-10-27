# fiber.dev.e2e.fsm.tester
"""
======================================================================================
FIBER FSM/DAG MULTIVERSE SPECIFICATION & E2E VERIFICATION SUITE
======================================================================================

This module serves both as the end-to-end verification suite and the canonical
reference specification for authoring Fiber ASL (Agent State Language) workflows.

--------------------------------------------------------------------------------------
1. STRUCTURAL & TOPOLOGY KEYWORDS ([FIXED] System-Bound vs [FREE] User-Defined)
--------------------------------------------------------------------------------------
• Top-Level Keys   : [FIXED] `"Version"`, `"StartAt"`, `"States"` (Required by compiler)
• State (Node) IDs : [FREE]  Any string (e.g., `"Plan_Intent_LLM"`, `"My_Custom_Step"`).
                             Must NOT use reserved terminal tokens: `"END"`, `"RETURN"`,
                             or prefixes `"blob:"`, `"branch:"`.
• `"Type"`         : [FIXED] Allowed values: `"Task"`, `"Choice"`, `"Succeed"`, `"Fail"`.
• `"Next"`         : [MIXED] Single node ID (`str`) for 1:1 step, or `List[str]` (len >= 2)
                             for 1:N parallel multiverse fork.
                             Can also point to `"branch:<hash>#<entry_node>"` for SYMLINK jump.
• `"Catch"`        : [FIXED] Mandatory on parallel branch `"Task"` nodes converging at a
                             `DagJoinBarrier` (in-degree >= 2). Supports shorthand string:
                             `"Catch": "Evaluate_And_Join"` or standard ASL list format.

--------------------------------------------------------------------------------------
2. `"Resource"` URI ROUTING RULES (`compiler.py` & `bridge.py`)
--------------------------------------------------------------------------------------
Supports clean shorthand schemes (`<scheme>://<target_id>`) or full `fiber://` URIs:

  [Shorthand / Keyword]                  [Compiler & Bridge Behaviour]
  • `"llm://<model>"`                ->  `NodeType.PROJECTOR` -> Auto-sets `model` & calls `acompletion()`
  • `"reflect://<model>"`             ->  `NodeType.REFLECT`   -> Auto-sets `model` & calls `acompletion()`
  • `"tension://<model>"`            ->  `NodeType.TENSION`   -> Calls `acompletion()` + `phase.wasm` bridge
  • `"mcp://<target_id>"`            ->  `NodeType.OPERSIG`   -> Calls `POST {mcp_gateway}/<target_id>/invoke`
  • `"pending://<target_id>"`        ->  `NodeType.PENDING`   -> Same MCP gateway invocation
  • `"act://<name>"`                 ->  `NodeType.ACT`       -> No-op control step (0 Fuel cost)

--------------------------------------------------------------------------------------
3. `"Parameters"` & `"__fiber_extensions"` (`Projection`) CONTRACT
--------------------------------------------------------------------------------------
All `"Parameters"` are stripped from the on-ledger topology graph (`branch:<hash> <= 2KB`)
and stored off-ledger in CAS (`blob:<hash>`).

• Dynamic Binding (`".$"` suffix) : [FIXED SYNTAX] Any parameter key ending with `".$"`
                                    (e.g., `"intent_filter.$": "$.intent_type"`) is resolved
                                    at runtime from `cursor.meta_ctx["intent_type"]`.
• `"__fiber_extensions"`          : [FIXED KEY] Optional engine configuration block:
    - `"Projection"`     : `{"offload_path": str, "extract_keys": List[str]}`
                           • `offload_path`: Sub-path of heavy output to store in CAS (`blob:`).
                           • `extract_keys`: Keys to lift into lightweight on-ledger `meta_ctx`.
                           (Note: Any variable referenced in `"Choice"` nodes plus `"$._score"`
                           are automatically inferred and added to `extract_keys` by the compiler!)
    - `"VCR"`            : `{"capture_mode": str, "coalescing_window_ms": int}`
    - `"pressure"`       : `float` (0.0 ~ 1.0)
    - `"max_failures"`   : `int` (default: 3)
    - `"allow_parallel"` : `bool` (default: False)

--------------------------------------------------------------------------------------
4. `"Choice"` (`ROUTER`) & RUNTIME `cursor.meta_ctx` RESERVED KEYS
--------------------------------------------------------------------------------------
• Choice Conditions   : Supports shorthand `"if": "$._score >= 0.95"` (`>=`, `<=`, `!=`,
                        `==`, `>`, `<`) as well as standard ASL `"Variable"` operators.
• `"_score"`          : (`float`) Used by Arbiter to select the winning branch on merge.
• `"_hitl_yield"`     : (`bool`) Parks cursor in-place with ZERO fuel spend on HTTP 202.
• `"_fork_ratios"`    : (`List[float]`) Optional weight list for asymmetric 1:N fuel slicing.
• `"_last_ptr"`       : (`str`) Auto-populated CAS pointer (`"blob:<hash>"`) to offloaded I/O.
======================================================================================
"""
import asyncio
import copy
from typing import Dict, Any, Optional, Tuple

from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.compiler import (
    CompactKey,
    NodeType,
    END_TOKEN,
    CAS_PREFIX,
    BRANCH_PREFIX,
)
from fiber.phase.fsm.runtime import BranchCursor
from fiber.phase.fsm.traverser import ExGraphTraverser

log = get_emitter("fsm.tester", phase="E2E_VERIFICATION")

# REFERENCE ASL WORKFLOW (Demonstrating Clean Shorthand & `Projection` Contract)
SAMPLE_MULTIVERSE_ASL: Dict[str, Any] = {
    "Version": "1.0",
    "Comment": "Reference Multiverse Flow: Planner -> Parallel Branches (Safe vs HITL) -> Join -> Final",
    "StartAt": "Plan_Intent_LLM",
    "States": {
        "Plan_Intent_LLM": {
            "Type": "Task",
            "Resource": "llm://gpt-4o",  # Shorthand: sets NodeType.PROJECTOR & model="gpt-4o"
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
            "Resource": "mcp://readonly_db_search",  # Shorthand: NodeType.OPERSIG, target="readonly_db_search"
            "Parameters": {
                "query": "SELECT count(*) FROM audit_logs",
                "intent_filter.$": "$.intent_type",
            },
            "Next": "Evaluate_And_Join",
            "Catch": "Evaluate_And_Join",  # Shorthand Catch target! (`$._score` is auto-extracted)
        },
        "Admin_Mutation_MCP": {
            "Type": "Task",
            "Resource": "mcp://prod_db_admin",  # Shorthand: NodeType.OPERSIG, target="prod_db_admin"
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
            "Resource": "reflect://gpt-4o-mini",  # Shorthand: NodeType.REFLECT & model="gpt-4o-mini"
            "Parameters": {"prompt": "Verify fallback safety."},
            "Next": "Final_Act",
        },
        "Final_Act": {
            "Type": "Succeed",
        },
    },
}

def _short_pta(cursor: BranchCursor) -> str:
    """Formats PTA pointer ('tx_hash:idx') into a readable short handle."""
    key = cursor.pointer.to_key()
    if ":" in key:
        tx_h, idx = key.split(":", 1)
        return f"{tx_h[:8]}..:{idx}"
    return key[:10]

def _build_oversized_asl(node_count: int = 45) -> Dict[str, Any]:
    """Generates a >2KB ASL definition to verify WASM Auto-Linker SESE subgraph partitioning."""
    states: Dict[str, Any] = {}
    for i in range(node_count):
        curr_id = f"Chain_Step_Node_Identifier_{i:03d}"
        next_id = f"Chain_Step_Node_Identifier_{i+1:03d}" if i < node_count - 1 else "Final_Done"
        states[curr_id] = {
            "Type": "Task",
            "Resource": f"fiber://mcp_sidecar/step_worker_{i:03d}",
            "Parameters": {
                "step_index": i,
                "__fiber_extensions": {"pressure": 0.5},
            },
            "Next": next_id,
        }
    states["Final_Done"] = {"Type": "Succeed"}
    return {
        "Version": "1.0",
        "StartAt": "Chain_Step_Node_Identifier_000",
        "States": states,
    }

# DETERMINISTIC MOCK ACTUATOR (Routes by Resource Target ID & NodeType, NOT Node Name)
class MockFiberActuator:
    """
    Simulates `FiberExecutionBridge` without hardcoding user-defined node names.
    Dispatches strictly by `NodeType` and the MCP `<target_id>` suffix in `task_io["resource_uri"]`.
    """

    def __init__(self) -> None:
        self.hitl_approved_tickets: set[str] = set()
        self.simulate_runaway_on_readonly: bool = False

    async def __call__(
        self,
        cursor: BranchCursor,
        node_spec: Dict[str, Any],
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        node_type = node_spec.get(CompactKey.TYPE)
        resource_uri = task_io.get("resource_uri", "")
        target_id = resource_uri.split("/")[-1] if "/" in resource_uri else ""
        call_params = task_io.get("call_params", {})

        # Control / Terminal Nodes (`NodeType.ROUTER`, `NodeType.ACT`)
        if node_type in (NodeType.ROUTER.value, NodeType.ACT.value):
            print(
                f"  [Actuator] Control Node '{cursor.curr_node}' ({node_type}) "
                f"on '{cursor.stream_id}' | Tick={cursor.tick} | Cost=0 Fuel (Remaining={cursor.fuel})"
            )
            return {}, None, 0

        # LLM Cognitive Nodes (`NodeType.PROJECTOR`, `NodeType.REFLECT`, `NodeType.TENSION`)
        if node_type in (NodeType.PROJECTOR.value, NodeType.REFLECT.value, NodeType.TENSION.value):
            cost = 20
            print(
                f"  [Actuator] LLM Node '{cursor.curr_node}' (target='{target_id}') on '{cursor.stream_id}' "
                f"| Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) "
                f"| Offloading response to CAS"
            )
            return (
                {"intent_type": "parallel_audit"},
                {"llm_output": f"Executed {cursor.curr_node} at tick {cursor.tick}"},
                cost,
            )

        # MCP Tool Target: `prod_db_admin` (Simulates HTTP 202 Pre-Flight HITL Lock on DELETE SQL)
        if target_id == "prod_db_admin" or "DELETE" in str(call_params.get("sql", "")):
            ticket_id = "hitl_ticket_drop_99"
            if ticket_id not in self.hitl_approved_tickets:
                print(
                    f"  [Actuator] MCP Pre-Flight HITL Trap (HTTP 202) at '{cursor.curr_node}' (target='{target_id}') "
                    f"on '{cursor.stream_id}'! Regex matched 'DELETE'. "
                    f"Parking cursor WITHOUT spending PTA ({_short_pta(cursor)}) or Fuel ({cursor.fuel})."
                )
                return (
                    {"_hitl_yield": True, "_hitl_ticket": ticket_id},
                    {"status": 202, "reason": "Pre-Flight regex matched DELETE"},
                    0,
                )
            cost = 15
            print(
                f"  [Actuator] MCP HITL Approved ('{ticket_id}')! Executing '{cursor.curr_node}' "
                f"on '{cursor.stream_id}' | Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) | Score=0.95"
            )
            return (
                {"_hitl_yield": False, "_score": 0.95, "db_status": "cleaned"},
                {"deleted_rows": 42},
                cost,
            )

        # MCP Tool Target: `readonly_db_search` (Normal or Runaway Fault Injection)
        if self.simulate_runaway_on_readonly and target_id == "readonly_db_search":
            runaway_cost = cursor.fuel + 500
            print(
                f"  [Actuator] 💥 Runaway Fault Injected at '{cursor.curr_node}' (target='{target_id}') on '{cursor.stream_id}'! "
                f"Attempting to spend {runaway_cost} Fuel (Available={cursor.fuel}) -> Triggering Catch Fallback"
            )
            return {"_score": 0.0}, None, runaway_cost

        cost = 5
        print(
            f"  [Actuator] 🛠️  Safe MCP Node '{cursor.curr_node}' (target='{target_id}') on '{cursor.stream_id}' "
            f"| Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) | Score=0.70"
        )
        return (
            {"_score": 0.70, "db_status": "readonly_ok"},
            {"count": 1024},
            cost,
        )

# E2E SCENARIO RUNNER (Compile -> Run & Park -> Resume -> Time-Travel -> Auto-Linker)
async def run_e2e_verification_suite() -> None:
    print("=" * 86)
    print(" [FIBER FSM/DAG MULTIVERSE TESTER] Starting E2E Lifecycle Verification")
    print("=" * 86)

    actuator = MockFiberActuator()

    async with ExGraphTraverser(node_executor=actuator) as traverser:
        # PHASE 1: ASL Compilation, Static Deadlock Guard & 2KB Separation
        print("\n" + "-" * 86)
        print(" [Phase 1: Compile] Stripping heavy Task I/O into CAS & projecting <=2KB Topology Blob")
        print("-" * 86)
        bundle = traverser.compiler.compile_from_asl(SAMPLE_MULTIVERSE_ASL, trace_id="test_suite_01")
        cas_io_count = sum(1 for k in bundle.cas_store if k.startswith(CAS_PREFIX))
        branch_count = sum(1 for k in bundle.cas_store if k.startswith(BRANCH_PREFIX))

        print(f"  • Root Graph Hash : {bundle.root_hash} (On-Ledger Compact IR)")
        print(f"  • Root Graph Size : {bundle.total_root_bytes}B / 2048B Limit ({round(bundle.total_root_bytes / 2048 * 100, 1)}% used)")
        print(f"  • Offloaded Blobs : {len(bundle.cas_store)} CAS objects ({cas_io_count} TaskIOSpecs + {branch_count} Branch Blob)")
        assert bundle.total_root_bytes <= 2048, "Root blob exceeded 2KB limit!"

        # Verify WASM Static Deadlock Linter catches missing `Catch` on parallel branch
        bad_asl = copy.deepcopy(SAMPLE_MULTIVERSE_ASL)
        bad_asl["States"]["Fast_Readonly_MCP"].pop("Catch", None)
        try:
            traverser.compiler.compile_from_asl(bad_asl, trace_id="bad_deadlock_check")
            raise AssertionError("Static sandbox should have rejected parallel branch without Catch!")
        except ValueError as e:
            assert "E_BARRIER_DEADLOCK_RISK" in str(e)
            print(f"  • Static Linter   : Verified pre-mount deadlock rejection ({str(e).splitlines()[0][:68]}...)")

        # PHASE 2: Genesis Spawn -> 1:N Fan-Out -> Safe Branch Join vs HITL Park
        print("\n" + "-" * 86)
        print(" [Phase 2: Initial Run & HITL Park] Genesis(1000 Fuel) -> Plan(-20) -> 1:2 Fork[490, 490]")
        print("-" * 86)
        res1 = await traverser.traverse_bundle(
            bundle=bundle,
            base_stream_id="run_101",
            initial_fuel=1000,
        )
        print(f"\n  [Phase 2 Summary: Multiverse Paused Safely]")
        print(f"  • Detected Join Nodes : {sorted(traverser.runtime.barrier.topology.join_nodes)}")
        print(f"  • Visited Trace       : {' -> '.join(res1.visited_history)}")
        print(f"  • Parked Cursors      : {[c.stream_id for c in res1.parked_cursors]}")

        assert len(res1.parked_cursors) == 2, "Expected both HITL-parked b1 and barrier-waiting sibling b0"
        by_stream = {c.stream_id: c for c in res1.parked_cursors}
        waiting_b0 = by_stream["run_101/b0"]
        parked_b1 = by_stream["run_101/b1"]

        print(
            f"  • Branch b0 (Sibling) : Node='{waiting_b0.curr_node}' (Waiting at Join Barrier) "
            f"| PTA={_short_pta(waiting_b0)} | Fuel={waiting_b0.fuel} (490 - 5) | Tick={waiting_b0.tick}"
        )
        print(
            f"  • Branch b1 (Locked)  : Node='{parked_b1.curr_node}' (Zero-Spend HITL Parked)  "
            f"| PTA={_short_pta(parked_b1)} | Fuel={parked_b1.fuel} (490 - 0) | Tick={parked_b1.tick}"
        )

        # Verify fuel accounting: 1000 - 20 (Plan_LLM) = 980 -> Sliced [490, 490]. b0 spent 5 -> 485, b1 spent 0 -> 490
        assert waiting_b0.fuel == 485 and parked_b1.fuel == 490

        # PHASE 3: O(1) Cursor Rehydration -> HITL Release -> N:1 Barrier Merge
        print("\n" + "-" * 86)
        print(" [Phase 3: HITL Approval & Mid-Flow Resume] O(1) Restore from PTA Pointers (Skipping Prior Nodes)")
        print("-" * 86)
        restored_b1 = traverser.runtime.restore_cursor(parked_b1.pointer.to_key())
        print(
            f"  • O(1) Rehydrated b1  : stream='{restored_b1.stream_id}' at node='{restored_b1.curr_node}' "
            f"| PTA={_short_pta(restored_b1)} | Preserved Tick={restored_b1.tick} | Preserved Fuel={restored_b1.fuel}"
        )

        # Simulate Admin TOTP / 1-Click Approval releasing ticket 'hitl_ticket_drop_99'
        actuator.hitl_approved_tickets.add("hitl_ticket_drop_99")
        print("  • Admin Action        : Approved TOTP/Slack Ticket 'hitl_ticket_drop_99'. Resuming branches...\n")

        # Resume directly using PTA pointer strings (automatically rehydrates & merges at Join Barrier)
        res2 = await traverser.resume(
            targets=[waiting_b0.pointer.to_key(), parked_b1.pointer.to_key()],
            base_stream_id="run_101",
        )
        print(f"\n  [Phase 3 Summary: Diamond Barrier Merged & Fuel Reclaimed]")
        print(f"  • Resumed Trace       : {' -> '.join(res2.visited_history)}")
        print(f"  • Arbiter Winner      : Selected b1 (_score=0.95 > b0 _score=0.70) at 'Evaluate_And_Join'")
        print(f"  • Final Node          : {res2.final_cursor.curr_node} | Join Collapses: {res2.join_collapses} | Final PTA={_short_pta(res2.final_cursor)}")
        print(f"  • Reclaimed Fuel      : {res2.reclaimed_fuel} (485 from b0 + 475 from b1 = 960 Total Conserved)")

        assert res2.final_cursor.curr_node == END_TOKEN
        assert res2.reclaimed_fuel == 960, f"Expected 960 reclaimed fuel, got {res2.reclaimed_fuel}"

        # PHASE 4: Time-Travel Re-Branching from Completed Checkpoint
        print("\n" + "-" * 86)
        print(" [Phase 4: Time-Travel Re-Branching] Forking 2 New Hypotheses from Completed Checkpoint (960 Fuel)")
        print("-" * 86)
        exp_cursors = await traverser.runtime.fork(
            parent=res2.final_cursor,
            branch_targets=[
                ("Reflect_Fallback_LLM", {"strategy_override": "fallback_reflect", "_score": 0.99}),
                ("Final_Act", {"strategy_override": "direct_exit", "_score": 0.50}),
            ],
            fuel_ratios=[0.5, 0.5],
        )
        print(f"  • Forked 2 Experimental Branches from Sealed PTA {_short_pta(res2.final_cursor)} (960 Fuel -> [480, 480]):")
        for ec in exp_cursors:
            print(
                f"    - {ec.stream_id}: Target='{ec.curr_node}' | PTA={_short_pta(ec)} "
                f"| Fuel={ec.fuel} | Override='{ec.meta_ctx.get('strategy_override')}' (_score={ec.meta_ctx.get('_score')})"
            )
        print("")

        # Resume both experimental branches; b1 waits at Join Node 'Final_Act' until b0 finishes 'Reflect_Fallback_LLM'
        res3 = await traverser.resume(
            targets=exp_cursors,
            base_stream_id="run_101_exp",
        )
        print(f"\n  [Phase 4 Summary: Time-Travel Hypotheses Converged]")
        print(f"  • Time-Travel Trace   : {' -> '.join(res3.visited_history)}")
        print(
            f"  • Final Exp State     : Stream='{res3.final_cursor.stream_id}' | Node='{res3.final_cursor.curr_node}' "
            f"| Reclaimed Fuel={res3.reclaimed_fuel} (460 from b0 + 480 from b1 = 940)"
        )
        print(f"  • Winning Strategy    : '{res3.final_cursor.meta_ctx.get('strategy_override')}' (_score={res3.final_cursor.meta_ctx.get('_score')})")

        # 960 split into [480, 480]. b0 runs Reflect_Fallback_LLM (-20 -> 460), b1 runs Final_Act (0 -> 480) -> Merged = 940
        assert res3.final_cursor.curr_node == END_TOKEN
        assert res3.reclaimed_fuel == 940, f"Expected 940 reclaimed fuel, got {res3.reclaimed_fuel}"

        # PHASE 5: >2KB Auto-Linker Partitioning & Runaway Catch Recovery
        print("\n" + "-" * 86)
        print(" [Phase 5: WASM Auto-Linker & Runaway Catch Recovery] >2KB Partitioning & Fault Isolation")
        print("-" * 86)
        oversized_bundle = traverser.compiler.compile_from_asl(_build_oversized_asl(45), trace_id="oversized_45")
        sub_branches = [k for k in oversized_bundle.cas_store if k.startswith(BRANCH_PREFIX)]
        print(
            f"  • >2KB Auto-Linker    : 45-Node ASL partitioned into {len(sub_branches)} sub-branches "
            f"(Root={oversized_bundle.total_root_bytes}B <= 2048B)"
        )
        assert oversized_bundle.total_root_bytes <= 2048 and len(sub_branches) >= 2

        # Inject runaway fault on b0 ('readonly_db_search') -> Verify it drains to 0 Fuel via Catch and merges with b1 (475 Fuel)
        actuator.simulate_runaway_on_readonly = True
        res4 = await traverser.traverse_bundle(
            bundle=bundle,
            base_stream_id="run_102_fault",
            initial_fuel=1000,
        )
        print(f"  • Fault Recovery Trace: {' -> '.join(res4.visited_history)}")
        print(
            f"  • Fault Isolation     : Runaway b0 drained to 0 Fuel via Catch; "
            f"Surviving b1 (475 Fuel) won merge -> Reclaimed={res4.reclaimed_fuel} Fuel"
        )
        assert res4.final_cursor.curr_node == END_TOKEN
        assert res4.reclaimed_fuel == 475, f"Expected 475 reclaimed fuel from surviving branch, got {res4.reclaimed_fuel}"

    print("\n" + "=" * 86)
    print(" ✅ ALL MULTIVERSE FSM/DAG, WASM AUTO-LINKER & PTA LEDGER ASSERTIONS PASSED")
    print("=" * 86)


if __name__ == "__main__":
    asyncio.run(run_e2e_verification_suite())