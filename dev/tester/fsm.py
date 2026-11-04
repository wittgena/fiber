# fiber.dev.tester.fsm
"""
FIBER FSM/DAG MULTIVERSE SPECIFICATION, CLI RUNNER & E2E VERIFICATION SUITE
======================================================================================

This module serves both as the canonical reference specification for authoring
Fiber ASL (Agent State Language) workflows and as a dual-mode CLI runner:
  1. Default Suite Mode (no `--asl` arg): Runs the 5-phase multiverse E2E verification suite.
  2. Custom ASL Runner Mode (`--asl <file.json | inline_json>`): Compiles and executes any
     user-provided ASL workflow using either `MockActuator` (`--mock true`) or the live
     `PolyglotExecutionBridge` (`--live` / `--mock false`).

--------------------------------------------------------------------------------------
CLI USAGE EXAMPLES
--------------------------------------------------------------------------------------
  # 1. Run default 5-phase E2E verification suite (Mock mode)
  python -m dev.tester.fsm

  # 2. Run a custom ASL JSON file in deterministic Mock mode
  python -m dev.tester.fsm --asl ./my_workflow.asl.json --fuel 2000

  # 3. Run a custom ASL JSON file in LIVE Polyglot mode (LLM + MCP + HTTPS + CLI)
  python -m dev.tester.fsm --asl ./my_workflow.asl.json --live --tenant finops_prod

  # 4. Run SAMPLE_MULTIVERSE_ASL in single-shot mode with custom initial meta_ctx
  python -m dev.tester.fsm --single --meta '{"intent_type": "custom_audit"}'

  # 5. Resume parked PTA pointers ('tx_hash:idx') after HITL approval
  python -m dev.tester.fsm --asl ./my_workflow.asl.json --live --resume "4421f309..:0,dbbc3268..:1"

--------------------------------------------------------------------------------------
`"Resource"` URI ROUTING RULES (`contract.py`, `compiler.py` & `polyglot.py`)
--------------------------------------------------------------------------------------
  [Shorthand / Keyword]                  [Compiler & Bridge Behaviour]
  • `"llm://<provider>/<model>"`     ->  `NodeType.PROJECTOR` -> Auto-sets `model` & calls `acompletion()`
  • `"reflect://<provider>/<model>"` ->  `NodeType.REFLECT`   -> Auto-sets `model` & calls `acompletion()`
  • `"tension://<provider>/<model>"` ->  `NodeType.TENSION`   -> Calls `acompletion()` + `phase.wasm` bridge
  • `"mcp://<target_id>"`            ->  `NodeType.OPERSIG`   -> Calls `POST {mcp_gateway}/<target_id>/invoke`
  • `"fn://<name>"` / `"py://m:fn"`  ->  `NodeType.OPERSIG`   -> Calls in-process Python function (`PolyglotBridge`)
  • `"http(s)://<url>"`              ->  `NodeType.OPERSIG`   -> Direct REST call + `HTTP 202` HITL park
  • `"exec://<cmd>"`                 ->  `NodeType.OPERSIG`   -> Async stdio subprocess + `exit 202` HITL park
  • `"pending://<target_id>"`        ->  `NodeType.PENDING`   -> Async external wait gate via MCP gateway
  • `"act://<name>"`                 ->  `NodeType.ACT`       -> No-op control step (0 Fuel cost)
"""
import argparse
import asyncio
import copy
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fiber.phase.fsm.bridge.polyglot import PolyglotExecutionBridge
from fiber.phase.fsm.contract import (
    BRANCH_PREFIX,
    CAS_PREFIX,
     END_TOKEN,
    CompactKey,
    NodeType,
    parse_resource_uri,
)
from fiber.phase.fsm.runtime import BranchCursor
from fiber.phase.fsm.traverser import GraphTraverser
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fsm.tester", phase="E2E_VERIFICATION")

# Canonical adapter model identifiers from `fiber-compats/adapters`
DEFAULT_PLANNER_LLM = "llm://cerebras/qwen-3.8-27b"
DEFAULT_REFLECT_LLM = "reflect://gemini/gemini-3.1-flash-lite"

# REFERENCE ASL WORKFLOW (Demonstrating Clean Shorthand & `Projection` Contract)
SAMPLE_MULTIVERSE_ASL: Dict[str, Any] = {
    "Version": "1.0",
    "Comment": "Reference Multiverse Flow: Planner -> Parallel Branches (Safe vs HITL) -> Join -> Final",
    "StartAt": "Plan_Intent_LLM",
    "States": {
        "Plan_Intent_LLM": {
            "Type": "Task",
            "Resource": DEFAULT_PLANNER_LLM,  # Shorthand: NodeType.PROJECTOR & model="cerebras/qwen-3.8-27b"
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
            "Resource": DEFAULT_REFLECT_LLM,  # Shorthand: NodeType.REFLECT & model="gemini/gemini-3.1-flash-lite"
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


def _load_asl_input(asl_arg: Optional[str]) -> Tuple[Dict[str, Any], str]:
    """
    Loads ASL definition from a JSON file path, stdin ('-'), or inline JSON string.
    Falls back to `SAMPLE_MULTIVERSE_ASL` if `asl_arg` is None.
    """
    if not asl_arg:
        return copy.deepcopy(SAMPLE_MULTIVERSE_ASL), "SAMPLE_MULTIVERSE_ASL (Built-in)"

    raw_str = asl_arg.strip()
    if raw_str == "-":
        return json.loads(sys.stdin.read()), "stdin"

    if raw_str.startswith("{"):
        return json.loads(raw_str), "inline_json"

    path = Path(raw_str).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"ASL file not found: '{path}'")
    return json.loads(path.read_text(encoding="utf-8")), str(path.name)


def _str_to_bool(val: str) -> bool:
    if isinstance(val, bool):
        return val
    v = val.strip().lower()
    if v in ("1", "true", "t", "yes", "y", "mock"):
        return True
    if v in ("0", "false", "f", "no", "n", "live"):
        return False
    raise argparse.ArgumentTypeError(f"Boolean value expected, got '{val}'")


# DETERMINISTIC MOCK ACTUATOR (Routes by ParsedResource & NodeType, NOT Node Name)
class MockActuator:
    """
    Simulates `FiberExecutionBridge` without hardcoding user-defined node names.
    Dispatches strictly by `NodeType` and `parse_resource_uri(resource_uri)`.
    """

    def __init__(self, auto_approve_hitl: bool = False) -> None:
        self.hitl_approved_tickets: set[str] = set()
        self.simulate_runaway_on_readonly: bool = False
        self.auto_approve_hitl: bool = auto_approve_hitl

    async def __call__(
        self,
        cursor: BranchCursor,
        node_spec: Dict[str, Any],
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        node_type = node_spec.get(CompactKey.TYPE)
        parsed_uri = parse_resource_uri(task_io.get("resource_uri", ""))
        target_id = parsed_uri.target_id
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
                {"llm_output": f"Executed {cursor.curr_node} ({target_id}) at tick {cursor.tick}"},
                cost,
            )

        # MCP Tool Target: `prod_db_admin` (Simulates HTTP 202 Pre-Flight HITL Lock on DELETE SQL)
        if target_id == "prod_db_admin" or "DELETE" in str(call_params.get("sql", "")):
            ticket_id = "hitl_ticket_drop_99"
            if not self.auto_approve_hitl and ticket_id not in self.hitl_approved_tickets:
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
                f"  [Actuator] Runaway Fault Injected at '{cursor.curr_node}' (target='{target_id}') on '{cursor.stream_id}'! "
                f"Attempting to spend {runaway_cost} Fuel (Available={cursor.fuel}) -> Triggering Catch Fallback"
            )
            return {"_score": 0.0}, None, runaway_cost

        cost = int(task_io.get("extras", {}).get("step_cost", 5))
        print(
            f"  [Actuator] Safe Tool Node '{cursor.curr_node}' (scheme='{parsed_uri.scheme}', target='{target_id}') on '{cursor.stream_id}' "
            f"| Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) | Score=0.70"
        )
        return (
            {"_score": 0.70, "db_status": "readonly_ok"},
            {"count": 1024, "uri": parsed_uri.raw_uri},
            cost,
        )


# MODE A: Custom ASL / Single-Workflow Runner (Supports `--mock` & `--live`)
async def run_custom_asl_workflow(
    asl_schema: Dict[str, Any],
    source_label: str,
    use_mock: bool = True,
    stream_id: str = "custom_run_01",
    initial_fuel: int = 1000,
    initial_meta: Optional[Dict[str, Any]] = None,
    tenant_id: str = "default_tenant",
    mcp_gateway_url: str = "http://127.0.0.1:8356/v1/mcp-gateway",
    allow_exec: bool = False,
    auto_approve_hitl: bool = False,
    resume_pointers: Optional[List[str]] = None,
) -> None:
    mode_str = "MOCK (MockActuator)" if use_mock else "LIVE (PolyglotExecutionBridge)"
    print("=" * 86)
    print(f" [FIBER FSM RUNNER] Source='{source_label}' | Mode={mode_str}")
    print("=" * 86)

    actuator = MockActuator(auto_approve_hitl=auto_approve_hitl) if use_mock else None
    async with GraphTraverser(
        node_executor=actuator,
        tenant_id=tenant_id,
        mcp_gateway_url=mcp_gateway_url,
    ) as traverser:
        live_bridge: Optional[PolyglotExecutionBridge] = None
        if not use_mock:
            live_bridge = PolyglotExecutionBridge(
                runtime=traverser.runtime,
                tenant_id=tenant_id,
                mcp_gateway_url=mcp_gateway_url,
                allow_exec_subprocess=allow_exec,
            )
            traverser.node_executor = live_bridge

        try:
            # 1. Compile & Static Sandbox Verification
            bundle = traverser.compiler.compile_from_asl(asl_schema, trace_id=stream_id)
            cas_io_count = sum(1 for k in bundle.cas_store if k.startswith(CAS_PREFIX))
            branch_count = sum(1 for k in bundle.cas_store if k.startswith(BRANCH_PREFIX))
            print(f"  • Compiled Root   : {bundle.root_hash} ({bundle.total_root_bytes}B / 2048B)")
            print(f"  • CAS Objects     : {len(bundle.cas_store)} ({cas_io_count} TaskIOSpecs + {branch_count} Sub-Branches)\n")

            # 2. Execute or Resume
            if resume_pointers:
                print(f"  • Resuming from PTA Pointers: {resume_pointers}\n")
                res = await traverser.resume(targets=resume_pointers, base_stream_id=stream_id)
            else:
                res = await traverser.traverse_bundle(
                    bundle=bundle,
                    base_stream_id=stream_id,
                    initial_fuel=initial_fuel,
                    initial_meta=initial_meta,
                )

            # 3. Report Summary
            print("\n" + "-" * 86)
            print(" [Execution Summary]")
            print("-" * 86)
            print(f"  • Visited Trace   : {' -> '.join(res.visited_history)}")
            if res.parked_cursors:
                print(f"  • Status          : PARKED ({len(res.parked_cursors)} cursor(s) awaiting HITL / Barrier)")
                for pc in res.parked_cursors:
                    print(
                        f"    - Stream='{pc.stream_id}' | Node='{pc.curr_node}' | "
                        f"PTA='{pc.pointer.to_key()}' | Fuel={pc.fuel} | Ticket='{pc.meta_ctx.get('_hitl_ticket', '')}'"
                    )
            else:
                print(f"  • Status          : COMPLETED (Final Node='{res.final_cursor.curr_node}')")
                print(f"  • Final PTA       : {_short_pta(res.final_cursor)} ({res.final_cursor.pointer.to_key()})")
                print(f"  • Reclaimed Fuel  : {res.reclaimed_fuel} / {initial_fuel}")
                print(f"  • Final meta_ctx  : {json.dumps(res.final_cursor.meta_ctx, ensure_ascii=False)}")
            print("=" * 86)
        finally:
            if live_bridge is not None:
                await live_bridge.aclose()

# MODE B: Canonical 5-Phase Multiverse E2E Verification Suite (Default)
async def run_e2e_verification_suite(asl_schema: Optional[Dict[str, Any]] = None) -> None:
    """E2E SCENARIO RUNNER (Compile -> Run & Park -> Resume -> Time-Travel -> Auto-Linker)"""
    target_asl = copy.deepcopy(asl_schema or SAMPLE_MULTIVERSE_ASL)
    print("=" * 86)
    print(" [FIBER FSM/DAG MULTIVERSE TESTER] Starting 5-Phase E2E Lifecycle Verification")
    print("=" * 86)

    actuator = MockActuator()
    async with GraphTraverser(node_executor=actuator) as traverser:
        # PHASE 1: ASL Compilation, Static Deadlock Guard & 2KB Separation
        print("\n" + "-" * 86)
        print(" [Phase 1: Compile] Stripping heavy Task I/O into CAS & projecting <=2KB Topology Blob")
        print("-" * 86)
        bundle = traverser.compiler.compile_from_asl(target_asl, trace_id="test_suite_01")
        cas_io_count = sum(1 for k in bundle.cas_store if k.startswith(CAS_PREFIX))
        branch_count = sum(1 for k in bundle.cas_store if k.startswith(BRANCH_PREFIX))

        print(f"  • Root Graph Hash : {bundle.root_hash} (On-AnchorStore Compact IR)")
        print(f"  • Root Graph Size : {bundle.total_root_bytes}B / 2048B Limit ({round(bundle.total_root_bytes / 2048 * 100, 1)}% used)")
        print(f"  • Offloaded Blobs : {len(bundle.cas_store)} CAS objects ({cas_io_count} TaskIOSpecs + {branch_count} Branch Blob)")
        assert bundle.total_root_bytes <= 2048, "Root blob exceeded 2KB limit!"

        # Verify WASM Static Deadlock Linter catches missing `Catch` on parallel branch
        bad_asl = copy.deepcopy(target_asl)
        if "Fast_Readonly_MCP" in bad_asl.get("States", {}):
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

        actuator.hitl_approved_tickets.add("hitl_ticket_drop_99")
        print("  • Admin Action        : Approved TOTP/Slack Ticket 'hitl_ticket_drop_99'. Resuming branches...\n")

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

    print("\n" + "=" * 70)
    print(" ALL MULTIVERSE FSM/DAG, WASM AUTO-LINKER & PTA LEDGER ASSERTIONS PASSED")
    print("=" * 70)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dev.tester.fsm",
        description="Fiber FSM/DAG Multiverse E2E Suite & Custom ASL Workflow Runner",
    )
    parser.add_argument(
        "-a",
        "--asl",
        type=str,
        default=None,
        help="Path to an ASL JSON file, inline JSON string, or '-' for stdin. If omitted, uses SAMPLE_MULTIVERSE_ASL.",
    )
    parser.add_argument(
        "--mock",
        type=_str_to_bool,
        nargs="?",
        const=True,
        default=True,
        help="Use deterministic MockActuator (true/false, default: true). Pass '--mock false' or '--live' for real execution.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Shortcut for '--mock false': binds PolyglotExecutionBridge for real LLM/MCP/HTTP/CLI execution.",
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Run a single workflow pass instead of the 5-phase verification suite even when '--asl' is omitted.",
    )
    parser.add_argument(
        "-f",
        "--fuel",
        type=int,
        default=1000,
        help="Initial fuel budget allocated at Genesis spawn (default: 1000).",
    )
    parser.add_argument(
        "-s",
        "--stream-id",
        type=str,
        default="run_custom_01",
        help="Base stream identifier for PTA ledger anchoring (default: 'run_custom_01').",
    )
    parser.add_argument(
        "-m",
        "--meta",
        type=str,
        default=None,
        help="Initial `meta_ctx` as a JSON object string (e.g. '{\"intent_type\": \"audit\"}').",
    )
    parser.add_argument(
        "--tenant",
        type=str,
        default="default_tenant",
        help="Tenant ID passed to FiberExecutionBridge / PolyglotExecutionBridge.",
    )
    parser.add_argument(
        "--gateway-url",
        type=str,
        default="http://127.0.0.1:8356/v1/mcp-gateway",
        help="Fiber Local MCP Gateway URL (default: http://127.0.0.1:8356/v1/mcp-gateway).",
    )
    parser.add_argument(
        "--allow-exec",
        action="store_true",
        help="Allow `exec://` subprocess execution in live PolyglotExecutionBridge mode.",
    )
    parser.add_argument(
        "--approve-hitl",
        action="store_true",
        help="Pre-approve HITL tickets in MockActuator so single-shot runs complete without parking.",
    )
    parser.add_argument(
        "--resume",
        type=str,
        default=None,
        help="Comma-separated PTA pointers ('tx_hash:idx,tx_hash:idx') to resume a parked workflow.",
    )
    return parser


async def _async_main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    use_mock = False if args.live else bool(args.mock)

    # Default behavior: if no custom ASL, no --single, no --live, and no --resume, run full 5-phase suite
    if args.asl is None and not args.single and use_mock and not args.resume:
        await run_e2e_verification_suite()
        return

    asl_schema, source_label = _load_asl_input(args.asl)
    initial_meta = json.loads(args.meta) if args.meta else None
    resume_list = [p.strip() for p in args.resume.split(",") if p.strip()] if args.resume else None

    await run_custom_asl_workflow(
        asl_schema=asl_schema,
        source_label=source_label,
        use_mock=use_mock,
        stream_id=args.stream_id,
        initial_fuel=args.fuel,
        initial_meta=initial_meta,
        tenant_id=args.tenant,
        mcp_gateway_url=args.gateway_url,
        allow_exec=args.allow_exec,
        auto_approve_hitl=args.approve_hitl,
        resume_pointers=resume_list,
    )

if __name__ == "__main__":
    asyncio.run(_async_main())