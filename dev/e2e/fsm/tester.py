# fiber.dev.e2e.fsm.tester
import asyncio
from typing import Dict, Any, Optional, Tuple

from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.compiler import (
    CompactKey,
    NodeType,
    END_TOKEN,
)
from fiber.phase.fsm.runtime import BranchCursor
from fiber.phase.fsm.traverser import ExGraphTraverser

log = get_emitter("fsm.tester", phase="E2E_VERIFICATION")

# SAMPLE ASL WORKFLOW (Fan-Out + HITL Trap + Diamond Join)
SAMPLE_MULTIVERSE_ASL: Dict[str, Any] = {
    "Version": "1.0",
    "Comment": "E2E Multiverse Agent Flow: Planner -> Parallel Branches (Safe vs HITL) -> Join -> Final",
    "StartAt": "Plan_Intent_LLM",
    "States": {
        "Plan_Intent_LLM": {
            "Type": "Task",
            "Resource": "fiber://wasm_sandbox/interllm_proxy",
            "Parameters": {
                "model": "gpt-4o",
                "prompt": "Analyze user request and dispatch parallel verification strategies.",
                "__fiber_extensions": {
                    "VCR": {"capture_mode": "socket_stream", "coalescing_window_ms": 100},
                    "PTA_Mint": {
                        "pointer_target": "$.raw_stream_payload",
                        "meta_projection": ["$.intent_type"],
                    },
                    "allow_parallel": True,
                },
            },
            # 1:N Fan-out directly into two parallel hypotheses
            "Next": ["Fast_Readonly_MCP", "Admin_Mutation_MCP"],
        },
        "Fast_Readonly_MCP": {
            "Type": "Task",
            "Resource": "fiber://mcp_sidecar/readonly_db_search",
            "Parameters": {"query": "SELECT count(*) FROM audit_logs"},
            "Next": "Evaluate_And_Join",
        },
        "Admin_Mutation_MCP": {
            "Type": "Task",
            "Resource": "fiber://mcp_sidecar/prod_db_admin",
            "Parameters": {"sql": "DELETE FROM stale_sessions WHERE expired = true"},
            "Next": "Evaluate_And_Join",
        },
        "Evaluate_And_Join": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.strategy_override",
                    "StringEquals": "fallback_reflect",
                    "Next": "Reflect_Fallback_LLM",
                }
            ],
            "Default": "Final_Act",
        },
        "Reflect_Fallback_LLM": {
            "Type": "Task",
            "Resource": "fiber://wasm_sandbox/reflect_llm",
            "Parameters": {"model": "gpt-4o-mini", "prompt": "Verify fallback safety."},
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


# DETERMINISTIC MOCK ACTUATOR (Simulates LLM, MCP Gateway & HTTP 202 HITL)
class MockFiberActuator:
    """Simulates Fiber LLM Pipeline & MCP Gateway with Pre-Flight HITL locking and step-by-step logging."""

    def __init__(self) -> None:
        self.hitl_approved_tickets: set[str] = set()

    async def __call__(
        self,
        cursor: BranchCursor,
        node_spec: Dict[str, Any],
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        node_type = node_spec.get(CompactKey.TYPE)
        if node_type in (NodeType.ROUTER.value, NodeType.ACT.value):
            print(
                f"  [Actuator] Control Node '{cursor.curr_node}' ({node_type}) "
                f"on '{cursor.stream_id}' | Tick={cursor.tick} | Cost=0 Fuel (Remaining={cursor.fuel})"
            )
            return {}, None, 0

        # Simulate `Plan_Intent_LLM` or `Reflect_Fallback_LLM`
        if node_type in (NodeType.PROJECTOR.value, NodeType.REFLECT.value):
            cost = 20
            print(
                f"  [Actuator] LLM Node '{cursor.curr_node}' on '{cursor.stream_id}' "
                f"| Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) "
                f"| Offloading response to CAS"
            )
            return (
                {"intent_type": "parallel_audit"},
                {"llm_output": f"Executed {cursor.curr_node} at tick {cursor.tick}"},
                cost,  # 20 Fuel consumed
            )

        # Simulate `Admin_Mutation_MCP` triggering HTTP 202 Pre-Flight HITL trap until approved
        if cursor.curr_node == "Admin_Mutation_MCP":
            ticket_id = "hitl_ticket_drop_99"
            if ticket_id not in self.hitl_approved_tickets:
                print(
                    f"  [Actuator] MCP Pre-Flight HITL Trap (HTTP 202) at '{cursor.curr_node}' "
                    f"on '{cursor.stream_id}'! Regex matched 'DELETE'. "
                    f"Parking cursor WITHOUT spending PTA ({_short_pta(cursor)}) or Fuel ({cursor.fuel})."
                )
                return (
                    {"_hitl_yield": True, "_hitl_ticket": ticket_id},
                    {"status": 202, "reason": "Pre-Flight regex matched DELETE"},
                    0,  # Zero fuel spent while parked
                )
            cost = 15
            print(
                f"  [Actuator] MCP HITL Approved ('{ticket_id}')! Executing '{cursor.curr_node}' "
                f"on '{cursor.stream_id}' | Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) | Score=0.95"
            )
            return (
                {"_hitl_yield": False, "_score": 0.95, "db_status": "cleaned"},
                {"deleted_rows": 42},
                cost,  # 15 Fuel consumed after approval
            )

        # Simulate `Fast_Readonly_MCP`
        cost = 5
        print(
            f"  [Actuator] 🛠️  Safe MCP Node '{cursor.curr_node}' on '{cursor.stream_id}' "
            f"| Tick={cursor.tick} | Spending {cost} Fuel ({cursor.fuel} -> {cursor.fuel - cost}) | Score=0.70"
        )
        return (
            {"_score": 0.70, "db_status": "readonly_ok"},
            {"count": 1024},
            cost,  # 5 Fuel consumed
        )

# E2E SCENARIO RUNNER (Compile -> Run & Park -> Resume -> Time-Travel Fork)
async def run_e2e_verification_suite() -> None:
    print("=" * 86)
    print(" [FIBER FSM/DAG MULTIVERSE TESTER] Starting E2E Lifecycle Verification")
    print("=" * 86)

    actuator = MockFiberActuator()

    async with ExGraphTraverser(node_executor=actuator) as traverser:
        # PHASE 1: ASL Compilation & 2KB Claim-Check Separation
        print("\n" + "-" * 86)
        print(" [Phase 1: Compile] Stripping heavy Task I/O into CAS & projecting <=2KB Topology Blob")
        print("-" * 86)
        bundle = traverser.compiler.compile_from_asl(SAMPLE_MULTIVERSE_ASL, trace_id="test_suite_01")
        join_nodes = traverser.runtime.barrier.topology.join_nodes if traverser.runtime.barrier else "N/A (Mounted at Spawn)"
        print(f"  • Root Graph Hash : {bundle.root_hash} (On-Ledger Compact IR)")
        print(f"  • Root Graph Size : {bundle.total_root_bytes}B / 2048B Limit ({round(bundle.total_root_bytes / 2048 * 100, 1)}% used)")
        print(f"  • Offloaded Blobs : {len(bundle.cas_store)} CAS objects (4 TaskIOSpecs + 1 Root Blob)")
        assert bundle.total_root_bytes <= 2048, "Root blob exceeded 2KB limit!"

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

    print("\n" + "=" * 86)
    print(" ✅ ALL MULTIVERSE FSM/DAG & PTA LEDGER ASSERTIONS PASSED")
    print("=" * 86)


if __name__ == "__main__":
    asyncio.run(run_e2e_verification_suite())