# fiber.phase.fsm.tester
## @lineage: fiber.dev.ex.fsm.tester
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


# ============================================================================
# 1. SAMPLE MULTIVERSE ASL WORKFLOW (Fan-Out + HITL Trap + Diamond Join)
# ============================================================================
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


# ============================================================================
# 2. DETERMINISTIC MOCK ACTUATOR (Simulates LLM, MCP Gateway & HTTP 202 HITL)
# ============================================================================
class MockFiberActuator:
    """Simulates Fiber LLM Pipeline & MCP Gateway with Pre-Flight HITL locking."""

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
            return {}, None, 0

        # Simulate `Plan_Intent_LLM` or `Reflect_Fallback_LLM`
        if node_type in (NodeType.PROJECTOR.value, NodeType.REFLECT.value):
            return (
                {"intent_type": "parallel_audit"},
                {"llm_output": f"Executed {cursor.curr_node} at tick {cursor.tick}"},
                20,  # 20 Fuel consumed
            )

        # Simulate `Admin_Mutation_MCP` triggering HTTP 202 Pre-Flight HITL trap until approved
        if cursor.curr_node == "Admin_Mutation_MCP":
            ticket_id = "hitl_ticket_drop_99"
            if ticket_id not in self.hitl_approved_tickets:
                return (
                    {"_hitl_yield": True, "_hitl_ticket": ticket_id},
                    {"status": 202, "reason": "Pre-Flight regex matched DELETE"},
                    0,  # Zero fuel spent while parked
                )
            return (
                {"_hitl_yield": False, "_score": 0.95, "db_status": "cleaned"},
                {"deleted_rows": 42},
                15,  # 15 Fuel consumed after approval
            )

        # Simulate `Fast_Readonly_MCP`
        return (
            {"_score": 0.70, "db_status": "readonly_ok"},
            {"count": 1024},
            5,  # 5 Fuel consumed
        )


# ============================================================================
# 3. E2E SCENARIO RUNNER (Compile -> Run & Park -> Resume -> Time-Travel Fork)
# ============================================================================
async def run_e2e_verification_suite() -> None:
    print("=" * 78)
    print(" [FIBER FSM/DAG MULTIVERSE TESTER] Starting E2E Lifecycle Verification")
    print("=" * 78)

    actuator = MockFiberActuator()

    async with ExGraphTraverser(node_executor=actuator) as traverser:
        # --------------------------------------------------------------------
        # Phase 1: Compile ASL & Verify 2KB Compact Budget
        # --------------------------------------------------------------------
        bundle = traverser.compiler.compile_from_asl(SAMPLE_MULTIVERSE_ASL, trace_id="test_suite_01")
        print(f"\n[Phase 1: Compile] Root Hash : {bundle.root_hash}")
        print(f"[Phase 1: Compile] Root Size : {bundle.total_root_bytes}B / 2048B Limit")
        print(f"[Phase 1: Compile] CAS Blobs : {len(bundle.cas_store)} offloaded objects")
        assert bundle.total_root_bytes <= 2048, "Root blob exceeded 2KB limit!"

        # --------------------------------------------------------------------
        # Phase 2: Initial Run -> 1:N Fork -> b0 waits at Join, b1 Parks on HITL (HTTP 202)
        # --------------------------------------------------------------------
        res1 = await traverser.traverse_bundle(
            bundle=bundle,
            base_stream_id="run_101",
            initial_fuel=1000,
        )
        print(f"\n[Phase 2: Initial Run & HITL Park]")
        print(f"  • Visited Trace  : {' -> '.join(res1.visited_history)}")
        print(f"  • Parked Cursors : {[c.stream_id for c in res1.parked_cursors]}")

        assert len(res1.parked_cursors) == 2, "Expected both HITL-parked b1 and barrier-waiting sibling b0"
        by_stream = {c.stream_id: c for c in res1.parked_cursors}
        waiting_b0 = by_stream["run_101/b0"]
        parked_b1 = by_stream["run_101/b1"]

        print(f"  • Branch b0 State: Node='{waiting_b0.curr_node}' (Barrier Waiting) | PTA={waiting_b0.pointer.to_key()} | Fuel={waiting_b0.fuel}")
        print(f"  • Branch b1 State: Node='{parked_b1.curr_node}' (HITL Parked)     | PTA={parked_b1.pointer.to_key()} | Fuel={parked_b1.fuel}")

        # Verify fuel accounting: 1000 - 20 (Plan_LLM) = 980 -> Sliced [490, 490]. b0 spent 5 -> 485, b1 spent 0 -> 490
        assert waiting_b0.fuel == 485 and parked_b1.fuel == 490

        # --------------------------------------------------------------------
        # Phase 3: Rehydrate from PTA Pointers, Approve HITL, and Resume Mid-Graph
        # --------------------------------------------------------------------
        restored_b1 = traverser.runtime.restore_cursor(parked_b1.pointer.to_key())
        print(f"\n[Phase 3: HITL Approval & Mid-Flow Resume]")
        print(f"  • Restored Cursor: stream='{restored_b1.stream_id}' at node='{restored_b1.curr_node}' (PTA={restored_b1.pointer.to_key()})")

        # Simulate Admin TOTP / 1-Click Approval releasing ticket 'hitl_ticket_drop_99'
        actuator.hitl_approved_tickets.add("hitl_ticket_drop_99")

        # Resume directly using PTA pointer strings (automatically rehydrates & merges at Join Barrier)
        res2 = await traverser.resume(
            targets=[waiting_b0.pointer.to_key(), parked_b1.pointer.to_key()],
            base_stream_id="run_101",
        )
        print(f"  • Resumed Trace  : {' -> '.join(res2.visited_history)}")
        print(f"  • Final Node     : {res2.final_cursor.curr_node} | Join Collapses: {res2.join_collapses}")
        print(f"  • Reclaimed Fuel : {res2.reclaimed_fuel} (485 from b0 + 475 from b1 = 960)")

        assert res2.final_cursor.curr_node == END_TOKEN
        assert res2.reclaimed_fuel == 960, f"Expected 960 reclaimed fuel, got {res2.reclaimed_fuel}"

        # --------------------------------------------------------------------
        # Phase 4: Time-Travel Fork from Checkpoint (Override Meta & Route to Fallback Branch)
        # --------------------------------------------------------------------
        print(f"\n[Phase 4: Time-Travel Re-Branching from Checkpoint]")
        exp_cursors = await traverser.runtime.fork(
            parent=res2.final_cursor,
            branch_targets=[
                ("Reflect_Fallback_LLM", {"strategy_override": "fallback_reflect", "_score": 0.99}),
                ("Final_Act", {"strategy_override": "direct_exit", "_score": 0.50}),
            ],
            fuel_ratios=[0.5, 0.5],
        )
        print(f"  • Forked 2 Experimental Branches from PTA {res2.final_cursor.pointer.to_key()}:")
        for ec in exp_cursors:
            print(f"    - {ec.stream_id}: Node='{ec.curr_node}' | Fuel={ec.fuel} | Override='{ec.meta_ctx.get('strategy_override')}'")

        # Resume both experimental branches; traverser automatically collapses completed terminals at the end
        res3 = await traverser.resume(
            targets=exp_cursors,
            base_stream_id="run_101_exp",
        )
        print(f"  • Time-Travel Trace: {' -> '.join(res3.visited_history)}")
        print(f"  • Final Exp State  : Stream='{res3.final_cursor.stream_id}' | Node='{res3.final_cursor.curr_node}' | Reclaimed Fuel={res3.reclaimed_fuel}")

        # 960 split into [480, 480]. b0 runs Reflect_Fallback_LLM (-20 -> 460), b1 runs Final_Act (0 -> 480) -> Merged = 940
        assert res3.final_cursor.curr_node == END_TOKEN
        assert res3.reclaimed_fuel == 940, f"Expected 940 reclaimed fuel, got {res3.reclaimed_fuel}"

    print("\n" + "=" * 78)
    print(" ✅ ALL MULTIVERSE FSM/DAG & PTA LEDGER ASSERTIONS PASSED")
    print("=" * 78)


if __name__ == "__main__":
    asyncio.run(run_e2e_verification_suite())