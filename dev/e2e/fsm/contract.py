# fiber.dev.e2e.fsm.contract
"""
FIBER FSM COMPONENT-LEVEL & POLYGLOT BRIDGE E2E VERIFICATION SUITE
======================================================================================

While `fiber.dev.tester.fsm` verifies high-level multiverse workflow lifecycles using
a mock actuator, this suite (`fiber.dev.e2e.fsm.contract`) focuses on granular, unit-by-unit
verification of the underlying FSM contracts, low-level PTA runtime primitives, and
real protocol dispatching across `FiberExecutionBridge` and `PolyglotExecutionBridge`:

  • [Suite 1: Contract & Projection]  URI scheme classification (`provider/model`
                                      preservation for `cerebras`, `gemini`, `llama_server`),
                                      `.$` dynamic parameter resolution, and `Projection`
                                      (`extract_keys` & `offload_path`) data slicing.
  • [Suite 2: Low-Level Runtime PTA]  Direct `BranchController` M:N transitions
                                      (0:1 Genesis, 1:1 Step with CAS `_last_ptr`
                                      verification, asymmetric 1:3 Fork `[0.5, 0.3, 0.2]`,
                                      3:1 Collapse, and O(1) PTA pointer rehydration).
  • [Suite 3: Bridge Protocol Matrix] Direct execution of `PolyglotExecutionBridge`
                                      across mocked `acompletion` (`llm://cerebras/...`,
                                      `reflect://gemini/...`), `mcp://`, `https://`
                                      (in-memory HTTP transport), `fn://` (1/2/3-arg
                                      auto-adaptation), `py://`, and `exec://` (stdio + 202).
  • [Suite 4: Hybrid Polyglot DAG]    End-to-end `GraphTraverser` execution combining
                                      `llm://cerebras/qwen-3.8-27b`, `https://` (202 park),
                                      `exec://`, and `reflect://llama_server/...` in one DAG.
"""
import asyncio
import json
import sys
from typing import Any, AsyncIterator, Dict, List
from unittest.mock import patch

import httpx

from fiber.phase.fsm.bridge.polyglot import PolyglotExecutionBridge
from fiber.phase.fsm.compiler import FSMCompiler
from fiber.phase.fsm.contract import (
    CAS_PREFIX,
    END_TOKEN,
    NodeType,
    apply_projection,
    extract_json_path,
    parse_resource_uri,
    resolve_dynamic_params,
)
from fiber.phase.fsm.runtime import BranchController, BranchCursor
from fiber.phase.fsm.traverser import GraphTraverser

from xphi.arch.bound.adapter.pta import PtaPointer
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fsm.e2e", phase="UNIT_AND_BRIDGE_VERIFICATION")

# Canonical model identifiers aligned with `fiber-compats/adapters/*`
MODEL_CEREBRAS = "cerebras/qwen-3.8-27b"
MODEL_GEMINI_LITE = "gemini/gemini-3.1-flash-lite"
MODEL_LLAMA_SERVER = "llama_server/gemma-3-1b-it-Q4_K_M.gguf"


def _short_pta(cursor: BranchCursor) -> str:
    key = cursor.pointer.to_key()
    if ":" in key:
        tx_h, idx = key.split(":", 1)
        return f"{tx_h[:8]}..:{idx}"
    return key[:10]


async def _mock_acompletion(model: str, messages: List[Dict[str, str]], stream: bool = False, **kwargs: Any) -> Any:
    """
    Deterministic in-memory mock for `fiber.llm.entry.acompletion`.
    Simulates `cerebras/qwen-3.8-27b`, `gemini/gemini-3.1-flash-lite`, and
    local `llama_server/gemma-3-1b-it-Q4_K_M.gguf` without external network I/O.
    """
    assert isinstance(messages, list) and len(messages) >= 1, "Expected valid OpenAI messages list"
    user_prompt = messages[-1].get("content", "")

    if stream:
        async def _chunk_gen() -> AsyncIterator[Dict[str, Any]]:
            fragments = ['{"verified_by": "', model, '", "_score": 0.98}']
            for frag in fragments:
                yield {"choices": [{"delta": {"content": frag}}]}
        return _chunk_gen()

    if MODEL_CEREBRAS in model:
        reply_json = json.dumps({
            "planner_model": MODEL_CEREBRAS,
            "computed_amount": 50000,
            "intent_type": "wire_audit",
            "_score": 0.94,
        })
        return {
            "id": "chatcmpl-cerebras-mock",
            "model": model,
            "choices": [{"message": {"role": "assistant", "content": reply_json}}],
            "usage": {"total_tokens": 10},
        }

    if MODEL_LLAMA_SERVER in model:
        reply_json = json.dumps({
            "local_gguf_audit": "passed",
            "prompt_echo": user_prompt[:32],
            "_score": 0.99,
        })
        return {
            "id": "chatcmpl-llamacpp-mock",
            "model": model,
            "choices": [{"message": {"role": "assistant", "content": reply_json}}],
            "usage": {"total_tokens": 8},
        }

    # Default (`gemini/gemini-3.1-flash-lite` unary fallback)
    reply_json = json.dumps({"model_used": model, "_score": 0.91})
    return {
        "id": "chatcmpl-gemini-mock",
        "model": model,
        "choices": [{"message": {"role": "assistant", "content": reply_json}}],
        "usage": {"total_tokens": 12},
    }


# ======================================================================================
# SUITE 1: Contract, URI Routing Matrix & Projection Unit Verification
# ======================================================================================
def verify_suite_1_contracts() -> None:
    print("\n" + "-" * 86)
    print(" [Suite 1: Contract & Projection] URI Classification, `.$` Binding & CAS Slicing")
    print("-" * 86)

    # 1. URI Scheme & NodeType Classification Matrix (Aligned with `fiber-compats/adapters`)
    uri_cases = [
        (
            f"llm://{MODEL_CEREBRAS}",
            NodeType.PROJECTOR,
            "llm",
            MODEL_CEREBRAS,
            MODEL_CEREBRAS,
            False,
            False,
        ),
        (
            f"reflect://{MODEL_GEMINI_LITE}",
            NodeType.REFLECT,
            "reflect",
            MODEL_GEMINI_LITE,
            MODEL_GEMINI_LITE,
            False,
            False,
        ),
        (
            f"tension://{MODEL_LLAMA_SERVER}",
            NodeType.TENSION,
            "tension",
            MODEL_LLAMA_SERVER,
            MODEL_LLAMA_SERVER,
            False,
            False,
        ),
        ("mcp://prod_db_admin", NodeType.OPERSIG, "mcp", "prod_db_admin", None, True, False),
        ("fiber://mcp_sidecar/audit_worker", NodeType.OPERSIG, "fiber", "audit_worker", None, True, False),
        ("fn://calculate_risk", NodeType.OPERSIG, "fn", "calculate_risk", None, False, True),
        ("py://math:factorial", NodeType.OPERSIG, "py", "math:factorial", None, False, True),
        ("https://api.internal.corp/v1/charge", NodeType.OPERSIG, "https", "charge", None, False, True),
        ("exec://python3 -c 'print(1)'", NodeType.OPERSIG, "exec", "python3 -c 'print(1)'", None, False, True),
        ("act://noop_gate", NodeType.ACT, "act", "noop_gate", None, False, False),
    ]

    for raw, exp_type, exp_scheme, exp_tid, exp_model, exp_mcp, exp_poly in uri_cases:
        p = parse_resource_uri(raw)
        assert p.node_type == exp_type, f"Type mismatch for {raw}: {p.node_type} != {exp_type}"
        assert p.scheme == exp_scheme, f"Scheme mismatch for {raw}: {p.scheme} != {exp_scheme}"
        assert p.target_id == exp_tid, f"Target ID mismatch for {raw}: {p.target_id} != {exp_tid}"
        assert p.model_hint == exp_model, f"Model hint mismatch for {raw}: {p.model_hint} != {exp_model}"
        assert p.is_native_mcp is exp_mcp, f"is_native_mcp mismatch for {raw}"
        assert p.is_polyglot_opersig is exp_poly, f"is_polyglot_opersig mismatch for {raw}"

    print(
        f"  • URI Matrix       : Verified {len(uri_cases)} schemes "
        f"({MODEL_CEREBRAS}, {MODEL_GEMINI_LITE}, {MODEL_LLAMA_SERVER}, MCP, fn://, py://, https://, exec://)"
    )

    # 2. Dynamic Parameter (`".$"`) Resolution against nested `meta_ctx`
    meta_ctx = {
        "tenant": {"id": "acme_finops", "tier": "enterprise"},
        "batch_items": [{"sku": "A-100"}, {"sku": "B-200"}],
        "flat_score": 0.88,
    }
    raw_params = {
        "static_mode": "strict",
        "org_id.$": "$.tenant.id",
        "second_sku.$": "$.batch_items.1.sku",
        "threshold.$": "flat_score",
        "missing_fallback.$": "$.non_existent_key",
    }
    resolved = resolve_dynamic_params(raw_params, meta_ctx)
    assert resolved["static_mode"] == "strict"
    assert resolved["org_id"] == "acme_finops"
    assert resolved["second_sku"] == "B-200"
    assert resolved["threshold"] == 0.88
    assert resolved["missing_fallback"] == "$.non_existent_key"
    print(f"  • Dynamic `.$`     : Resolved nested dict/list JSONPaths -> {resolved}")

    # 3. Projection (`extract_keys` lift + `offload_path` CAS slicing)
    heavy_actuator_output = {
        "metrics": {"_score": 0.97, "risk_level": "low"},
        "raw_blob": {"records": [{"id": 1, "blob": "X" * 512}], "cursor": "next_page_token"},
        "ignored_noise": "should_not_appear_in_offload_when_offload_path_is_set",
    }
    projection_spec = {
        "extract_keys": ["$.metrics._score", "$.metrics.risk_level"],
        "offload_path": "$.raw_blob",
    }
    meta_updates, offloaded = apply_projection(
        raw_output=heavy_actuator_output,
        projection=projection_spec,
        base_meta={"_hitl_yield": False},
    )
    assert meta_updates["_score"] == 0.97 and meta_updates["$.metrics._score"] == 0.97
    assert meta_updates["risk_level"] == "low"
    assert "records" in offloaded and "ignored_noise" not in offloaded
    print(f"  • Projection Slice : Lifted meta={meta_updates} | Offloaded keys={list(offloaded.keys())}")


# ======================================================================================
# SUITE 2: Low-Level `BranchController` Atomic M:N PTA Transitions & CAS Dereferencing
# ======================================================================================
async def verify_suite_2_runtime_primitives() -> None:
    print("\n" + "-" * 86)
    print(" [Suite 2: Low-Level Runtime PTA] Direct 0:1 Spawn, 1:1 Step, Asymmetric 1:3 Fork & 3:1 Merge")
    print("-" * 86)

    minimal_asl = {
        "Version": "1.0",
        "StartAt": "Entry_Node",
        "States": {
            "Entry_Node": {
                "Type": "Task",
                "Resource": f"llm://{MODEL_CEREBRAS}",
                "Parameters": {"prompt": "Dispatch 3 parallel workers", "seed": 42},
                "Next": ["Worker_A", "Worker_B", "Worker_C"],
            },
            "Worker_A": {"Type": "Task", "Resource": "fn://w_a", "Next": "Barrier_Join", "Catch": "Barrier_Join"},
            "Worker_B": {"Type": "Task", "Resource": "fn://w_b", "Next": "Barrier_Join", "Catch": "Barrier_Join"},
            "Worker_C": {"Type": "Task", "Resource": "fn://w_c", "Next": "Barrier_Join", "Catch": "Barrier_Join"},
            "Barrier_Join": {"Type": "Succeed"},
        },
    }

    compiler = FSMCompiler()
    bundle = compiler.compile_from_asl(minimal_asl, trace_id="unit_runtime_02")
    controller = BranchController()

    # 1. 0:1 Genesis Spawn & verify compiler auto-injected `model_hint` (`cerebras/qwen-3.8-27b`)
    c0 = await controller.mount_bundle_and_spawn(
        bundle=bundle,
        base_stream_id="unit_pta",
        initial_fuel=1000,
        initial_meta={"env": "staging"},
    )
    node_spec = controller.get_node_spec(c0)
    task_io = controller.resolve_task_io(node_spec)
    assert c0.curr_node == "Entry_Node" and c0.fuel == 1000
    assert task_io["call_params"]["seed"] == 42
    assert task_io["call_params"]["model"] == MODEL_CEREBRAS
    print(
        f"  • [0:1 GENESIS]    : Spawned '{c0.stream_id}' at '{c0.curr_node}' "
        f"(model='{task_io['call_params']['model']}') | PTA={_short_pta(c0)} | Fuel={c0.fuel}"
    )

    # 2. Asymmetric 1:3 Fork (`fuel_ratios=[0.5, 0.3, 0.2]`) with CAS Offload Inscription
    children = await controller.fork(
        parent=c0,
        branch_targets=[
            ("Worker_A", {"branch_tag": "alpha"}),
            ("Worker_B", {"branch_tag": "beta"}),
            ("Worker_C", {"branch_tag": "gamma"}),
        ],
        fuel_ratios=[0.5, 0.3, 0.2],
        step_cost=100,
        offload_data={"heavy_parent_artifact": [1, 2, 3, 4]},
    )
    fuels = [c.fuel for c in children]
    assert fuels == [450, 270, 180], f"Expected asymmetric slice [450, 270, 180], got {fuels}"
    last_ptr = children[0].meta_ctx.get("_last_ptr", "")
    assert last_ptr.startswith(CAS_PREFIX), "Expected parent offload_data CAS pointer in `_last_ptr`"
    restored_blob = controller._load_blob(last_ptr)
    assert restored_blob["heavy_parent_artifact"] == [1, 2, 3, 4]
    print(
        f"  • [1:3 FORK]       : Asymmetric slice 900 Fuel -> {fuels} | "
        f"Verified CAS `_last_ptr`={last_ptr} ({restored_blob})"
    )

    # 3. 1:1 Step on each branch & O(1) Cursor Rehydration check
    stepped_children: List[BranchCursor] = []
    for idx, child in enumerate(children):
        spend = (idx + 1) * 10  # b0=-10 (440), b1=-20 (250), b2=-30 (150)
        sc = await controller.step(
            cursor=child,
            step_cost=spend,
            meta_updates={"_score": 0.5 + idx * 0.2},
        )
        rehydrated = controller.restore_cursor(sc.pointer.to_key())
        assert rehydrated.fuel == sc.fuel and rehydrated.curr_node == "Barrier_Join"
        stepped_children.append(rehydrated)

    print(
        f"  • [1:1 STEP x3]    : Transitioned all 3 branches to 'Barrier_Join' | "
        f"Remaining Fuels={[c.fuel for c in stepped_children]} (Rehydration Verified)"
    )

    # 4. 3:1 Collapse & Merge (Reclaim 440 + 250 + 150 = 840 Fuel)
    merged = await controller.collapse_and_merge(
        cursors=stepped_children,
        winner_idx=2,  # Select b2 (gamma, _score=0.9)
        merged_stream_id="unit_pta",
    )
    assert merged.fuel == 840, f"Expected 840 reclaimed fuel, got {merged.fuel}"
    assert merged.meta_ctx["branch_tag"] == "gamma"
    print(
        f"  • [3:1 MERGE]      : Collapsed 3 PTAs -> Winner='{merged.meta_ctx['branch_tag']}' | "
        f"Reclaimed Fuel={merged.fuel} | Final PTA={_short_pta(merged)}"
    )


# ======================================================================================
# SUITE 3: `PolyglotExecutionBridge` Protocol Dispatch Matrix (Mocked LLM + HTTP + CLI)
# ======================================================================================
async def verify_suite_3_polyglot_bridge() -> None:
    print("\n" + "-" * 86)
    print(" [Suite 3: Bridge Protocol Matrix] Direct Dispatch for llm://, reflect://, fn://, py://, https://, mcp://, exec://")
    print("-" * 86)

    controller = BranchController()
    approved_http_tickets: set[str] = set()

    async def _mock_http_router(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        body = json.loads(request.content.decode("utf-8")) if request.content else {}

        if "/v1/mcp-gateway/" in url_str:
            assert "checkpoint_handle" in body, "MCP payload missing checkpoint_handle"
            return httpx.Response(
                200,
                json={"mcp_ok": True, "echo_args": body["arguments"], "_score": 0.85, "step_cost": 7},
            )

        if "api.corp.internal/v1/transfer" in url_str:
            ckpt = request.headers.get("X-Fiber-Checkpoint", "")
            tenant = request.headers.get("X-Fiber-Tenant", "")
            assert ckpt and tenant == "finops_unit", "Missing X-Fiber-Checkpoint or X-Fiber-Tenant header!"

            if body.get("amount", 0) > 10000 and "tkt_http_202" not in approved_http_tickets:
                return httpx.Response(
                    202,
                    headers={"X-Fiber-HITL-Ticket": "tkt_http_202"},
                    json={"reason": "Transfer > $10,000 requires CFO approval"},
                )
            return httpx.Response(
                200,
                json={"tx_status": "settled", "settled_amount": body.get("amount"), "_score": 0.96, "step_cost": 12},
            )

        return httpx.Response(404, json={"error": "not_found"})

    bridge = PolyglotExecutionBridge(
        runtime=controller,
        tenant_id="finops_unit",
        allow_py_import=True,
        allow_exec_subprocess=True,
    )
    await bridge._http_client.aclose()
    bridge._http_client = httpx.AsyncClient(transport=httpx.MockTransport(_mock_http_router))

    bridge.register_fn("fn_1arg", lambda args: {"mode": "1arg", "val": args["x"] * 2, "_score": 0.75})
    bridge.register_fn("fn_kwargs", lambda a=1, b=2: {"mode": "kwargs", "sum": a + b, "_score": 0.80})

    async def _fn_3arg(args: Dict[str, Any], meta: Dict[str, Any], ckpt: str) -> Dict[str, Any]:
        return {"mode": "3arg_async", "ckpt_seen": bool(ckpt), "user": meta.get("user"), "_score": 0.92}

    bridge.register_fn("fn_3arg", _fn_3arg)
    dummy_cursor = BranchCursor(
        stream_id="bridge_test",
        pointer=PtaPointer("deadbeef12345678", 0),
        active_blob_hash="branch:0000",
        curr_node="Test_Node",
        fuel=500,
        owner_pubkey="pub_test",
        meta_ctx={"user": "alice", "dynamic_amt": 50000},
        tick=1,
    )
    opersig_spec = {"t": NodeType.OPERSIG.value}
    projector_spec = {"t": NodeType.PROJECTOR.value}
    reflect_spec = {"t": NodeType.REFLECT.value}

    # 0. Test `llm://` (Cerebras unary JSON auto-unpack) & `reflect://` (Gemini streaming chunk assembly)
    with patch("fiber.phase.fsm.bridge.execution.acompletion", side_effect=_mock_acompletion):
        m_llm, out_llm, cost_llm = await bridge(
            dummy_cursor,
            projector_spec,
            {
                "resource_uri": f"llm://{MODEL_CEREBRAS}",
                "call_params": {"prompt": "Plan wire audit", "system": "You are a FinOps planner."},
                "projection": {"extract_keys": ["$.intent_type", "$._score"], "offload_path": "$.raw_stream_payload"},
            },
        )
        assert m_llm["intent_type"] == "wire_audit" and m_llm["_score"] == 0.94 and cost_llm == 10
        assert MODEL_CEREBRAS in str(out_llm.get("data", ""))

        m_ref, out_ref, cost_ref = await bridge(
            dummy_cursor,
            reflect_spec,
            {
                "resource_uri": f"reflect://{MODEL_GEMINI_LITE}",
                "call_params": {"prompt": "Verify stream chunks", "stream": True},
                "projection": {"extract_keys": ["$.verified_by", "$._score"]},
            },
        )
        assert m_ref["verified_by"] == MODEL_GEMINI_LITE and m_ref["_score"] == 0.98 and cost_ref == 3
        print(
            f"  • [llm/reflect://] : Verified `{MODEL_CEREBRAS}` (JSON unpack) & "
            f"`{MODEL_GEMINI_LITE}` (stream assembly)"
        )

    # 1. Test `fn://` flexible signature matching (1-arg, **kwargs, 3-arg async)
    m1, out1, c1 = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": "fn://fn_1arg", "call_params": {"x": 21}, "projection": {"extract_keys": ["$.val"]}},
    )
    assert m1["val"] == 42 and out1["mode"] == "1arg" and c1 == 5

    m2, out2, _ = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": "fn://fn_kwargs", "call_params": {"a": 10, "b": 25}, "projection": {"extract_keys": ["$.sum"]}},
    )
    assert m2["sum"] == 35

    m3, out3, _ = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": "fn://fn_3arg", "call_params": {}, "projection": {"extract_keys": ["$.ckpt_seen"]}},
    )
    assert m3["ckpt_seen"] is True and out3["user"] == "alice"
    print("  • [fn://]          : Verified 1-arg sync, **kwargs sync, and 3-arg async callable signatures")

    # 2. Test `py://` zero-registration dynamic module import
    _, py_out, _ = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": "py://os.path:basename", "call_params": {"p": "/var/log/fiber.log"}},
    )
    assert py_out["data"] == "fiber.log"
    print(f"  • [py://]          : Verified dynamic module import (`py://os.path:basename` -> '{py_out['data']}')")

    # 3. Test `mcp://` passthrough to parent `FiberExecutionBridge`
    m_mcp, out_mcp, cost_mcp = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": "mcp://audit_worker", "call_params": {"table": "ledger"}, "projection": {"extract_keys": ["$._score"]}},
    )
    assert out_mcp["mcp_ok"] is True and m_mcp["_score"] == 0.85 and cost_mcp == 7
    print(f"  • [mcp://]         : Verified canonical MCP Gateway delegation (Cost={cost_mcp}, Score={m_mcp['_score']})")

    # 4. Test `https://` Direct REST API (HTTP 202 HITL Park -> Approval -> HTTP 200 Settlement)
    http_task_io = {
        "resource_uri": "https://api.corp.internal/v1/transfer",
        "call_params": {"amount.$": "$.dynamic_amt"},
        "projection": {"extract_keys": ["$.tx_status", "$._score"]},
    }
    m_202, _, cost_202 = await bridge(dummy_cursor, opersig_spec, http_task_io)
    assert m_202["_hitl_yield"] is True and m_202["_hitl_ticket"] == "tkt_http_202" and cost_202 == 0
    print(f"  • [https:// (202)] : Verified Direct REST Pre-Flight HITL Park (Ticket='{m_202['_hitl_ticket']}', Cost=0)")

    approved_http_tickets.add("tkt_http_202")
    m_200, out_200, cost_200 = await bridge(dummy_cursor, opersig_spec, http_task_io)
    assert m_200["_hitl_yield"] is False and m_200["tx_status"] == "settled" and cost_200 == 12
    print(f"  • [https:// (200)] : Verified Direct REST Settlement after approval (Amount={out_200['settled_amount']}, Cost={cost_200})")

    # 5. Test `exec://` Subprocess JSON stdin/stdout pipe & Exit Code 202 HITL Yield
    py_bin = sys.executable
    exec_ok_uri = (
        f'exec://{py_bin} -c "import sys, json; '
        f'req = json.loads(sys.stdin.read()); '
        f'print(json.dumps({{\'cli_echo\': req[\'arguments\'][\'msg\'], \'_score\': 0.91, \'step_cost\': 9}}))"'
    )
    m_exec, out_exec, cost_exec = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": exec_ok_uri, "call_params": {"msg": "hello_stdio"}, "projection": {"extract_keys": ["$.cli_echo"]}},
    )
    assert m_exec["cli_echo"] == "hello_stdio" and cost_exec == 9
    print(f"  • [exec:// (0)]    : Verified async stdio subprocess pipe (Output='{m_exec['cli_echo']}', Cost={cost_exec})")

    exec_202_uri = (
        f'exec://{py_bin} -c "import sys, json; '
        f'print(json.dumps({{\'ticket_id\': \'cli_tkt_77\', \'reason\': \'sudo required\'}})); '
        f'sys.exit(202)"'
    )
    m_exec_202, _, cost_exec_202 = await bridge(
        dummy_cursor,
        opersig_spec,
        {"resource_uri": exec_202_uri, "call_params": {}},
    )
    assert m_exec_202["_hitl_yield"] is True and m_exec_202["_hitl_ticket"] == "cli_tkt_77" and cost_exec_202 == 0
    print(f"  • [exec:// (202)]  : Verified CLI Exit Code 202 zero-spend HITL parking (Ticket='{m_exec_202['_hitl_ticket']}')")

    await bridge.aclose()


# ======================================================================================
# SUITE 4: End-to-End Heterogeneous Polyglot DAG (`llm://cerebras` -> `https://` || `exec://` -> `reflect://llama_server`)
# ======================================================================================
async def verify_suite_4_heterogeneous_dag() -> None:
    print("\n" + "-" * 86)
    print(
        f" [Suite 4: Hybrid Polyglot DAG] llm://{MODEL_CEREBRAS} -> "
        f"(https:// [202 Park] || exec://) -> reflect://{MODEL_LLAMA_SERVER}"
    )
    print("-" * 86)

    py_bin = sys.executable
    polyglot_asl: Dict[str, Any] = {
        "Version": "1.0",
        "StartAt": "Plan_Wire_Cerebras",
        "States": {
            "Plan_Wire_Cerebras": {
                "Type": "Task",
                "Resource": f"llm://{MODEL_CEREBRAS}",
                "Parameters": {
                    "prompt": "Calculate wire transfer amount and dispatch parallel verification.",
                    "__fiber_extensions": {
                        "Projection": {"extract_keys": ["$.computed_amount", "$.intent_type"]},
                    },
                },
                "Next": ["Legacy_REST_Branch", "Local_Binary_Branch"],
            },
            "Legacy_REST_Branch": {
                "Type": "Task",
                "Resource": "https://api.erp.internal/v1/commit",
                "Parameters": {
                    "wire_amount.$": "$.computed_amount",
                    "__fiber_extensions": {"Projection": {"extract_keys": ["$.rest_status", "$._score"]}},
                },
                "Next": "Converge_Choice",
                "Catch": "Converge_Choice",
            },
            "Local_Binary_Branch": {
                "Type": "Task",
                "Resource": (
                    f'exec://{py_bin} -c "import sys, json; '
                    f'd = json.loads(sys.stdin.read()); '
                    f'print(json.dumps({{\'bin_verified\': True, \'_score\': 0.82, \'step_cost\': 8}}))"'
                ),
                "Parameters": {"check_amount.$": "$.computed_amount"},
                "Next": "Converge_Choice",
                "Catch": "Converge_Choice",
            },
            "Converge_Choice": {
                "Type": "Choice",
                "Choices": [{"if": "$._score >= 0.95", "Next": "Local_Reflect_LlamaServer"}],
                "Default": "Success_End",
            },
            "Local_Reflect_LlamaServer": {
                "Type": "Task",
                "Resource": f"reflect://{MODEL_LLAMA_SERVER}",
                "Parameters": {
                    "prompt": "Audit final settled transaction state locally.",
                    "__fiber_extensions": {"Projection": {"extract_keys": ["$.local_gguf_audit"]}},
                },
                "Next": "Success_End",
            },
            "Success_End": {"Type": "Succeed"},
        },
    }

    rest_approved: set[str] = set()

    async def _dag_http_handler(request: httpx.Request) -> httpx.Response:
        if "tkt_erp_900" not in rest_approved:
            return httpx.Response(202, json={"ticket_id": "tkt_erp_900", "msg": "ERP wire lock"})
        return httpx.Response(200, json={"rest_status": "committed", "_score": 0.99, "step_cost": 15})

    with patch("fiber.phase.fsm.bridge.execution.acompletion", side_effect=_mock_acompletion):
        async with GraphTraverser() as traverser:
            bridge = PolyglotExecutionBridge(
                runtime=traverser.runtime,
                tenant_id="hybrid_dag_tenant",
                allow_exec_subprocess=True,
            )
            await bridge._http_client.aclose()
            bridge._http_client = httpx.AsyncClient(transport=httpx.MockTransport(_dag_http_handler))
            traverser.node_executor = bridge

            # Run 1: `llm://cerebras/qwen-3.8-27b` (-10 -> 990) -> Fork [495, 495] ->
            # `https://` parks on HTTP 202 (495 Fuel), `exec://` completes (-8 -> 487 Fuel) and waits at barrier
            res1 = await traverser.run(
                asl_schema=polyglot_asl,
                base_stream_id="poly_dag_01",
                initial_fuel=1000,
            )
            by_stream = {c.stream_id: c for c in res1.parked_cursors}
            assert len(by_stream) == 2
            assert by_stream["poly_dag_01/b0"].curr_node == "Legacy_REST_Branch" and by_stream["poly_dag_01/b0"].fuel == 495
            assert by_stream["poly_dag_01/b1"].curr_node == "Converge_Choice" and by_stream["poly_dag_01/b1"].fuel == 487
            print(
                f"  • [Phase A: Parked]: llm://cerebras(-10) -> Fork[495, 495] | "
                f"b0(https://) Parked at 495 Fuel | b1(exec://) Waiting at Barrier with 487 Fuel"
            )

            # Run 2: Approve REST ticket & resume -> Merge (480 + 487 = 967) ->
            # `reflect://llama_server/gemma-3-1b-it-Q4_K_M.gguf` (-8 -> 959 Fuel) -> END
            rest_approved.add("tkt_erp_900")
            res2 = await traverser.resume(
                targets=[c.pointer.to_key() for c in res1.parked_cursors],
                base_stream_id="poly_dag_01",
            )
            assert res2.final_cursor.curr_node == END_TOKEN
            assert res2.reclaimed_fuel == 959, f"Expected 959 reclaimed fuel (967 - 8), got {res2.reclaimed_fuel}"
            assert res2.final_cursor.meta_ctx.get("rest_status") == "committed"
            assert res2.final_cursor.meta_ctx.get("local_gguf_audit") == "passed"
            print(
                f"  • [Phase B: Merged]: Approved HTTP 202 -> Merge(967) -> "
                f"reflect://{MODEL_LLAMA_SERVER}(-8 -> {res2.reclaimed_fuel} Fuel) | "
                f"GGUF Audit='{res2.final_cursor.meta_ctx.get('local_gguf_audit')}' | Final Node={res2.final_cursor.curr_node}"
            )

            await bridge.aclose()


async def run_all_unit_and_bridge_suites() -> None:
    print("=" * 86)
    print(" [FIBER FSM UNIT & POLYGLOT BRIDGE E2E SUITE] Starting Granular Verification")
    print("=" * 86)

    verify_suite_1_contracts()
    await verify_suite_2_runtime_primitives()
    await verify_suite_3_polyglot_bridge()
    await verify_suite_4_heterogeneous_dag()

    print("\n" + "=" * 86)
    print(" ✅ ALL CONTRACT, RUNTIME PTA, AND POLYGLOT BRIDGE E2E ASSERTIONS PASSED")
    print("=" * 86)


if __name__ == "__main__":
    asyncio.run(run_all_unit_and_bridge_suites())