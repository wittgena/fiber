# fiber.dev.e2e.inter.wasm.fsm
import sys
import json
import argparse
import importlib
from dataclasses import dataclass, field
from typing import Any, List, Dict, Optional

from fiber.dev.base.scene.runner import SandboxRunner

from xphi.arch.bound.adapter.fsm import FsmAdapter, FsmTarget, FsmOp
from xphi.arch.bound.wasm.builder import WasmBuilder
from xphi.arch.bound.wasm.tester import WasmTester
from xphi.node.space.bind.resolver import resolve_path
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("inter.wasm.fsm")

MODULE_PATH = __name__

# 1. [Test Fixtures & Topologies for FSM WASM Verification]
class FsmTestTopologies:
    """fsm.wasm 커널의 정적 검증기(Linter), 2KB 분할기(Linker), 멀티버스 런타임 검증용 픽스처"""

    # 1. 정상 다이아몬드 분기 그래프 (Catch 폴백 완비)
    VALID_DIAMOND_IR = {
        "_m": {"ep": "Triage_LLM", "tr": "turn_01", "v": "1.0"},
        "g": {
            "Triage_LLM": {"t": "projector", "nx": ["Branch_B0", "Branch_B1", "Branch_B2"]},
            "Branch_B0": {"t": "opersig", "fb": "Verify_And_Join", "nx": "Verify_And_Join"},
            "Branch_B1": {"t": "opersig", "fb": "Verify_And_Join", "nx": "Verify_And_Join"},
            "Branch_B2": {"t": "opersig", "fb": "Verify_And_Join", "nx": "Verify_And_Join"},
            "Verify_And_Join": {
                "t": "router",
                "rl": [{"if": "$._score>=0.9", "nx": "Postmortem_Act"}],
                "dn": "END",
            },
            "Postmortem_Act": {"t": "act", "nx": "END"},
        },
    }

    # 2. 병렬 브랜치에 Catch(`fb`)가 누락되어 배리어 데드락 위험이 있는 불량 그래프
    DEADLOCK_RISK_IR = {
        "_m": {"ep": "Triage_LLM", "v": "1.0"},
        "g": {
            "Triage_LLM": {"t": "projector", "nx": ["Safe_Branch", "Unsafe_Branch"]},
            "Safe_Branch": {"t": "opersig", "fb": "Join_Barrier", "nx": "Join_Barrier"},
            "Unsafe_Branch": {"t": "opersig", "nx": "Join_Barrier"},  # 🚨 fb 누락!
            "Join_Barrier": {"t": "router", "dn": "END"},
        },
    }

    # 3. DB 진단 서브그래프 (`branch:db_diag#entry` 역할을 할 독립 모듈)
    DB_DIAG_SUBGRAPH_IR = {
        "_m": {"ep": "Check_Pool_Metrics", "v": "1.0"},
        "g": {
            "Check_Pool_Metrics": {"t": "opersig", "nx": "Analyze_Slow_Query"},
            "Analyze_Slow_Query": {"t": "projector", "nx": "RETURN"},
        },
    }

    @staticmethod
    def build_oversized_ir(node_count: int = 45) -> Dict[str, Any]:
        """2,048 Bytes 한도를 초과하도록 긴 체인 노드를 생성하여 Auto-Linker 분할 유도"""
        g: Dict[str, Any] = {}
        for i in range(node_count):
            curr_id = f"Chain_Step_Node_Identifier_{i:03d}"
            next_id = f"Chain_Step_Node_Identifier_{i+1:03d}" if i < node_count - 1 else "END"
            g[curr_id] = {
                "t": "opersig",
                "a": {"io_ref": f"blob:deadbeefcafe{i:04x}", "pressure": 0.5},
                "nx": next_id,
            }
        return {"_m": {"ep": "Chain_Step_Node_Identifier_000", "v": "1.0"}, "g": g}


# =========================================================================
# 2. [Embedded FSM Test Scenes (Inheriting SandboxRunner)]
# =========================================================================
class FsmCompileScene(SandboxRunner):
    """
    @suite: compile
    @desc : L1 정적 검증(Deadlock Linter), 2KB 초과 그래프 자동 분할(Auto-Linker),
            FFI 경계 방어 및 JCS 해시(`branch:<hash>`) 결정론 검증
    """

    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🛠️ [START] Suite 1: FSM Compiler, Auto-Linker & Static Sandbox")
        log.info("=======================================================\n")

        await self._set_worker_policy("SYSTEM")
        await self._test_valid_compile_and_determinism()
        await self._test_barrier_deadlock_linter()
        await self._test_auto_linker_2kb_partition()
        await self._test_ffi_malformed_guard()
        self.report()

    async def _test_valid_compile_and_determinism(self):
        log.info("\n--- [Compile 1] Canonical Hashing & Determinism ---")
        payload = FsmAdapter.build_compile_payload(
            trace_id="compile_det_01",
            compact_ir=FsmTestTopologies.VALID_DIAMOND_IR,
        )

        hashes = []
        for _ in range(3):
            res = await self.broker.execute(code=payload, context={"fuel": 1000})
            if res.success:
                parsed = FsmAdapter.parse_broker_output(res.output)
                hashes.append(parsed.get("state_root", ""))

        if len(hashes) == 3 and len(set(hashes)) == 1 and hashes[0].startswith("branch:"):
            self._record_success(0, f"100% Deterministic Root Hash: {hashes[0]}")
        else:
            self._record_fail(0, f"Hash divergence or invalid root: {hashes}", "Compile Determinism")

    async def _test_barrier_deadlock_linter(self):
        log.info("\n--- [Compile 2] Static Sandbox: DagJoinBarrier Deadlock Detection ---")
        payload = FsmAdapter.build_compile_payload(
            trace_id="compile_deadlock_02",
            compact_ir=FsmTestTopologies.DEADLOCK_RISK_IR,
        )
        res = await self.broker.execute(code=payload, context={"fuel": 1000})
        parsed = FsmAdapter.parse_broker_output(res.output)
        revert_reason = str(parsed.get("revert_reason") or getattr(res, "error", ""))

        if not res.success and "E_BARRIER_DEADLOCK_RISK" in revert_reason:
            self._record_success(0, f"Deadlock risk caught before mount: {revert_reason}")
        else:
            self._record_fail(0, f"Failed to trap missing Catch edge. Output: {parsed}", "Deadlock Linter")

    async def _test_auto_linker_2kb_partition(self):
        log.info("\n--- [Compile 3] Auto-Linker: >2KB Graph SESE Partition & SYMLINK ---")
        oversized_ir = FsmTestTopologies.build_oversized_ir(node_count=45)
        payload = FsmAdapter.build_compile_payload(
            trace_id="compile_oversized_03",
            compact_ir=oversized_ir,
        )
        res = await self.broker.execute(code=payload, context={"fuel": 2000})
        parsed = FsmAdapter.parse_broker_output(res.output)
        inner = parsed.get("parsed_output", {})
        state_diff = parsed.get("state_diff", {})

        total_root_bytes = inner.get("total_root_bytes", 9999)
        bundle_count = inner.get("bundle_count", len(state_diff))

        if res.success and total_root_bytes <= 2048 and bundle_count >= 2:
            self._record_success(
                0,
                f"Oversized graph auto-partitioned into {bundle_count} bundles (Root={total_root_bytes}B <= 2048B)",
            )
        else:
            self._record_fail(0, f"Auto-Linker failed: {parsed}", "2KB Auto-Linker")

    async def _test_ffi_malformed_guard(self):
        log.info("\n--- [Compile 4] Missing Entry Point Guard ---")
        bad_ir = {"_m": {"ep": "NonExistent_Node", "v": "1.0"}, "g": {"Node_A": {"t": "act", "nx": "END"}}}
        payload = FsmAdapter.build_compile_payload(trace_id="bad_ep", compact_ir=bad_ir)
        res = await self.broker.execute(code=payload)
        parsed = FsmAdapter.parse_broker_output(res.output)
        reason = str(parsed.get("revert_reason") or getattr(res, "error", ""))

        if not res.success and "E_MISSING_ENTRY" in reason:
            self._record_success(0, f"Invalid entry point rejected cleanly: {reason}")
        else:
            self._record_fail(0, f"Expected E_MISSING_ENTRY, got: {parsed}", "FFI Entry Guard")


class FsmMultiverseScene(SandboxRunner):
    """
    @suite: multiverse
    @desc : L2 런타임 전이 원자성 검증 — 1:N 차등 연료 분할(_slice_fuel),
            SYMLINK Call-Stack Push/Pop, HTTP 202 무비용 파킹, FuelBreaker 차단
    """

    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🌌 [START] Suite 2: Multiverse Transitions & Fuel Conservation")
        log.info("=======================================================\n")

        await self._set_worker_policy("SYSTEM")
        await self._test_weighted_fork_fuel_slicing()
        await self._test_symlink_call_and_return_stack()
        await self._test_zero_spend_hitl_parking()
        await self._test_fuel_breaker_exhaustion_trap()
        self.report()

    async def _test_weighted_fork_fuel_slicing(self):
        log.info("\n--- [Multiverse 1] 1:3 Asymmetric Fuel Slicing ([0.4, 0.4, 0.2]) ---")
        branch_id = "branch:root_test"
        snapshot = FsmAdapter.build_runtime_snapshot(branches={branch_id: FsmTestTopologies.VALID_DIAMOND_IR})
        calldata = FsmAdapter.build_fork_calldata(
            curr_node="Triage_LLM",
            step_cost=30,
            fuel_ratios=[0.4, 0.4, 0.2],
            meta_ctx={"_last_ptr": "blob:triage_log_01"},
        )
        payload = FsmAdapter.build_fsm_payload(
            target_address=branch_id,
            calldata=calldata,
            state_snapshot=snapshot,
        )

        res = await self.broker.execute(code=payload, context={"fuel": 3000})
        parsed = FsmAdapter.parse_broker_output(res.output)
        children = parsed.get("parsed_output", {}).get("children", [])
        fuels = [c.get("fuel", 0) for c in children]

        # 3000 - 30 = 2970 -> [1188, 1188, 594]
        if res.success and fuels == [1188, 1188, 594] and sum(fuels) + parsed.get("gas_used", 0) == 3000:
            self._record_success(0, f"Exact Fuel Conservation Verified: 3000 -> -30 -> {fuels}")
        else:
            self._record_fail(0, f"Fuel slicing mismatch: {fuels} (Output: {parsed})", "Weighted Fork")

    async def _test_symlink_call_and_return_stack(self):
        log.info("\n--- [Multiverse 2] SYMLINK Subgraph Jump & Call-Stack RETURN ---")
        root_id = "branch:main_graph"
        sub_id = "branch:db_diag"
        root_ir = {
            "_m": {"ep": "Start_Node", "v": "1.0"},
            "g": {
                "Start_Node": {
                    "t": "opersig",
                    "nx": f"{sub_id}#Check_Pool_Metrics",
                    "fb": "Resume_After_Subgraph",
                },
                "Resume_After_Subgraph": {"t": "act", "nx": "END"},
            },
        }
        snapshot = FsmAdapter.build_runtime_snapshot(
            branches={root_id: root_ir, sub_id: FsmTestTopologies.DB_DIAG_SUBGRAPH_IR}
        )

        # Step A: Root -> Subgraph SYMLINK Call (call_stack Push 확인)
        p1 = FsmAdapter.build_fsm_payload(
            target_address=root_id,
            calldata=FsmAdapter.build_step_calldata(curr_node="Start_Node", step_cost=10),
            state_snapshot=snapshot,
        )
        r1 = await self.broker.execute(code=p1, context={"fuel": 500})
        out1 = FsmAdapter.parse_broker_output(r1.output).get("parsed_output", {})

        # Step B: Subgraph 마지막 노드에서 RETURN 토큰 실행 -> Root의 Resume_After_Subgraph 복귀 확인
        p2 = FsmAdapter.build_fsm_payload(
            target_address=out1.get("branch", sub_id),
            calldata=FsmAdapter.build_step_calldata(
                curr_node="Analyze_Slow_Query",
                step_cost=15,
                call_stack=out1.get("call_stack", []),
            ),
            state_snapshot=snapshot,
        )
        r2 = await self.broker.execute(code=p2, context={"fuel": out1.get("remaining_fuel", 490)})
        out2 = FsmAdapter.parse_broker_output(r2.output).get("parsed_output", {})

        if (
            r1.success
            and r2.success
            and out1.get("transition_mode") == "SYMLINK_CALL"
            and out2.get("transition_mode") == "SYMLINK_RETURN"
            and out2.get("branch") == root_id
            and out2.get("curr_node") == "Resume_After_Subgraph"
            and out2.get("remaining_fuel") == 475
        ):
            self._record_success(0, f"SYMLINK Call -> RETURN restored parent frame '{root_id}#Resume_After_Subgraph'")
        else:
            self._record_fail(0, f"Call-stack transition failed: out1={out1}, out2={out2}", "SYMLINK Call-Stack")

    async def _test_zero_spend_hitl_parking(self):
        log.info("\n--- [Multiverse 3] HTTP 202 Pre-Flight Zero-Spend Parking ---")
        payload = FsmAdapter.build_fsm_payload(
            target_address="branch:root_test",
            calldata=FsmAdapter.build_hitl_park_calldata(
                curr_node="K8s_Rollback_MCP",
                meta_ctx={"hitl_ticket": "ticket_99"},
            ),
        )
        res = await self.broker.execute(code=payload, context={"fuel": 1118})
        parsed = FsmAdapter.parse_broker_output(res.output)
        inner = parsed.get("parsed_output", {})

        if (
            res.success
            and parsed.get("gas_used") == 0
            and inner.get("status") == "HITL_PARKED"
            and inner.get("remaining_fuel") == 1118
        ):
            self._record_success(0, "Zero-Spend HITL Parking verified (gas_used=0, Preserved Fuel=1118)")
        else:
            self._record_fail(0, f"HITL Parking deducted fuel or failed: {parsed}", "Zero-Spend HITL")

    async def _test_fuel_breaker_exhaustion_trap(self):
        log.info("\n--- [Multiverse 4] FuelBreaker Exhaustion Guard (wasm.reject) ---")
        branch_id = "branch:root_test"
        snapshot = FsmAdapter.build_runtime_snapshot(branches={branch_id: FsmTestTopologies.VALID_DIAMOND_IR})
        # 잔여 연료는 10인데 step_cost가 50인 노드 실행 시도 -> FuelExhausted Revert 유도
        payload = FsmAdapter.build_fsm_payload(
            target_address=branch_id,
            calldata=FsmAdapter.build_step_calldata(curr_node="Branch_B2", step_cost=50),
            state_snapshot=snapshot,
        )
        res = await self.broker.execute(code=payload, context={"fuel": 10})
        parsed = FsmAdapter.parse_broker_output(res.output)
        reason = str(parsed.get("revert_reason") or getattr(res, "error", ""))

        if not res.success and "FuelExhausted" in reason:
            self._record_success(0, f"Runaway branch cleanly cut off by FuelBreaker: {reason}")
        else:
            self._record_fail(0, f"Failed to trap fuel exhaustion: {parsed}", "FuelBreaker Guard")


class FsmKillerIncidentScene(SandboxRunner):
    """
    @suite: incident
    @desc : L3 '새벽 3시 결제 장애' 엔드투엔드 멀티버스 라이프사이클 검증
            (차등 Fork -> b0/b1 파킹 & b2 연료 소진 -> N:1 Arbiter 병합 & 연료 회수 -> Time-Travel 재분기)
    """

    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🚨 [START] Suite 3: 03:00 AM Payment Outage E2E Killer Showcase")
        log.info("=======================================================\n")

        await self._set_worker_policy("SYSTEM")
        await self._run_full_incident_lifecycle()
        self.report()

    async def _run_full_incident_lifecycle(self):
        # 1. 런북 컴파일 및 마운트
        comp_res = await self.broker.execute(
            code=FsmAdapter.build_compile_payload("incident_03am", FsmTestTopologies.VALID_DIAMOND_IR),
            context={"fuel": 3000},
        )
        comp_data = FsmAdapter.parse_broker_output(comp_res.output)
        root_hash = comp_data.get("state_root", "branch:default")
        snapshot = FsmAdapter.build_runtime_snapshot(branches=comp_data.get("state_diff", {}))

        # 2. 03:02 — Triage_LLM (-30 Fuel) & 1:3 확신도 비율 Fork ([0.4, 0.4, 0.2] -> [1188, 1188, 594])
        fork_res = await self.broker.execute(
            code=FsmAdapter.build_fsm_payload(
                target_address=root_hash,
                calldata=FsmAdapter.build_fork_calldata(
                    curr_node="Triage_LLM",
                    step_cost=30,
                    fuel_ratios=[0.4, 0.4, 0.2],
                ),
                state_snapshot=snapshot,
            ),
            context={"fuel": 3000},
        )
        children = FsmAdapter.parse_broker_output(fork_res.output).get("parsed_output", {}).get("children", [])
        b0_fuel, b1_fuel, b2_fuel = children[0]["fuel"], children[1]["fuel"], children[2]["fuel"]

        # 3. 03:17 ~ 03:25 — b0(롤백 후 1098 잔여, _score=0.95), b1(DB 진단 후 1113 잔여, _score=0.70),
        #    b2(연료 594 전액 소진 후 Catch 폴백으로 0 잔여, _score=0.0)가 Verify_And_Join 배리어에 도착해 N:1 MERGE
        arriving_cursors = [
            {"branch_idx": 0, "fuel": b0_fuel - 90, "meta_ctx": {"_score": 0.95, "cause": "deploy_regression"}},
            {"branch_idx": 1, "fuel": b1_fuel - 75, "meta_ctx": {"_score": 0.70, "cause": "db_pool"}},
            {"branch_idx": 2, "fuel": 0, "meta_ctx": {"_score": 0.0, "cause": "pg_timeout_exhausted"}},
        ]
        merge_res = await self.broker.execute(
            code=FsmAdapter.build_fsm_payload(
                target_address=root_hash,
                calldata=FsmAdapter.build_merge_calldata(
                    join_node="Verify_And_Join",
                    arriving_cursors=arriving_cursors,
                ),
                state_snapshot=snapshot,
            ),
            context={"fuel": 0},
        )
        merge_out = FsmAdapter.parse_broker_output(merge_res.output).get("parsed_output", {})
        reclaimed_fuel = merge_out.get("reclaimed_fuel", 0)
        winning_meta = merge_out.get("winning_meta", {})

        # 1098 + 1113 + 0 = 2211 Fuel 회수 확인
        if not (merge_res.success and reclaimed_fuel == 2211 and winning_meta.get("cause") == "deploy_regression"):
            self._record_fail(0, f"Barrier Merge failed: {merge_out}", "Incident N:1 Merge")
            return
        self._record_success(0, f"Barrier Collapsed: Winner='deploy_regression' (_score=0.95), Reclaimed Fuel={reclaimed_fuel}")

        # 4. Verify_And_Join (Router: $._score >= 0.9 -> Postmortem_Act) 전이 확인
        router_res = await self.broker.execute(
            code=FsmAdapter.build_fsm_payload(
                target_address=root_hash,
                calldata=FsmAdapter.build_step_calldata(
                    curr_node="Verify_And_Join",
                    step_cost=10,
                    meta_ctx=winning_meta,
                ),
                state_snapshot=snapshot,
            ),
            context={"fuel": reclaimed_fuel},
        )
        router_out = FsmAdapter.parse_broker_output(router_res.output).get("parsed_output", {})

        if router_res.success and router_out.get("curr_node") == "Postmortem_Act" and router_out.get("remaining_fuel") == 2201:
            self._record_success(0, "Router evaluated '$._score>=0.9' -> Routed to 'Postmortem_Act' (Fuel=2201)")
        else:
            self._record_fail(0, f"Router condition evaluation failed: {router_out}", "Incident Router")


# =========================================================================
# 3. [Pipeline Configuration & Orchestrator (FsmWasmFlow)]
# =========================================================================
@dataclass
class FsmPipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "compile": f"{MODULE_PATH}:FsmCompileScene",
        "multiverse": f"{MODULE_PATH}:FsmMultiverseScene",
        "incident": f"{MODULE_PATH}:FsmKillerIncidentScene",
    })

    default_suites: List[str] = field(default_factory=lambda: [
        "compile",     # 1. L1: 정적 데드락 Linter, 2KB Auto-Linker, 해시 결정론 검증
        "multiverse",  # 2. L2: 1:N 연료 분할, SYMLINK 콜스택, HTTP 202 무비용 파킹, FuelBreaker
        "incident",    # 3. L3: 새벽 3시 결제 장애 E2E 멀티버스 합류 및 라우팅 검증
    ])
    wasm_filename: str = "fsm.wasm"


class FsmWasmFlow:
    """@role: FSM WASM Pipeline Orchestrator (Build & Isolated E2E Test)"""

    def __init__(self, should_build: bool = False, suites: List[str] = None, config: FsmPipelineConfig = None):
        self.config = config or FsmPipelineConfig()
        self.should_build = should_build

        if not suites or suites == ["all"]:
            self.suites = self.config.default_suites
        else:
            self.suites = suites

        self.log = get_emitter("wasm.fsm.entry")
        self.time_root = resolve_path("time")
        self.dest_wasm_file = self.time_root / self.config.wasm_filename

    def _resolve_suite_class(self, suite_name_or_path: str):
        module_path_str = self.config.suites_registry.get(suite_name_or_path, suite_name_or_path)
        try:
            if ":" not in module_path_str:
                raise ValueError(f"Invalid suite path format '{module_path_str}'. Expected 'module.path:ClassName'")

            mod_name, cls_name = module_path_str.split(":")
            if mod_name in (__name__, "__main__") and cls_name in globals():
                return globals()[cls_name]

            module = importlib.import_module(mod_name)
            return getattr(module, cls_name)
        except Exception as e:
            self.log.error(f"[CLI] Failed to dynamically load suite '{suite_name_or_path}': {e}")
            return None

    async def build(self) -> bool:
        self.log.info("\n[CLI] Starting standalone WasmBuilder for fsm.wasm...")
        builder = WasmBuilder()
        await builder.trace()

        if builder.rupture_confirmed:
            self.log.error("❌ [CLI] Builder encountered a fatal rupture.")
            return False

        self.log.info("✅ [CLI] Builder completed successfully.")
        return True

    async def test(self) -> bool:
        self.log.info(f"\n[CLI] Starting Isolated FSM WasmTester for suites: {self.suites}...")

        if not self.dest_wasm_file.exists():
            self.log.error(
                f"❌ [CLI] Missing WASM binary at {self.dest_wasm_file}. Please run with '--build' or compile fsm.wasm."
            )
            return False

        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue

            resolved_class = self._resolve_suite_class(suite_name)
            if not resolved_class:
                return False
            suite_map[suite_name] = resolved_class

        if not suite_map:
            self.log.error("❌ [CLI] No valid FSM test suites found to execute.")
            return False

        tester = WasmTester(
            wasm_module_path=str(self.dest_wasm_file),
            sandbox_root=str(self.time_root),
            suites=suite_map,
        )

        success, err_msg = await tester.execute()

        log.info("\n" + "=" * 75)
        log.info("🚀 FSM WASM PIPELINE EXECUTION REPORT".center(75))
        log.info("=" * 75)

        if success:
            self.log.info(f"🟢 [SUCCESS] All FSM Test Suites ({', '.join(suite_map.keys())}) PASSED.")
            if getattr(tester, "test_execution_hash", None):
                self.log.info(f"🔗 Execution Canonical Hash: {tester.test_execution_hash}")
            log.info("=" * 75 + "\n")
            return True
        else:
            self.log.critical("🔴 [FAILED] FSM Test execution terminated with errors.")
            self.log.critical(f"📝 [SUMMARY] {err_msg}")

            if hasattr(tester, "suite_runners") and tester.suite_runners:
                log.info("\n" + "🔥" * 37)
                log.info("🚨 DETAILED FAILURE TRACES 🚨".center(75))
                log.info("🔥" * 37)

                for s_name, runner in tester.suite_runners.items():
                    fail_cnt = getattr(runner, "fail_count", 0)
                    failed_cases = getattr(runner, "failed_cases", [])

                    if fail_cnt > 0 or failed_cases:
                        log.info(f"\n❌ [SUITE: {s_name.upper()}] ➔ {fail_cnt} Test(s) Failed")
                        for idx, fc in enumerate(failed_cases, 1):
                            title = fc.get("title", "Unknown Test Case")
                            err = fc.get("error", "No error details provided")
                            log.info(f"  └─ {idx}. {title}")
                            log.info(f"     [Reason] {err}\n")

            log.info("=" * 75 + "\n")
            return False

    async def run(self):
        try:
            if self.should_build:
                if not await self.build():
                    sys.exit(1)

            if not await self.test():
                sys.exit(1)

        except SystemExit:
            raise
        except Exception as e:
            self.log.error(f"💥 [FATAL] Unhandled exception in FSM E2E runner: {e}", exc_info=True)
            sys.exit(1)


def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="FSM WASM Multiverse Engine E2E Verification CLI")
    parser.add_argument(
        "--suites",
        nargs="+",
        default=["all"],
        help="List of suites to run (compile, multiverse, incident)",
    )
    parser.add_argument("--build", action="store_true", help="Force compile the Rust FSM WASM artifact before testing.")

    args, _ = parser.parse_known_args(args_list)
    suites = getattr(args, "suites", ["all"])
    should_build = getattr(args, "build", False)

    config = FsmPipelineConfig()
    app = FsmWasmFlow(should_build=should_build, suites=suites, config=config)

    try:
        PhaseReactor.ignite(app.run)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        log.error(f"💥 [FATAL] PhaseReactor crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()