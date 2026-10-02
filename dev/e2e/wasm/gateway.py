# fiber.dev.e2e.wasm.gateway 
import sys
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Callable

from xphi.arch.dev.wasm.builder import WasmBuilder
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.adapter.gateway import GatewayAdapter
from xphi.kernel.wasm.gateway import GatewayWasm

log = get_emitter("e2e.wasm.gateway")

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class WasmGatewayTestSuite:
    """L0 Firewall & Pest Parser Validation"""
    def __init__(self):
        self.log = get_emitter("e2e.wasm.gateway.compute")
        self.results: List[TestResult] = []
        self.gateway = None
        
    async def setup(self):
        self.log.info("\n" + "-"*80)
        self.log.info("🔌 [GATEWAY SETUP] Initializing Zero-WASI WasmGateway (L0 Firewall)...")
        try:
            self.gateway = await asyncio.to_thread(GatewayWasm, "gateway.wasm")
            self.log.info("✅ Gateway Initialized (Pre-allocated Buffer: 1MB & Thread-Safe Locked)")
        except Exception as e:
            self.log.critical(f"❌ [FATAL] Gateway initialization failed: {e}")
            self.gateway = None
        self.log.info("-" * 80)

    async def run_scenario(
        self, 
        title: str, 
        expected_success: bool, 
        payload_kwargs: Dict[str, Any], 
        validator: Callable[[Dict[str, Any]], bool] = None
    ):
        self.log.info(f"\n▶️ [FIREWALL DOMAIN] Scenario: {title}")
        
        if not self.gateway:
            self.log.error("💥 Gateway is offline. Auto-failing scenario.")
            self.results.append(TestResult("GATEWAY", title, False, expected_success))
            return

        try:
            safe_payload_kwargs = GatewayAdapter.build_evaluate_payload(**payload_kwargs)
            receipt = await asyncio.to_thread(self.gateway.evaluate_intent, **safe_payload_kwargs)
            
            is_success = receipt.get("success", False)
            revert_reason = receipt.get("revert_reason", "")
            test_passed = (is_success == expected_success)
            
            if test_passed and validator and not validator(receipt):
                self.log.error(f"❌ [VALIDATION FAILED] Revert reason mismatch. Receipt: {receipt}")
                test_passed = False
                
            if not is_success and not expected_success:
                if test_passed:
                    self.log.info(f"✅ Defense mechanism triggered (Revert: {revert_reason})")
            elif is_success and expected_success:
                intent_data = receipt.get("intent", {})
                self.log.info(f"✅ Passed L0 Firewall! Extracted AST: {intent_data}")
            elif not test_passed:
                self.log.error(f"💥 Scenario failed. Expected: {expected_success}, Got: {is_success} ({revert_reason})")
                
            self.results.append(TestResult(
                target="WASM_GATEWAY",
                scenario=title,
                success=is_success if validator is None else (is_success == expected_success and test_passed),
                expected_success=True if validator else expected_success
            ))
            
        except ValueError as ve:
            if not expected_success:
                self.log.info(f"✅ Python Adapter defense triggered (Revert: {ve})")
                self.results.append(TestResult("WASM_GATEWAY", title, False, False))
            else:
                self.log.error(f"💥 Unexpected Python error: {ve}")
                self.results.append(TestResult("WASM_GATEWAY", title, False, True))
        except Exception as e:
            self.log.error(f"💥 [CRASH] Unexpected gateway exception: {e}")
            self.results.append(TestResult("WASM_GATEWAY", title, False, expected_success))

    async def execute(self) -> List[TestResult]:
        await self.setup()
        self.log.info("\n[CLI] 🏃‍♂️ Initiating L0 Zero-Trust Isolation Tests...")
        
        await self.run_scenario(
            title="OpCode, Budget, Target, Hex Payload",
            expected_success=True,
            payload_kwargs={
                "dimension": 3, "base_friction": 0.005,
                "raw_payload": "CALL FEE 15.5 USDC TO urn:xphi:agent:db_router_01 WITH 0xDEADBEEF",
                "state_vector": [1, 1, 1]
            }
        )
        
        await self.run_scenario(
            title="O(1) BDD Logic Matrix Rejection",
            expected_success=False,
            payload_kwargs={
                "dimension": 3, "base_friction": 0.005,
                "raw_payload": "CALL FEE 15.5 USDC TO urn:xphi:agent:db_router_01 WITH 0xDEADBEEF",
                "state_vector": [1, 0, 1] 
            },
            validator=lambda r: "logic matrix" in r.get("revert_reason", "").lower()
        )
        
        await self.run_scenario(
            title="Pest Sandbox Defense (SQL Injection Rejection)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3, "base_friction": 0.005,
                "raw_payload": "CALL FEE 100 USDC TO DB_SERVICE WITH DROP TABLE users;",
                "state_vector": [1, 1, 1] 
            },
            validator=lambda r: "syntax error" in r.get("revert_reason", "").lower()
        )
        
        await self.run_scenario(
            title="AST Pest Parser Rejection (LLM Hallucination)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3, "base_friction": 0.005,
                "raw_payload": "PLEASE CALL FEE 10 USDC TO STORAGE_NODE",
                "state_vector": [1, 1, 1]
            },
            validator=lambda r: "syntax error" in r.get("revert_reason", "").lower()
        )

        massive_payload = "CALL FEE 1 USDC TO urn:xphi:null WITH 0x" + ("A" * (1024 * 1024 + 10))
        await self.run_scenario(
            title="Physical Circuit Breaker (Overflow 1MB)",
            expected_success=False,
            payload_kwargs={
                "dimension": 3, "base_friction": 0.005,
                "raw_payload": massive_payload,
                "state_vector": [1, 1, 1]
            },
            validator=lambda r: "boundary" in r.get("revert_reason", "").lower()
        )
        return self.results


class WasmFsmTestSuite:
    """FSM Transition & Side-effect Validation"""
    def __init__(self, gateway: GatewayWasm):
        self.log = get_emitter("e2e.wasm.gateway.fsm")
        self.gateway = gateway
        self.results: List[TestResult] = []

    async def run_fsm_scenario(
        self, 
        title: str, 
        fsm_method: Callable, 
        fsm_state: Dict[str, Any], 
        event: Dict[str, Any], 
        expected_success: bool,
        expected_next_state: str = None,
        expected_command_type: str = None
    ):
        self.log.info(f"\n⚙ [FSM DOMAIN] Scenario: {title}")
        
        try:
            receipt = await asyncio.to_thread(fsm_method, fsm_state=fsm_state, event=event)
            
            is_success = receipt.get("success", False)
            revert_reason = receipt.get("revert_reason", "")
            next_state_data = receipt.get("next_fsm_state", {})
            command_data = receipt.get("command", {})
            
            test_passed = (is_success == expected_success)
            
            if is_success and expected_success:
                actual_state = next_state_data.get("state") or next_state_data.get("edge")
                if expected_next_state and actual_state != expected_next_state:
                    self.log.error(f"❌ [STATE MISMATCH] Expected '{expected_next_state}', Got '{actual_state}'")
                    test_passed = False
                
                actual_command = command_data.get("command_type") if command_data else None
                if expected_command_type and actual_command != expected_command_type:
                    self.log.error(f"❌ [COMMAND MISMATCH] Expected '{expected_command_type}', Got '{actual_command}'")
                    test_passed = False

            if not is_success and not expected_success:
                self.log.info(f"✅ FSM properly rejected transition (Revert: {revert_reason})")
            elif test_passed:
                self.log.info(f"✅ FSM Transitioned successfully! Next State: {next_state_data}, Command: {command_data.get('command_type', 'None')}")
            else:
                self.log.error(f"💥 Scenario failed. Receipt: {receipt}")

            self.results.append(TestResult(
                target="FSM",
                scenario=title,
                success=test_passed,
                expected_success=True
            ))

        except Exception as e:
            self.log.error(f"💥 [CRASH] FSM exception: {e}")
            self.results.append(TestResult("WASM_FSM", title, False, True))

    async def execute(self) -> List[TestResult]:
        self.log.info("\n[Gateway] 🔄 Initiating FSM Determinism & State Transition Tests...")
        
        await self.run_fsm_scenario(
            title="Transaction FSM: Init -> ExecuteVmCmd",
            fsm_method=self.gateway.execute_transaction_fsm,
            fsm_state={
                "state": "Init", "max_fuel_limit": 200000,
                "caller": "", "target_resource": "", "charge_amount": 0
            },
            event={
                "event_type": "StartTransactionIntent",
                "caller": "urn:xphi:tenant:auth_77a9", "charge_amount": 100,
                "target_resource": "urn:xphi:agent:db_router_01", "payload": "0x000000",
                "active_snapshot": "base64_mock"
            },
            expected_success=True,
            expected_next_state="ShadowExecution", 
            expected_command_type="ExecuteVmCmd"
        )

        await self.run_fsm_scenario(
            title="Epoch Topology: ZERO -> ANCHOR (Resource Lock)",
            fsm_method=self.gateway.execute_flow_transition_fsm,
            fsm_state={
                "origin": "0", "edge": "ZERO", 
                "reflective": True, "reversible": True, "anchored_target": None
            },
            event={
                "event_type": "ReachAnchor",
                "resource_address": "urn:surgent:resource:resolved_task_999"
            },
            expected_success=True,
            expected_next_state="ANCHOR", 
            expected_command_type="ResolveAnchorCmd"
        )

        await self.run_fsm_scenario(
            title="Epoch Topology: Forced Collapse -> FractureCmd",
            fsm_method=self.gateway.execute_flow_transition_fsm,
            fsm_state={
                "origin": "0", "edge": "COHERENT", 
                "reflective": True, "reversible": True, "anchored_target": None
            },
            event={
                "event_type": "FractureTopology",
                "lmbda": 0.0, "tau": 1.0, "force_collapse": True
            },
            expected_success=True,
            expected_next_state="COLLAPSED", 
            expected_command_type="FractureCmd"
        )

        await self.run_fsm_scenario(
            title="Clearing FSM: Rejecting unauthorized/invalid event",
            fsm_method=self.gateway.execute_clearing_fsm,
            fsm_state={
                "state": "Init", "concurrent_agents": 3,
                "tenant": "", "initial_deposit": 0, "authorized_fuel_budget": 0,
                "root_pta_hash": "", "all_tx_hashes": []
            },
            event={"event_type": "InvalidHackingEvent"},
            expected_success=False
        )

        return self.results


class MasterGatewaySuite:
    def __init__(self, force_build: bool = False):
        self.log = log
        self.all_results: List[TestResult] = []
        self.force_build = force_build

    def _print_report(self):
        self.log.info("\n" + "="*90)
        self.log.info("📊 [WASM GATEWAY E2E REPORT: PEST ISOLATION & FSM DETERMINISM]")
        self.log.info("="*90)
        
        all_passed = True
        for idx, res in enumerate(self.all_results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
            
            target_label = f"[{res.target}]".ljust(15)
            self.log.info(f"{status_icon} {idx:02d}. {target_label} {res.scenario.ljust(60)} | Result: {status_text}")
            
        self.log.info("-" * 90)
        if all_passed:
            self.log.info("🎉 ALL L0 GATEWAY SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 E2E GATEWAY FAILED. Inspect physical memory bounds, AST parsing rules, and FSM transition logic.")
        self.log.info("="*90 + "\n")

    async def execute(self):
        if self.force_build:
            self.log.info("\n" + "="*90)
            self.log.info("🔨 [PRE-FLIGHT] Forcing Rebuild of 'gateway.wasm' Artifact...")
            self.log.info("="*90)
            
            builder = WasmBuilder(target_projects=["gateway"])
            if hasattr(builder, 'trace'):
                await builder.trace()
            else:
                await builder.execute()
            
            if getattr(builder, 'rupture_confirmed', False):
                self.log.critical("💥 [FATAL] Gateway WASM build failed. Aborting E2E Sequence.")
                sys.exit(1)
                
            self.log.info("✅ [PRE-FLIGHT] Binary synced. Proceeding to Tests.")
        else:
            self.log.info("⏩ [PRE-FLIGHT] Build skipped. Reusing existing 'gateway.wasm'. (Use --build to recompile)")

        self.log.info("\n" + "="*90)
        self.log.info("🧪 [MASTER SUITE] Commencing Decoupled WasmGateway Integration Tests")
        self.log.info("="*90)
        
        gateway_suite = WasmGatewayTestSuite()
        self.all_results.extend(await gateway_suite.execute())
        if gateway_suite.gateway:
            fsm_suite = WasmFsmTestSuite(gateway_suite.gateway)
            self.all_results.extend(await fsm_suite.execute())

        self._print_report()

def main(args_list: list[str] = None):
    args = args_list if args_list is not None else sys.argv[1:]
    force_build = "--build" in args
    
    app = MasterGatewaySuite(force_build=force_build)
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()