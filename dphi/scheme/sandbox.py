# dphi.scheme.sandbox
## @lineage: dphi.resolver.scene.sandbox
import time
import asyncio
from kernel.dphi.scheme.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scene.sandbox")

class SandboxScene(SchemeRunner):
    """@desc: Core execution engine, resource isolation, and state generation scenarios"""
    
    async def run_all(self):
        log.info("\n=== [START] Executing Sandbox & Compute Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        # 1. Sandbox Security & Resource Isolation
        await self._test_wasmcg_resilience()
        await self._test_legacy_execution()
        await self._test_non_deterministic_isolation()
        await self._test_guardrail_validation()
        
        # 2. Causality Control & Dependency Injection
        await self._test_topos_and_phase()
        await self._test_tripartite_parity()
        await self._test_dependency_injection()
        await self._test_ffi_robustness()
        
        # 3. Fuel Control & Structural Boundary Tests
        await self._test_fuel_linear_profiling()
        await self._test_dynamic_policy_and_exhaustion()
        await self._test_concurrency_and_pool_scaling()
        await self._test_fault_isolation()
        
        self.report()

    async def _test_wasmcg_resilience(self):
        log.info("\n--- Running Suite: WasmCG Resilience & Resource Control ---")
        await self._run_case("WasmCG: Unregistered API Call (O(1) Guard)", "hack_system_memory", {}, expected_success=False)

    async def _test_fuel_linear_profiling(self):
        log.info("\n--- Running Suite: Deterministic Fuel Profiling ---")
        await self._run_case("Profile: 10KB Payload Hashing", "compute_root_fingerprint", {"dummy_data": "A" * 10_000}, expected_success=True)
        await self._run_case("Profile: 50KB Payload Hashing", "compute_root_fingerprint", {"dummy_data": "A" * 50_000}, expected_success=True)

    async def _test_dynamic_policy_and_exhaustion(self):
        log.info("\n--- Running Suite: Dynamic Policy & Resource Exhaustion ---")
        await self._set_worker_policy("STANDARD")
        large_payload = {"dummy_data": "A" * 150_000}
        await self._run_case("Boundary Test: 150KB Hashing under STANDARD Tier (Expect Trap)", "compute_root_fingerprint", large_payload, expected_success=False)

    async def _test_legacy_execution(self):
        log.info("\n--- Running Suite: Legacy Code Execution ---")
        await self._run_case("Legacy: Normal Execution", "execute_code", "x = 10\nprint(x)", expected_success=True)

    async def _test_non_deterministic_isolation(self):
        log.info("\n--- Running Suite: Determinism & System Call Isolation ---")
        
        # 1. System Resource (File/Net) Isolation Tests (Negative)
        io_code = "with open('/etc/passwd', 'r') as f:\n    print(f.read())"
        await self._run_case("Isolation: Deny File System Access", "execute_code", io_code, expected_success=False)
        
        net_code = "import urllib.request\nreq = urllib.request.urlopen('http://localhost')\nprint(req.read())"
        await self._run_case("Isolation: Deny Network Access", "execute_code", net_code, expected_success=False)

        # 2. Zero-Trust Fallback Mechanism Test
        # When context is not injected, the sandbox must fallback to 0.0, preventing OS time leak.
        time_code = "import time\nprint(time.time())"
        res_time = await self.broker.execute(code=time_code)
        if res_time.success and float(res_time.output.strip()) == 0.0:
            log.info("  [PASS] Determinism: Sandbox enforces 0.0s fallback time.")
            self.success_count += 1
        else:
            log.error(f"  [FAIL] Determinism: Time fallback failed. Output: {res_time.output}")
            self.fail_count += 1

        # 3. Idempotency & Deterministic PRNG Verification
        random_code = "import random, os\nprint(f'{random.random()}|{os.urandom(4).hex()}')"
        
        # Execute the same code twice without injecting context (Expect identical fallback seed application)
        res_rand1 = await self.broker.execute(code=random_code)
        res_rand2 = await self.broker.execute(code=random_code)
        
        if res_rand1.success and res_rand2.success and (res_rand1.output == res_rand2.output):
            log.info(f"  [PASS] Determinism: PRNG sequences are 100% identical ({res_rand1.output.strip()}).")
            self.success_count += 1
        else:
            log.error("  [FAIL] Determinism: PRNG outputs diverge. Sandbox is not deterministic.")
            self.fail_count += 1

        # 4. Dynamic Context Injection Verification
        injected_context = {
            "timestamp": 1600000000,
            "seed": "proof_of_compute_seed_777"
        }
        
        injection_code = "import time, random\nprint(f'{time.time()}|{random.random()}')"
        
        # Explicitly inject context via Broker
        res_inject = await self.broker.execute(code=injection_code, context=injected_context)
        
        if res_inject.success:
            out_time, out_rand = res_inject.output.strip().split('|')
            # Cast to float first to handle potential "1600000000.0" from python's time.time(), then to int
            if int(float(out_time)) == 1600000000:
                log.info(f"  [PASS] Determinism: Sandbox correctly adopted injected context (Time: {out_time}, Rand: {out_rand}).")
                self.success_count += 1
            else:
                log.error(f"  [FAIL] Determinism: Context injection failed. Output: {res_inject.output}")
                self.fail_count += 1
        else:
            log.error("  [FAIL] Determinism: Context injected execution crashed.")
            self.fail_count += 1

    async def _test_guardrail_validation(self):
        log.info("\n--- Running Suite: Guardrail Validation ---")
        await self._run_case("Guardrail: Missing Files", "verify_packet", {"packet_id": "123"}, expected_success=False)
        await self._run_case("Guardrail: Valid Packet", "verify_packet", {"packet_id": "123", "files": {"model.bin": "hash"}}, expected_success=True)

    async def _test_topos_and_phase(self):
        log.info("\n--- Running Suite: Topos & Phase Generation (Epoch-Tick) ---")
        current_ts = int(time.time() * 1000)
        await self._run_case("Event: Generate Topos Anchor ID", "generate_topos_id", {"ts": current_ts}, expected_success=True)

        payload_phase = {"topo": 50, "press": -10, "rupture": False}
        await self._run_case("Event: Generate Phase & Nexus ID (Tick 0)", "generate_phase_id", payload_phase, expected_success=True)
        await self._run_case("Event: Generate Phase & Nexus ID (Tick 1)", "generate_phase_id", payload_phase, expected_success=True)

    async def _test_tripartite_parity(self):
        log.info("\n--- Running Suite: Tripartite XOR Parity (Nexus Recovery) ---")
        t_id, p_id, n_id = 101010, 999999, 907049 
        await self._run_case("Parity: Validate All 3 IDs (Integrity)", "verify_parity", {"topos_id_low32": t_id, "phase_id": p_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Recover Missing Phase ID", "verify_parity", {"topos_id_low32": t_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Recover Missing Topos ID", "verify_parity", {"phase_id": p_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Error on Insufficient Info (1 ID)", "verify_parity", {"nexus_id": n_id}, expected_success=False)

    async def _test_dependency_injection(self):
        log.info("\n--- Running Suite: Dependency Injection (State Override) ---")
        injected_payload = {"topo": 100, "press": 200, "rupture": True, "injected_anchor": 999999, "injected_tick": 77}
        await self._run_case("DI: Phase Generation with Injected State", "generate_phase_id", injected_payload, expected_success=True)

    async def _test_ffi_robustness(self):
        log.info("\n--- Running Suite: FFI Robustness & Zero-Copy Guard ---")
        malformed_payload = '{"topo": 50, "press": -10, "rupture": '
        await self._run_case("FFI Guard: Malformed JSON Payload (Expect Clean Error)", "generate_phase_id", malformed_payload, expected_success=False)

    async def _test_concurrency_and_pool_scaling(self):
        log.info("\n--- Running Suite: Concurrency & Interpreter Pool Scaling ---")
        request_count = 10
        target_func = "compute_root_fingerprint"
        payload = {"dummy_data": "Parallel Test"}
        log.info(f"Firing {request_count} concurrent requests to trigger dynamic spawning...")
        
        start_time = time.time()
        tasks = [self.broker.invoke(target_func, payload) for _ in range(request_count)]
        results = await asyncio.gather(*tasks)
        elapsed_ms = (time.time() - start_time) * 1000
        successes = sum(1 for r in results if r.success)
        
        if successes == request_count:
            log.info(f"  [PASS] Successfully handled {request_count} concurrent WASM executions in {elapsed_ms:.2f}ms.")
            self.success_count += 1
        else:
            log.error(f"  [FAIL] Concurrency test failed. Only {successes}/{request_count} succeeded.")
            self.fail_count += 1

    async def _test_fault_isolation(self):
        log.info("\n--- Running Suite: Supervisor Fault Isolation ---")
        
        toxic_result = await self.broker.invoke("non_existent_function", {})
        if not toxic_result.success:
            log.info("  [EXPECTED] Toxic request failed cleanly.")
        else:
            self.fail_count += 1
            return
            
        recovery_result = await self.broker.execute(code="print('I survived')")
        if recovery_result.success:
            log.info("  [PASS] System survived the crash and isolated the fault.")
            self.success_count += 1
        else:
            self.fail_count += 1