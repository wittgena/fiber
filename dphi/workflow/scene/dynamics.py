# fiber.dphi.workflow.scene.dynamics
## @lineage: fiber.workflow.scene.dynamics
import math
import json
from dataclasses import dataclass
from typing import Any, Dict

from fiber.kernel.debug.sandbox import SandboxRunner
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("scene.dynamics")

class DynamicsScene(SandboxRunner):
    """
    @spec: Tests the Rust-native O(N^2) Math Kernels for correct Phase calculations,
           wrapping behaviors, and system boundary conditions.
    """
    def __init__(self, broker: Any):
        super().__init__(broker)

    async def run_all(self):
        log.info("\n=======================================================")
        log.info("🌌 [START] Field Dynamics & Continuous Math Kernel Suite")
        log.info("=======================================================\n")

        # 고비용 연산이므로 SYSTEM Tier 사용
        await self._set_worker_policy("SYSTEM")

        await self._test_kuramoto_sync()
        await self._test_fitzhugh_spiking()
        await self._test_attractor_reflector_bypass()
        await self._test_dynamics_payload_robustness()

        self.report()

    # -------------------------------------------------------------------------
    # Domain A: Kuramoto Model (Phase Sync & Tension)
    # -------------------------------------------------------------------------
    async def _test_kuramoto_sync(self):
        log.info("\n--- [Kernel] Kuramoto Oscillator Sync ---")
        
        payload = {
            "states": {
                "node_1": {"phase": 0.0, "omega": 1.0, "state": "NORMAL", "tension": 0.0, "is_spiking": False},
                "node_2": {"phase": math.pi, "omega": 1.0, "state": "NORMAL", "tension": 0.0, "is_spiking": False},
            },
            "kernel_type": "kernel.kuramoto",
            "params": {"global_coupling": 1.0},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(
            target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, 
            payload=json.dumps(payload),
            tier="SYSTEM"
        )
        
        if res.success:
            deltas = json.loads(res.output)
            # node_1(0)과 node_2(pi)의 위상차는 pi. sin(pi)는 0이므로 coupling_force는 0이어야 함
            # 따라서 d_phase = (omega + 0) * dt = 1.0 * 0.1 = 0.1
            d_phase_n1 = deltas.get("node_1", {}).get("d_phase")
            if d_phase_n1 is not None and math.isclose(d_phase_n1, 0.1, abs_tol=1e-5):
                self._record_success(0, f"Kuramoto Math Verified. d_phase: {d_phase_n1:.4f}")
            else:
                self._record_fail(0, f"Unexpected d_phase output: {deltas}", "Kuramoto Sync")
        else:
            self._record_fail(0, f"WASM execution failed: {res.error}", "Kuramoto Sync")

    # -------------------------------------------------------------------------
    # Domain B: FitzHugh-Nagumo Model (Spiking & Recovery)
    # -------------------------------------------------------------------------
    async def _test_fitzhugh_spiking(self):
        log.info("\n--- [Kernel] FitzHugh-Nagumo Action Potential ---")
        
        payload = {
            "states": {
                # 높은 Phase(V)를 주어 강제 스파이킹 유발 (v = phase - pi > 1.0 이 되도록)
                "node_1": {"phase": math.pi + 2.0, "omega": 0.5, "state": "NORMAL", "tension": 0.0, "recovery": 0.0, "is_spiking": False},
            },
            "kernel_type": "kernel.fitzhugh",
            "params": {"global_coupling": 0.5, "a": 0.7, "b": 0.8, "fh_epsilon": 0.08},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=json.dumps(payload), tier="SYSTEM")
        
        if res.success:
            deltas = json.loads(res.output)
            is_spiking = deltas.get("node_1", {}).get("is_spiking", False)
            tension = deltas.get("node_1", {}).get("target_tension", 0.0)
            
            if is_spiking and tension == 100.0:
                self._record_success(0, "FHN Spiking logic and high tension (100.0) accurately triggered.")
            else:
                self._record_fail(0, f"Spike detection failed: {deltas}", "FitzHugh-Nagumo Spike")
        else:
            self._record_fail(0, f"WASM execution failed: {res.error}", "FitzHugh-Nagumo Spike")

    # -------------------------------------------------------------------------
    # Domain C: Topological Roles (Bypass Math)
    # -------------------------------------------------------------------------
    async def _test_attractor_reflector_bypass(self):
        log.info("\n--- [Topology] Attractor / Reflector Bypass ---")
        
        payload = {
            "states": {
                "node_attractor": {"phase": 0.0, "omega": 2.0, "state": "ATTRACTOR", "tension": 5.0, "is_spiking": False},
                "node_reflector": {"phase": 0.0, "omega": 2.0, "state": "REFLECTOR", "tension": 5.0, "is_spiking": False},
            },
            "kernel_type": "kernel.kuramoto",
            "params": {},
            "dt": 0.1
        }
        
        res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=json.dumps(payload), tier="SYSTEM")
        
        if res.success:
            deltas = json.loads(res.output)
            attractor_dphase = deltas.get("node_attractor", {}).get("d_phase", 0.0)
            reflector_dphase = deltas.get("node_reflector", {}).get("d_phase", 0.0)
            
            # Attractor: omega * 0.1 * dt = 2.0 * 0.1 * 0.1 = 0.02
            # Reflector: omega * 1.5 * dt = 2.0 * 1.5 * 0.1 = 0.3
            if math.isclose(attractor_dphase, 0.02) and math.isclose(reflector_dphase, 0.3):
                self._record_success(0, "Attractor & Reflector correctly bypassed O(N^2) interactions.")
            else:
                self._record_fail(0, f"Bypass multiplier incorrect. Output: {deltas}", "Role Bypass")
        else:
            self._record_fail(0, f"Execution failed: {res.error}", "Role Bypass")

    # -------------------------------------------------------------------------
    # Domain D: Robustness & FFI Guard
    # -------------------------------------------------------------------------
    async def _test_dynamics_payload_robustness(self):
        log.info("\n--- [Guard] Dynamics Payload FFI Boundary ---")
        
        bad_payloads = [
            ("Malformed JSON", '{"states": {"node_1": }, "kernel_type": "kuramoto"}'),
            ("Missing Type", '{"states": {}, "params": {}, "dt": 0.1}'),
        ]
        
        for title, raw_str in bad_payloads:
            res = await self.broker.invoke(target_func=DphiMethod.PROCESS_FIELD_DYNAMICS, payload=raw_str, tier="SYSTEM")
            if not res.success and "Invalid DynamicsPayload" in str(res.error) or "malformed" in str(res.error):
                self._record_success(0, f"Correctly trapped bad payload: {title}")
            else:
                self._record_fail(0, f"Failed to guard against: {title}", "Dynamics Payload Guard")