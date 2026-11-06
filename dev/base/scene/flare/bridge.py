# fiber.dev.base.scene.flare.bridge
import time
import json
from typing import Any

from fiber.dev.base.scene.runner import SandboxRunner

from xphi.arch.bound.adapter.pta import compute_merkle_root
from xphi.arch.contract.config.env import config as env_config
from xphi.node.runtime.wasm.broker import WasmBroker
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.flare.tunnel import FlareTunnelFactory

log = get_emitter("scene.flare.bridge")


class FlareBridgeScene(SandboxRunner):
    """Validates XPHI_WASM_PLANE config auto-binding and Local <-> Edge Bridge routing parity."""
    def __init__(self, broker: Any):
        super().__init__(broker)
        self.collected_edge_hashes = []

    def _harvest_tx_hash(self, res: Any) -> bool:
        edge_hash = getattr(res, "edge_hash", None)
        if edge_hash and edge_hash != "HASH_GENERATION_FAILED":
            self.collected_edge_hashes.append(edge_hash)
            return True
        return False

    async def run_all(self):
        log.info("\n" + "=" * 70)
        log.info("[START] LOCAL <-> EDGE BRIDGE & XPHI_WASM_PLANE BINDING SUITE")
        log.info("=" * 70)

        await self._test_env_plane_auto_binding()
        await self._test_control_and_bridge_routing()
        await self._test_bridge_merkle_alignment()

        self.report()
        return {
            "status": "success" if getattr(self, "fail_count", 0) == 0 else "failed",
            "passed_tests": getattr(self, "pass_count", 0),
            "failed_tests": getattr(self, "fail_count", 0),
        }

    async def _test_env_plane_auto_binding(self):
        log.info("\n--- [Bridge 1] XPHI_WASM_PLANE Config Resolution & Singleton Isolation ---")
        t0 = time.perf_counter()
        prev_val = env_config.get("XPHI_WASM_PLANE", None)
        try:
            env_config.set_override("XPHI_WASM_PLANE", "flare:http://127.0.0.1:8787")
            auto_broker = WasmBroker.get_shared(timeout=15.0)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if auto_broker.tunnel_factory is FlareTunnelFactory:
                self._record_success(
                    elapsed_ms,
                    f"WasmBroker.get_shared() auto-bound to FlareTunnelFactory via XPHI_WASM_PLANE ({auto_broker._plane_key}).",
                )
            else:
                self._record_fail(
                    elapsed_ms,
                    f"Expected FlareTunnelFactory, got {auto_broker.tunnel_factory}",
                    "Plane Auto-Binding",
                )
        finally:
            if prev_val is not None:
                env_config.set_override("XPHI_WASM_PLANE", prev_val)

    async def _test_control_and_bridge_routing(self):
        log.info("\n--- [Bridge 2] Control Channel, PHASE Wasm ABI & PYTHON Service Binding ---")

        # 1. Control Channel Sync (publish -> host boundary ack)
        t0 = time.perf_counter()
        policy_ok = await self.broker.update_policy(tier="SYSTEM")
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if policy_ok:
            self._record_success(elapsed_ms, "Control Channel policy sync ('update_policy') acknowledged across bridge.")
        else:
            self._record_fail(elapsed_ms, "Control Channel policy sync timed out or failed.", "Control Bridge")

        # 2. PHASE Target: Direct Wasm ABI Invocation (phase.wasm -> invoke_wasm)
        t0 = time.perf_counter()
        res_phase = await self.broker.invoke(
            target_func="verify_parity",
            payload={"topos_id_low32": 101010, "phase_id": 999999, "nexus_id": 907049},
            wasm_path="phase.wasm",
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if res_phase.success and "is_valid" in str(res_phase.output):
            self._record_success(elapsed_ms, f"PHASE VM (phase.wasm) direct ABI call verified: {res_phase.output.strip()}")
        else:
            self._record_fail(elapsed_ms, f"PHASE VM routing failed: {getattr(res_phase, 'error', res_phase.output)}", "PHASE Bridge")

        # 3. PHASE Target: Direct Fingerprint Seal Generation (phase.wasm -> compute_fingerprint)
        t0 = time.perf_counter()
        res_fp = await self.broker.invoke(
            target_func="compute_fingerprint",
            payload=[{"action": "bridge_verify", "tier": "EDGE_WASM", "fuel": 100, "mem_usage": 512}],
            wasm_path="phase.wasm",
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if res_fp.success and "fingerprint" in str(res_fp.output):
            try:
                parsed = json.loads(res_fp.output)
                data_obj = json.loads(parsed["data"]) if isinstance(parsed.get("data"), str) else parsed.get("data", {})
                fp_hash = data_obj.get("fingerprint")
                if fp_hash:
                    self.collected_edge_hashes.append(fp_hash)
            except Exception:
                pass
            self._record_success(elapsed_ms, "PHASE VM cryptographic fingerprint generation verified.")
        else:
            self._record_fail(elapsed_ms, f"PHASE VM fingerprint call failed: {getattr(res_fp, 'error', '')}", "PHASE Fingerprint")

        # 4. PYTHON Target: Service Binding (router.ts -> PYTHON_ENGINE -> phase.wasm X-XPHI-Canonical-Hash)
        t0 = time.perf_counter()
        res_py = await self.broker.execute(
            code="print(bridge_var * 2)",
            variables={"bridge_var": 21},
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if res_py.success and res_py.output.strip() == "42" and self._harvest_tx_hash(res_py):
            self._record_success(
                elapsed_ms,
                f"PYTHON VM Service Binding & X-XPHI-Canonical-Hash seal verified (output=42, hash={res_py.edge_hash[:12]}...).",
            )
        else:
            self._record_fail(
                elapsed_ms,
                f"PYTHON VM bridge mismatch: output={res_py.output}, hash={getattr(res_py, 'edge_hash', None)}",
                "PYTHON Bridge",
            )

    async def _test_bridge_merkle_alignment(self):
        log.info("\n--- [Bridge 3] Local-Edge Transaction Hash Rollup ---")
        t0 = time.perf_counter()
        if not self.collected_edge_hashes:
            self._record_fail(0.0, "No edge hashes harvested across bridge transactions.", "Bridge Merkle Rollup")
            return

        root_hash = compute_merkle_root(self.collected_edge_hashes)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if root_hash:
            self.broker.epoch_root_hash = root_hash
            self._record_success(elapsed_ms, f"Bridge transactions sealed into Local PTA Merkle Root: {root_hash[:16]}...")
        else:
            self._record_fail(elapsed_ms, "Failed to compute Merkle Root from bridge hashes.", "Bridge Merkle Rollup")