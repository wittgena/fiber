# dphi.net.tracer
## @lineage: dphi.wasm.tracer
import sys
import asyncio
import base64
import json
from pathlib import Path
from typing import Tuple, Optional
from dataclasses import asdict

from kernel.bind.resolver import resolve_path
from kernel.bind.inter.wasm import WasmInterpreter
from kernel.dphi.wasm.builder import WasmBuilder

from kernel.dphi.adapter.sign import LedgerAuthAdapter
from kernel.dphi.cgroup import CgroupPolicy
from kernel.dphi.ledger.consensus import KernelLedger, KernelCommit, LedgerRole
from watcher.tracer.bound import BaseTracer
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.tracer")

TIME_ROOT = resolve_path("time")
DEST_WASM_FILE = TIME_ROOT / "dphi.wasm"
REGISTRY_FILE = TIME_ROOT / "registry.json"
STREAM_ID = "wasm_binary_lineage"
PROOF_STREAM_ID = "wasm_compute_proofs" 

class WasmTracer(BaseTracer):
    def __init__(self, timeout: int = 300, tester=None):
        super().__init__(tracer_name="wasm.tracer", timeout=timeout)
        self.store = KernelLedger()
        self.tester = tester
        self.current_lineage_hash = None 

    async def verify_cache_or_build(self) -> Tuple[bool, str]:
        head_hash = self.store.get_head_hash(STREAM_ID) or "0000000"
        registry = {}
        if REGISTRY_FILE.exists():
            with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                registry = json.load(f)

        needs_build = True

        if DEST_WASM_FILE.exists() and registry.get("lineage_hash") == head_hash:
            self.log.info("[Ledger] Local registry matches KernelStore. Verifying binary integrity...")
            try:
                with open(DEST_WASM_FILE, "rb") as f:
                    wasm_b64 = base64.b64encode(f.read()).decode('utf-8')

                sys_policy = CgroupPolicy.system()
                temp_interp = WasmInterpreter(str(DEST_WASM_FILE), policy=sys_policy)
                
                payload_data = json.dumps({"parent_hash": registry.get("parent_hash", "0000000"), "wasm_base64": wasm_b64})
                result = temp_interp.invoke("verify_build_lineage", payload_data)

                if result.success:
                    res_data = json.loads(result.output)
                    if res_data.get("lineage_hash") == head_hash:
                        self.log.info(f"[Ledger] Integrity verified. Hash: {head_hash[:8]}")
                        self.current_lineage_hash = head_hash
                        needs_build = False
            except Exception as e:
                self.log.error(f"[ERROR] Verification crashed: {e}")

        if needs_build:
            self.log.info("[STATUS] Cache miss/invalid. Initiating Builder...")
            builder = WasmBuilder()
            await builder.execute()
            if builder.rupture_confirmed:
                return False, builder.build_error

            self.log.info("[Ledger] Registering new WASM lineage...")
            with open(DEST_WASM_FILE, "rb") as f:
                new_wasm_b64 = base64.b64encode(f.read()).decode('utf-8')

            sys_policy = CgroupPolicy.system()
            temp_interp = WasmInterpreter(str(DEST_WASM_FILE), policy=sys_policy)
            payload_data = json.dumps({"parent_hash": head_hash, "wasm_base64": new_wasm_b64})
            result = temp_interp.invoke("verify_build_lineage", payload_data)

            if result.success:
                res_data = json.loads(result.output)
                new_hash = res_data["lineage_hash"]
                self.current_lineage_hash = new_hash 
                
                commit = KernelCommit(
                    stream_id=STREAM_ID, 
                    executable_payload="WASM Build Transition",
                    tension_at_seal=0.0, 
                    blob_hashes=[], 
                    parent_hash=head_hash
                )
                
                try:
                    signature_hex = LedgerAuthAdapter.sign_state_payload(asdict(commit))
                    
                    if hasattr(self.store, 'role') and self.store.role == LedgerRole.FOLLOWER:
                        self.log.warning("[Ledger] Node is FOLLOWER. Bypassing physical disk seal and proposing to Mempool.")
                        self.store._put_object("commit_proposal", asdict(commit))
                        self.store.update_head(STREAM_ID, new_hash)
                    else:
                        self.store.seal_system_epoch(
                            commit=commit,
                            signatures=[signature_hex],
                            threshold=1
                        )
                        self.store.update_head(STREAM_ID, new_hash)
                    
                except PermissionError as pe:
                    self.log.error(f"[FATAL] WASM Lineage commit rejected by Multi-Sig Guard: {pe}")
                    return False, f"Privilege Denied: {pe}"

                with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                    reg_data = json.load(f)
                reg_data["parent_hash"] = head_hash
                reg_data["lineage_hash"] = new_hash
                with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
                    json.dump(reg_data, f, indent=4)
                self.log.info(f"[Ledger] New WASM lineage sealed: {new_hash[:8]}")
            else:
                return False, f"Lineage generation failed: {result.error}"

        return True, ""

    async def orchestrate_tests(self) -> Tuple[bool, str]:
        self.log.info("\n--- [START] Delegating Orchestration to WasmTester ---")
        try:
            if not self.tester:
                from kernel.dphi.wasm.tester.dphi import WasmTester
                self.log.info("[SYSTEM] Initializing default WasmTester...")
                self.tester = WasmTester(
                    wasm_module_path=str(DEST_WASM_FILE),
                    sandbox_root=str(TIME_ROOT)
                )
            else:
                self.log.info("[SYSTEM] Using injected WasmTester instance.")
            
            return await self.tester.execute()
            
        except Exception as e:
            self.log.error(f"[FATAL] Failed to delegate orchestration: {e}")
            return False, str(e)

    def _gather_telemetry_context(self) -> dict:
        context = {}
        if not self.tester:
            return context

        if hasattr(self.tester, 'auditors'):
            for auditor in self.tester.auditors:
                if auditor.__class__.__name__ == "WasmTelemetryAuditor":
                    context["max_recursion_depth"] = getattr(auditor, 'max_depth', 0)
                    context["health_flag"] = getattr(auditor, 'health_flag', 0)
                elif auditor.__class__.__name__ == "WasmEntropyAuditor":
                    context["fuel_consumed"] = getattr(auditor, 'total_fuel_used', 0)
                    
        # context["canonical_events_captured"] = getattr(self.tester, 'canonical_events_captured', 0)
        context["canonical_events_captured"] = getattr(self.tester, 'canonical_events_captured', 0)
        return context

    async def seal_test_proof(self) -> None:
        proof_hash = getattr(self.tester, "test_execution_hash", None)
        if not proof_hash or not self.current_lineage_hash:
            err_msg = "Critical Structural Fault: Test execution succeeded but Canonical Proof Hash is missing."
            self.log.error(f"[Ledger] {err_msg}")
            raise RuntimeError(err_msg)

        self.log.info(f"[Ledger] Binding Canonical Proof ({proof_hash[:8]}) to WASM Code ({self.current_lineage_hash[:8]})...")
        
        if REGISTRY_FILE.exists():
            with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                reg_data = json.load(f)
            
            proofs = reg_data.get("execution_proofs", [])
            if proof_hash not in proofs:
                proofs.append(proof_hash)
            reg_data["execution_proofs"] = proofs
            reg_data["latest_proof"] = proof_hash
            
            with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
                json.dump(reg_data, f, indent=4)
                
        commit = KernelCommit(
            stream_id=PROOF_STREAM_ID,
            executable_payload="Canonical Test Execution Proof",
            tension_at_seal=0.0,
            blob_hashes=[self.current_lineage_hash, proof_hash], 
            parent_hash=self.store.get_head_hash(PROOF_STREAM_ID) or "0000000"
        )
        try:
            signature_hex = LedgerAuthAdapter.sign_state_payload(asdict(commit))
            if hasattr(self.store, 'role') and self.store.role != LedgerRole.FOLLOWER:
                 self.store.seal_system_epoch(commit=commit, signatures=[signature_hex], threshold=1)
                 self.store.update_head(PROOF_STREAM_ID, proof_hash)
                 self.log.info("[Ledger] Canonical Proof Inscription Successful.")
        except Exception as e:
            self.log.warning(f"[Ledger] Proof Inscription Failed (Non-fatal): {e}")

    async def execute(self) -> None:
        self.log.crit("## @trace.init: WASM Lifecycle (AgentBot Removed)")
        build_ok, build_err = await self.verify_cache_or_build()
        
        if build_ok:
            test_ok, test_err = await self.orchestrate_tests()
            if test_ok:
                try:
                    await self.seal_test_proof()
                    self.log.info("## @trace.success: System stabilized.")
                    self.log.info(f"Final Telemetry: {self._gather_telemetry_context()}")
                    return
                except Exception as e:
                    last_error = str(e)
            else:
                last_error = test_err
        else:
            last_error = build_err

        self.log.crit("## @trace.collapse: COMPILATION OR TEST FAILED.")
        self.log.error(f"[FATAL] Module: wasm.tracer | Reason: Process failed without self-healing agent.")
        self.log.error(f"Last Error: {last_error[-1000:]}")
        self.log.error(f"Final Telemetry: {self._gather_telemetry_context()}")
        self.rupture_confirmed = True