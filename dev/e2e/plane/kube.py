# fiber.dev.e2e.plane.kube
"""
@desc:
- CLI Control Plane for orchestrating Kubernetes-based CI pipeline validations.
- Validates internal shell workflows (WASM, VCR) via `kubectl exec`.
- Validates external ingress defenses (WAF, Audit) via Port-forwarding & SDK Client.
"""
import os
import sys
import argparse
import logging
import httpx
import random
from pathlib import Path
from typing import Any, List, Dict

from fiber.dev.sdk.gateway import DphiPublicClient, StrictPayloadFactory

from xphi.arch.dev.transport.sentinel import ChaosPayloadLibrary
from xphi.watcher.plane.infra.kube import KubeOrchestrator, KubeContext
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

FIBER_ROOT = resolve_path("fiber")
log = get_emitter("e2e.plane.kube")

DEFAULT_E2E_SUITE = [
    "VCR_MODE=replay python -m fiber.dev.ex.switch",
    "fiber e2e dphi.wasm.entry"
]

class KubeWorkflowScene:
    """
    Executes a pure Kubernetes runtime to cross-validate System E2E tests,
    ensuring topology binding, shell intent execution, and SDK-driven HTTP compliance.
    """
    def __init__(self, broker: Any = None, context: KubeContext = None):
        self.broker = broker
        self.context = context
        
        self.adapter = context.adapter if context else None
        self.auditors = context.auditors if context else {}
        
        self.fail_count = 0
        self.failed_cases: List[Dict[str, str]] = []
        self.log = get_emitter("scene.kube")

    # SCENE A: Internal Shell Execution (kubectl exec)
    async def phase_system_e2e_test(self):
        self.log.info("  ▶️ [TEST-A1] System E2E Job (Infrastructure & WASM Validation)")
        try:
            success = await self.adapter.apply_job(
                job_name="system-e2e-test", 
                env={
                    "XPHI_ENV": "ci",
                    "FIBER_E2E_STEPS": " && ".join(DEFAULT_E2E_SUITE) 
                }
            )
            
            if not success:
                raise RuntimeError("System E2E job fractured via kubectl exec. Check pod logs.")
                
            self.log.info("  └─ System E2E Test Passed ✅")
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "System E2E Test Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] System E2E Job: {e}")

    async def phase_build_release_audit(self):
        self.log.info("  ▶️ [TEST-A2] Release Build Job & Determinism Audit")
        try:
            success = await self.adapter.apply_job(
                job_name="build-release", 
                env={"FIBER_BUILD_DIST": "1"}
            )
            
            if not success:
                raise RuntimeError("Release Build job fractured. Artifact extraction failed.")
            
            if "determinism" in self.auditors:
                auditor = self.auditors["determinism"]
                is_clean = await auditor.verify()
                
                if not is_clean:
                    msg = "FATAL: Artifact Determinism check failed! Local paths detected."
                    self.log.error(f"  [FATAL_RUPTURE] {msg}")
                    raise RuntimeError(msg)
                else:
                    self.log.info("  └─ Artifact Boundary Intact: Wheel determinism verified ✅")
                    
        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "Release Build & Audit Job", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] Release Build Job: {e}")


    # SCENE B: External SDK Ingress Validation (Port Forwarding & Compliance)
    async def phase_compliance_sdk_test(self):
        self.log.info("  ▶️ [TEST-B1] Edge Compliance & WAF Audit (via Port Forwarding)")
        local_port = 8000
        
        try:
            # 1. Start Port Forwarding Tunnel
            tunnel_ok = await self.adapter.start_port_forward(
                local_port=local_port, target_port=8000, service_name="fiber-gateway"
            )
            if not tunnel_ok:
                raise RuntimeError("Failed to establish port-forwarding for SDK tests.")

            # 2. Initialize SDK Client pointing to local tunnel
            sdk_client = DphiPublicClient(base_url=f"http://127.0.0.1:{local_port}")

            # 3. Test 1: Telemetry Golden Path
            self.log.info("  ├─ Testing: OTLP Telemetry Seal (Golden Path)...")
            payload = StrictPayloadFactory.create_telemetry_payload(
                tenant_id="tenant-456", model_name="gpt-4o", prompt_tokens=150, completion_tokens=50
            )
            res = await sdk_client.push_telemetry(request=payload, payment_receipt="mock_valid_receipt")
            if res.get("status") != "success":
                raise RuntimeError("SDK failed to confirm telemetry success.")
            self.log.info(f"  │  └─ Sealed Successfully. Fingerprint: {res.get('fingerprint')}")

            # 4. Test 2: Chaos WAF Injection
            self.log.info("  ├─ Testing: WAF Defenses (Chaos Injection)...")
            attack_vectors = ChaosPayloadLibrary.get_all_vectors()
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{local_port}", timeout=5.0) as client:
                for vector_name, rule_list in attack_vectors:
                    chaos_payload = random.choice(rule_list)() if isinstance(rule_list, list) else rule_list()
                    chaos_res = await client.post("/v1/public/telemetry/logs", content=chaos_payload)
                    if chaos_res.status_code >= 500 or chaos_res.status_code < 400:
                        raise RuntimeError(f"Compliance WAF Breach! '{vector_name}' bypassed defenses.")
            self.log.info("  │  └─ WAF Defense fully operational against raw payloads.")
            
            self.log.info("  └─ Compliance SDK Tests Passed ✅")

        except Exception as e:
            self.fail_count += 1
            self.failed_cases.append({"title": "Edge Compliance SDK Test", "error": str(e)})
            self.log.error(f"  [SCENARIO HALTED] Edge Compliance Test: {e}")
        finally:
            # Explicitly stop tunnel to release socket for future steps if needed
            await self.adapter.stop_port_forward(local_port=local_port)

    async def run_all(self):
        self.log.info("\n=== [START] Executing KUBERNETES CI/CD Workflow Scenes ===")
        if not self.adapter:
            self.log.error("  └─ Kubernetes Runtime Availability: Failed 🔴")
            self.fail_count += 1
            return

        self.log.info("  └─ Kubernetes Runtime Availability: Confirmed 🟢")

        await self.phase_system_e2e_test()
        await self.phase_build_release_audit()
        await self.phase_compliance_sdk_test()
        
        if self.fail_count == 0:
            self.log.info("=== [DONE] All Workflow Scenes Passed Successfully ===")
        else:
            self.log.warning(f"=== [DONE] Workflow Scenes Completed with {self.fail_count} Failures ===")

class KubeFlow:
    """CLI Control Plane for orchestrating Kube-based CI pipeline validations."""
    def __init__(self, mode: str = "dev", keep_workspace: bool = False, rebuild: bool = False):
        self.mode = mode
        self.keep_workspace = keep_workspace
        self.rebuild = rebuild

    async def test(self):
        log.info(f"\n[PHASE 1] Initializing KUBERNETES Orchestrator in [{self.mode.upper()}] mode")
        
        original_cwd = Path.cwd()
        if FIBER_ROOT:
            os.chdir(FIBER_ROOT)
            log.info(f"  └─ Workspace Context Switched to FIBER_ROOT: {FIBER_ROOT}")

        try:
            controller = KubeOrchestrator(
                mode=self.mode,
                suites={"workflow_validation": KubeWorkflowScene},
                rebuild=self.rebuild,
                # Optional: Pass external redis url via base_env if targeting an existing cluster
                # base_env={"REDIS_URL": "redis://external-redis-cluster:6379/0"}
            )
            controller.keep_workspace = self.keep_workspace
            
            success, err_msg = await controller.execute(broker=None)
            
            log.info("\n" + "="*75)
            log.info(f"🚀 KUBERNETES CI/CD PIPELINE EXECUTION REPORT 🚀".center(75))
            log.info("="*75)
            
            if success:
                log.info(f"🟢 [SUCCESS] All Workflow Declarative Tests PASSED.")
                log.info("="*75 + "\n")
            else:
                log.critical(f"🔴 [FAILED] KUBERNETES CI Test execution terminated with errors.")
                if err_msg:
                     log.error(f"Reason: {err_msg}")
                sys.exit(1)
                
        finally:
            os.chdir(original_cwd)

    async def run(self):
        await self.test()


def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber CI/CD E2E Orchestrator via KUBERNETES (Minikube)")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev")
    parser.add_argument("--keep-workspace", action="store_true", help="Preserve K8s namespace and artifacts after run")
    parser.add_argument("--debug", action="store_true", help="Enable verbose internal logging")
    parser.add_argument("--rebuild", action="store_true", help="Force no-cache image build in Minikube")
    
    args, _ = parser.parse_known_args(args_list)
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal execution logging is ENABLED.")

    app = KubeFlow(mode=args.mode, keep_workspace=args.keep_workspace, rebuild=args.rebuild)
    PhaseReactor.ignite(main_coro_func=app.run)

if __name__ == "__main__":
    main()