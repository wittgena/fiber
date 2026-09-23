# fiber.infra.plane.kube
import os
import shutil
import socket
import asyncio
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Optional, Type

from xphi.arch.dev.tracer.kube import KubeTracer, KubeStatusAuditor
from xphi.arch.dev.tracer.base import SystemBound, PhaseOp, ExecutorOp
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.plane.infra.yaml.deployment import KubeBlueprint
from xphi.kernel.space.bind.resolver import resolve_path

COMPOSE_ROOT = resolve_path("time") / "compose"

class MinikubeNativeAdapter:
    def __init__(self, workspace: Path, boundary: SystemBound, artifact_dir: Path, 
                 namespace: str = "fiber-topos", rebuild: bool = False, base_env: Optional[Dict[str, str]] = None):
        self.log = get_emitter("infra.adapter")
        self.workspace = workspace
        self.boundary = boundary
        self.artifact_dir = artifact_dir
        self.namespace = namespace
        self.rebuild = rebuild
        
        self.workspace_str = str(self.workspace)
        self.manifest_file = self.workspace / "gateway.yaml"
        self.manifest_file_str = str(self.manifest_file)
        
        self._port_forward_processes: Dict[int, asyncio.subprocess.Process] = {}
        
        self.global_env = {
            "XPHI_ENV": "ci",
            "VCR_MODE": "live",
            "NODE_PROFILE": "EDGE"
        }
        if base_env:
            self.global_env.update(base_env)

        self.deploy_internal_redis = "REDIS_URL" not in self.global_env
        if self.deploy_internal_redis:
            self.global_env["REDIS_URL"] = "redis://fiber-tunnel:6379/0"

    def render_manifest(self) -> None:
        """YAML 매니페스트를 생성하고 저장합니다."""
        blueprint_yaml = KubeBlueprint.generate_manifest(
            namespace=self.namespace,
            global_env=self.global_env,
            deploy_redis=self.deploy_internal_redis
        )
        self.manifest_file.write_text(blueprint_yaml, encoding="utf-8")
        if self.deploy_internal_redis:
            self.log.info("  ├─ [K8s] Provisioning Internal Redis Tunnel...")
        else:
            self.log.info(f"  ├─ [K8s] Binding to External Redis: {self.global_env['REDIS_URL']}")

    async def _get_gateway_pod_name(self) -> str:
        cmd = ["kubectl", "get", "pods", "-n", self.namespace, "-l", "app=fiber-gateway", "-o", "jsonpath='{.items[0].metadata.name}'"]
        code, out, _ = await self.boundary.run_command(cmd, capture=True)
        return out.strip().replace("'", "") if code == 0 else ""

    async def start_port_forward(self, local_port: int = 8000, target_port: int = 8000, service_name: str = "fiber-gateway") -> bool:
        if local_port in self._port_forward_processes:
            self.log.warning(f"  ├─ [PortForward] Local port {local_port} is already being forwarded.")
            return True

        self.log.info(f"  ├─ [PortForward] Tunneling: 127.0.0.1:{local_port} -> svc/{service_name}:{target_port}")
        cmd = ["kubectl", "port-forward", f"svc/{service_name}", f"{local_port}:{target_port}", "-n", self.namespace]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=self.workspace_str
            )
            
            is_open = False
            for _ in range(30):
                await asyncio.sleep(0.1)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    if s.connect_ex(('127.0.0.1', local_port)) == 0:
                        is_open = True
                        break
                        
            if not is_open:
                self.log.error(f"  └─ 💥 [PortForward] Failed to bind local port {local_port} within timeout.")
                process.terminate()
                return False
                
            self._port_forward_processes[local_port] = process
            self.log.info("  └─ ✨ [PortForward] Tunnel established successfully.")
            return True
            
        except Exception as e:
            self.log.error(f"  └─ 💥 [PortForward] Failed: {e}")
            return False

    async def stop_port_forward(self, local_port: int = None):
        ports_to_close = [local_port] if local_port else list(self._port_forward_processes.keys())
        for p in ports_to_close:
            process = self._port_forward_processes.pop(p, None)
            if process:
                self.log.info(f"  ├─ [PortForward] Closing tunnel on port {p}...")
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    self.log.warning(f"  ├─ [PortForward] Process on port {p} did not exit cleanly, killing it.")
                    process.kill()

    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        self.log.info(f"[Adapter:KUBE] Executing Job Phase: {job_name}")
        
        pod_name = await self._get_gateway_pod_name()
        if not pod_name:
            self.log.error("  └─ 💥 Gateway Pod not found.")
            return False

        if job_name == "system-e2e-test" or "FIBER_E2E_STEPS" in env:
            exec_command = env.get("FIBER_E2E_STEPS", "fiber e2e dphi.wasm.phase && VCR_MODE=replay fiber ex switch")
        elif job_name == "build-release":
            exec_command = "python -m build --wheel && cp dist/*.whl /artifact_mount/"
        else:
            self.log.error(f"  └─ Unknown job phase: {job_name}")
            return False

        env_exports = " ".join([f"export {k}='{v}';" for k, v in env.items()])
        full_command = f"{env_exports} {exec_command}"
        
        for k, v in env.items():
            self.log.info(f"  ├─ Injecting Env Override: {k}={v}")

        cmd = ["kubectl", "exec", "-i", pod_name, "-n", self.namespace, "--", "bash", "-c", full_command]
        code, _, _ = await self.boundary.run_command(cmd, cwd=self.workspace_str, capture=False)
        
        if code != 0:
            self.log.error(f"[Adapter:KUBE] Phase '{job_name}' fractured.")
            return False

        if job_name == "build-release":
            self.log.info("  ├─ Extracting built artifacts from K8s pod volume...")
            cp_cmd = ["kubectl", "cp", f"{self.namespace}/{pod_name}:/artifact_mount/.", str(self.artifact_dir)]
            cp_code, _, _ = await self.boundary.run_command(cp_cmd, capture=True)
            if cp_code != 0:
                 self.log.error("  └─ 💥 Failed to extract artifacts via kubectl cp.")
                 return False

        self.log.info(f"  └─ Phase '{job_name}' completed successfully.")
        return True

    async def teardown(self):
        await self.stop_port_forward()
        self.log.info(f"  ├─ [K8s] Tearing down Kube topology (Namespace: {self.namespace})...")
        cmd = ["kubectl", "delete", "namespace", self.namespace, "--ignore-not-found=true"]
        await self.boundary.run_command(cmd, capture=False)

class DeterminismAuditor:
    def __init__(self, target_dir: Path, boundary: SystemBound):
        self.target_dir = target_dir
        self.boundary = boundary
        self.log = get_emitter("auditor.determinism")
        self.is_clean = False

    def attach(self): pass 
    def detach(self): pass

    async def verify(self) -> bool:
        self.log.info(f"[Auditor:Determinism] Inspecting extracted Artifacts in {self.target_dir}...")
        wheel_files = list(self.target_dir.rglob("fiber-*.whl"))
        
        if not wheel_files:
            self.log.error("  └─ 💥 Artifact missing. Wheel not found in the extracted directory.")
            return False
            
        target_wheel = wheel_files[0]
        cmd = f"unzip -p {target_wheel} fiber-*/METADATA | grep 'Requires-Dist: xphi'"
        code, out, _ = await self.boundary.run_command(["bash", "-c", cmd], capture=True)
        
        if code != 0:
            self.log.error("  └─ 💥 Metadata extraction failed. Zip structure might be corrupted.")
            return False
            
        metadata_line = out.strip()
        self.log.info(f"  ├─ Target Wheel: {target_wheel.name}")
        self.log.info(f"  ├─ Extracted Dependency: {metadata_line}")
        
        if "file://" in metadata_line:
            self.log.error("  └─ 💥 FATAL BREACH: Local absolute path leaked into Distribution Artifact!")
            self.is_clean = False
            return False
            
        self.log.info("  └─ ✨ AUDIT PASSED: Remote Binding Enforced mathematically.")
        self.is_clean = True
        return True

@dataclass
class KubeContext:
    boundary: SystemBound
    adapter: MinikubeNativeAdapter
    auditors: Dict[str, Any]

class KubeOrchestrator(KubeTracer):
    def __init__(self, mode: str = "dev", suites: Dict[str, Type] = None, rebuild: bool = False, base_env: Dict[str, str] = None):
        super().__init__(tracer_name="kube_orchestrator", timeout=600)
        
        self.mode = mode
        self.suites = suites or {}
        self.keep_workspace = False
        
        self.workspace = COMPOSE_ROOT
        self.workspace.mkdir(parents=True, exist_ok=True) 
        
        self.artifact_dir = Path(tempfile.mkdtemp(prefix="fiber-kube-artifacts-"))
        os.environ["FIBER_ARTIFACT_MOUNT"] = str(self.artifact_dir)
        self.adapter = MinikubeNativeAdapter(
            workspace=self.workspace, 
            boundary=self.boundary, 
            artifact_dir=self.artifact_dir,
            rebuild=rebuild,
            base_env=base_env
        )
        self.determinism_auditor = DeterminismAuditor(self.artifact_dir, self.boundary)
        self.workspace_str = self.adapter.workspace_str
        self.manifest_file_str = self.adapter.manifest_file_str
        self.namespace_str = self.adapter.namespace
        
        self.status_auditor = KubeStatusAuditor(
            target="kube_pods", 
            boundary=self.boundary, 
            namespace=self.adapter.namespace, 
            delay=10
        )
        self.register_auditors(self.status_auditor)
        
        self.suite_runners = {}

    async def _run_all_suites(self, broker: Any, context: KubeContext) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            self.log.info(f"\n>>> [PHASE] Executing K8s CI Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker=broker, context=context)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                self.log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self) -> Tuple[bool, str]:
        """BaseTracer의 trace() 내부에서 실행되는 메인 로직"""
        self.log.info(f"\n--- [START] Orchestrating CI/CD Plane via Kubernetes ({self.mode.upper()}) ---")
        self.log.info(f"[SYSTEM] Artifact Extraction Target: {self.artifact_dir}")
        
        try:
            self.determinism_auditor.attach()
            context = KubeContext(
                boundary=self.boundary,
                adapter=self.adapter,
                auditors={"determinism": self.determinism_auditor}
            )
            
            self.adapter.render_manifest()
            dockerfile_path = self.workspace / "Dockerfile.xphi" if (self.workspace / "Dockerfile.xphi").exists() else self.workspace / "Dockerfile"
            self.dockerfile_path_str = str(dockerfile_path)

            build_cmd = ["minikube", "image", "build", "-t", "fiber-node:local", "-f", "{dockerfile_path_str}", "{workspace_str}"]
            if self.adapter.rebuild:
                build_cmd.append("--no-cache")

            apply_cmds = [
                build_cmd,
                ["kubectl", "create", "namespace", "{namespace_str}"],
                ["kubectl", "apply", "-f", "{manifest_file_str}", "-n", "{namespace_str}"]
            ]

            if not await ExecutorOp.run_sequence(self, apply_cmds, phase_name="K8s.Provision", cwd=self.workspace_str, strict=True):
                return False, "Infrastructure provisioning sequence failed."

            self.log.info("  ├─ [K8s] Awaiting Pod Readiness (Timeout: 180s)...")
            wait_cmd = ["kubectl", "wait", "--for=condition=Ready", "pod", "--all", "-n", self.namespace_str, "--timeout=180s"]
            code, _, _ = await self.boundary.run_command(wait_cmd, capture=False)
            
            if code != 0:
                self.rupture_confirmed = True
                return False, "Pods failed to reach Ready state within timeout."
                
            self.log.info("  ├─ Runtime Topology (KUBE Mode) Ready ✅")
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    total_fails = await self._run_all_suites(broker=None, context=context)
                    if total_fails > 0:
                        self.rupture_confirmed = True
                        return False, f"Topology verified, but {total_fails} logical E2E tests failed."
            
            self.equilibrium_achieved = True
            return True, ""
            
        except Exception as e:
            self.log.error(f"[FATAL] Orchestration process crashed: {e}")
            self.rupture_confirmed = True
            return False, str(e)
            
        finally:
            self.log.info("\n[SYSTEM] Initiating Teardown Sequence...")
            self.determinism_auditor.detach()
            if not self.keep_workspace:
                await self.adapter.teardown()
                if self.artifact_dir.exists():
                    shutil.rmtree(self.artifact_dir, ignore_errors=True)
                    self.log.info(f"  └─ Cleaned up temporary artifact directory: {self.artifact_dir}")
            else:
                self.log.info(f"  └─ Workspace preserved. Inspect via: kubectl get pods -n {self.adapter.namespace}")
            
            self.log.info("[SYSTEM] Kube Orchestration finalized.")