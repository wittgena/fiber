# fiber.infra.plane.compose
## @lineage: fiber.dev.infra.plane.compose
## @lineage: xphi.watcher.plane.infra.compose
import os
import shutil
import asyncio
import tempfile
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Tuple

from xphi.arch.dev.tracer.base import SystemBound
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.kernel.space.bind.resolver import resolve_path

COMPOSE_ROOT = resolve_path("time") / "compose"
log = get_emitter("infra.compose")

# Self-bootstrapping Topology Blueprint (Local source mounts removed for pure USER mode)
DOCKER_COMPOSE_BLUEPRINT = """\
services:
  redis:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  test-runner:
    build:
      context: .
      dockerfile: ${COMPOSE_ROOT}/Dockerfile.xphi
    volumes:
      - ${FIBER_ARTIFACT_MOUNT:-/tmp}:/artifact_mount
    environment:
      - XPHI_ENV=ci
    
    network_mode: "service:redis"
    depends_on:
      redis:
        condition: service_healthy
    command: sleep infinity
"""

class ComposeBlueprint:
    @staticmethod
    def get_ci_spec() -> Dict[str, Dict[str, Any]]:
        return {
            "system-e2e-test": {
                "env": {"XPHI_ENV": "ci", "VCR_MODE": "replay"}, 
                "description": "System E2E validation via Docker Compose (USER Mode)"
            },
            "build-release": {
                "env": {"FIBER_BUILD_DIST": "1"}, 
                "description": "Deterministic remote binding for Wheel artifact"
            }
        }

class BaseComposeAdapter:
    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        raise NotImplementedError

class DockerComposeAdapter(BaseComposeAdapter):
    def __init__(self, workspace: Path, boundary: SystemBound, artifact_dir: Path, rebuild: bool = False):
        self.workspace = workspace
        self.boundary = boundary
        self.artifact_dir = artifact_dir
        self.rebuild = rebuild
        
        if not shutil.which("docker-compose") and not shutil.which("docker"):
            raise RuntimeError("[CRITICAL] 'docker-compose' binary not found.")
            
        self.compose_file = COMPOSE_ROOT / "docker-compose.test.yml"

    def _sync_compose_blueprint(self):
        """Validates and updates the compose topology blueprint against the cache."""
        COMPOSE_ROOT.mkdir(parents=True, exist_ok=True)
        
        needs_update = True
        if self.compose_file.exists():
            existing_content = self.compose_file.read_text(encoding="utf-8")
            if existing_content.strip() == DOCKER_COMPOSE_BLUEPRINT.strip():
                needs_update = False
                
        if needs_update:
            log.info(f"  ├─ 📝 Synthesizing topology blueprint at {self.compose_file.name}")
            self.compose_file.write_text(DOCKER_COMPOSE_BLUEPRINT, encoding="utf-8")
        else:
            log.info("  ├─ 📝 Topology blueprint matches cache.")

    async def _provision_topology(self) -> bool:
        """Bootstraps background infrastructure."""
        self._sync_compose_blueprint()
        
        os.environ["COMPOSE_ROOT"] = str(COMPOSE_ROOT)
        
        # --- NEW CACHE-BUSTING LOGIC ---
        # If rebuild is requested, forcefully build without cache before bringing up
        if self.rebuild:
            log.info(f"  ├─ [Force Rebuild] Igniting topology build without cache...")
            build_cmd = [
                "docker-compose", 
                "-f", str(self.compose_file), 
                "--project-directory", str(self.workspace),
                "build", "--no-cache"
            ]
            code, _, _ = await self.boundary.run_command(build_cmd, cwd=str(self.workspace), capture=False)
            if code != 0:
                log.error("  └─ 💥 Topology Build (no-cache) Failed. Inspect Docker logs.")
                return False
            log.info("  ├─ Clean Build Completed.")
        # -------------------------------

        log.info(f"  ├─ Provisioning Topology from {self.compose_file.name}...")
        
        cmd = [
            "docker-compose", 
            "-f", str(self.compose_file), 
            "--project-directory", str(self.workspace),
            "up", "-d"
        ]
        
        log.info("  ├─ [Streaming Infrastructure Provisioning Logs...]")
        code, _, _ = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        
        if code != 0:
            log.error("  └─ 💥 Topology Boot Failed. Inspect Docker logs.")
            return False
            
        log.info("  ├─ Runtime Topology (USER Mode) Ready ✅")
        return True

    async def apply_job(self, job_name: str, env: Dict[str, str]) -> bool:
        log.info(f"[Adapter:COMPOSE] Executing Job Phase: {job_name}")
        
        if not await self._provision_topology():
            return False

        env_vars = []
        for k, v in env.items():
            env_vars.extend(["-e", f"{k}={v}"])
            log.info(f"  ├─ Injecting Env: {k}={v}")

        if job_name == "system-e2e-test" or "FIBER_E2E_STEPS" in env:
            exec_command = env.get("FIBER_E2E_STEPS", "fiber e2e dphi.wasm.phase && VCR_MODE=replay fiber ex switch")
        elif job_name == "build-release":
            exec_command = (
                "python -m build --wheel && "
                "cp dist/*.whl /artifact_mount/"
            )
        else:
            log.error(f"  └─ Unknown job phase: {job_name}")
            return False

        cmd = [
            "docker-compose", 
            "-f", str(self.compose_file), 
            "--project-directory", str(self.workspace),
            "exec", "-T"
        ] + env_vars + ["test-runner", "bash", "-c", exec_command]
        
        code, _, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
        
        if code != 0:
            log.error(f"[Adapter:COMPOSE] Phase '{job_name}' fractured.")
            return False
            
        log.info(f"  └─ Phase '{job_name}' completed successfully.")
        return True

    async def teardown(self):
        log.info("  ├─ Tearing down Docker Compose topology...")
        cmd = [
            "docker-compose", 
            "-f", str(self.compose_file), 
            "--project-directory", str(self.workspace),
            "down", "-v", "--remove-orphans"
        ]
        await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=True)

class DeterminismAuditor:
    """Audits built artifacts for dependency integrity to prevent local path leakage."""
    def __init__(self, target_dir: Path, boundary: SystemBound):
        self.target_dir = target_dir
        self.boundary = boundary
        self.is_clean = False

    def attach(self): pass 
    def detach(self): pass

    async def verify(self) -> bool:
        log.info(f"[Auditor:Determinism] Inspecting extracted Artifacts in {self.target_dir}...")
        wheel_files = list(self.target_dir.rglob("fiber-*.whl"))
        
        if not wheel_files:
            log.error("  └─ 💥 Artifact missing. Wheel not found in the shared volume.")
            return False
            
        target_wheel = wheel_files[0]
        cmd = f"unzip -p {target_wheel} fiber-*/METADATA | grep 'Requires-Dist: xphi'"
        code, out, _ = await self.boundary.run_command(["bash", "-c", cmd], capture=True)
        
        if code != 0:
            log.error("  └─ 💥 Metadata extraction failed. Zip structure might be corrupted.")
            return False
            
        metadata_line = out.strip()
        log.info(f"  ├─ Target Wheel: {target_wheel.name}")
        log.info(f"  ├─ Extracted Dependency: {metadata_line}")
        
        if "file://" in metadata_line:
            log.error("  └─ 💥 FATAL BREACH: Local absolute path leaked into Distribution Artifact!")
            self.is_clean = False
            return False
            
        log.info("  └─ ✨ AUDIT PASSED: Remote Binding Enforced mathematically.")
        self.is_clean = True
        return True

@dataclass
class ComposeContext:
    boundary: SystemBound
    adapter: BaseComposeAdapter
    auditors: Dict[str, Any]

class ComposeOrchestrator:
    def __init__(self, mode: str = "dev", suites: Dict[str, Any] = None, rebuild: bool = False):
        self.mode = mode
        self.suites = suites or {}
        self.keep_workspace = False
        
        self.workspace = Path.cwd()
        self.boundary = SystemBound()
        
        self.artifact_dir = Path(tempfile.mkdtemp(prefix="fiber-compose-artifacts-"))
        os.environ["FIBER_ARTIFACT_MOUNT"] = str(self.artifact_dir)
        
        self.adapter = DockerComposeAdapter(self.workspace, self.boundary, self.artifact_dir, rebuild)
        self.determinism_auditor = DeterminismAuditor(self.artifact_dir, self.boundary)
        self.suite_runners = {}

    async def _run_all_suites(self, broker: Any, context: ComposeContext) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Executing CI Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker=broker, context=context)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating CI/CD Plane via Docker Compose ({self.mode.upper()}) ---")
        log.info(f"[SYSTEM] Artifact Extraction Target: {self.artifact_dir}")
        
        try:
            self.determinism_auditor.attach()
            context = ComposeContext(
                boundary=self.boundary,
                adapter=self.adapter,
                auditors={"determinism": self.determinism_auditor}
            )
            
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    total_fails = await self._run_all_suites(broker, context)
                    if total_fails > 0:
                        return False, f"Topology verified, but {total_fails} logical E2E tests failed."
            
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Orchestration process crashed: {e}")
            return False, str(e)
            
        finally:
            log.info("\n[SYSTEM] Initiating Teardown Sequence...")
            self.determinism_auditor.detach()
            
            if self.adapter:
                await self.adapter.teardown()
            
            if not self.keep_workspace:
                if self.artifact_dir.exists():
                    shutil.rmtree(self.artifact_dir, ignore_errors=True)
                    log.info(f"  └─ Cleaned up temporary artifact directory: {self.artifact_dir}")
            else:
                log.info(f"  └─ Artifacts preserved at: {self.artifact_dir}")
            
            log.info("[SYSTEM] CI Orchestration finalized.")