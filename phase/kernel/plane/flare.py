# fiber.phase.kernel.plane.flare
import json
import asyncio
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Type, Tuple, List

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.tracer.bound import BaseStreamAuditor, SystemBound, log_streamer
from xphi.arch.wasm.auditor import CanonicalProofAuditor

TIME_ROOT = resolve_path("time")
FLARETIME_ROOT = resolve_path("flaretime")

log = get_emitter("flare.controller")

class WranglerDevAuditor(BaseStreamAuditor):
    def __init__(self, workspace: Path, port: int, worker_name: str, boundary: SystemBound = None):
        super().__init__(target=f"wrangler_dev_{port}", boundary=boundary, delay=1)
        self.workspace = workspace
        self.port = port
        self.inspector_port = port + 500
        self.worker_name = worker_name
        self.log = get_emitter(f"auditor.flare.dev.{worker_name}", phase="agent")
        
        self.hit_cpu_limit = False
        self.is_ready = False
        self.startup_logs: List[str] = []

    @log_streamer(["npx", "wrangler", "dev", "--port", "{port}", "--inspector-port", "{inspector_port}", "--local"], cwd="{workspace}")
    async def run_stream(self, line: str) -> None:
        if not line: return
        line_stripped = line.strip()
        
        self.startup_logs.append(line_stripped)
        if len(self.startup_logs) > 50: self.startup_logs.pop(0)
        
        if f"Ready on http://127.0.0.1:{self.port}" in line or f"Ready on http://localhost:{self.port}" in line:
            self.is_ready = True
            self.log.info(f"  [{self.worker_name.upper()}] ⚡ V8 Hologram Materialized (Port {self.port} / Insp {self.inspector_port}).")
        elif "1102" in line or "CPU time limit exceeded" in line:
            self.hit_cpu_limit = True
            self.log.warning(f"  [{self.worker_name.upper()}] [V8_RUPTURE] Hard Kinetic Trap (Error 1102) Triggered!")
        elif "error" in line.lower() or "exception" in line.lower():
            self.log.error(f"  [{self.worker_name.upper()}_FAULT] {line_stripped}")
        else:
            self.log.debug(f"  [{self.worker_name.upper()}_STREAM] {line_stripped}")

class FlareController:
    def __init__(self, target_name: str = "dphi-edge-sandbox", mode: str = "dev", timeout: int = 120, suites: Dict[str, Type] = None):
        self.worker_name = target_name
        self.mode = mode
        self.timeout = timeout
        self.suites = suites or {}
        self.keep_workspace = False  
        
        self.workspace = Path("/tmp/dphi_flare_workspace")
        self.router_dir = self.workspace / "router"
        self.python_dir = self.workspace / "python_node"
        
        self.auditor_router: Optional[WranglerDevAuditor] = None
        self.auditor_python: Optional[WranglerDevAuditor] = None
        self.proof_auditor = CanonicalProofAuditor()
        
        self.rupture_confirmed = False
        self.last_error_context = ""
        self.suite_runners: Dict[str, Any] = {}

    def _provision_microservices(self):
        time_root = Path(TIME_ROOT)
        flare_root = Path(FLARETIME_ROOT)

        ## Setup Router Worker (TypeScript + WASM)
        self.router_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(flare_root / "router.ts", self.router_dir / "index.ts")
        
        wasm_targets = ["cw20_base.wasm", "dphi.wasm", "dvm.wasm"]
        for wasm in wasm_targets:
            if (time_root / wasm).exists():
                shutil.copy2(time_root / wasm, self.router_dir / wasm)
                
        router_toml = f"""
name = "dphi-router"
main = "index.ts"
compatibility_date = "2024-01-01"

[[rules]]
type = "CompiledWasm"
globs = ["**/*.wasm"]
fallthrough = true

# Service Binding: 라우터가 Python 워커를 호출할 수 있도록 내부망 연결
[[services]]
binding = "PYTHON_ENGINE"
service = "dphi-python-node"
"""
        (self.router_dir / "wrangler.toml").write_text(router_toml.strip())

        ## Setup Native Python Worker
        self.python_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(flare_root / "engine.py", self.python_dir / "index.py")
        
        python_toml = f"""
name = "dphi-python-node"
main = "index.py"
compatibility_date = "2024-03-20"
compatibility_flags = ["python_workers"]
"""
        (self.python_dir / "wrangler.toml").write_text(python_toml.strip())
        log.info("📦 Multi-Worker Microservices Provisioned: JS Router & Native Python Engine.")

    async def _await_rupture(self) -> None:
        """Monitors for intentional CPU Limits (Error 1102) during tests"""
        try:
            while not self.rupture_confirmed:
                if self.proof_auditor and (getattr(self.proof_auditor, 'is_collapsed', False) or getattr(self.proof_auditor, 'is_exhausted', False)):
                    self.rupture_confirmed = True
                    self.last_error_context = "CanonicalProofAuditor detected fatal logic collapse."
                    log.critical(f"[FATAL] {self.last_error_context}")
                    return
                
                # Check both workers for Cgroup/CPU limits
                router_limit = self.auditor_router and getattr(self.auditor_router, 'hit_cpu_limit', False)
                python_limit = self.auditor_python and getattr(self.auditor_python, 'hit_cpu_limit', False)
                
                if router_limit or python_limit:
                    self.rupture_confirmed = True
                    self.last_error_context = "Kinetic Trap Triggered! Edge physical defense is fully functional."
                    log.critical(f"[EDGE_KINETIC] {self.last_error_context}")
                    return
                    
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    async def _run_all_suites(self, broker: Any) -> int:
        """Executes all injected test scenarios"""
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Starting Flare Edge Test Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += suite_instance.fail_count
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed unexpectedly: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating Cloudflare Microservices Sandbox ({self.mode.upper()}) ---")
        self.proof_auditor.attach()
        if hasattr(broker, 'target_auditor'):
            broker.target_auditor = self.proof_auditor
        
        try:
            if self.workspace.exists(): shutil.rmtree(self.workspace)
            self._provision_microservices()
            
            if self.mode == "dev":
                log.info("[SYSTEM] Igniting Dual V8 Holograms (Python Engine & JS Router)...")
                
                ## Python 엔진 부팅 (Port 8788, Insp 9288)
                self.auditor_python = WranglerDevAuditor(self.python_dir, 8788, "python_engine")
                self.auditor_python.attach()
                
                ## Router 엔진 부팅 (Port 8787, Insp 9287)
                self.auditor_router = WranglerDevAuditor(self.router_dir, 8787, "js_router")
                self.auditor_router.attach()
                
                ## Wait for both workers to be ready
                for _ in range(20):
                    if getattr(self.auditor_router, 'is_ready', False) and getattr(self.auditor_python, 'is_ready', False):
                        break
                    await asyncio.sleep(1)
                    
                if not (self.auditor_router.is_ready and self.auditor_python.is_ready):
                    raise TimeoutError("Microservice Holograms failed to materialize in time.")

            with flow_scope(phase="TEST_EXECUTION", flow_id=self.proof_auditor.flow_id):
                observer_task = asyncio.create_task(self._await_rupture())
                scenario_task = asyncio.create_task(self._run_all_suites(broker))
                
                done, pending = await asyncio.wait(
                    [observer_task, scenario_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending: task.cancel()
                if pending: await asyncio.gather(*pending, return_exceptions=True)
                
                if self.rupture_confirmed:
                    return False, self.last_error_context
                    
                if scenario_task in done:
                    total_fails = scenario_task.result()
                    if total_fails > 0:
                        return False, f"Logical execution failed with {total_fails} errors."
                    
                    log.info("\n[SYSTEM] Generating Canonical Proof for Edge execution...")
                    canonical_payload = self.proof_auditor.generate_payload()
                    
                    if not canonical_payload or canonical_payload == "[]":
                        return False, "[Ledger] Canonical payload is empty."
                    
                    proof_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
                    if getattr(proof_res, 'success', False):
                        self.test_execution_hash = json.loads(proof_res.output).get("fingerprint")
                        log.info(f"[Ledger] Edge Execution Proof sealed! Hash: {self.test_execution_hash}")
                        return True, ""
                    else:
                        return False, "[Ledger] Failed to seal test execution at Edge."

        except Exception as e:
            log.error(f"[FATAL] Orchestration crashed: {e}")
            return False, str(e)
        finally:
            self.proof_auditor.detach()
            if self.auditor_router: self.auditor_router.detach()
            if self.auditor_python: self.auditor_python.detach()
            if not self.keep_workspace and self.workspace.exists():
                shutil.rmtree(self.workspace)