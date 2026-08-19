# bound.proof.tracer.repro
## @lineage: bound.tracer.repro
## @lineage: dphi.tracer.repro
## @lineage: dphi.node.tracer.repro
import sys
import asyncio
from functools import wraps
from typing import Dict, Callable

from arch.contract.registry.tracer import TracerRegistry
from bound.proof.tracer.auditor.topos import LeakObserverAuditor, ContainerStateAuditor, EntropyAuditor, UniversalLogAuditor

from watcher.tracer.bound import ReproBaseTracer, LifecycleOp, PhaseOp

INFRA_CMD_TABLE = {
    "compose": {
        "deploy": lambda c: [["docker-compose", "-f", c["compose_file"], "down", "-v"], ["docker-compose", "-f", c["compose_file"], "up", "--build", "-d"]],
        "teardown": lambda c: [["docker-compose", "-f", c.get("compose_file", "docker-compose.yml"), "down", "-v"]]
    },
    "docker": {
        "deploy": lambda c: [["docker", "build", "-t", c["image_name"], "."], ["docker", "run", "-d", "--name", c["container_name"]] + c.get("env_vars", []) + ["-m", c.get("mem_limit", "512m"), "--cpus", "1", c["image_name"]]],
        "teardown": lambda c: [["docker", "rm", "-f", c["container_name"]]]
    }
}

HANG_VERDICT_TABLE: Dict[str, Callable[[UniversalLogAuditor], bool]] = {
    "rustc_recursion": lambda semantic: getattr(semantic, 'max_type_depth', 0) > 20,
    "cranelift_loop": lambda semantic: getattr(semantic, 'optimization_loop_count', 0) > 500,
}

class InfrastructureMixin:
    def get_deploy_cmds(self):
        infra = self.config["infra_type"]
        return INFRA_CMD_TABLE[infra]["deploy"](self.config)

    def get_teardown_cmds(self):
        infra = self.config["infra_type"]
        return INFRA_CMD_TABLE[infra]["teardown"](self.config)

    @LifecycleOp.dynamic_sequence("get_deploy_cmds")
    async def deploy_infrastructure(self):
        pass

    @LifecycleOp.dynamic_sequence("get_teardown_cmds")
    async def teardown_infrastructure(self):
        pass


class ReproTracer(ReproBaseTracer, InfrastructureMixin):
    def __init__(self, target_name: str = "repro_worker", timeout: int = 35):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        
        self.compose_file = self.config.get("compose_file", "docker-compose.yml")
        self.container_name = self.config.get("container_name", "worker")
        self.observer = LeakObserverAuditor(self.boundary)

    async def await_stabilization(self) -> None:
        self.log.info(f"## @phase.1.5: Waiting for Surgent Hand ({self.container_name}) to stabilize...")
        cmd = ["docker-compose", "-f", self.compose_file, "ps", "--filter", "status=running", "--format", "json"]
        
        for _ in range(20):
            code, out, _ = await self.boundary.run_command(cmd, cwd=self.workspace, capture=True)
            if self.container_name in out:
                self.log.info("  -> Surgent Hand detected. Manifold stable.")
                await asyncio.sleep(2)
                return
            await asyncio.sleep(1)
        raise TimeoutError(f"Hand materialization failed: {self.container_name} did not start.")

    @PhaseOp.stimulus(
        ["docker-compose", "-f", "{compose_file}", "exec", "-T", "{container_name}", "python", "app.py"], 
        cwd="{workspace}", capture=True, strict=True
    )
    async def inject_stimulus(self, exit_code: int = 0, stdout: str = "") -> None:
        self.log.info("## @phase.3: Injecting Stimulus (Delayed Messages)...")

    async def execute(self) -> None:
        try:
            await self.deploy_infrastructure()
            await self.await_stabilization()
            
            self.log.info("## @phase.2: Tuning Entropy Observatory...")
            self.register_auditors(self.observer)

            await self.inject_stimulus()
            
            self.log.info(f"## @phase.4: Waiting for Signal Transition (ETA: {self.timeout}s)...")
            await self.await_rupture()

            self.log.info("## @phase.5: Finalizing Observation...")
            await asyncio.sleep(2)
        finally:
            self.log.info("## @phase.6: Initiating Teardown...")
            await self.teardown_infrastructure()


class OOMTracer(ReproBaseTracer, InfrastructureMixin):
    def __init__(self, target_name: str, timeout: int = 60):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        
        c_name = self.config["container_name"]
        v_type = self.config["verify_type"]
        
        self.state_auditor = ContainerStateAuditor(c_name, self.boundary)
        self.entropy_auditor = EntropyAuditor(c_name, self.boundary)
        self.semantic_auditor = UniversalLogAuditor(c_name, v_type, self.boundary)

    async def _check_boundary_hook(self, remaining: int) -> None:
        if not getattr(self.state_auditor, 'is_running', True):
            exit_code = getattr(self.state_auditor, 'exit_code', 'Unknown')
            self.log.warning(f"  [BOUNDARY RUPTURE] Target Container Collapsed! (ExitCode: {exit_code})")
            
            if exit_code == "137":
                self.log.crit("[SUCCESS] Absolute OOM (137) confirmed. Sandbox boundary crushed.")
            elif exit_code != "0":
                if getattr(self.semantic_auditor, 'hit_fatal_limit', False):
                    self.log.crit(f"[SUCCESS] Semantic fatal divergence confirmed for {self.config['verify_type']}.")
                else:
                    self.log.error(f"[FAIL] Container died with code {exit_code}, lacking structural proof.")
            
            self.rupture_confirmed = True 

    async def execute(self) -> None:
        self.log.crit(f"## @trace.init Injecting Target Topology: {self.config.get('desc', self.config['verify_type'])}")
        
        try:
            await self.deploy_infrastructure()
            
            self.log.info("## @phase.2: Attaching Multidimensional Auditors (State, Entropy, Semantics)...")
            self.register_auditors(self.state_auditor, self.entropy_auditor, self.semantic_auditor)

            self.log.info("## @phase.3: Waiting for Boundary Collapse...")
            await self.await_rupture(hook_fn=self._check_boundary_hook)

            if not self.rupture_confirmed and getattr(self.state_auditor, 'is_running', True):
                self.log.info("## @phase.4: Evaluating Hang/Livelock Judgment...")
                
                if getattr(self.entropy_auditor, 'last_cpu_usage', 0.0) > 95.0:
                    v_type = self.config["verify_type"]
                    verdict_fn = HANG_VERDICT_TABLE.get(v_type)
                    
                    if verdict_fn and verdict_fn(self.semantic_auditor):
                        self.log.crit(f"[SUCCESS] Semantic Hang confirmed in {v_type}.")
                    else:
                        self.log.error("[FAIL] High CPU detected, but logical divergence depth is insufficient.")
                else:
                    self.log.error(f"[FAIL] Target survived without collapsing. CPU: {getattr(self.entropy_auditor, 'last_cpu_usage', 0.0)}%")
                    
        finally:
            self.log.info("## @phase.5: Initiating Teardown...")
            await self.teardown_infrastructure()