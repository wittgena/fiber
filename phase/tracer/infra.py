# phase.tracer.infra
## @lineage: watcher.tracer.phase.infra
import sys
import asyncio
from functools import wraps
from typing import Dict, Any, Optional, Union, Callable

from arch.xor.parser.ruleset import CompiledEngine
from arch.xor.parser.stream import LocalStreamRulesetParser
from arch.contract.resolver.log import LogResolver
from arch.contract.registry.tracer import TracerRegistry

from watcher.plane.emitter import get_emitter
from watcher.tracer.bound import (
    BaseAuditor, 
    BaseStreamAuditor, 
    BaseBoundary,
    SensorOp, 
    SystemBound,
    log_streamer,
    ReproBaseTracer, 
    LifecycleOp, 
    PhaseOp
)

class ToposAuditor(BaseAuditor):
    """@desc: Kubernetes의 Topology(형상) 상태를 주기적으로 수집하는 센서."""
    def __init__(self, target: str, namespace: str, boundary: SystemBound):
        super().__init__(target, namespace, boundary)
        self.current_replicas = 0
        self.peak_replicas = 0
        self.restart_count = 0

    @SensorOp.poll([
        "kubectl", "get", "deployment", "{target}", "-n", "{namespace}",
        "-o", "jsonpath={.spec.replicas}:{.status.readyReplicas}"
    ])
    def parse_replicas(self, out: str) -> None:
        parts = out.split(":")
        spec_replicas = int(parts[0]) if parts[0] else 0
        self.current_replicas = spec_replicas
        if spec_replicas > self.peak_replicas:
            self.peak_replicas = spec_replicas

    @SensorOp.poll([
        "kubectl", "get", "pods", "-l", "app={target}", "-n", "{namespace}",
        "-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}"
    ])
    def parse_restarts(self, out: str) -> None:
        self.restart_count = sum(int(r) for r in out.split() if r.isdigit())


class ContainerStateAuditor(BaseAuditor):
    """@desc: [Boundary Axis] Docker 컨테이너의 종료(Collapse) 상태를 확인합니다."""
    def __init__(self, container_name: str, boundary: SystemBound):
        super().__init__(target=container_name, namespace="", boundary=boundary)
        self.container_name = container_name
        self.is_running = True
        self.exit_code = "0"

    @SensorOp.poll(["docker", "inspect", "{container_name}", "--format", "{{.State.Running}}:{{.State.ExitCode}}"])
    def parse_state(self, out: str) -> None:
        if out and out.startswith("false"):
            self.is_running = False
            self.exit_code = out.split(":")[1] if ":" in out else "Unknown"


class EntropyAuditor(BaseAuditor):
    """@desc: [Energy Axis] 컨테이너의 물리적 에너지(CPU/Mem) 변화를 관측합니다."""
    def __init__(self, container_name: str, boundary: SystemBound):
        super().__init__(target=container_name, namespace="", boundary=boundary)
        self.container_name = container_name
        self.log = get_emitter(f"auditor.entropy.{container_name}", phase="agent")
        self.last_cpu_usage = 0.0

    @SensorOp.poll(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} | {{.MemUsage}}", "{container_name}"])
    def parse_stats(self, out: str) -> None:
        self.log.info(f"  [METRICS] {out}")
        try:
            self.last_cpu_usage = float(out.split("%")[0].strip())
        except ValueError:
            pass


class LeakObserverAuditor(BaseStreamAuditor):
    """@desc: 외부 모듈(Observer)을 서브프로세스로 실행하고 그 출력을 관측합니다."""
    def __init__(self, boundary: SystemBound):
        super().__init__(target="leak_observer", boundary=boundary, delay=0)

    @log_streamer([sys.executable, "-m", "OBSERVER_MODULE"])
    async def run_stream(self, line: str) -> None:
        if "🚨" in line or "└─" in line or "online" in line:
            print(f"  [OBSERVER] {line}")


class UniversalLogAuditor(BaseStreamAuditor):
    def __init__(self, container_name: str, verify_type: str, boundary: SystemBound, ruleset: Optional[Dict] = None):
        super().__init__(target=container_name, boundary=boundary, delay=1)
        self.container_name = container_name
        self.log = get_emitter(f"auditor.universal_log.{container_name}", phase="agent")
        
        _ruleset = ruleset or {"targets": []}
        
        self.resolver = LogResolver[CompiledEngine](
            ruleset=_ruleset, 
            parser=LocalStreamRulesetParser()
        )
        self.rule_engine: CompiledEngine = self.resolver.resolve()
        self.max_type_depth = 0
        self.hit_fatal_limit = False

    @log_streamer(["docker", "logs", "-f", "{container_name}"])
    async def run_stream(self, line: str) -> None:
        if not line or not self.rule_engine: return
        matched_tags = self.rule_engine.execute(line)
        for tag in matched_tags:
            self._handle_matched_tag(tag, line)

    def _handle_matched_tag(self, tag: str, line: str):
        """@desc: 매칭된 태그에 따른 비즈니스 상태 변화 처리"""
        if tag == "rustc-recursion-depth":
            depth = line.count("SkipWhile")
            if depth > self.max_type_depth:
                self.max_type_depth = depth
                if depth % 5 == 0:
                    self.log.info(f"  [DIVERGENCE] Type Depth reached: {depth}")
                    
        elif tag in ["rustc-fatal-limit", "wasm-fatal-panic"]:
            self.hit_fatal_limit = True
            self.log.warning(f"  [FATAL] Boundary rupture detected: {tag}")


class SemanticLogAuditor(BaseStreamAuditor):
    """@desc: 내부 WASM/VM 로그를 스트리밍하여 Livelock(진행 정지) 상태를 관측"""
    def __init__(self, target: str, namespace: str, boundary: Union[BaseBoundary, Any]):
        super().__init__(target=target, boundary=boundary, delay=3)
        self.namespace = namespace
        self.log = get_emitter(f"auditor.semantic_log.{target}", phase="agent")
        self.vm_livelock_iterations = 0

    @log_streamer([
        "kubectl", "logs", "-l", "app={target}", "-n", "{namespace}", "-f", "--tail=10"
    ])
    async def run_stream(self, line: str) -> None:
        if "wasi::fd_readdir" in line or "Weight exhausted" in line:
            self.vm_livelock_iterations += 1
            if self.vm_livelock_iterations % 50 == 0:
                self.log.warning(f"  [VM_LIVELOCK] Internal progress stalled (Iteration {self.vm_livelock_iterations})")


class ResonanceSemanticAuditor(SemanticLogAuditor):
    """@desc: Parses dimensional waves based on specific target topologies."""
    def __init__(self, target_container: str, verify_type: str, boundary: Union[BaseBoundary, Any]):
        super().__init__(target=target_container, namespace="", boundary=boundary)
        self.verify_type = verify_type
        self.gas_divergence_flag = {"evm_success": False, "pallet_revert": False}
        self.is_ruptured = False
        
    @log_streamer(["docker", "logs", "-f", "{target}"])
    async def run_stream(self, line: str) -> None:
        decoded = line.lower()
        if self.verify_type == "temporal_fixation":
            if "wasi::fd_readdir" in decoded or "adapter state loop" in decoded:
                self.vm_livelock_iterations += 1
                if self.vm_livelock_iterations > 2000:
                    self.is_ruptured = True
        elif self.verify_type == "consensus_divergence":
            if "evm: execution successful" in decoded:
                self.gas_divergence_flag["evm_success"] = True
            if "weight exhausted" in decoded or "pallet_revive::revert" in decoded:
                self.gas_divergence_flag["pallet_revert"] = True
                
            if self.gas_divergence_flag["evm_success"] and self.gas_divergence_flag["pallet_revert"]:
                self.is_ruptured = True


# =====================================================================
# 2. CONFIG TABLES
# =====================================================================

INFRA_CMD_TABLE = {
    "compose": {
        "deploy": lambda c: [
            ["docker-compose", "-f", c["compose_file"], "down", "-v"], 
            ["docker-compose", "-f", c["compose_file"], "up", "--build", "-d"]
        ],
        "teardown": lambda c: [
            ["docker-compose", "-f", c.get("compose_file", "docker-compose.yml"), "down", "-v"]
        ]
    },
    "docker": {
        "deploy": lambda c: [
            ["docker", "build", "-t", c["image_name"], "."], 
            ["docker", "run", "-d", "--name", c["container_name"]] + c.get("env_vars", []) + ["-m", c.get("mem_limit", "512m"), "--cpus", "1", c["image_name"]]
        ],
        "teardown": lambda c: [
            ["docker", "rm", "-f", c["container_name"]]
        ]
    }
}

HANG_VERDICT_TABLE: Dict[str, Callable[['UniversalLogAuditor'], bool]] = {
    "rustc_recursion": lambda semantic: getattr(semantic, 'max_type_depth', 0) > 20,
    "cranelift_loop": lambda semantic: getattr(semantic, 'optimization_loop_count', 0) > 500,
}


# =====================================================================
# 3. TRACERS & MIXINS
# =====================================================================

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