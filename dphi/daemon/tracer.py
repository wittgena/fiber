# fiber.dphi.daemon.tracer
"""
@desc: Integrated Tracer Controller Daemon for Internal System Monitoring.
       Tracks OOM (Out of Memory), Deadlocks, Memory Leaks, and WASM Traps across internal infrastructure.
"""
import os
import asyncio
from typing import Optional, Dict, Any
from contextlib import suppress

from xphi.arch.contract.registry.unified import contract
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.dev.tracer.base import ReproBaseTracer, PhaseOp
from xphi.arch.dev.infra.topos import (
    ContainerStateAuditor,
    EntropyAuditor,
    UniversalLogAuditor,
    LeakObserverAuditor,
    HANG_VERDICT_TABLE
)

daemon_log = get_emitter("daemon.tracer")

# =====================================================================
# 1. TRACER REGISTRY (Internal Infrastructure Configurations)
# =====================================================================

class TracerRegistry:
    # --- Internal Telemetry Rulesets ---
    _RULESET_CORE_DAEMON = {
        "targets": [
            {
                "tag": "core-memory-leak",
                "keywords": [{"AND": ["Memory growth", "uncollected objects"]}],
                "action": "count_max"
            },
            {
                "tag": "core-oom-crash",
                "keywords": [{"OR": ["MemoryError", "OOMKilled", "out of memory", "137"]}],
                "action": "trigger_crash"
            }
        ]
    }
    
    _RULESET_WASM_SANDBOX = {
         "targets": [
            {
                "tag": "wasm-sandbox-trap",
                "keywords": [{"OR": ["wasm trap", "out of bounds memory access", "fuel exhausted"]}],
                "action": "trigger_crash"
            },
            {
                "tag": "wasm-ffi-panic",
                "keywords": [{"AND": ["host call failed", "panic"]}],
                "action": "trigger_crash"
            }
        ]
    }

    _RULESET_EVENT_BUS = {
         "targets": [
            {
                "tag": "event-bus-flood",
                "keywords": [{"AND": ["Backpressure", "dropping events"]}],
                "action": "count_max"
            },
            {
                "tag": "channel-deadlock",
                "keywords": [{"AND": ["TimeoutError", "waiting for PsiEvent"]}],
                "action": "trigger_crash"
            }
        ]
    }
    
    # --- Topology Manifests ---
    CONFIGS = {
        "xphi_core_oom": {
            "infra_type": "docker",
            "workspace_suffix": "core_monitor",
            "image_name": "xphi/core-daemon:latest",
            "container_name": "xphi_core_main",
            "env_vars": ["-e", "LOG_LEVEL=DEBUG", "-e", "XPHI_METRICS=1"],
            "mem_limit": "256m",
            "verify_type": "oom_crash",
            "desc": "Xphi Core Daemon OOM (Out of Memory) Tracker",
            "ruleset": _RULESET_CORE_DAEMON
        },
        "xphi_wasm_trap": {
            "infra_type": "wasm_native",
            "workspace_suffix": "wasm_sandbox",
            "target_wasm_modules": ["gateway.wasm", "dphi.wasm", "dvm.wasm"],
            "limits": {
                "fuel": 5_000_000,
                "mem_limit": "32m"
            },
            "verify_type": "wasm_trap",
            "desc": "WASM Sandbox Resource Limit & Trap Monitor (Gateway/Dphi/DVM)",
            "ruleset": _RULESET_WASM_SANDBOX
        },
        "xphi_bus_deadlock": {
            "infra_type": "compose",
            "workspace_suffix": "bus_cluster",
            "compose_file": "docker-compose.xphi.yml",
            "container_name": "xphi_async_worker",
            "verify_type": "deadlock",
            "desc": "PsiEvent Bus Backpressure Flood and Deadlock Tracker",
            "ruleset": _RULESET_EVENT_BUS
        },
        "xphi_delay_worker": {
            "infra_type": "compose",
            "workspace_suffix": "worker_node",
            "compose_file": "docker-compose.worker.yml",
            "container_name": "xphi_delay_worker",
            "verify_type": "memory_leak",
            "desc": "Background Worker Delayed Message Memory Leak Detection",
            "ruleset": _RULESET_CORE_DAEMON
        }
    }

    @classmethod
    def get(cls, target_name: str) -> Optional[dict]:
        if target_name not in cls.CONFIGS:
            return None
        
        config = cls.CONFIGS[target_name].copy()
        suffix = config.get("workspace_suffix", "default")
        
        # 내부 구조 추적을 위한 Workspace 맵핑
        config["workspace_path"] = str(resolve_path("workspace") / "internal_trace" / suffix)
        
        # WASM 샌드박스의 3대 코어 타겟 동적 바인딩
        if config.get("infra_type") == "wasm_native":
            sandbox_root = resolve_path("sandbox")
            config["sandbox_root"] = str(sandbox_root)
            
            modules = config.get("target_wasm_modules", [])
            config["target_wasm_paths"] = {
                mod.replace(".wasm", ""): str(sandbox_root / mod) for mod in modules
            }
            
        return config

    @classmethod
    def register(cls, target_name: str, config: dict):
        cls.CONFIGS[target_name] = config


# =====================================================================
# 2. TRACERS (Standard Observation Layer for Bugs and Limits)
# =====================================================================

class LeakTracer(ReproBaseTracer):
    """@desc: Observes background processes for memory leaks and resource lingering."""
    def __init__(self, target_name: str = "xphi_delay_worker", timeout: int = 35):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        self.container_name = self.config.get("container_name", "worker")
        self.observer = LeakObserverAuditor(self.boundary)

    @PhaseOp.stimulus(
        ["docker-compose", "-f", "{compose_file}", "exec", "-T", "{container_name}", "python", "-m", "xphi.core.inject"], 
        cwd="{workspace}", capture=True, strict=True
    )
    async def inject_stimulus(self, exit_code: int = 0, stdout: str = "") -> None:
        self.log.info("## @trace.3: Injecting Test Workload (Delayed Events)...")

    async def execute(self) -> None:
        try:
            self.log.info("## @trace.1: Provisioning Infrastructure and attaching Leak Auditors...")
            self.register_auditors(self.observer)

            await self.inject_stimulus()
            
            self.log.info(f"## @trace.2: Awaiting Crash or Leak Detection (ETA: {self.timeout}s)...")
            await self.await_rupture()

            self.log.info("## @trace.3: Finalizing Telemetry...")
            await asyncio.sleep(2)
        finally:
            self.log.info("## @trace.4: Releasing Observers and cleaning up.")


class OOMTracer(ReproBaseTracer):
    """@desc: Specialized tracer for verifying structural limits like OOM, WASM Traps, and Deadlocks."""
    def __init__(self, target_name: str, timeout: int = 60, infra_type: str = "compose", namespace: str = "default"):
        super().__init__(target_name=target_name, timeout=timeout)
        self.config = TracerRegistry.get(target_name)
        self.workspace = self.config["workspace_path"]
        
        c_name = self.config["container_name"]
        v_type = self.config["verify_type"]
        
        self.state_auditor = ContainerStateAuditor(c_name, self.boundary, infra_type, namespace)
        self.entropy_auditor = EntropyAuditor(c_name, self.boundary, infra_type, namespace)
        self.semantic_auditor = UniversalLogAuditor(c_name, v_type, self.boundary, infra_type, namespace, ruleset=self.config.get("ruleset"))

    async def _check_boundary_hook(self, remaining: int) -> None:
        if not getattr(self.state_auditor, 'is_running', True):
            exit_code = getattr(self.state_auditor, 'exit_code', 'Unknown')
            self.log.warning(f"  [CRASH] Target Workload Terminated Unexpectedly! (ExitCode: {exit_code})")
            
            if "137" in exit_code or "OOM" in exit_code.upper():
                self.log.crit("[SUCCESS] Absolute OOM (Out of Memory) confirmed.")
            elif "134" in exit_code or "1102" in exit_code:
                self.log.crit("[SUCCESS] WASM Trap or CPU Time Limit triggered.")
            elif exit_code not in ["0", "Unknown"]:
                if getattr(self.semantic_auditor, 'hit_fatal_limit', False):
                    self.log.crit(f"[SUCCESS] Fatal error logs confirmed for verification type: {self.config['verify_type']}.")
                else:
                    self.log.error(f"[FAIL] Container died with code {exit_code}, lacking log evidence.")
            
            self.rupture_confirmed = True 

    async def execute(self) -> None:
        self.log.crit(f"## @trace.init Injecting Monitor for: {self.config.get('desc', self.config['verify_type'])}")
        
        try:
            self.log.info("## @trace.1: Attaching Container, Metrics(CPU/Mem), and Log Auditors...")
            self.register_auditors(self.state_auditor, self.entropy_auditor, self.semantic_auditor)

            self.log.info("## @trace.2: Waiting for Target Crash or Limit Trigger...")
            await self.await_rupture(hook_fn=self._check_boundary_hook)

            if not self.rupture_confirmed and getattr(self.state_auditor, 'is_running', True):
                self.log.info("## @trace.3: Target is still running. Evaluating Deadlock/Hang metrics...")
                
                if getattr(self.entropy_auditor, 'last_cpu_usage', 0.0) > 95.0:
                    v_type = self.config["verify_type"]
                    verdict_fn = HANG_VERDICT_TABLE.get(v_type)
                    
                    if verdict_fn and verdict_fn(self.semantic_auditor):
                        self.log.crit(f"[SUCCESS] High CPU Hang / Deadlock confirmed in {v_type}.")
                    else:
                        self.log.error("[FAIL] High CPU usage detected, but semantic deadlock logs are missing.")
                else:
                    self.log.error(f"[FAIL] Target survived without crashing. Stable CPU: {getattr(self.entropy_auditor, 'last_cpu_usage', 0.0)}%")
                    
        finally:
            self.log.info("## @trace.4: Releasing Observers and Teardown.")


# =====================================================================
# 3. DAEMON LAYER (Internal Monitoring Controller)
# =====================================================================

@contract.daemon("tracer_controller")
class TracerControllerDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("TracerControllerDaemon")
        self.ctx = ctx
        self.target = os.getenv("TRACE_TARGET")
        self._trace_task = None

    def _ensure_tracer_config(self, target: str):
        if TracerRegistry.get(target):
            return

        daemon_log.warning(f"[{self.name}] No config found for '{target}' in Registry. Injecting fail-safe fallback config.")
        
        if target in ["xphi_core_oom", "xphi_wasm_trap", "xphi_bus_deadlock"]:
            TracerRegistry.register(target, {
                "desc": f"Fallback Config for {target}",
                "infra_type": "docker",
                "container_name": f"{target}_target",
                "workspace_path": "/tmp/fallback",
                "verify_type": "oom_crash",
            })
        elif target == "xphi_delay_worker":
            TracerRegistry.register(target, {
                "desc": "Fallback Config for Leak Worker",
                "infra_type": "compose",
                "compose_file": "docker-compose.yml",
                "container_name": "xphi_delay_worker",
                "workspace_path": "/tmp/fallback",
                "verify_type": "memory_leak"
            })

    async def run(self):
        daemon_log.info(f"[{self.name}] Initiating Tracer Controller for Internal Monitoring...")
        
        if not self.target:
            daemon_log.error(f"[{self.name}] TRACE_TARGET environment variable is missing. Terminating daemon.")
            return

        daemon_log.info(f"[{self.name}] 🎯 Targeting Monitor Scenario: {self.target}")
        
        tracer_instance = None
        self._ensure_tracer_config(self.target)
        
        # Route to exact Tracers
        if self.target in ["xphi_core_oom", "xphi_wasm_trap", "xphi_bus_deadlock"]:
            tracer_instance = OOMTracer(target_name=self.target)
        elif self.target == "xphi_delay_worker":
            tracer_instance = LeakTracer(target_name=self.target)
        else:
            daemon_log.error(f"[{self.name}] Unknown TRACE_TARGET: {self.target}. Review TracerRegistry.CONFIGS.")
            return

        try:
            self._trace_task = asyncio.create_task(tracer_instance.execute())
            while self.running:
                if self._trace_task.done():
                    exc = self._trace_task.exception()
                    if exc:
                        daemon_log.error(f"[{self.name}] Tracer execution crashed: {exc}", exc_info=exc)
                    else:
                        daemon_log.info(f"[{self.name}] ✅ Trace Scenario '{self.target}' verified successfully.")
                    
                    break 
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            daemon_log.info(f"[{self.name}] Cancel signal received. Aborting trace scenario.")
        except Exception as e:
            daemon_log.error(f"[{self.name}] Fatal tracer error: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        daemon_log.info(f"[{self.name}] Releasing Monitor resources...")
        if self._trace_task and not self._trace_task.done():
            self._trace_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._trace_task
        daemon_log.info(f"[{self.name}] Tracer Controller shut down safely.")