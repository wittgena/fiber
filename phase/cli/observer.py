# fiber.phase.cli.observer
import asyncio
import sys
from typing import List, Optional

import xphi.arch.dev.tracer.chaos as chaos_module
from xphi.arch.dev.tracer.base import SystemBound, ExecutorOp
from xphi.arch.dev.tracer.kube import KubeStatusAuditor, KubeTracer
from xphi.arch.dev.infra.topos import (
    ContainerStateAuditor,
    EntropyAuditor,
    UniversalLogAuditor,
    LeakObserverAuditor,
    HANG_VERDICT_TABLE
)
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fiber.cli.observer")

class KubeTracerRegistry:
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
            }
        ]
    }
    _RULESET_EVENT_BUS = {
         "targets": [
            {
                "tag": "channel-deadlock",
                "keywords": [{"AND": ["TimeoutError", "waiting for PsiEvent"]}],
                "action": "trigger_crash"
            }
        ]
    }
    CONFIGS = {
        "kube_core_oom": {
            "namespace": "fiber-topos",
            "pod_label": "app=fiber-core",
            "mem_limit": "256m",
            "verify_type": "oom_crash",
            "desc": "Chaos Test: Core Daemon OOM (Out of Memory) Injection",
            "ruleset": _RULESET_CORE_DAEMON,
            "chaos_cmd": ["python", "-m", chaos_module.__name__, "oom", "--quiet"]
        },
        "kube_wasm_trap": {
            "namespace": "fiber-topos",
            "pod_label": "app=fiber-gateway",
            "limits": {"fuel": 5_000_000, "mem_limit": "32m"},
            "verify_type": "wasm_trap",
            "desc": "Chaos Test: WASM Sandbox Resource Trap Monitor",
            "ruleset": _RULESET_WASM_SANDBOX,
            "chaos_cmd": ["fiber", "e2e", "dphi.wasm.phase", "--force-trap"]
        },
        "kube_bus_deadlock": {
            "namespace": "fiber-topos",
            "pod_label": "app=fiber-worker",
            "verify_type": "deadlock",
            "desc": "Chaos Test: Event Bus Deadlock Tracker",
            "ruleset": _RULESET_EVENT_BUS,
            "chaos_cmd": ["python", "-m", chaos_module.__name__, "deadlock", "--quiet"]
        },
        "kube_delay_worker": {
            "namespace": "fiber-topos",
            "pod_label": "app=fiber-worker",
            "verify_type": "memory_leak",
            "desc": "Chaos Test: Background Worker Memory Leak Detection",
            "ruleset": _RULESET_CORE_DAEMON,
            "chaos_cmd": ["python", "-m", chaos_module.__name__, "leak", "--quiet"]
        }
    }

    @classmethod
    def get(cls, target_name: str) -> Optional[dict]:
        return cls.CONFIGS.get(target_name, None)


class KubeChaosTracer(KubeTracer):
    def __init__(self, target_name: str, timeout: int = 60):
        self.config = KubeTracerRegistry.get(target_name)
        super().__init__(tracer_name=target_name, namespace=self.config["namespace"], timeout=timeout)
        
        self.pod_label = self.config["pod_label"]
        self.verify_type = self.config["verify_type"]
        self.chaos_cmd = self.config.get("chaos_cmd", ["echo", "No chaos command defined"])
        
        self.state_auditor = ContainerStateAuditor(self.pod_label, self.boundary, self.namespace)
        self.entropy_auditor = EntropyAuditor(self.pod_label, self.boundary, self.namespace)
        self.semantic_auditor = UniversalLogAuditor(self.pod_label, self.verify_type, self.boundary, self.namespace, ruleset=self.config.get("ruleset"))
        
        self.leak_auditor = LeakObserverAuditor(self.boundary) if self.verify_type == "memory_leak" else None

    async def get_pod_name(self) -> str:
        cmd = ["kubectl", "get", "pods", "-n", self.namespace, "-l", self.pod_label, "-o", "jsonpath='{.items[0].metadata.name}'"]
        code, out, _ = await self.boundary.run_command(cmd, capture=True)
        return out.strip().replace("'", "") if code == 0 else ""

    async def inject_chaos(self) -> bool:
        pod_name = await self.get_pod_name()
        if not pod_name:
            self.log.error(f"## @trace.fault: Target pod for chaos ({self.pod_label}) not found.")
            return False

        self.log.info(f"## @chaos: Injecting Chaos Workload {self.chaos_cmd} to pod '{pod_name}'...")
        cmd = ["kubectl", "exec", "-i", pod_name, "-n", self.namespace, "--"] + self.chaos_cmd
        return await ExecutorOp.run_sequence(self, [cmd], phase_name="Chaos.Inject", cwd=".", strict=False)

    async def _check_boundary_hook(self, remaining: int) -> None:
        if not getattr(self.state_auditor, 'is_running', True):
            exit_code = getattr(self.state_auditor, 'exit_code', 'Unknown')
            self.log.warning(f"  [CHAOS SUCCESS] Workload Terminated Unexpectedly! (ExitCode: {exit_code})")
            
            if "137" in exit_code or "OOM" in exit_code.upper():
                self.log.crit("    └─ Absolute OOM (Out of Memory) confirmed by Kubernetes API.")
            elif "134" in exit_code or "1102" in exit_code:
                self.log.crit("    └─ WASM Trap or CPU Time Limit triggered.")
            
            self.rupture_confirmed = True 
        elif getattr(self.semantic_auditor, 'hit_fatal_limit', False):
            self.log.crit("  [CHAOS SUCCESS] Fatal semantic logs confirmed evidence of crash/trap.")
            self.rupture_confirmed = True
        elif self.verify_type == "deadlock" and getattr(self.entropy_auditor, 'last_cpu_usage', 0.0) > 95.0:
            verdict_fn = HANG_VERDICT_TABLE.get(self.verify_type)
            if verdict_fn and verdict_fn(self.semantic_auditor):
                self.log.crit(f"  [CHAOS SUCCESS] High CPU Hang / Deadlock confirmed in logs.")
                self.rupture_confirmed = True

    async def execute(self) -> None:
        self.log.crit(f"## @trace.init Initiating Chaos Scenario: {self.config.get('desc')}")
        try:
            self.log.info("## @trace.1: Attaching Kube Chaos Auditors (State, Metrics, Logs)...")
            auditors_to_register = [self.state_auditor, self.entropy_auditor, self.semantic_auditor]
            if self.leak_auditor:
                auditors_to_register.append(self.leak_auditor)
            
            self.register_auditors(*auditors_to_register)
            if not await self.inject_chaos():
                self.rupture_confirmed = True
                raise RuntimeError("Failed to inject chaos stimulus.")

            self.log.info(f"## @trace.2: Waiting for Chaos Trigger ({self.verify_type})... ETA: {self.timeout}s")
            await self.await_rupture(hook_fn=self._check_boundary_hook)

        finally:
            self.log.info("## @trace.teardown: Releasing Chaos Observers.")


async def run_trace_scenario(target: str) -> None:
    log.info(f"👁️ [Trace Observer] Dispatching Chaos Scenario: '{target}'")
    if not KubeTracerRegistry.get(target):
        log.warning(f"No config found for '{target}'. Injecting fail-safe fallback config.")
        KubeTracerRegistry.CONFIGS[target] = {
            "desc": f"Fallback Kube Config for {target}",
            "namespace": "fiber-topos",
            "pod_label": f"app={target}_target",
            "verify_type": "oom_crash",
            "ruleset": KubeTracerRegistry._RULESET_CORE_DAEMON,
            "chaos_cmd": ["python", "-m", chaos_module.__name__, "oom", "--quiet"]
        }

    tracer_instance = KubeChaosTracer(target_name=target)
    try:
        await tracer_instance.trace()
        log.info(f"👁️ [Trace Observer] ✅ Chaos Scenario '{target}' verified and safely concluded.")
    except asyncio.CancelledError:
        log.info(f"👁️ [Trace Observer] Cancel signal received. Aborting trace scenario.")
    except Exception as e:
        log.error(f"👁️ [Trace Observer] Fatal tracer error: {e}", exc_info=True)


async def run_observer(target: str, namespace: str, delay: int, chaos: bool = False) -> None:
    mode_str = "ACTIVE (Chaos Injection)" if chaos else "PASSIVE (Monitor Only)"
    log.info(f"👁️ [Observer] Initializing Telemetry [{mode_str}] for target: '{target}'")
    
    boundary = SystemBound()
    auditors: List[KubeStatusAuditor] = []
    
    try:
        is_kube_ecosystem = target == "kube" or target.startswith("kube_")
        if is_kube_ecosystem:
            log.info(f"👁️ [Observer] Attaching KubeStatusAuditor (namespace: {namespace}, delay: {delay}s)")
            kube_auditor = KubeStatusAuditor(
                target="kube_pods", 
                boundary=boundary, 
                namespace=namespace, 
                delay=delay
            )
            kube_auditor.attach()
            auditors.append(kube_auditor)
        else:
            log.error(f"[Observer] Unknown ecosystem for target: {target}")
            sys.exit(1)

        if chaos:
            is_valid_scenario = bool(KubeTracerRegistry.get(target))
            if not is_valid_scenario:
                log.error(f"❌ [Observer] Target '{target}' is not a valid Chaos/Trace scenario in registry.")
                sys.exit(1)

            log.warning(f"⚠️ [Observer] Dispatching Active Chaos Scenario: '{target}'")
            await run_trace_scenario(target)
            log.info("👁️ [Observer] Active Chaos Scenario concluded.")
            
        else:
            log.info("👁️ [Observer] Resonance Window established. Streaming data... (Press Ctrl+C to stop)")
            while True:
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info("\n👁️ [Observer] Shutdown signal received.")
    except Exception as e:
        log.error(f"[Observer] Fault in observation loop: {e}", exc_info=True)
    finally:
        log.info("👁️ [Observer] Detaching sensors and collapsing boundary...")
        for auditor in auditors:
            auditor.detach()
        boundary.collapse()