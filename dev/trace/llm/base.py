# fiber.dev.trace.llm.base
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import List, Tuple, Any, Optional

from fiber.llm.model.tier import model_tier_registry
from fiber.phase.scope.manager import managed_scope

from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.base")

@dataclass
class TraceTestResult:
    phase_num: int
    scenario: str
    success: bool
    total_latency_ms: float = 0.0
    llm_latency_ms: float = 0.0
    usage_tokens: Any = "N/A"

    @property
    def overhead_ms(self) -> float:
        return max(0.0, self.total_latency_ms - self.llm_latency_ms)

    @property
    def status_icon(self) -> str:
        return "✅ PASS" if self.success else "❌ FAIL"

# 워크플로우 공통 메시지
class ReportMsg(WorkflowMessage): pass

class E2EBaseWorkflow(Workflow):
    class Meta:
        trans_rules = {"error": ErrorMessage}

    def __init__(self, name: str, run_context: dict, **kwargs):
        timeout = kwargs.pop("timeout", 300.0)
        super().__init__(name=name, timeout=timeout, **kwargs)
        self.target_model = run_context.get("target_model")
        self.run_context = run_context
        self.log = get_emitter(f"e2e.{name.lower()}")
        self.all_results: List[TraceTestResult] = []

    def record_result(self, phase: int, scenario: str, success: bool, t_start: float, llm_latency_ms: float = 0.0, usage_tokens: Any = "N/A") -> None:
        """각 Phase의 결과를 측정하고 리포트용 버퍼에 저장합니다."""
        total_latency = (time.perf_counter() - t_start) * 1000
        res = TraceTestResult(phase, scenario, success, total_latency, llm_latency_ms, usage_tokens)
        self.all_results.append(res)
        
        if success:
            self.log.info(f"[{self.name}] ✅ Passed: {scenario} (Total: {total_latency:.2f}ms | Tokens: {usage_tokens})")
        else:
            self.log.error(f"[{self.name}] ❌ Failed: {scenario}")

    @step
    async def generate_report(self, msg: ReportMsg) -> WorkflowMessage:
        lines = []
        lines.append("\n" + "=" * 120)
        lines.append(f"📊 [{self.name.upper()}] E2E LATENCY & USAGE BREAKDOWN REPORT")
        lines.append("-" * 120)
        
        all_passed = True
        for res in self.all_results:
            if not res.success: all_passed = False
            target_label = f"[PHASE {res.phase_num}]".ljust(10)
            
            # VCR 등에서 I/O 지연을 제공한 경우 오버헤드 분리 출력
            if res.llm_latency_ms > 0:
                breakdown = f"(I/O: {res.llm_latency_ms:7.2f}ms | Overhead: {res.overhead_ms:7.2f}ms)"
            else:
                breakdown = f"(Overhead Only: {res.overhead_ms:7.2f}ms)"

            lines.append(
                f"{res.phase_num:02d}. {res.status_icon} | {target_label} | "
                f"{res.scenario.ljust(40)} | Total: {res.total_latency_ms:7.2f}ms {breakdown.ljust(42)} | Tokens: {str(res.usage_tokens).rjust(5)}"
            )
            
        lines.append("-" * 120)
        if all_passed:
            lines.append(f"🎉 ALL {self.name.upper()} SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            lines.append(f"💥 {self.name.upper()} PIPELINE FAILED. Inspect structural logs for deviations.")
        lines.append("=" * 120 + "\n")
        
        full_report = "\n".join(lines)
        if all_passed:
            self.log.info(full_report)
        else:
            self.log.critical(full_report)
        
        return StopMessage(result=all_passed)        

    @step
    async def handle_rupture(self, msg: ErrorMessage) -> None:
        self.log.error(f"[{self.name}] 🚨 Fatal topological rupture: {msg.msg}")
        self.post_message(StopMessage(result=False))

class E2EBaseApplication:
    def __init__(self, workflow_cls: type[E2EBaseWorkflow], workflow_name: str, scope_kwargs: dict, run_context: dict):
        self.workflow_cls = workflow_cls
        self.workflow_name = workflow_name
        self.scope_kwargs = scope_kwargs
        self.run_context = run_context
        self.workflow: Optional[E2EBaseWorkflow] = None
        self.log = get_emitter(f"app.{workflow_name.lower()}")

    async def _startup_hook(self):
        async with managed_scope(**self.scope_kwargs):
            self.workflow = self.workflow_cls(self.workflow_name, self.run_context)
            workflow_task = asyncio.create_task(self.workflow.run())
            
            await getattr(self.workflow, "execute")()
            await workflow_task
            if any(not res.success for res in self.workflow.all_results):
                self.log.error(f"🚨 {self.workflow_name} finished with failures.")
                sys.exit(1)

    async def _teardown_hook(self):
        self.log.info(f"🧹 Reclaiming {self.workflow_name} suite resources...")

    def execute(self):
        self.log.info(f"🚀 Igniting {self.workflow_name} Workflow via Reactor...")
        PhaseReactor.ignite(
            main_coro_func=self._startup_hook,
            teardown_hook=self._teardown_hook
        )

def create_e2e_parser(description: str = "LLM E2E Test Runner") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-m", "--model", type=str, help="Target LLM model to use (ex: ollama/gemma:2b)")
    parser.add_argument("-p", "--proxy", action="store_true", help="Enable remote proxy extension layout.")
    return parser

def build_e2e_context(args: argparse.Namespace) -> Tuple[dict, dict]:
    """네트워크 상태, 프록시, 최적 모델 등의 런타임 환경 컨텍스트를 구성합니다."""
    def check_online(host: str = "8.8.8.8", port: int = 53, timeout: float = 0.5) -> bool:
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    is_online = check_online()
    resolved_model = getattr(args, 'model', None) or os.environ.get("LLM_COMPAT_MODEL")
    use_proxy = getattr(args, 'proxy', False) or os.environ.get("LLM_COMPAT_PROXY", "false").lower() == "true"
    vcr_mode = getattr(args, 'vcr', "live")
    
    if not is_online and vcr_mode != "replay":
        log.warning("🚨 [System Offline] Forcing fallback to Local Engine.")
        resolved_model = "ollama/local-gemma-3"
    elif not resolved_model:
        optimal = model_tier_registry.get_optimal_model(requires_tools=False, min_cognitive_score=2)
        resolved_model = f"{optimal[0]}/{optimal[1]}" if isinstance(optimal, tuple) else (f"gemini/{optimal}" if optimal else "gemini/gemini-3.1-flash-lite")

    scope_kwargs = {"use_proxy": use_proxy if is_online else False, "show_logs": True}
    run_context = {
        "use_proxy": scope_kwargs["use_proxy"], 
        "target_model": resolved_model,
    }
    
    return scope_kwargs, run_context