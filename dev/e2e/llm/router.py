# fiber.dev.e2e.llm.router
from __future__ import annotations

import time
from typing import Dict, Any, Optional, List

from fiber.llm.entry import acompletion
from fiber.dev.trace.llm.base import (
    E2EBaseWorkflow, 
    E2EBaseApplication, 
    create_e2e_parser, 
    build_e2e_context, 
    ReportMsg
)
from fiber.dev.trace.llm.debugger import DebugTracer
from fiber.dev.trace.llm.router import OnnxSemanticRouter
from fiber.gateway.llm.state.traverser import StateTraverser
from xphi.arch.contract.workflow import WorkflowMessage, step

class HardRuleMsg(WorkflowMessage): pass
class SoftRuleMsg(WorkflowMessage): pass
class FallbackRuleMsg(WorkflowMessage): pass

class TestableOnnxRouter(OnnxSemanticRouter):
    """Router instance wrapped with mock logic for deterministic intent routing."""
    def __init__(self, routing_rules, tool_model, fallback_model, mock_intent_probs: Optional[Dict[str, float]] = None):
        self.routing_rules = routing_rules
        self.tool_model = tool_model
        self.fallback_model = fallback_model
        self.labels = ["Transform", "Knowledge_Retrieval", "Complex_Reasoning"]
        self.mock_intent_probs = mock_intent_probs or {}

    def _run_onnx_inference(self, text: str) -> dict:
        return self.mock_intent_probs


class LlmRoutingWorkflow(E2EBaseWorkflow):
    ROUTER_CONFIG = {
        "rules": {
            "Transform": {
                "target": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
                "valid": "gemma-3-1b-it",
                "threshold": 0.8
            },
            "Complex_Reasoning": {
                "target": "cohere/command-r-08-2024",
                "valid": "command-r",
                "threshold": 0.5
            }
        },
        "tool": {
            "target": "gemini/gemini-3.1-flash-lite",
            "valid": "gemini-3.1-flash-lite"
        },
        "fallback": {
            "target": "llama_server/qwen-3.5-9b-Q4_K_M.gguf",
            "valid": "qwen-3.5-9b"
        }
    }

    def _create_router(self, mock_probs: Optional[Dict[str, float]] = None) -> TestableOnnxRouter:
        return TestableOnnxRouter(
            routing_rules=self.ROUTER_CONFIG["rules"],
            tool_model=self.ROUTER_CONFIG["tool"]["target"],
            fallback_model=self.ROUTER_CONFIG["fallback"]["target"],
            mock_intent_probs=mock_probs
        )

    # ✅ 중복 코드를 제거하고 검증을 표준화한 핵심 헬퍼 메서드
    async def _execute_and_verify_routing(
        self, 
        phase: int, 
        scenario: str, 
        messages: List[Dict[str, str]], 
        expected_model: str, 
        mock_probs: Optional[Dict[str, float]] = None,
        tools: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        self.log.info(f"\n[{self.name}] [Phase {phase}] {scenario}")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = 0
        
        try:
            router = self._create_router(mock_probs=mock_probs)
            tracer = DebugTracer()
            
            # 파이프라인 호출
            response = await acompletion(
                model="auto",
                messages=messages,
                tools=tools,
                interceptors=[router, tracer]
            )
            
            # 메타데이터 무결성 엄격 검증
            if tracer.meta is None:
                raise RuntimeError("ExecutionMetadata is missing. Pipeline tracer failed to capture state.")
            
            actual_model = tracer.meta.base_model
            if expected_model not in actual_model:
                raise ValueError(f"Routing mismatch. Expected [{expected_model}], but Pipeline executed [{actual_model}]")
            
            # ✅ 객체/딕셔너리 다형성을 완벽히 지원하는 프레임워크 표준 방식으로 토큰 추출
            usage_tokens = StateTraverser.resolve(response, "usage.total_tokens", 0)
            is_success = True
            
        except Exception as e:
            self.log.error(f"Routing Phase {phase} Failed: {str(e)}")
            
        self.record_result(phase, scenario, is_success, t0, usage_tokens=usage_tokens)

    async def execute(self) -> None:
        self.log.info(f"[{self.name}] Igniting LLM Semantic Routing Suite")
        self.post_message(HardRuleMsg())
        await self.run()

    @step
    async def phase_hard_rule_tools(self, msg: HardRuleMsg) -> WorkflowMessage:
        await self._execute_and_verify_routing(
            phase=1,
            scenario="HARD RULE: Tool Request Bypass",
            messages=[{"role": "user", "content": "Fetch the weather."}],
            tools=[{"type": "function", "function": {"name": "get_weather"}}],
            expected_model=self.ROUTER_CONFIG["tool"]["valid"]
        )
        return SoftRuleMsg()

    @step
    async def phase_soft_rule_intent(self, msg: SoftRuleMsg) -> WorkflowMessage:
        await self._execute_and_verify_routing(
            phase=2,
            scenario="SOFT RULE: Threshold-based Intent Routing",
            messages=[{"role": "user", "content": "Format this to JSON."}],
            mock_probs={"Transform": 0.95},
            expected_model=self.ROUTER_CONFIG["rules"]["Transform"]["valid"]
        )
        return FallbackRuleMsg()

    @step
    async def phase_fallback_rule(self, msg: FallbackRuleMsg) -> WorkflowMessage:
        await self._execute_and_verify_routing(
            phase=3,
            scenario="FALLBACK: Low Confidence Shielding",
            messages=[{"role": "user", "content": "Who is the CEO of Apple?"}],
            mock_probs={"Transform": 0.1, "Complex_Reasoning": 0.2},
            expected_model=self.ROUTER_CONFIG["fallback"]["valid"]
        )
        return ReportMsg()


def main(args: list[str] = None):
    parser = create_e2e_parser("LLM Semantic Routing E2E Suite")
    parsed_args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = build_e2e_context(parsed_args)
    
    app = E2EBaseApplication(
        workflow_cls=LlmRoutingWorkflow,
        workflow_name="RoutingSuiteApp",
        scope_kwargs=scope_kwargs,
        run_context=run_context
    )
    app.execute()


if __name__ == "__main__":
    main()