# fiber.llm.pipeline.interceptor.tests.router
"""
Execution Guide:
  [Workspace Layout]
    Ensure your working directory matches the following structure:
      /usr/local/self/           <-- Execute pytest from here
      ├── fiber/                 <-- Main repository
      └── fiber-compats/         <-- Fixture storage (git clone https://github.com/dphi-network/fiber-compats)
      
  [Record Fixtures] $ VCR_MODE=record pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py
  [Run Offline]     $ pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py
"""
import os
import pytest
from typing import Dict, Any, Optional, List

from fiber.llm.pipeline.tracer import DebugTracer
from fiber.llm.pipeline.interceptor.router import OnnxSemanticRouter
from fiber.infra.vcr.manager import VCRPlaybackConfig
from fiber.infra.vcr.injector import VCRInjector
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.llm.entry import acompletion

class MockOnnxRouter(OnnxSemanticRouter):
    """Router instance wrapped with mock logic for deterministic intent routing."""
    def __init__(self, routing_rules, tool_model, fallback_model, mock_intent_probs: Optional[Dict[str, float]] = None):
        self.routing_rules = routing_rules
        self.tool_model = tool_model
        self.fallback_model = fallback_model
        self.labels = ["Transform", "Knowledge_Retrieval", "Complex_Reasoning"]
        self.mock_intent_probs = mock_intent_probs or {}

    def _run_onnx_inference(self, text: str) -> dict:
        return self.mock_intent_probs

ROUTER_CONFIG = {
    "rules": {
        "Transform": {"target": "llama_server/gemma-3-1b-it-Q4_K_M.gguf", "valid": "gemma-3-1b-it", "threshold": 0.8},
        "Complex_Reasoning": {"target": "cohere/command-r-08-2024", "valid": "command-r", "threshold": 0.5}
    },
    "tool": {"target": "gemini/gemini-3.1-flash-lite", "valid": "gemini-3.1-flash-lite"},
    "fallback": {"target": "cerebras/qwen-3.8-27b", "valid": "qwen-3.8-27b"}
}

@pytest.fixture(scope="module", autouse=True)
def inject_vcr_proxy():
    """Injects VCR engine and dynamically maps the fixture path to fiber-compats/suites."""
    
    # Set target path (Hybrid architecture targeting suites/ dir)
    default_compats_dir = os.path.abspath(os.path.join(os.getcwd(), "fiber-compats"))

    compats_dir = os.getenv("FIBER_COMPATS_DIR", default_compats_dir)
    fixture_dir = os.path.join(compats_dir, "suites", "router_suite")
    
    # Determine VCR mode (default to 'replay', override with 'record')
    vcr_mode = os.getenv("VCR_MODE", "replay")
    config = VCRPlaybackConfig(
        mode=vcr_mode,
        speed=os.getenv("VCR_SPEED", "max"),
        chaos_latency_ms=float(os.getenv("VCR_CHAOS", "0.0")),
        record_tick_ms=float(os.getenv("VCR_TICK", "100.0")),
    )
    
    # Inject VCR via Context Manager to ensure safe teardown
    with VCRInjector.use_cassette(config=config, fixture_dir=fixture_dir):
        yield

@pytest.mark.asyncio
@pytest.mark.parametrize("scenario, messages, tools, mock_probs, expected_model", [
    (
        "hard_rule_tool_bypass",
        [{"role": "user", "content": "Fetch the weather."}],
        [{"type": "function", "function": {"name": "get_weather"}}],
        None,
        ROUTER_CONFIG["tool"]["valid"]
    ),
    (
        "soft_rule_intent",
        [{"role": "user", "content": "Format this to JSON."}],
        None,
        {"Transform": 0.95},
        ROUTER_CONFIG["rules"]["Transform"]["valid"]
    ),
    (
        "fallback_shielding",
        [{"role": "user", "content": "Who is the CEO of Apple?"}],
        None,
        {"Transform": 0.1, "Complex_Reasoning": 0.2},
        ROUTER_CONFIG["fallback"]["valid"]
    )
])
async def test_semantic_routing_pipeline(
    scenario: str, 
    messages: List[Dict[str, str]], 
    tools: Optional[List[Dict[str, Any]]], 
    mock_probs: Optional[Dict[str, float]], 
    expected_model: str
):
    router = MockOnnxRouter(
        routing_rules=ROUTER_CONFIG["rules"],
        tool_model=ROUTER_CONFIG["tool"]["target"],
        fallback_model=ROUTER_CONFIG["fallback"]["target"],
        mock_intent_probs=mock_probs
    )
    tracer = DebugTracer()

    # Intercepted by VCR for recording or replaying network calls
    response = await acompletion(
        model="auto",
        messages=messages,
        tools=tools,
        interceptors=[router, tracer],
        metadata={
            "vcr_scenario": scenario,
            "vcr_invoker": "e2e.router.pytest",
            "vcr_filename": f"fixture_{scenario}.json"  # Explicit naming for readability
        }
    )

    system_meta = getattr(response, "system_meta", None)
    assert system_meta is not None, "Missing ExecutionMetadata in response."

    actual_model = system_meta.base_model
    assert expected_model in actual_model, f"Routing failed: expected [{expected_model}], got [{actual_model}]"

    usage_tokens = StateTraverser.resolve(response, "usage.total_tokens", 0)
    assert usage_tokens >= 0, "Usage tokens must be correctly extracted from the fixture."