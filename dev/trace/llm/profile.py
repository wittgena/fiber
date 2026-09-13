# fiber.dev.trace.llm.profile
## @lineage: fiber.phase.dev.trace.llm
from typing import Dict, Any, Union, List, Optional
import json

class LlmTraceProfile:
    """
    내부 엔진 호출(acompletion)과 HTTP API 호출(httpx) 모두에서 
    공통으로 사용할 수 있는 LLM Trace 시나리오 명세 및 검증기
    """
    def __init__(self, target_model: str):
        self.target_model = target_model

    def _normalize_response(self, response: Union[Dict[str, Any], Any]) -> Dict[str, Any]:
        """Pydantic 객체와 일반 Dict 형식을 하나의 인터페이스로 정규화합니다."""
        if isinstance(response, dict):
            return response
        if hasattr(response, "model_dump"):
            return response.model_dump(exclude_none=True)
        if hasattr(response, "__dict__"):
            return response.__dict__
        return dict(response)

    # =========================================================================
    # [시나리오 1] Mock Bypass (파이프라인 Short-circuit 검증)
    # =========================================================================
    def build_mock_bypass_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Cost me nothing!"}],
            "mock_response": "This is a bypassed mock response.",
            "mock_delay": 0.1,
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_mock_shared"}}
        }

    def verify_mock_bypass(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        try:
            choices = res_dict.get("choices", [{}])
            if not choices:
                raise ValueError("Trace Miss: Choices array is empty.")
                
            content = choices[0].get("message", {}).get("content", "")
            if "This is a bypassed mock response." not in content:
                raise ValueError(f"Trace Miss: Expected mock string not found. Got: {content}")
            return True
        except (IndexError, AttributeError) as e:
            raise ValueError(f"Trace Miss: Invalid response structure. {e}")

    # =========================================================================
    # [시나리오 2] Fuel Trap (Kinetic Membrane 강제 차단 검증)
    # =========================================================================
    def build_fuel_trap_payload(self, budget: int = 5) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Write a very long essay about the universe."}],
            "stream": True,
            "metadata": {"kernel_auth": {"fuel_budget": budget}}
        }

    def extract_stream_finish_reason(self, chunk: Union[Dict[str, Any], Any]) -> Optional[str]:
        """스트리밍 청크에서 finish_reason을 추출합니다."""
        chunk_dict = self._normalize_response(chunk)
        choices = chunk_dict.get("choices", [])
        if choices and isinstance(choices, list) and len(choices) > 0:
            # Delta 구조 또는 Message 구조 모두 대응
            return choices[0].get("finish_reason")
        return None

    # =========================================================================
    # [시나리오 3] Fallback Provenance (동적 우회 추적성 검증)
    # =========================================================================
    def build_fallback_payload(self, fallbacks: List[Any]) -> Dict[str, Any]:
        return {
            "model": "invalid-trigger-model", 
            "messages": [{"role": "user", "content": "Trigger fallback mechanism"}],
            "fallbacks": fallbacks,
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_fallback_shared"}}
        }

    def verify_fallback(self, response: Union[Dict[str, Any], Any], expected_models: List[str]) -> bool:
        res_dict = self._normalize_response(response)
        actual_model = res_dict.get("model", "")
        
        # 모델명 검증
        model_matched = any(em in actual_model for em in expected_models)
        if not model_matched:
             raise ValueError(f"Trace Miss: Fallback model mismatch. Actual '{actual_model}' not in expected {expected_models}.")
             
        # E2E Trace 환경일 경우, 메타데이터에 Graph가 기록되었는지 검증
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            fallback_traced = any(t.get("stage") == "FallbackHandler" for t in trace_graph)
            if not fallback_traced:
                raise ValueError("Trace Miss: Fallback occurred but was not traced in provider_metadata.trace_graph.")
                
        return True

    # =========================================================================
    # [시나리오 4] Prompt Mutation (부수효과 추적성 검증)
    # =========================================================================
    def build_prompt_mutation_payload(self) -> Dict[str, Any]:
        return {
            "model": self.target_model,
            "messages": [{"role": "user", "content": "Testing prompt transformer"}],
            "tools": [],  # 빈 리스트 주입 (의도적 Mutation 유발)
            "stream": False,
            "metadata": {"kernel_auth": {"audit_hash": "audit_mutation_shared"}}
        }

    def verify_prompt_mutation(self, response: Union[Dict[str, Any], Any]) -> bool:
        res_dict = self._normalize_response(response)
        provider_metadata = res_dict.get("provider_metadata") or {}
        trace_graph = provider_metadata.get("trace_graph", [])
        
        if trace_graph:
            mutation_traced = any(
                t.get("stage") == "PromptTransformer" and t.get("action") == "tools_nulled" 
                for t in trace_graph
            )
            if not mutation_traced:
                raise ValueError("Trace Miss: Prompt mutation (empty tools -> None) was not traced in breadcrumbs.")
        
        return True