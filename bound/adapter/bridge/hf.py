# bound.adapter.bridge.hf
## @lineage: anchor.surface.hf.wrapper
from typing import Any, Dict, List, Optional

class HFInferenceBridge:
    """
    HuggingFace 라이브러리와의 모든 의존성을 격리하고 대리 호출하는 Bridge 클래스.
    이 클래스 외부의 어떤 스크립트에서도 huggingface_hub를 직접 import하지 않습니다.
    """
    def __init__(self, **kwargs: Any) -> None:
        try:
            from huggingface_hub import InferenceClient, AsyncInferenceClient, model_info
        except ImportError:
            raise ImportError(
                "HuggingFace 엔진이 누락되었습니다. `pip install huggingface_hub`를 실행하십시오."
            )
        self._sync = InferenceClient(**kwargs)
        self._async = AsyncInferenceClient(**kwargs)
        self._model_info_func = model_info

    @staticmethod
    def get_recommended_model(task: str) -> str:
        try:
            from huggingface_hub import InferenceClient
            return InferenceClient.get_recommended_model(task=task)
        except ImportError:
            raise ImportError("HuggingFace 엔진이 누락되었습니다.")

    # --- Configuration & Metadata ---
    def list_deployed_models(self, frameworks: str = "all") -> Dict[str, Any]:
        return self._sync.list_deployed_models(frameworks=frameworks)

    def get_endpoint_info(self) -> Dict[str, Any]:
        return self._sync.get_endpoint_info()

    def get_model_info(self, model_name: str, **kwargs: Any) -> Any:
        return self._model_info_func(model_name, **kwargs)

    # --- Feature Extraction (Embeddings) ---
    def feature_extraction(self, text: str) -> Any:
        return self._sync.feature_extraction(text)

    async def afeature_extraction(self, text: str) -> Any:
        return await self._async.feature_extraction(text)

    # --- Chat Completion ---
    def chat_completion(self, messages: List[Dict[str, Any]], stream: bool = False, **kwargs: Any) -> Any:
        return self._sync.chat_completion(messages=messages, stream=stream, **kwargs)

    async def achat_completion(self, messages: List[Dict[str, Any]], stream: bool = False, **kwargs: Any) -> Any:
        return await self._async.chat_completion(messages=messages, stream=stream, **kwargs)

    # --- Text Generation ---
    def text_generation(self, prompt: str, stream: bool = False, **kwargs: Any) -> Any:
        return self._sync.text_generation(prompt, stream=stream, **kwargs)

    async def atext_generation(self, prompt: str, stream: bool = False, **kwargs: Any) -> Any:
        return await self._async.text_generation(prompt, stream=stream, **kwargs)

    # --- Utils ---
    def create_tool_call_object(self, name: str, args: dict) -> Any:
        """HF 내부 규격인 ToolCall 객체를 동적으로 생성하여 반환합니다."""
        from huggingface_hub.inference._generated.types import (
            ChatCompletionOutputToolCall,
            ChatCompletionOutputFunctionDefinition,
        )
        return ChatCompletionOutputToolCall(
            id=name,
            type="function",
            function=ChatCompletionOutputFunctionDefinition(arguments=args, name=name),
        )

    async def close_async(self) -> None:
        """비동기 클라이언트 세션을 종료합니다."""
        # AsyncInferenceClient가 close 메서드를 제공할 경우 호출
        if hasattr(self._async, "close"):
            await self._async.close()