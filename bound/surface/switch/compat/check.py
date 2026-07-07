# bound.surface.switch.compat.check
## @lineage: bound.channel.compat.check
from typing import Any, List, Optional, Protocol, runtime_checkable

@runtime_checkable
class DeltaProtocol(Protocol):
    """스트림 청크의 델타(delta) 구조"""
    content: Optional[str]

@runtime_checkable
class StreamingChoiceProtocol(Protocol):
    """스트림 청크의 choices 리스트 내부 요소 구조"""
    delta: DeltaProtocol

@runtime_checkable
class ModelResponseStreamProtocol(Protocol):
    """ModelResponseStream이 반드시 갖춰야 할 구조 (litellm/blm 공통)"""
    id: str
    object: str
    choices: List[StreamingChoiceProtocol]

    def get(self, key: str, default: Any = None) -> Any: ...
    def json(self, **kwargs) -> Any: ...


@runtime_checkable
class ModelResponseProtocol(Protocol):
    """ModelResponse가 반드시 갖춰야 할 구조 (litellm/blm 공통)"""
    id: str
    object: str
    choices: List[Any]

    def get(self, key: str, default: Any = None) -> Any: ...
    def json(self, **kwargs) -> Any: ...


# ==============================================================================
# 2. Type Guard 함수 (외부에서 사용할 검사 및 추출기)
# ==============================================================================

def is_model_response_stream(obj: Any) -> bool:
    """
    객체가 ModelResponseStream의 구조(Protocol)를 완벽히 따르는지 검사합니다.
    (litellm, blm 클래스 모두 True 반환)
    """
    return isinstance(obj, ModelResponseStreamProtocol)

def is_model_response(obj: Any) -> bool:
    """객체가 ModelResponse의 구조(Protocol)를 완벽히 따르는지 검사합니다."""
    return isinstance(obj, ModelResponseProtocol)

def extract_stream_content(chunk: Any) -> Optional[str]:
    """
    스트림 청크에서 안전하게 텍스트 컨텐츠를 추출합니다.
    객체 속성 접근(obj.choices)과 딕셔너리 접근(dict["choices"])을 모두 지원합니다.
    """
    try:
        ## Protocol 형태 (Pydantic 모델 등)인 경우
        if is_model_response_stream(chunk):
            if chunk.choices and len(chunk.choices) > 0:
                delta = getattr(chunk.choices[0], "delta", None)
                if delta:
                    return getattr(delta, "content", None)
        ## 순수 딕셔너리 형태인 경우 (최소한의 Fallback)
        elif isinstance(chunk, dict):
            choices = chunk.get("choices", [])
            if choices and isinstance(choices[0], dict):
                return choices[0].get("delta", {}).get("content")
    except Exception:
        pass
    return None