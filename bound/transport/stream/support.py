# bound.transport.stream.support
## @lineage: bound.surface.stream.support
from typing import Any, List
from adapter.switch.params import ModelResponseStream, Usage
from watcher.plane.emitter import get_emitter

_SYNC_ITER_EXHAUSTED = object()
log = get_emitter("stream.support")

def preserve_upstream_non_openai_attributes(
    model_response: ModelResponseStream, original_chunk: Any
) -> None:
    if not hasattr(original_chunk, "model_dump"):
        return

    ## Pydantic v2 호환성을 위해 type(obj).model_fields 사용
    expected_keys = set(type(model_response).model_fields.keys()).union({"usage"})
    for key, value in original_chunk.model_dump().items():
        if key not in expected_keys:
            setattr(model_response, key, value)


def _next_sync_or_exhausted(it: Any) -> Any:
    try:
        return next(it)
    except StopIteration:
        return _SYNC_ITER_EXHAUSTED


def calculate_total_usage(chunks: List[Any]) -> Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0

    for chunk in chunks:
        usage = chunk.get("usage") if isinstance(chunk, dict) else getattr(chunk, "usage", None)
        if usage is not None:
            if isinstance(usage, dict):
                prompt_tokens = usage.get("prompt_tokens", 0) or 0
                completion_tokens = usage.get("completion_tokens", 0) or 0
            else:
                prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
                completion_tokens = getattr(usage, "completion_tokens", 0) or 0

    return Usage(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
    )