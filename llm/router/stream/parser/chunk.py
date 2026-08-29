# fiber.llm.router.stream.parser.chunk
## @lineage: fiber.llm.stream.parser.chunk
## @lineage: llm.stream.parser.chunk
## @lineage: agent.llm.stream.parser.chunk
## @lineage: ator.driver.llm.stream.parser.chunk
## @lineage: phase.llm.stream.parser.chunk
## @lineage: phase.stream.parser.chunk
## @lineage: engine.stream.parser.chunk
## @lineage: engine.parser.stream.chunk
import json
from typing import Any, Dict, List, Optional, Union
from typing_extensions import TypedDict
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("chunk.parser")

STREAM_EXTRACTION_RULES = {
    "openai": {
        "text": "choices.0.delta.content",
        "finish_reason": "choices.0.finish_reason",
        "logprobs": "choices.0.logprobs",
        "usage": "usage",
        "tool_calls": "choices.0.delta.tool_calls"
    },
    "text-completion-openai": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
        "usage": "usage"
    },
    "text-completion-codestral": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
        "usage": "usage"
    },
    "azure": {
        "text": "choices.0.delta.content",
        "finish_reason": "choices.0.finish_reason",
    },
    "azure_text": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
    },
    "replicate": {
        "text": "output",
        "error": "error",
        "is_finished_cond": {"path": "status", "value": "succeeded"},
        "finish_reason_static": "stop"
    },
    "predibase": {
        "text": "token.text",
        "finish_reason": ["details.finish_reason", "generated_text"]
    },
    "baseten": {
        "text": ["token.text", "model_output.data.0", "model_output", "completion"]
    },
    "ai21": {
        "text": "completions.0.data.text",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "maritalk": {
        "text": "answer",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "aleph_alpha": {
        "text": "completions.0.completion",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "triton": {
        "text": "text_output",
        "finish_reason": "stop_reason",
        "is_finished_cond": {"path": "is_finished", "value": True},
        "usage": {
            "prompt_tokens": "input_token_count",
            "completion_tokens": "generated_token_count"
        }
    }
}

PROVIDER_RULE_ALIAS = {
    "azure": "openai",
    "azure_ai": "openai",
    "custom_openai": "openai",
    "sagemaker_chat": "openai",
    "nlp_cloud": "openai",
    "gemini": "vertex_ai",
}


class ParsedChunk(TypedDict):
    """Parser가 Accumulator로 넘겨주는 단일화된 표준 데이터 규격입니다."""
    id: Optional[str]
    text: str
    is_finished: bool
    finish_reason: Optional[str]
    usage: Optional[Dict[str, Any]]
    logprobs: Optional[Any]
    tool_calls: Optional[List[Any]]
    system_fingerprint: Optional[str]
    provider_specific_fields: Optional[Dict[str, Any]]
    original_chunk: Any

class StateTraverser:
    """Dict, List, Object 혼합 토폴로지를 Dot(.) 표기법으로 안전하게 탐색합니다."""
    
    @staticmethod
    def resolve(obj: Any, paths: Union[str, List[str]], default: Any = None) -> Any:
        if not paths or obj is None:
            return default

        if isinstance(paths, str):
            paths = [paths]

        for path in paths:
            keys = path.split('.')
            current = obj
            found = True
            
            for k in keys:
                if current is None:
                    found = False
                    break
                
                # 1. Dictionary 탐색
                if isinstance(current, dict):
                    current = current.get(k)
                # 2. List/Tuple 탐색
                elif isinstance(current, (list, tuple)):
                    try:
                        current = current[int(k)]
                    except (IndexError, ValueError):
                        found = False
                        break
                # 3. Pydantic V2 및 일반 Object 탐색
                else:
                    if hasattr(current, "model_dump") and callable(getattr(current, "model_dump")):
                        # Pydantic v2 객체일 경우 dict로 덤프 후 탐색 시도 (안전성 강화)
                        try:
                            current = current.model_dump(exclude_unset=True).get(k)
                            continue
                        except Exception:
                            pass
                    current = getattr(current, k, None)
            
            if found and current is not None:
                return current  # 매칭되는 첫 번째 유효 경로 반환
        return default


# =====================================================================
# 4. Stream Chunk Parser (통합 파서)
# =====================================================================
class StreamChunkParser:
    """거대 분기문 없이 선언적 룰셋을 기반으로 스트림 청크를 파싱하는 통합 파서입니다."""

    @staticmethod
    def _preprocess_chunk(chunk: Any) -> Any:
        """원시 Bytes/Str 데이터 및 SSE 포맷을 탐색 가능한 객체(Dict)로 변환합니다."""
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
            
        if isinstance(chunk, str):
            chunk = chunk.strip()
            
            # 1. SSE [DONE] 시그널 처리
            if "data: [DONE]" in chunk or chunk == "[DONE]":
                return {"_internal_signal": "DONE"}
                
            # 2. SSE Prefix 제거 (data: 또는 data: )
            # 다중 스트림 이벤트 방어를 위해 첫 번째 유효 JSON 포맷만 추출 시도
            if chunk.startswith("data:"):
                chunk = chunk.replace("data:", "", 1).strip()
                
            # 3. JSON 파싱 시도
            try:
                if chunk.startswith("{") or chunk.startswith("["):
                    return json.loads(chunk)
            except json.JSONDecodeError:
                pass
                
            # JSON이 아닌 순수 문자열일 경우 그대로 반환
            return chunk
            
        return chunk

    @classmethod
    def get_ruleset(cls, provider: Optional[str]) -> Dict[str, Any]:
        """Provider 문자열을 기반으로 적절한 룰셋을 로드합니다."""
        if not provider:
            return STREAM_EXTRACTION_RULES["openai"]
            
        rule_key = PROVIDER_RULE_ALIAS.get(provider, provider)
        return STREAM_EXTRACTION_RULES.get(rule_key, STREAM_EXTRACTION_RULES["openai"])

    @classmethod
    def parse(cls, provider: Optional[str], raw_chunk: Any) -> Optional[ParsedChunk]:
        """
        [핵심 라우터] 원시 청크를 받아 Accumulator가 소비할 수 있는 순수 데이터(ParsedChunk)로 변환합니다.
        """
        # 1. 원시 데이터 전처리
        obj = cls._preprocess_chunk(raw_chunk)
        
        # 빈 데이터 방어
        if obj is None or obj == "":
            return cls._empty_parsed_chunk()

        # 2. 시스템 종료 시그널 처리
        if isinstance(obj, dict) and obj.get("_internal_signal") == "DONE":
            return cls._empty_parsed_chunk(is_finished=True, finish_reason="stop")

        # 3. 에러 감지 및 예외 발생
        if isinstance(obj, dict) and obj.get("error"):
            raise ValueError(f"Provider '{provider}' returned stream error: {obj.get('error')}")

        # 4. 룰셋 로드 및 필드 추출
        rules = cls.get_ruleset(provider)
        
        # 텍스트 추출 (raw_chunk 자체가 순수 문자열인 경우 방어)
        text = obj if isinstance(obj, str) else StateTraverser.resolve(obj, rules.get("text", []), "")
        
        # 메타데이터 추출
        finish_reason = StateTraverser.resolve(obj, rules.get("finish_reason", []), None)
        logprobs = StateTraverser.resolve(obj, rules.get("logprobs", []), None)
        chunk_id = StateTraverser.resolve(obj, rules.get("id", []), None)
        sys_fp = StateTraverser.resolve(obj, rules.get("system_fingerprint", []), None)
        tool_calls = StateTraverser.resolve(obj, rules.get("tool_calls", []), None)
        provider_specific = StateTraverser.resolve(obj, rules.get("provider_specific_fields", []), None)

        # Usage 특수 매핑
        usage = cls._extract_usage(obj, rules.get("usage"))

        # 5. 상태(is_finished) 추론 로직
        is_finished = False
        if "is_finished_static" in rules:
            is_finished = rules["is_finished_static"]
            finish_reason = rules.get("finish_reason_static", finish_reason)
            
        elif "is_finished_cond" in rules:
            cond = rules["is_finished_cond"]
            val = StateTraverser.resolve(obj, cond["path"])
            if val == cond["value"]:
                is_finished = True
                finish_reason = rules.get("finish_reason_static", "stop")
                
        elif finish_reason is not None:
            is_finished = True

        return ParsedChunk(
            id=chunk_id,
            text=text,
            is_finished=is_finished,
            finish_reason=finish_reason,
            usage=usage,
            logprobs=logprobs,
            tool_calls=tool_calls,
            system_fingerprint=sys_fp,
            provider_specific_fields=provider_specific,
            original_chunk=raw_chunk
        )

    @staticmethod
    def _extract_usage(obj: Any, usage_rule: Any) -> Optional[Dict[str, Any]]:
        """Usage 규칙이 복잡한 경우(사전 매핑)를 처리합니다."""
        if not usage_rule:
            return None
        if isinstance(usage_rule, dict):
            return {
                "prompt_tokens": StateTraverser.resolve(obj, usage_rule.get("prompt_tokens")),
                "completion_tokens": StateTraverser.resolve(obj, usage_rule.get("completion_tokens"))
            }
        return StateTraverser.resolve(obj, usage_rule, None)

    @staticmethod
    def _empty_parsed_chunk(is_finished: bool = False, finish_reason: Optional[str] = None) -> ParsedChunk:
        """기본값이 채워진 빈 ParsedChunk를 반환합니다."""
        return ParsedChunk(
            id=None, text="", is_finished=is_finished, finish_reason=finish_reason,
            usage=None, logprobs=None, tool_calls=None,
            system_fingerprint=None, provider_specific_fields=None, original_chunk=None
        )