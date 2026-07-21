# bound.transport.stream.parser
## @lineage: xor.opt.analyzer.parser.stream.chunk
## @lineage: xphi.analyzer.parser.stream.chunk
## @lineage: bound.transport.stream.chunk.parser
import json
import logging
from typing import Any, Dict, List, Optional, Union
from watcher.plane.emitter import get_emitter

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

# 벤더 이름 별칭(Alias) 맵: 동일한 포맷을 사용하는 벤더 라우팅
PROVIDER_RULE_ALIAS = {
    "azure_ai": "azure",
    "custom_openai": "openai",
    "sagemaker_chat": "openai",
    "nlp_cloud": "openai" # nlp_cloud가 dolphin 모델 등에서 openai 호환을 쓸 경우
}


# =====================================================================
# 2. 상태 탐색 엔진 (State Traverser Engine)
# =====================================================================
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
                
                if isinstance(current, dict):
                    current = current.get(k)
                elif isinstance(current, (list, tuple)):
                    try:
                        current = current[int(k)]
                    except (IndexError, ValueError):
                        found = False
                        break
                else:
                    current = getattr(current, k, None)
            
            if found and current is not None:
                return current  # 매칭되는 첫 번째 유효 경로 반환

        return default


# =====================================================================
# 3. 통합 파서 (Declarative Stream Parser)
# =====================================================================
class StreamChunkParser:
    """거대 분기문 없이 선언적 룰셋을 기반으로 스트림 청크를 파싱하는 통합 클래스"""

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
            if chunk.startswith("data:"):
                chunk = chunk.replace("data:", "", 1).strip()
                
            # 3. JSON 파싱 시도
            try:
                if chunk.startswith("{") or chunk.startswith("["):
                    return json.loads(chunk)
            except json.JSONDecodeError:
                pass
                
            # 일반 문자열일 경우 그대로 반환
            return chunk
            
        # 이미 Pydantic 모델(Object)이거나 Dict인 경우 원본 반환
        return chunk

    @classmethod
    def get_ruleset(cls, provider: Optional[str]) -> Dict[str, Any]:
        """Provider 문자열을 기반으로 적절한 룰셋을 로드합니다."""
        if not provider:
            return STREAM_EXTRACTION_RULES["openai"]
            
        rule_key = PROVIDER_RULE_ALIAS.get(provider, provider)
        return STREAM_EXTRACTION_RULES.get(rule_key, STREAM_EXTRACTION_RULES["openai"])

    @classmethod
    def parse(cls, provider: Optional[str], raw_chunk: Any) -> Optional[Dict[str, Any]]:
        """
        [핵심 라우터] 모든 스트림 청크의 파싱을 일원화하여 처리합니다.
        StreamWrapper에서는 오직 이 메서드만 호출하면 됩니다.
        """
        # 1. 전처리 (Bytes -> Str -> Dict/Object)
        obj = cls._preprocess_chunk(raw_chunk)
        
        if obj is None or obj == "":
            return {"text": "", "is_finished": False, "finish_reason": None}

        # 2. 시스템 종료 시그널 처리
        if isinstance(obj, dict) and obj.get("_internal_signal") == "DONE":
            return {"text": "", "is_finished": True, "finish_reason": "stop"}

        # 3. 에러 감지 및 방어적 프로그래밍
        if isinstance(obj, dict) and obj.get("error"):
            raise ValueError(f"Provider '{provider}' returned stream error: {obj.get('error')}")

        # 4. 룰셋 로드 및 데이터 추출
        rules = cls.get_ruleset(provider)
        
        # text 추출 시, raw_chunk 자체가 순수 텍스트인 경우 방어
        if isinstance(obj, str):
            text = obj
        else:
            text = StateTraverser.resolve(obj, rules.get("text", []), "")

        finish_reason = StateTraverser.resolve(obj, rules.get("finish_reason", []), None)
        logprobs = StateTraverser.resolve(obj, rules.get("logprobs", []), None)
        
        # Usage 특수 처리 (Dict 매핑이 필요한 경우 방어)
        usage_rule = rules.get("usage")
        if isinstance(usage_rule, dict):
            usage = {
                "prompt_tokens": StateTraverser.resolve(obj, usage_rule.get("prompt_tokens")),
                "completion_tokens": StateTraverser.resolve(obj, usage_rule.get("completion_tokens"))
            }
        else:
            usage = StateTraverser.resolve(obj, usage_rule, None)

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

        return {
            "text": text,
            "is_finished": is_finished,
            "finish_reason": finish_reason,
            "usage": usage,
            "logprobs": logprobs,
            "original_chunk": raw_chunk,
            "tool_calls": StateTraverser.resolve(obj, rules.get("tool_calls", []), None)
        }