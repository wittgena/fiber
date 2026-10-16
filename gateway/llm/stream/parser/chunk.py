# fiber.gateway.llm.stream.parser.chunk
import json
from typing import Any, Dict, List, Optional, Union
from typing_extensions import TypedDict
from fiber.llm.compat.registry import EXTRACTION_RULES
from fiber.llm.compat.extraction import PROVIDER_RULE_ALIAS
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("chunk.parser")


class ParsedChunk(TypedDict):
    """Parser가 Accumulator로 넘겨주는 단일화된 표준 데이터 규격"""
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
    """Dict, List, Object 혼합 토폴로지를 Dot(.) 표기법으로 안전하게 탐색"""
    
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
                
                # Dictionary 탐색
                if isinstance(current, dict):
                    current = current.get(k)

                # List/Tuple 탐색
                elif isinstance(current, (list, tuple)):
                    try:
                        current = current[int(k)]
                    except (IndexError, ValueError):
                        found = False
                        break
                
                # Pydantic V2 및 일반 Object 탐색
                else:
                    if hasattr(current, "model_dump") and callable(getattr(current, "model_dump")):
                        try:
                            current = current.model_dump(exclude_unset=True).get(k)
                            continue
                        except Exception:
                            pass
                    current = getattr(current, k, None)
            
            if found and current is not None:
                return current
        return default


class StreamChunkParser:
    @staticmethod
    def _preprocess_chunk(chunk: Any) -> Any:
        """원시 Bytes/Str 데이터 및 SSE 포맷을 탐색 가능한 객체(Dict)로 변환"""
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8")
            
        if isinstance(chunk, str):
            chunk = chunk.strip()
            
            # SSE [DONE] 시그널 처리
            if "data: [DONE]" in chunk or chunk == "[DONE]":
                return {"_internal_signal": "DONE"}
                
            # SSE Prefix 제거 (data: 또는 data: )
            if chunk.startswith("data:"):
                chunk = chunk.replace("data:", "", 1).strip()
                
            # JSON 파싱 시도
            try:
                if chunk.startswith("{") or chunk.startswith("["):
                    return json.loads(chunk)
            except json.JSONDecodeError:
                pass
                
            return chunk
            
        return chunk

    @classmethod
    def get_ruleset(cls, provider: Optional[str]) -> Dict[str, Any]:
        """Provider 문자열을 기반으로 적절한 룰셋을 로드합니다."""
        if not provider:
            return EXTRACTION_RULES["openai"]
            
        rule_key = PROVIDER_RULE_ALIAS.get(provider, provider)
        return EXTRACTION_RULES.get(rule_key, EXTRACTION_RULES.get("openai"))

    @classmethod
    def parse(cls, provider: Optional[str], raw_chunk: Any) -> Optional[ParsedChunk]:
        """원시 청크를 받아 Accumulator가 소비할 수 있는 순수 데이터(ParsedChunk)로 변환"""
        if isinstance(raw_chunk, dict) and "original_chunk" in raw_chunk:
            return raw_chunk

        # 원시 데이터 전처리 (SSE 파싱 및 JSON 디코딩)
        obj = cls._preprocess_chunk(raw_chunk)
        
        # 빈 데이터 방어
        if obj is None or obj == "":
            return cls._empty_parsed_chunk()

        # 시스템 종료 시그널 처리
        if isinstance(obj, dict) and obj.get("_internal_signal") == "DONE":
            return cls._empty_parsed_chunk(is_finished=True, finish_reason="stop")

        # 에러 감지 및 예외 발생
        if isinstance(obj, dict) and obj.get("error"):
            raise ValueError(f"Provider '{provider}' returned stream error: {obj.get('error')}")

        # 룰셋 로드 및 필드 추출
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

        # 상태(is_finished) 추론 로직
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
        if not usage_rule or obj is None or isinstance(obj, str):
            return None
            
        # 1. 다중 룰(리스트)인 경우 순차적으로 탐색(Fallback)
        if isinstance(usage_rule, list):
            for rule in usage_rule:
                res = StreamChunkParser._extract_usage(obj, rule)
                if res and any(v is not None for v in res.values()):
                    return res
            return None

        # 2. Dict 매핑 룰인 경우 (prompt_tokens, completion_tokens, total_tokens)
        if isinstance(usage_rule, dict):
            p_raw = StateTraverser.resolve(obj, usage_rule.get("prompt_tokens"))
            c_raw = StateTraverser.resolve(obj, usage_rule.get("completion_tokens"))
            t_raw = StateTraverser.resolve(obj, usage_rule.get("total_tokens"))
            
            # 세 필드 모두 추출되지 않은 일반 청크면 즉시 None 반환 (빈 Dict 반환 방지)
            if p_raw is None and c_raw is None and t_raw is None:
                return None

            def _to_int(val: Any) -> Optional[int]:
                if val is None:
                    return None
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return None

            p_tokens = _to_int(p_raw)
            c_tokens = _to_int(c_raw)
            t_tokens = _to_int(t_raw)

            # total_tokens가 없는데 prompt나 completion이 있으면 합산 보정
            if t_tokens is None and (p_tokens is not None or c_tokens is not None):
                t_tokens = (p_tokens or 0) + (c_tokens or 0)

            # 기존 Accumulator 하위 호환성을 위해 키 구조 유지
            result: Dict[str, Any] = {
                "prompt_tokens": p_tokens,
                "completion_tokens": c_tokens,
            }
            if t_tokens is not None:
                result["total_tokens"] = t_tokens

            return result if any(v is not None for v in result.values()) else None
            
        # 3. 단일 문자열 경로(예: "usage", "usage_metadata")인 경우
        usage_obj = StateTraverser.resolve(obj, usage_rule, None)
        if usage_obj is None:
            return None

        if isinstance(usage_obj, dict):
            return usage_obj if any(v is not None for v in usage_obj.values()) else None
            
        if hasattr(usage_obj, "model_dump") and callable(getattr(usage_obj, "model_dump")):
            try:
                dumped = usage_obj.model_dump(exclude_unset=True)
                return dumped if dumped else None
            except Exception:
                pass
                
        if hasattr(usage_obj, "__dict__"):
            dumped = {k: v for k, v in vars(usage_obj).items() if not k.startswith("_") and v is not None}
            return dumped if dumped else None

        try:
            return dict(usage_obj)
        except Exception:
            return None

    @staticmethod
    def _empty_parsed_chunk(is_finished: bool = False, finish_reason: Optional[str] = None) -> ParsedChunk:
        return ParsedChunk(
            id=None, text="", is_finished=is_finished, finish_reason=finish_reason,
            usage=None, logprobs=None, tool_calls=None,
            system_fingerprint=None, provider_specific_fields=None, original_chunk=None
        )