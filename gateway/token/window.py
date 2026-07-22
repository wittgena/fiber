# gateway.token.window
## @lineage: bound.eco.token.window
## @lineage: bound.token.window
## @lineage: bound.surface.token.window
## @lineage: bound.adapter.surface.token.window
## @lineage: anchor.provider.token.window
## @lineage: anchor.registry.provider.token.window
## @lineage: anchor.provider.model.token.window
## @lineage: anchor.model.token.window
import copy
from typing import Any, Dict, List, Optional, Tuple, Union
import tiktoken

from gateway.token.counter import token_counter
from gateway.token.convert import get_default_encoding
from resolver.model.cost import model_cost
from eco.legacy.openai.types import AllMessageValues
from resolver.model.config.constants import DEFAULT_TRIM_RATIO

from watcher.plane.emitter import get_emitter

log = get_emitter("token.window")

class ContextWindow:
    """
    @manifold: Safe Context Truncator (Internal Engine)
    @desc: 문자열 길이나 나이브한 슬라이싱에 의존하지 않고, 토큰 인코딩 기반의 정밀한 절단을 수행합니다.
           외부 모듈(encoder) 의존 없이 내부적으로 tiktoken을 활용하여 자급자족합니다.
    """
    def __init__(self, model: str = "gpt-3.5-turbo"):
        self.model = model
        # convert.py에 구현된 안전한 레이스 컨디션 방지 로드 함수를 사용합니다.
        try:
            if "gpt-4o" in self.model:
                self.encoding = get_default_encoding("o200k_base")
            else:
                # tiktoken 기본 지원 모델인지 확인, 실패시 cl100k_base 폴백
                self.encoding = tiktoken.encoding_for_model(self.model)
        except KeyError:
            self.encoding = get_default_encoding("cl100k_base")

    def encode(self, text: str) -> List[int]:
        """내장된 인코더를 사용하여 텍스트를 토큰 ID 리스트로 변환"""
        return self.encoding.encode(text, disallowed_special=())

    def decode(self, tokens: List[int]) -> str:
        """토큰 ID 리스트를 다시 문자열로 디코딩"""
        return self.encoding.decode(tokens)

    def truncate_to_limit(self, text: str, max_tokens: int) -> str:
        """
        문자열을 주어진 토큰 수 이하가 되도록 안전하게 잘라냅니다.
        """
        if not text or max_tokens <= 0:
            return ""

        token_ids = self.encode(text)
        if len(token_ids) <= max_tokens:
            return text

        ## @truncate: 명시적으로 허용된 토큰 한계선까지만 슬라이싱
        truncated_token_ids = token_ids[:max_tokens]
        
        try:
            return self.decode(truncated_token_ids)
        except Exception as e:
            log.error(f"[ContextWindow] Truncation 디코딩 실패: {e}")
            return text  # 복구 불가능 시 원본 반환


# ==========================================
# External Export Facade (Legacy Signature Match)
# ==========================================

def trim_messages(
    messages: List[AllMessageValues],
    model: Optional[str] = None,
    trim_ratio: float = DEFAULT_TRIM_RATIO,
    return_response_tokens: bool = False,
    max_tokens: Optional[int] = None,
) -> Union[List[AllMessageValues], Tuple[List[AllMessageValues], int]]:
    """
    @manifold: Token Trim Interface
    @desc: 
    - 기존 외부 모듈들과의 시그니처 호환성을 완벽히 유지하는 단일 진입점입니다.
    - 내부적으로 ContextWindow 객체를 생성하여 안전한 텍스트 절단을 수행합니다.
    """
    original_messages = messages
    messages = copy.deepcopy(messages)
    safe_model = model or "gpt-3.5-turbo"
    
    try:
        ## 1. 토큰 한계치(Limit) 결정
        if max_tokens is None:
            if model in model_cost:
                max_tokens_for_model = model_cost[model].get("max_input_tokens", model_cost[model].get("max_tokens", 4096))
                max_tokens = int(max_tokens_for_model * trim_ratio)
            else:
                log.warning(f"[token.window] 모델 '{model}'의 최대 토큰 정보를 찾을 수 없어 원본을 반환합니다.")
                return messages if not return_response_tokens else (messages, 0)

        current_tokens = token_counter(model=safe_model, messages=messages)
        log.debug(f"[token.window] Current tokens: {current_tokens}, max tokens: {max_tokens}")

        ## 2. 한계치 이내라면 그대로 반환
        if current_tokens <= max_tokens:
            return messages if not return_response_tokens else (messages, 0)

        ## 3. Tool 메시지 분리 (마지막 tool 호출 기록은 필수 보존)
        tool_messages = []
        for message in reversed(messages):
            if message.get("role") != "tool":
                break
            tool_messages.append(message)
        tool_messages.reverse()
        
        if tool_messages:
            messages = messages[: -len(tool_messages)]
            # 툴 메시지가 차지하는 공간 차감
            max_tokens -= token_counter(model=safe_model, messages=tool_messages)
            if max_tokens <= 0:
                log.warning("[token.window] Tool 메시지만으로도 한계치를 초과했습니다.")
                return tool_messages if not return_response_tokens else (tool_messages, 0)

        ## 4. 엔진 초기화 (자체 인코딩 능력을 갖춘 윈도우 엔진)
        window_engine = ContextWindow(model=safe_model)

        ## 5. 메시지 분리 및 시스템 프롬프트 병합
        system_message_content = ""
        other_messages = []
        
        for msg in messages:
            if msg.get("role") == "system":
                system_message_content += "\n" + str(msg.get("content", "")) if system_message_content else str(msg.get("content", ""))
            else:
                other_messages.append(msg)

        system_message_event = None
        
        ## 6. 시스템 프롬프트 우선 보존 및 절단
        if system_message_content:
            mock_sys_msg = {"role": "system", "content": system_message_content}
            system_tokens = token_counter(model=safe_model, messages=[mock_sys_msg])
            
            if system_tokens > max_tokens:
                log.warning("[token.window] 시스템 메시지가 최대 한계치를 초과했습니다. 안전 절단을 시도합니다.")
                safe_truncate_limit = max(1, max_tokens - 5) # 딕셔너리 오버헤드 보정
                truncated_content = window_engine.truncate_to_limit(system_message_content, safe_truncate_limit)
                system_message_event = {"role": "system", "content": truncated_content}
                max_tokens = 0
            else:
                system_message_event = mock_sys_msg
                max_tokens -= system_tokens

        ## 7. 일반 메시지 롤링 탈락 (가장 오래된 것부터 Eviction)
        final_messages = []
        if max_tokens > 0 and other_messages:
            for msg in reversed(other_messages):
                msg_tokens = token_counter(model=safe_model, messages=[msg])
                if max_tokens - msg_tokens >= 0:
                    final_messages.insert(0, msg)
                    max_tokens -= msg_tokens
                else:
                    break # 한계 초과 시 남은 과거 대화 폐기

        ## 8. 조립 및 반환
        if system_message_event:
            final_messages.insert(0, system_message_event)
            
        if tool_messages:
            final_messages.extend(tool_messages)

        log.debug(f"[token.window] Final trimmed messages length: {len(final_messages)}")
        if return_response_tokens:
            final_token_count = token_counter(model=safe_model, messages=final_messages)
            response_tokens = max(0, int((model_cost.get(safe_model, {}).get("max_input_tokens", 4096) * trim_ratio) - final_token_count))
            return final_messages, response_tokens
            
        return final_messages

    except Exception as e:
        log.exception(f"[token.window] Got exception while token trimming (Fallback to original): {e}")
        return original_messages if not return_response_tokens else (original_messages, 0)