# fiber.llm.model.token.window
import copy
from typing import Any, Dict, List, Optional, Tuple, Union
import tiktoken

from fiber.llm.model.token.encoder import get_default_encoding
from fiber.infra.ops.finops.cost.registry import ModelCostRegistry
from fiber.llm.model.types.provider.openai import AllMessageValues
from xphi.arch.contract.config.constants import DEFAULT_TRIM_RATIO

from fiber.llm.model.token.counter import token_counter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("token.window")

class ContextWindow:
    def __init__(self, model: str = "gpt-3.5-turbo"):
        self.model = model
        try:
            if "gpt-4o" in self.model:
                self.encoding = get_default_encoding("o200k_base")
            else:
                self.encoding = tiktoken.encoding_for_model(self.model)
        except KeyError:
            self.encoding = get_default_encoding("cl100k_base")

    def encode(self, text: str) -> List[int]:
        return self.encoding.encode(text, disallowed_special=())

    def decode(self, tokens: List[int]) -> str:
        return self.encoding.decode(tokens)

    def truncate_to_limit(self, text: str, max_tokens: int) -> str:
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


"""External Export Facade (Legacy Signature Match)"""
def trim_messages(
    messages: List[AllMessageValues],
    model: Optional[str] = None,
    trim_ratio: float = DEFAULT_TRIM_RATIO,
    return_response_tokens: bool = False,
    max_tokens: Optional[int] = None,
) -> Union[List[AllMessageValues], Tuple[List[AllMessageValues], int]]:
    original_messages = messages
    messages = copy.deepcopy(messages)
    safe_model = model or "gpt-3.5-turbo"
    
    try:
        if max_tokens is None:
            cost_info = ModelCostRegistry.get_cost(model) if model else {}
            if cost_info:
                max_tokens_for_model = cost_info.get("max_input_tokens", cost_info.get("max_tokens", 4096))
                max_tokens = int(max_tokens_for_model * trim_ratio)
            else:
                log.warning(f"[token.window] 모델 '{model}'의 최대 토큰 정보를 찾을 수 없어 원본을 반환합니다.")
                return messages if not return_response_tokens else (messages, 0)

        current_tokens = token_counter(model=safe_model, messages=messages)
        log.debug(f"[token.window] Current tokens: {current_tokens}, max tokens: {max_tokens}")

        if current_tokens <= max_tokens:
            return messages if not return_response_tokens else (messages, 0)

        tool_messages = []
        for message in reversed(messages):
            if message.get("role") != "tool":
                break
            tool_messages.append(message)
        tool_messages.reverse()
        
        if tool_messages:
            messages = messages[: -len(tool_messages)]
            max_tokens -= token_counter(model=safe_model, messages=tool_messages)
            if max_tokens <= 0:
                log.warning("[token.window] Tool 메시지만으로도 한계치를 초과했습니다.")
                return tool_messages if not return_response_tokens else (tool_messages, 0)

        window_engine = ContextWindow(model=safe_model)
        system_message_content = ""
        other_messages = []
        
        for msg in messages:
            if msg.get("role") == "system":
                system_message_content += "\n" + str(msg.get("content", "")) if system_message_content else str(msg.get("content", ""))
            else:
                other_messages.append(msg)

        system_message_event = None
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

        final_messages = []
        if max_tokens > 0 and other_messages:
            for msg in reversed(other_messages):
                msg_tokens = token_counter(model=safe_model, messages=[msg])
                if max_tokens - msg_tokens >= 0:
                    final_messages.insert(0, msg)
                    max_tokens -= msg_tokens
                else:
                    break

        if system_message_event:
            final_messages.insert(0, system_message_event)
            
        if tool_messages:
            final_messages.extend(tool_messages)

        log.debug(f"[token.window] Final trimmed messages length: {len(final_messages)}")
        if return_response_tokens:
            final_token_count = token_counter(model=safe_model, messages=final_messages)
            safe_model_cost = ModelCostRegistry.get_cost(safe_model)
            response_tokens = max(0, int((safe_model_cost.get("max_input_tokens", 4096) * trim_ratio) - final_token_count))
            return final_messages, response_tokens
            
        return final_messages
    except Exception as e:
        log.exception(f"[token.window] Got exception while token trimming (Fallback to original): {e}")
        return original_messages if not return_response_tokens else (original_messages, 0)