# fiber.gateway.llm.mapper.traverser
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List, Tuple, Optional, Union, Dict

from fiber.llm.types.inter.block import MessageRole
from fiber.llm.types.inter.response import ChatMessage

from xphi.arch.bound.event.next import uuid4 
from xphi.kernel.space.bind.resolver import get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

STATE_EXTRACTION_RULES = {
    "gemini": {
        "fallback_tool_name": "content.parts.0.function_call.name",
        "fallback_tool_args": "content.parts.0.function_call.args",
        "sync_content_paths": ["candidates.0.content.parts.0.text", "choices.0.message.content"],
        "sync_usage_paths": ["usageMetadata", "usage"]
    },
    "ollama": {
        "sync_content_paths": ["message.content", "response", "choices.0.message.content"],
        "sync_usage_paths": ["prompt_eval_count", "usage"]
    },
    "defaults": {
        "role": "assistant",
        "finish_stop": "stop",
        "finish_tool": "tool_calls",
        "stream_content_paths": [
            "delta",                     # Default
            "choices.0.delta.content",   # OpenAI/LiteLLM 표준 경로
            "content.parts.0.text"       # Gemini Native JSON 경로
        ],
        "sync_content_paths": [
            "choices.0.message.content", # Default
            "message.content",           # Generic
            "output"                     # Replicate 등
        ],
        "sync_usage_paths": [
            "usage",                     # Default
            "meta.usage"
        ]
    }
}

ENDPOINT_ROUTING_RULES = {
    "ollama": {
        "native_suffix": "/api/chat",
        "openai_suffix": "/chat/completions",
        "v1_indicator": "/v1"
    },
    "defaults": {
        "openai_suffix": "/chat/completions"
    }
}

class StateTraverser:
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
                return current
                
        return default


class ImperativeFallbackRule:
    @staticmethod
    def parse_content_blocks(raw_content: Any) -> str:
        if isinstance(raw_content, str):
            return raw_content
            
        if not isinstance(raw_content, list):
            return str(raw_content)

        text_chunks = []
        for block in raw_content:
            if hasattr(block, "get"):  # dict 형태
                if block.get("type") == "text":
                    text_chunks.append(block.get("text", ""))
            elif hasattr(block, "text"):  # TextContent 같은 객체 형태
                text_chunks.append(block.text)
            elif isinstance(block, str):
                text_chunks.append(block)
        return "".join(text_chunks)

    @staticmethod
    def recover_tool_call(raw_resp: Any) -> Tuple[Optional[str], Optional[Any]]:
        try:
            content = raw_resp.get("content", {}) if isinstance(raw_resp, dict) else getattr(raw_resp, "content", None)
            if not content:
                return None, None
                
            parts = content.get("parts", []) if isinstance(content, dict) else getattr(content, "parts", [])
            if not parts or len(parts) == 0:
                return None, None
                
            first_part = parts[0]
            f_call = first_part.get("function_call") if isinstance(first_part, dict) else getattr(first_part, "function_call", None)
            if not f_call:
                return None, None
                
            f_name = f_call.get("name") if isinstance(f_call, dict) else getattr(f_call, "name", None)
            f_args = f_call.get("args") if isinstance(f_call, dict) else getattr(f_call, "args", None)
            
            return f_name, f_args
        except Exception:
            return None, None


class StateMapper:
    @staticmethod
    def extract_sync_response(response: Any, provider: Optional[str] = None) -> Tuple[str, Optional[Dict]]:
        """
        [신규] Non-stream(Sync) 응답에서 Provider 룰에 맞춰 Content와 Usage를 우아하게 추출합니다.
        하위호환성(Backward Compatibility)을 보장하기 위해 기본값은 OpenAI 규격을 따릅니다.
        """
        # 1. Provider에 맞는 룰셋 로드 (없으면 defaults)
        rules = STATE_EXTRACTION_RULES.get(provider) or STATE_EXTRACTION_RULES["defaults"]
        
        # 2. 다중 경로 탐색을 통한 텍스트 안전 추출
        content_paths = rules.get("sync_content_paths", STATE_EXTRACTION_RULES["defaults"]["sync_content_paths"])
        raw_content = StateTraverser.resolve(response, content_paths)
        
        # 3. 텍스트 블록 정규화 (Claude 등 복합 블록이 들어올 경우를 대비한 2차 안전망)
        content = ImperativeFallbackRule.parse_content_blocks(raw_content) or ""

        # 4. Usage 추출 및 정규화 (Pydantic V1/V2, Dict, Object 혼용 방어)
        usage_paths = rules.get("sync_usage_paths", STATE_EXTRACTION_RULES["defaults"]["sync_usage_paths"])
        usage_obj = StateTraverser.resolve(response, usage_paths)
        usage_dict = None
        
        if usage_obj:
            if hasattr(usage_obj, "model_dump") and callable(getattr(usage_obj, "model_dump")):
                try:
                    usage_dict = usage_obj.model_dump(exclude_unset=True)
                except Exception:
                    usage_dict = dict(usage_obj)
            elif isinstance(usage_obj, dict):
                usage_dict = usage_obj
            else:
                try:
                    usage_dict = dict(usage_obj)
                except Exception:
                    # Dict 변환 불가능한 원시 타입일 경우 방어
                    pass 

        return content, usage_dict

    @staticmethod
    def resolve_chat_endpoint(provider: Optional[str], base_url: str) -> str:
        if not base_url:
            return ""
            
        clean_base = base_url.rstrip("/")
        
        if clean_base.endswith("/api/chat") or clean_base.endswith("/chat/completions"):
            return clean_base
            
        if provider == "ollama":
            rules = ENDPOINT_ROUTING_RULES["ollama"]
            if clean_base.endswith(rules["v1_indicator"]):
                return f"{clean_base}{rules['openai_suffix']}"
            return f"{clean_base}{rules['v1_indicator']}{rules['openai_suffix']}"
            
        default_suffix = ENDPOINT_ROUTING_RULES["defaults"]["openai_suffix"]
        return f"{clean_base}{default_suffix}"

    @staticmethod
    def extract_stream_content(chunk: Any, default: str = "") -> str:
        paths = STATE_EXTRACTION_RULES["defaults"]["stream_content_paths"]
        content = StateTraverser.resolve(chunk, paths)
        
        if content:
            return content
            
        log.debug(f"[Traverser] Failed to extract content from chunk type: {type(chunk)}")
        return default

    @staticmethod
    def to_chat_messages(messages: List[dict]) -> List[ChatMessage]:
        chat_messages = []
        for msg in messages:
            role = StateTraverser.resolve(msg, "role") or msg.get("role", STATE_EXTRACTION_RULES["defaults"]["role"])
            raw_content = StateTraverser.resolve(msg, "content") or msg.get("content", "")
            
            parsed_content = ImperativeFallbackRule.parse_content_blocks(raw_content)
            
            chat_messages.append(ChatMessage(role=MessageRole(role), content=parsed_content))
            
        return chat_messages

    @staticmethod
    def to_openai_choice(response: Any, req_id: str, logger: Any, provider: str = "gemini") -> dict:
        message_content = StateTraverser.resolve(response, "message.content") or getattr(getattr(response, "message", object()), "content", "") or ""
        
        tool_calls = StateTraverser.resolve(response, "message.additional_kwargs.tool_calls")
        if tool_calls is None and hasattr(response, "message"):
            tool_calls = response.message.additional_kwargs.get("tool_calls", None)

        role_val = StateTraverser.resolve(response, "message.role.value") or STATE_EXTRACTION_RULES["defaults"]["role"]

        if not tool_calls and hasattr(response, "raw") and response.raw:
            f_name, f_args = None, None
            
            rule = STATE_EXTRACTION_RULES.get(provider, STATE_EXTRACTION_RULES.get("gemini", {}))
            try:
                f_name = StateTraverser.resolve(response.raw, rule.get("fallback_tool_name"))
                f_args = StateTraverser.resolve(response.raw, rule.get("fallback_tool_args"))
                if f_name:
                    logger.debug(f"[InterLLM-{req_id}] 🎯 [Traverser] Tool Call 복원 성공: {f_name}")
            except Exception as e:
                logger.debug(f"[InterLLM-{req_id}] ⚠️ [Traverser] 탐색 에러: {e}")

            if not f_name:
                f_name, f_args = ImperativeFallbackRule.recover_tool_call(response.raw)
                if f_name:
                    logger.debug(f"[InterLLM-{req_id}] 🛠️ [Fallback] 하드코딩 로직으로 복원 성공: {f_name}")
                else:
                    logger.debug(f"[InterLLM-{req_id}] ⏭️ [Recovery] 복원할 Tool Call 없음")

            if f_name:
                tool_calls = [{
                    "id": f"call_{str(uuid4())[:8]}",
                    "type": "function",
                    "function": {
                        "name": f_name,
                        "arguments": json.dumps(f_args) if isinstance(f_args, (dict, list)) else str(f_args or "{}")
                    }
                }]

        choice_data = {
            "index": 0,
            "message": {
                "role": role_val,
                "content": message_content,
            },
            "finish_reason": STATE_EXTRACTION_RULES["defaults"]["finish_stop"]
        }
        
        if tool_calls:
            choice_data["message"]["tool_calls"] = tool_calls
            choice_data["finish_reason"] = STATE_EXTRACTION_RULES["defaults"]["finish_tool"]
            
        return choice_data