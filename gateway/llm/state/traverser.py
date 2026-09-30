# fiber.gateway.llm.state.traverser
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List, Tuple, Optional, Union, Dict

from fiber.llm.compat.registry import STATE_EXTRACTION_RULES
from fiber.llm.compat.state import ENDPOINT_ROUTING_RULES
from fiber.llm.types.inter.block import MessageRole
from fiber.llm.types.inter.response import ChatMessage

from xphi.arch.bound.event.next import uuid4 
from xphi.kernel.space.bind.resolver import get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

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
    def _extract_sync_usage(obj: Any, usage_rule: Any) -> Optional[Dict[str, int]]:
        if not usage_rule:
            return None
            
        if isinstance(usage_rule, list):
            for rule in usage_rule:
                res = StateMapper._extract_sync_usage(obj, rule)
                if res and any(v is not None for v in res.values()):
                    return res
            return None

        # 룰셋이 Dict인 경우 (벤더별 매핑 룰 해석)
        if isinstance(usage_rule, dict):
            p_tokens = StateTraverser.resolve(obj, usage_rule.get("prompt_tokens")) or 0
            c_tokens = StateTraverser.resolve(obj, usage_rule.get("completion_tokens")) or 0
            t_tokens = StateTraverser.resolve(obj, usage_rule.get("total_tokens")) or 0
            
            if t_tokens == 0 and (p_tokens > 0 or c_tokens > 0):
                t_tokens = p_tokens + c_tokens
                
            if p_tokens > 0 or c_tokens > 0 or t_tokens > 0:
                return {
                    "prompt_tokens": int(p_tokens),
                    "completion_tokens": int(c_tokens),
                    "total_tokens": int(t_tokens)
                }
            return None
            
        # 기존 단일 경로 탐색
        usage_obj = StateTraverser.resolve(obj, usage_rule)
        if hasattr(usage_obj, "model_dump") and callable(getattr(usage_obj, "model_dump")):
            try: return usage_obj.model_dump(exclude_unset=True)
            except Exception: return dict(usage_obj)
        elif isinstance(usage_obj, dict):
            return usage_obj
        else:
            try: return dict(usage_obj)
            except Exception: return None
    
    @staticmethod
    def extract_sync_response(response: Any, provider: Optional[str] = None) -> Tuple[str, Optional[Dict]]:
        rules = STATE_EXTRACTION_RULES.get(provider) or STATE_EXTRACTION_RULES["defaults"]
        
        content_paths = rules.get("sync_content_paths", STATE_EXTRACTION_RULES["defaults"]["sync_content_paths"])
        raw_content = StateTraverser.resolve(response, content_paths)
        content = ImperativeFallbackRule.parse_content_blocks(raw_content) or ""

        usage_paths = rules.get("sync_usage_paths", STATE_EXTRACTION_RULES["defaults"]["sync_usage_paths"])
        usage_dict = StateMapper._extract_sync_usage(response, usage_paths)

        return content, usage_dict

    # @staticmethod
    # def extract_sync_response(response: Any, provider: Optional[str] = None) -> Tuple[str, Optional[Dict]]:
    #     rules = STATE_EXTRACTION_RULES.get(provider) or STATE_EXTRACTION_RULES["defaults"]
    #     content_paths = rules.get("sync_content_paths", STATE_EXTRACTION_RULES["defaults"]["sync_content_paths"])
    #     raw_content = StateTraverser.resolve(response, content_paths)
        
    #     content = ImperativeFallbackRule.parse_content_blocks(raw_content) or ""
    #     usage_paths = rules.get("sync_usage_paths", STATE_EXTRACTION_RULES["defaults"]["sync_usage_paths"])
    #     usage_obj = StateTraverser.resolve(response, usage_paths)
    #     usage_dict = None
        
    #     if usage_obj:
    #         if hasattr(usage_obj, "model_dump") and callable(getattr(usage_obj, "model_dump")):
    #             try:
    #                 usage_dict = usage_obj.model_dump(exclude_unset=True)
    #             except Exception:
    #                 usage_dict = dict(usage_obj)
    #         elif isinstance(usage_obj, dict):
    #             usage_dict = usage_obj
    #         else:
    #             try:
    #                 usage_dict = dict(usage_obj)
    #             except Exception:
    #                 pass 

    #     return content, usage_dict

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