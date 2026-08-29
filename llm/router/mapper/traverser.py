# fiber.llm.router.mapper.traverser
## @lineage: fiber.dphi.model.mapper.traverser
## @lineage: dphi.model.mapper.traverser
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List

from fiber.llm.router.ext.llm.model.types.block import ChatMessage, MessageRole

from xphi.arch.event.next import uuid4 
from xphi.kernel.space.bind.resolver import get_invoker
from xphi.watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

STATE_EXTRACTION_RULES = {
    "gemini": {
        "fallback_tool_name": "content.parts.0.function_call.name",
        "fallback_tool_args": "content.parts.0.function_call.args"
    },
    "defaults": {
        "role": "assistant",
        "finish_stop": "stop",
        "finish_tool": "tool_calls"
    }
}

class StateTraverser:
    """
    @desc: 
    - 혼합된 데이터 토폴로지(Dict, Object, List)를 통합하여 탐색
    - 탐색 중 속성이나 인덱스가 존재하지 않으면 방어적 예외처리 없이 안전하게 default를 반환
    """
    @staticmethod
    def resolve(obj: Any, path: str, default: Any = None) -> Any:
        if not path or obj is None:
            return default

        keys = path.split('.')
        current = obj
        for k in keys:
            if current is None:
                return default
            if isinstance(current, dict):
                current = current.get(k)
            elif isinstance(current, (list, tuple)):
                try:
                    current = current[int(k)]
                except (IndexError, ValueError):
                    return default
            else:
                current = getattr(current, k, None)
        return current if current is not None else default

class StateTraverseRule:
    """@delegate: Declarative State Translation Engine"""
    
    @staticmethod
    def to_llama_messages(messages: List[dict]) -> List[ChatMessage]:
        """Brane Context Messages -> LlamaIndex ChatMessage"""
        llama_messages = []
        for msg in messages:
            role = msg.get("role", STATE_EXTRACTION_RULES["defaults"]["role"])
            raw_content = msg.get("content", "")
            parsed_content = ""
            
            if isinstance(raw_content, str):
                parsed_content = raw_content
            elif isinstance(raw_content, list):
                text_chunks = []
                for block in raw_content:
                    if hasattr(block, "get"):  # dict 형태
                        if block.get("type") == "text":
                            text_chunks.append(block.get("text", ""))
                    elif hasattr(block, "text"):  # TextContent 같은 객체 형태
                        text_chunks.append(block.text)
                    elif isinstance(block, str):
                        text_chunks.append(block)
                parsed_content = "".join(text_chunks)
            else:
                parsed_content = str(raw_content)
            llama_messages.append(ChatMessage(role=MessageRole(role), content=parsed_content))
        return llama_messages

    @staticmethod
    def to_openai_choice(response: Any, req_id: str, logger: Any) -> dict:
        """LlamaIndex ChatResponse -> OpenAI Compatible Choice Dict (with Declarative Fallback)"""
        message_content = StateTraverser.resolve(response, "message.content", "")
        tool_calls = StateTraverser.resolve(response, "message.additional_kwargs.tool_calls")
        
        default_role = STATE_EXTRACTION_RULES["defaults"]["role"]
        role_val = StateTraverser.resolve(response, "message.role.value", default_role)
        raw_resp = getattr(response, "raw", None)
        if not tool_calls and raw_resp:
            f_name_path = STATE_EXTRACTION_RULES["gemini"]["fallback_tool_name"]
            f_args_path = STATE_EXTRACTION_RULES["gemini"]["fallback_tool_args"]
            
            f_name = StateTraverser.resolve(raw_resp, f_name_path)
            f_args = StateTraverser.resolve(raw_resp, f_args_path)
            
            if f_name:
                args_str = json.dumps(f_args) if isinstance(f_args, dict) else str(f_args or "{}")
                tool_calls = [{
                    "id": f"call_{str(uuid4())[:8]}",
                    "type": "function",
                    "function": {
                        "name": f_name,
                        "arguments": args_str
                    }
                }]
                logger.debug(f"[InterLLM-{req_id}] 🎯 [TraverseRule] Reconstructed leaked tool_call: {f_name}")

        ## 최종 규격 조립 (OpenAI 호환)
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