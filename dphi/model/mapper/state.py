# dphi.model.mapper.state
## @lineage: agent.llm.router.mapper.state
## @lineage: bound.agent.mapper.state
## @lineage: bound.eco.agent.adapter.mapper.state
## @lineage: eco.bound.agent.adapter.mapper.state
## @lineage: bound.agent.adapter.mapper.state
## @lineage: ext.router.adapter.mapper.state
## @lineage: router.adapter.mapper.state
## @lineage: engine.adapter.mapper.state
## @lineage: bound.adapter.mapper.state
## @lineage: eco.adapter.mapper.state
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List, Tuple, Optional

from fiber.phase.client.ext.llm.model.types.block import ChatMessage, MessageRole
from xphi.arch.model.phase.gate import uuid4 
from xphi.kernel.bind.resolver import get_invoker
from xphi.watcher.plane.emitter import get_emitter

from fiber.dphi.model.mapper.traverser import StateTraverser, STATE_EXTRACTION_RULES

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

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
    """@desc: [Traverser 우선 시도] -> [ImperativeFallback 2차 시도] 흐름을 제어"""
    
    @staticmethod
    def to_llama_messages(messages: List[dict]) -> List[ChatMessage]:
        llama_messages = []
        for msg in messages:
            role = StateTraverser.resolve(msg, "role") or msg.get("role", "user")
            raw_content = StateTraverser.resolve(msg, "content") or msg.get("content", "")
            parsed_content = ImperativeFallbackRule.parse_content_blocks(raw_content)
            llama_messages.append(ChatMessage(role=MessageRole(role), content=parsed_content))
            
        return llama_messages

    @staticmethod
    def to_openai_choice(response: Any, req_id: str, logger: Any, provider: str = "gemini") -> dict:
        """LlamaIndex ChatResponse -> OpenAI Compatible Choice Dict (with Dual Guard)"""
        
        ## [Step 1] 속성 추출 파이프라인
        message_content = StateTraverser.resolve(response, "message.content") or getattr(getattr(response, "message", object()), "content", "") or ""
        
        tool_calls = StateTraverser.resolve(response, "message.additional_kwargs.tool_calls")
        if tool_calls is None and hasattr(response, "message"):
            tool_calls = response.message.additional_kwargs.get("tool_calls", None)

        role_val = StateTraverser.resolve(response, "message.role.value") or "assistant"

        ## [Step 2] 누락된 Tool Call 복원 파이프라인
        if not tool_calls and hasattr(response, "raw") and response.raw:
            f_name, f_args = None, None
            
            ## Phase 1: Traverser (선언적 탐색) 우선 시도
            rule = STATE_EXTRACTION_RULES.get(provider, STATE_EXTRACTION_RULES.get("gemini", {}))
            try:
                f_name = StateTraverser.resolve(response.raw, rule.get("fallback_tool_name"))
                f_args = StateTraverser.resolve(response.raw, rule.get("fallback_tool_args"))
                if f_name:
                    logger.debug(f"[InterLLM-{req_id}] 🎯 [Traverser] Tool Call 복원 성공: {f_name}")
            except Exception as e:
                logger.debug(f"[InterLLM-{req_id}] ⚠️ [Traverser] 탐색 에러: {e}")

            ## Phase 2: Traverser 실패 시 Legacy Fallback 2차 시도
            if not f_name:
                f_name, f_args = ImperativeFallbackRule.recover_tool_call(response.raw)
                if f_name:
                    logger.debug(f"[InterLLM-{req_id}] 🛠️ [Fallback] 하드코딩 로직으로 복원 성공: {f_name}")
                else:
                    logger.debug(f"[InterLLM-{req_id}] ⏭️ [Recovery] 복원할 Tool Call 없음")

            ## Phase 3: 복원 데이터 조립
            if f_name:
                tool_calls = [{
                    "id": f"call_{str(uuid4())[:8]}",
                    "type": "function",
                    "function": {
                        "name": f_name,
                        "arguments": json.dumps(f_args) if isinstance(f_args, (dict, list)) else str(f_args or "{}")
                    }
                }]

        ## [Step 3] 최종 반환 규격 조립
        choice_data = {
            "index": 0,
            "message": {
                "role": role_val,
                "content": message_content,
            },
            "finish_reason": "stop"
        }
        
        if tool_calls:
            choice_data["message"]["tool_calls"] = tool_calls
            choice_data["finish_reason"] = "tool_calls"
            
        return choice_data