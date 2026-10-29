# fiber.gateway.llm.state.traverser
import os
import json
import asyncio
import functools
from pathlib import Path
from typing import AsyncGenerator, Generator, Any, List, Tuple, Optional, Union, Dict

from fiber.llm.compat.registry import EXTRACTION_RULES
from fiber.llm.compat.extraction import ENDPOINT_ROUTING_RULES, PROVIDER_RULE_ALIAS
from fiber.llm.model.types.inter.block import MessageRole
from fiber.llm.model.types.inter.response import ChatMessage

from xphi.arch.contract.event.next import uuid4
from xphi.arch.model.surge.model import _melt_alien_objects
from xphi.node.space.bind.resolver import get_invoker
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
            keys = path.split(".")
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
            return str(raw_content) if raw_content is not None else ""

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
            f_call = (
                first_part.get("function_call")
                if isinstance(first_part, dict)
                else getattr(first_part, "function_call", None)
            )
            if not f_call:
                return None, None

            f_name = f_call.get("name") if isinstance(f_call, dict) else getattr(f_call, "name", None)
            f_args = f_call.get("args") if isinstance(f_call, dict) else getattr(f_call, "args", None)

            return f_name, f_args
        except Exception:
            return None, None


class StateMapper:
    @staticmethod
    def _get_provider_rules(provider: Optional[str]) -> Dict[str, Any]:
        """PROVIDER_RULE_ALIAS를 거쳐 해당 provider의 선언적 EXTRACTION_RULES와 defaults를 병합 반환합니다."""
        defaults = EXTRACTION_RULES.get("defaults", {})
        if not provider:
            return dict(defaults)

        norm_prov = provider.lower().strip()
        target_key = PROVIDER_RULE_ALIAS.get(norm_prov, norm_prov)
        rule = EXTRACTION_RULES.get(target_key) or EXTRACTION_RULES.get(norm_prov) or {}
        return {**defaults, **rule}

    @staticmethod
    def _normalize_tool_call_list(raw_list: Any) -> Optional[List[Dict[str, Any]]]:
        """추출된 tool_calls 리스트를 OpenAI 호환 Dict 리스트로 정규화합니다."""
        melted = _melt_alien_objects(raw_list)
        if not isinstance(melted, list) or not melted:
            return None

        normalized: List[Dict[str, Any]] = []
        for idx, item in enumerate(melted):
            if not isinstance(item, dict):
                continue
            # 이미 OpenAI 표준 function 구조를 가진 경우
            if "function" in item and isinstance(item["function"], dict):
                fn_obj = dict(item["function"])
                args_val = fn_obj.get("arguments", "{}")
                if not isinstance(args_val, str):
                    fn_obj["arguments"] = json.dumps(args_val, ensure_ascii=False)
                normalized.append(
                    {
                        "id": item.get("id") or f"call_{str(uuid4())[:8]}",
                        "type": item.get("type", "function"),
                        "function": fn_obj,
                    }
                )
            # Cohere / Ollama 등 {"name": ..., "parameters" / "arguments": ...} 형태인 경우
            elif "name" in item:
                args_val = item.get("arguments") or item.get("parameters") or item.get("args") or "{}"
                if not isinstance(args_val, str):
                    args_val = json.dumps(args_val, ensure_ascii=False)
                normalized.append(
                    {
                        "id": item.get("id") or f"call_{idx}_{str(uuid4())[:6]}",
                        "type": "function",
                        "function": {
                            "name": str(item["name"]),
                            "arguments": args_val,
                        },
                    }
                )
        return normalized if normalized else None

    @staticmethod
    def extract_tool_calls(
        response: Any,
        provider: Optional[str] = None,
        logger: Any = None,
        req_id: str = "",
    ) -> Optional[List[Dict[str, Any]]]:
        """
        [SSOT] provider의 선언적 EXTRACTION_RULES ('tool_calls' 및 'fallback_tool_name'/'args')를
        기반으로 응답 객체(및 response.raw)에서 OpenAI 표준 tool_calls 배열을 추출합니다.
        """
        if response is None:
            return None

        rules = StateMapper._get_provider_rules(provider)

        # 1. InterLLM / LangChain 스타일 message.additional_kwargs.tool_calls 확인
        add_tc = StateTraverser.resolve(response, "message.additional_kwargs.tool_calls")
        if add_tc is None and hasattr(response, "message"):
            add_kwargs = getattr(response.message, "additional_kwargs", None)
            if isinstance(add_kwargs, dict):
                add_tc = add_kwargs.get("tool_calls")
        if add_tc:
            norm = StateMapper._normalize_tool_call_list(add_tc)
            if norm:
                return norm

        # 2. 선언적 'tool_calls' 경로 리스트 탐색 (response 및 response.raw 대상)
        tc_paths = rules.get("tool_calls")
        if tc_paths:
            for target in (response, getattr(response, "raw", None)):
                if target is None:
                    continue
                resolved = StateTraverser.resolve(target, tc_paths)
                if resolved:
                    norm = StateMapper._normalize_tool_call_list(resolved)
                    if norm:
                        return norm

        # 3. 선언적 'fallback_tool_name' & 'fallback_tool_args' 탐색 (Gemini 등 단일 function_call 구조)
        fb_name_rule = rules.get("fallback_tool_name")
        fb_args_rule = rules.get("fallback_tool_args")
        f_name, f_args = None, None

        if fb_name_rule:
            for target in (response, getattr(response, "raw", None)):
                if target is None:
                    continue
                try:
                    f_name = StateTraverser.resolve(target, fb_name_rule)
                    if f_name:
                        f_args = StateTraverser.resolve(target, fb_args_rule) if fb_args_rule else "{}"
                        if logger and req_id:
                            logger.debug(f"[InterLLM-{req_id}] 🎯 [Traverser] Tool Call 복원 성공: {f_name}")
                        break
                except Exception as e:
                    if logger and req_id:
                        logger.debug(f"[InterLLM-{req_id}] ⚠️ [Traverser] 탐색 에러: {e}")

        # 4. 최후 Imperative Fallback (response.raw 또는 response 자체)
        if not f_name:
            for target in (getattr(response, "raw", None), response):
                if target is None:
                    continue
                f_name, f_args = ImperativeFallbackRule.recover_tool_call(target)
                if f_name:
                    if logger and req_id:
                        logger.debug(f"[InterLLM-{req_id}] 🛠️ [Fallback] 하드코딩 로직으로 복원 성공: {f_name}")
                    break

        if f_name:
            if isinstance(f_args, (dict, list)):
                args_str = json.dumps(_melt_alien_objects(f_args), ensure_ascii=False)
            elif f_args is not None:
                args_str = str(f_args)
            else:
                args_str = "{}"

            return [
                {
                    "id": f"call_{str(uuid4())[:8]}",
                    "type": "function",
                    "function": {
                        "name": str(f_name),
                        "arguments": args_str,
                    },
                }
            ]

        if logger and req_id:
            logger.debug(f"[InterLLM-{req_id}] ⏭️ [Recovery] 복원할 Tool Call 없음")
        return None

    @staticmethod
    def _extract_usage(obj: Any, usage_rule: Any) -> Optional[Dict[str, int]]:
        if not usage_rule:
            return None

        if isinstance(usage_rule, list):
            for rule in usage_rule:
                res = StateMapper._extract_usage(obj, rule)
                if res and any(v is not None for v in res.values()):
                    return res
            return None

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
                    "total_tokens": int(t_tokens),
                }
            return None

        usage_obj = StateTraverser.resolve(obj, usage_rule)
        if hasattr(usage_obj, "model_dump") and callable(getattr(usage_obj, "model_dump")):
            try:
                return usage_obj.model_dump(exclude_unset=True)
            except Exception:
                return dict(usage_obj)
        elif isinstance(usage_obj, dict):
            return usage_obj
        else:
            try:
                return dict(usage_obj)
            except Exception:
                return None

    @staticmethod
    def extract_response(response: Any, provider: Optional[str] = None) -> Tuple[str, Optional[Dict]]:
        rules = StateMapper._get_provider_rules(provider)

        text = rules.get("text", EXTRACTION_RULES["defaults"]["text"])
        raw_content = StateTraverser.resolve(response, text)
        content = ImperativeFallbackRule.parse_content_blocks(raw_content) if raw_content is not None else ""

        usage = rules.get("usage", EXTRACTION_RULES["defaults"]["usage"])
        usage_dict = StateMapper._extract_usage(response, usage)

        return content, usage_dict

    @staticmethod
    def resolve_chat_endpoint(provider: Optional[str], base_url: str) -> str:
        if not base_url:
            return ""

        clean_base = base_url.rstrip("/")

        if clean_base.endswith("/api/chat") or clean_base.endswith("/chat/completions"):
            return clean_base

        rules = ENDPOINT_ROUTING_RULES.get(provider)
        if rules:
            if provider == "ollama":
                if clean_base.endswith(rules["v1_indicator"]):
                    return f"{clean_base}{rules['openai_suffix']}"
                return f"{clean_base}{rules['v1_indicator']}{rules['openai_suffix']}"
            elif provider == "cohere":
                if clean_base.endswith("/v1"):
                    return f"{clean_base}/chat"
                return f"{clean_base}{rules['native_suffix']}"
        default_suffix = ENDPOINT_ROUTING_RULES["defaults"]["openai_suffix"]
        return f"{clean_base}{default_suffix}"

    @staticmethod
    def to_chat_messages(messages: List[dict]) -> List[ChatMessage]:
        chat_messages = []
        for msg in messages:
            role = StateTraverser.resolve(msg, "role") or msg.get("role", EXTRACTION_RULES["defaults"]["role"])
            raw_content = StateTraverser.resolve(msg, "content") or msg.get("content", "")

            parsed_content = ImperativeFallbackRule.parse_content_blocks(raw_content)

            chat_messages.append(ChatMessage(role=MessageRole(role), content=parsed_content))

        return chat_messages

    @staticmethod
    def to_openai_choice(response: Any, req_id: str, logger: Any, provider: str = "gemini") -> dict:
        message_content = (
            StateTraverser.resolve(response, "message.content")
            or getattr(getattr(response, "message", object()), "content", "")
            or ""
        )
        role_val = StateTraverser.resolve(response, "message.role.value") or EXTRACTION_RULES["defaults"]["role"]

        tool_calls = StateMapper.extract_tool_calls(
            response=response,
            provider=provider,
            logger=logger,
            req_id=req_id,
        )

        choice_data = {
            "index": 0,
            "message": {
                "role": role_val,
                "content": message_content,
            },
            "finish_reason": EXTRACTION_RULES["defaults"]["finish_stop"],
        }

        if tool_calls:
            choice_data["message"]["tool_calls"] = tool_calls
            choice_data["finish_reason"] = EXTRACTION_RULES["defaults"]["finish_tool"]

        return choice_data