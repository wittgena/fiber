# dphi.space.action.message
## @lineage: agent.space.action.message
## @lineage: bound.space.action.message
## @lineage: bound.adapter.schema.message
import json
from collections.abc import Sequence
from typing import Any, ClassVar

from openai.types.responses.response_output_message import ResponseOutputMessage
from openai.types.responses.response_reasoning_item import ResponseReasoningItem
from pydantic import ConfigDict, Field, model_validator

from xphi.arch.xor.parser.mark.truncate import DEFAULT_TEXT_CONTENT_LIMIT, maybe_truncate
from xphi.arch.xor.parser.mark.depre import handle_deprecated_model_fields

from fiber.llm.param import (
    ChatCompletionMessageToolCall,
    ResponseFunctionToolCall,
    OutputFunctionToolCall,
    GenericResponseOutputItem,
)

from xphi.arch.model.message import (
    MessageToolCall as CoreMessageToolCall,
    TextContent as CoreTextContent,
    ImageContent,
    ThinkingBlock,
    RedactedThinkingBlock,
    ReasoningItemModel,
    Message as CoreMessage,
    content_to_str,
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class SafeAttributeObjectProxy:
    """OpenAI API 응답 객체를 안전하게 탐색하기 위한 유틸리티 프록시"""
    def __init__(self, obj: Any):
        self._obj = obj

    def __getattr__(self, name: str) -> Any:
        if isinstance(self._obj, dict):
            val = self._obj.get(name)
        else:
            val = getattr(self._obj, name, None)

        if isinstance(val, dict):
            return SafeAttributeObjectProxy(val)
        if isinstance(val, list):
            return [SafeAttributeObjectProxy(item) if isinstance(item, dict) else item for item in val]
        return val

    def get(self, key: str, default: Any = None) -> Any:
        val = self.__getattr__(key)
        return default if val is None else val


# 2. 순수 모델 확장: OpenAI 파싱 로직 주입 (어댑터 패턴)

class MessageToolCall(CoreMessageToolCall):
    @classmethod
    def from_chat_tool_call(cls, tool_call: Any) -> "MessageToolCall":
        """Create a MessageToolCall from a Chat Completions tool call."""
        tc = SafeAttributeObjectProxy(tool_call)
        if not tc.type == "function":
            raise ValueError(
                f"Unsupported tool call type for {tool_call=}, expected 'function' "
                f"not {tc.type}'"
            )
        if tc.function is None:
            raise ValueError(f"tool_call.function is None for {tool_call=}")
        if tc.function.name is None:
            raise ValueError(f"tool_call.function.name is None for {tool_call=}")

        return cls(
            id=str(tc.id),
            name=str(tc.function.name),
            arguments=str(tc.function.arguments),
            origin="completion",
        )

    @classmethod
    def from_responses_function_call(
        cls, item: ResponseFunctionToolCall | OutputFunctionToolCall
    ) -> "MessageToolCall":
        call_id = item.call_id or item.id or ""
        name = item.name or ""
        arguments_str = item.arguments or ""

        if not call_id:
            raise ValueError(f"Responses function_call missing call_id/id: {item!r}")
        if not name:
            raise ValueError(f"Responses function_call missing name: {item!r}")

        return cls(
            id=str(call_id),
            name=str(name),
            arguments=arguments_str,
            origin="responses",
        )


class TextContent(CoreTextContent):
    _DEPRECATED_FIELDS: ClassVar[tuple[str, ...]] = ("enable_truncation",)

    @model_validator(mode="before")
    @classmethod
    def _handle_deprecated_fields(cls, data: Any) -> Any:
        return handle_deprecated_model_fields(data, cls._DEPRECATED_FIELDS)


class Message(CoreMessage):
    # Pydantic 필드 타입을 서브클래스(어댑터) 타입으로 오버라이드
    content: Sequence[TextContent | ImageContent] = Field(default_factory=list)
    tool_calls: list[MessageToolCall] | None = None

    _DEPRECATED_FIELDS: ClassVar[tuple[str, ...]] = (
        "cache_enabled",
        "vision_enabled",
        "function_calling_enabled",
        "force_string_serializer",
        "send_reasoning_content",
    )

    model_config = ConfigDict(extra="ignore")

    @model_validator(mode="before")
    @classmethod
    def _handle_deprecated_fields(cls, data: Any) -> Any:
        return handle_deprecated_model_fields(data, cls._DEPRECATED_FIELDS)

    def _maybe_truncate_tool_text(self, text: str) -> str:
        """CoreMessage의 truncate를 덮어쓰고, 내부 watcher 로거를 사용합니다."""
        if not text or len(text) <= DEFAULT_TEXT_CONTENT_LIMIT:
            return text

        log.warning(
            "Tool TextContent text length (%s) exceeds limit (%s), truncating",
            len(text),
            DEFAULT_TEXT_CONTENT_LIMIT,
        )
        return maybe_truncate(text, DEFAULT_TEXT_CONTENT_LIMIT)

    # -------------------------------------------------------------------------
    # 직렬화 (Serialization) 로직 - OpenAI 포맷
    # -------------------------------------------------------------------------
    def to_chat_dict(
        self,
        *,
        cache_enabled: bool,
        vision_enabled: bool,
        function_calling_enabled: bool,
        force_string_serializer: bool,
        send_reasoning_content: bool,
    ) -> dict[str, Any]:
        if not force_string_serializer and (
            cache_enabled or vision_enabled or function_calling_enabled
        ):
            message_dict = self._list_serializer(vision_enabled=vision_enabled)
        else:
            message_dict = self._string_serializer()

        if self.role == "assistant" and self.tool_calls:
            message_dict["tool_calls"] = [tc.to_chat_dict() for tc in self.tool_calls]
            self._remove_content_if_empty(message_dict)

        if self.role == "tool" and self.tool_call_id is not None:
            assert self.name is not None, "name is required when tool_call_id is not None"
            message_dict["tool_call_id"] = self.tool_call_id
            message_dict["name"] = self.name

        if send_reasoning_content and self.reasoning_content:
            message_dict["reasoning_content"] = self.reasoning_content

        return message_dict

    def _string_serializer(self) -> dict[str, Any]:
        content = "\n".join(
            item.text for item in self.content if isinstance(item, TextContent)
        )
        if self.role == "tool":
            content = self._maybe_truncate_tool_text(content)
        message_dict: dict[str, Any] = {"content": content, "role": self.role}
        return message_dict

    def _list_serializer(self, *, vision_enabled: bool) -> dict[str, Any]:
        content: list[dict[str, Any]] = []
        role_tool_with_prompt_caching = False
        thinking_blocks_dicts = []
        
        if self.role == "assistant":
            thinking_blocks = list(self.thinking_blocks)
            for thinking_block in thinking_blocks:
                thinking_dict = thinking_block.model_dump()
                thinking_blocks_dicts.append(thinking_dict)

        for item in self.content:
            item_dicts = item.to_llm_dict()

            if self.role == "tool" and item_dicts:
                for d in item_dicts:
                    text_val = d.get("text")
                    if d.get("type") == "text" and isinstance(text_val, str):
                        d["text"] = self._maybe_truncate_tool_text(text_val)

            if self.role == "tool" and item.cache_prompt:
                role_tool_with_prompt_caching = True
                for d in item_dicts:
                    d.pop("cache_control", None)

            if isinstance(item, ImageContent) and vision_enabled:
                content.extend(item_dicts)
            elif not isinstance(item, ImageContent):
                content.extend(item_dicts)

        message_dict: dict[str, Any] = {"content": content, "role": self.role}
        if role_tool_with_prompt_caching:
            message_dict["cache_control"] = {"type": "ephemeral"}
        if thinking_blocks_dicts:
            message_dict["thinking_blocks"] = thinking_blocks_dicts
        return message_dict

    def _remove_content_if_empty(self, message_dict: dict[str, Any]) -> None:
        if "content" not in message_dict:
            return
        content = message_dict["content"]
        if isinstance(content, str):
            if content.strip() == "":
                message_dict.pop("content", None)
            return

        if isinstance(content, list):
            normalized: list[Any] = []
            for item in content:
                if not isinstance(item, dict):
                    normalized.append(item)
                    continue
                if item.get("type") == "text":
                    text_value = item.get("text", "")
                    if isinstance(text_value, str):
                        if text_value.strip() == "":
                            continue
                    else:
                        raise ValueError(f"Text content item has non-string text value: {text_value!r}")
                normalized.append(item)

            if normalized:
                message_dict["content"] = normalized
            else:
                message_dict.pop("content", None)
            return

    def to_responses_value(self, *, vision_enabled: bool) -> str | list[dict[str, Any]]:
        if self.role == "system":
            parts: list[str] = []
            for c in self.content:
                if isinstance(c, TextContent) and c.text:
                    parts.append(c.text)
            return "\n".join(parts)
        return self.to_responses_dict(vision_enabled=vision_enabled)

    def to_responses_dict(self, *, vision_enabled: bool) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if self.role == "system":
            return items

        if self.role == "user":
            content_items: list[dict[str, Any]] = []
            for c in self.content:
                if isinstance(c, TextContent):
                    content_items.append({"type": "input_text", "text": c.text})
                elif isinstance(c, ImageContent) and vision_enabled:
                    for url in c.image_urls:
                        content_items.append({"type": "input_image", "image_url": url, "detail": "auto"})
            items.append({
                "type": "message",
                "role": "user",
                "content": content_items or [{"type": "input_text", "text": ""}],
            })
            return items

        if self.role == "assistant":
            if self.responses_reasoning_item is not None:
                ri = self.responses_reasoning_item
                if ri.id is not None:
                    reasoning_item: dict[str, Any] = {
                        "type": "reasoning",
                        "id": ri.id,
                        "summary": [{"type": "summary_text", "text": s} for s in (ri.summary or [])],
                    }
                    if ri.content:
                        reasoning_item["content"] = [{"type": "reasoning_text", "text": t} for t in ri.content]
                    if ri.encrypted_content:
                        reasoning_item["encrypted_content"] = ri.encrypted_content
                    if ri.status:
                        reasoning_item["status"] = ri.status
                    items.append(reasoning_item)

            content_items: list[dict[str, Any]] = []
            for c in self.content:
                if isinstance(c, TextContent) and c.text:
                    content_items.append({"type": "output_text", "text": c.text})
            if content_items:
                items.append({
                    "type": "message",
                    "role": "assistant",
                    "content": content_items,
                })
            if self.tool_calls:
                for tc in self.tool_calls:
                    items.append(tc.to_responses_dict())
            return items

        if self.role == "tool":
            if self.tool_call_id is not None:
                resp_call_id = self.tool_call_id if str(self.tool_call_id).startswith("fc") else f"fc_{self.tool_call_id}"
                for c in self.content:
                    if isinstance(c, TextContent):
                        output_text = self._maybe_truncate_tool_text(c.text)
                        items.append({
                            "type": "function_call_output",
                            "call_id": resp_call_id,
                            "output": output_text,
                        })
                    elif isinstance(c, ImageContent) and vision_enabled:
                        for url in c.image_urls:
                            items.append({
                                "type": "function_call_output",
                                "call_id": resp_call_id,
                                "output": [{"type": "input_image", "image_url": url, "detail": "auto"}],
                            })
            return items
        return items

    # -------------------------------------------------------------------------
    # 역직렬화/파싱 (Deserialization/Parsing) 로직 - OpenAPI 객체 -> 스키마
    # -------------------------------------------------------------------------
    @classmethod
    def from_llm_chat_message(cls, message: Any) -> "Message":
        msg = SafeAttributeObjectProxy(message)
        assert msg.role != "function", "Function role is not supported"

        rc = msg.reasoning_content
        thinking_blocks = msg.thinking_blocks

        if thinking_blocks is not None:
            normalized_tb = []
            for tb_item in thinking_blocks:
                tb_proxy = SafeAttributeObjectProxy(tb_item)
                tb_data = tb_item if isinstance(tb_item, dict) else getattr(tb_item, "__dict__", {})
                
                if tb_proxy.type == "thinking":
                    normalized_tb.append(ThinkingBlock(**tb_data))
                else:
                    normalized_tb.append(RedactedThinkingBlock(**tb_data))
            thinking_blocks = normalized_tb
        else:
            thinking_blocks = []

        tool_calls = None
        if msg.tool_calls:
            function_tool_calls = []
            for tc in msg.tool_calls:
                tc_proxy = SafeAttributeObjectProxy(tc)
                if tc_proxy.type == "function":
                    function_tool_calls.append(tc)
                else:
                    log.warning("LLM returned tool calls but some are not of type 'function' - ignoring those")

            if len(function_tool_calls) > 0:
                tool_calls = [
                    MessageToolCall.from_chat_tool_call(tc)
                    for tc in function_tool_calls
                ]
            else:
                raise ValueError("LLM returned tool calls but none are of type 'function'")

        content_text = msg.content if isinstance(msg.content, str) else ""
        has_text = bool(content_text.strip())
        has_tools = bool(tool_calls)
        
        if not has_text and not has_tools:
            raise ValueError("LLM returned an entirely empty response (no text, no tool calls).")

        return cls(
            role=msg.role,
            content=[TextContent(text=content_text)] if has_text else [],
            tool_calls=tool_calls,
            reasoning_content=rc,
            thinking_blocks=thinking_blocks,
        )

    @classmethod
    def from_llm_responses_output(cls, output: Any) -> "Message":
        assistant_text_parts: list[str] = []
        tool_calls: list[MessageToolCall] = []
        responses_reasoning_item: ReasoningItemModel | None = None

        for item in output or []:
            if (isinstance(item, GenericResponseOutputItem) or isinstance(item, ResponseOutputMessage)) and item.type == "message":
                for part in item.content or []:
                    if part.type == "output_text" and part.text:
                        assistant_text_parts.append(part.text)
            elif (
                isinstance(item, (OutputFunctionToolCall, ResponseFunctionToolCall))
                and item.type == "function_call"
            ):
                tc = MessageToolCall.from_responses_function_call(item)
                tool_calls.append(tc)
            elif isinstance(item, ResponseReasoningItem) and item.type == "reasoning":
                rid = item.id
                summaries = item.summary or []
                contents = item.content or []
                enc = item.encrypted_content
                status = item.status

                summary_list: list[str] = [s.text for s in summaries]
                content_texts: list[str] = [c.text for c in contents]
                content_list: list[str] | None = content_texts or None

                responses_reasoning_item = ReasoningItemModel(
                    id=rid,
                    summary=summary_list,
                    content=content_list,
                    encrypted_content=enc,
                    status=status,
                )

        assistant_text = "\n".join(assistant_text_parts).strip()
        return cls(
            role="assistant",
            content=[TextContent(text=assistant_text)] if assistant_text else [],
            tool_calls=tool_calls or None,
            responses_reasoning_item=responses_reasoning_item,
        )