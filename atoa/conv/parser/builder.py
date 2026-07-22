# atoa.conv.parser.builder
## @lineage: atoa.gov.context.message.parser.builder
## @lineage: atoa.context.gov.message.parser.builder
## @lineage: gov.conv.message.parser.builder
from collections.abc import Sequence

from eco.agent.event.base import Event, LLMConvertibleEvent
from atoa.agent.types import ConversationTokenCallbackType
from atoa.agent.driver.tensor import Driver
from atoa.agent.response import LLMResponse
from eco.agent.action.message import Message
from atoa.agent.action.definition import ActionDefinition

from atoa.conv.view import View

from watcher.plane.emitter import get_emitter

log = get_emitter("message.builder")

class MessageBuilder:
    @staticmethod
    def prepare_llm_messages(
        events: Sequence[Event],
        additional_messages: list[Message] | None = None,
        llm: Driver | None = None,
    ) -> list[Message]:
        log.debug("[message.builder] prepare_llm_messages")
        view = View.from_events(events)
        messages = LLMConvertibleEvent.events_to_messages(view.events)
        
        if additional_messages:
            messages.extend(additional_messages)

        log.debug('[message.builder] Flatting nested text blocks for LLM constraints')
        for msg in messages:
            is_dict = isinstance(msg, dict)
            role = msg.get("role") if is_dict else getattr(msg, "role", None)
            content = msg.get("content") if is_dict else getattr(msg, "content", None)
            
            if role == "assistant" and isinstance(content, list):
                if not content:
                    new_content = ""
                else:
                    text_parts = []
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif isinstance(block, str):
                            text_parts.append(block)
                    new_content = "\n".join(text_parts) if text_parts else content

                if is_dict:
                    msg["content"] = new_content
                else:
                    msg.content = new_content

        return messages


class LLMFacade:
    """@desc: LLM 호출 시 파라미터 컨벤션을 통합하는 래퍼 클래스"""
    @staticmethod
    def make_completion(
        llm: Driver,
        messages: list[Message],
        tools: list[ActionDefinition] | None = None,
        on_token: ConversationTokenCallbackType | None = None,
    ) -> LLMResponse:
        common_kwargs = {
            "messages": messages,
            "tools": tools or [],
            "add_security_risk_prediction": True,
            "on_token": on_token,
        }
        
        if llm.uses_responses_api():
            return llm.responses(include=None, store=False, **common_kwargs)
        else:
            return llm.completion(**common_kwargs)