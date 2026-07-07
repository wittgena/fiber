# bound.channel.param.validator
## @lineage: bound.bridge.channel.param.validator
## @lineage: bound.channel.action.param.validator
## @lineage: bound.channel.client.action.param.validator
## @lineage: anchor.channel.client.action.param.validator
## @lineage: anchor.channel.action.param.validator
## @lineage: bound.channel.support.param.validator
## @lineage: bound.channel.action.handler.param.validator
## @lineage: bound.bridge.action.handler.param.validator
## @lineage: bound.client.handler.param.validator
## @lineage: bound.param.validator
## @lineage: bound.router.action.param.validator
## @lineage: bound.channel.router.action.param.validator
## @lineage: channel.model.validator.params
from typing import Any, Dict, List, Optional, Type, Union, cast
from pydantic import BaseModel
from typing import Literal, Optional

from bound.surface.legacy.openai.types import AllMessageValues
from bound.surface.legacy.openai.types import ValidUserMessageContentTypes

from watcher.plane.emitter import get_emitter

log = get_emitter("param.validator")

def jsonify_tools(tools: List[Any]) -> List[Dict]:
    new_tools: List[Dict] = []
    for tool in tools:
        if isinstance(tool, BaseModel):
            tool = tool.model_dump(exclude_none=True)
        elif isinstance(tool, dict):
            tool = tool.copy()
        if isinstance(tool, dict):
            new_tools.append(tool)
    return new_tools

def convert_to_dict(message: Union[BaseModel, dict]) -> dict:
    if isinstance(message, BaseModel):
        return message.model_dump(exclude_none=True)  # type: ignore
    elif isinstance(message, dict):
        return message
    else:
        raise TypeError(
            f"Invalid message type: {type(message)}. Expected dict or Pydantic model."
        )

def validate_and_fix_openai_messages(messages: List):
    new_messages = []
    for message in messages:
        if not message.get("role"):
            message["role"] = "assistant"
        if message.get("tool_calls"):
            message["tool_calls"] = jsonify_tools(tools=message["tool_calls"])
        convert_msg_to_dict = cast(AllMessageValues, convert_to_dict(message))
        cleaned_message = cleanup_none_field_in_message(message=convert_msg_to_dict)
        new_messages.append(cleaned_message)
    return validate_chat_completion_user_messages(messages=new_messages)

def validate_and_fix_openai_tools(tools: Optional[List]) -> Optional[List[dict]]:
    new_tools = []
    if tools is None:
        return tools
    for tool in tools:
        if isinstance(tool, BaseModel):
            new_tools.append(tool.model_dump())
        elif isinstance(tool, dict):
            new_tools.append(tool)
    return new_tools

def validate_and_fix_thinking_param(
    thinking: Optional["AnthropicThinkingParam"],
) -> Optional["AnthropicThinkingParam"]:
    if thinking is None or not isinstance(thinking, dict):
        return thinking
    normalized = dict(thinking)
    if "budgetTokens" in normalized and "budget_tokens" not in normalized:
        normalized["budget_tokens"] = normalized.pop("budgetTokens")
    elif "budgetTokens" in normalized and "budget_tokens" in normalized:
        normalized.pop("budgetTokens")
    return cast("AnthropicThinkingParam", normalized)

def cleanup_none_field_in_message(message: AllMessageValues):
    new_message = message.copy()
    return {k: v for k, v in new_message.items() if v is not None}

def validate_chat_completion_user_messages(messages: List[AllMessageValues]):
    for idx, m in enumerate(messages):
        try:
            if m["role"] == "user":
                user_content = m.get("content")
                if user_content is not None:
                    if isinstance(user_content, str):
                        continue
                    elif isinstance(user_content, list):
                        for item in user_content:
                            if isinstance(item, dict):
                                if item.get("type") not in ValidUserMessageContentTypes:
                                    raise Exception(
                                        f"invalid content type={item.get('type')}"
                                    )
        except Exception as e:
            if isinstance(e, KeyError):
                raise Exception(
                    f"Invalid message at index {idx}. Please ensure all messages are valid OpenAI chat completion messages."
                )
            if "invalid content type" in str(e):
                raise Exception(
                    f"Invalid user message at index {idx}. Please ensure all user messages are valid OpenAI chat completion messages."
                )
            else:
                raise e

    return messages


def validate_chat_completion_tool_choice(
    tool_choice: Optional[Union[dict, str]],
) -> Optional[Union[dict, str]]:
    if tool_choice is None:
        return tool_choice
    elif isinstance(tool_choice, str):
        return tool_choice
    elif isinstance(tool_choice, dict):
        if (tool_choice.get("type") in ["auto", "none", "required"] and "function" not in tool_choice):
            return tool_choice

        if tool_choice.get("type") is None or tool_choice.get("function") is None:
            raise Exception(f"Invalid tool choice, tool_choice={tool_choice}")
        return tool_choice
    raise Exception(f"Invalid tool choice, tool_choice={tool_choice}. Got={type(tool_choice)}. Expecting str, or dict.")

def validate_openai_optional_params(
    stop: Optional[Union[str, List[str]]] = None,
    disable_stop_limit: bool = False,
    **kwargs
) -> Optional[Union[str, List[str]]]:
    if (stop is not None and isinstance(stop, list) and not disable_stop_sequence_limit):
        if len(stop) > 4:
            stop = stop[:4]
    return stop