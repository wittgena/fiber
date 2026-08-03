# engine.parser.action
## @lineage: engine.atoa.conv.parser.action
## @lineage: engine.protocol.atoa.conv.parser.action
## @lineage: phi.agent.atoa.conv.parser.action
## @lineage: agent.atoa.conv.parser.action
from __future__ import annotations
import os
import sys
import json
import re
from typing import Any
from pydantic import ValidationError
from functools import lru_cache
from ator.state.event.llm.action import ActionEvent
from ator.state.event.llm.observation import AgentErrorEvent
import engine.driver.security.eval as risk
from engine.driver.security.analyzer import SecurityAnalyzerBase
from engine.driver.disc.action import Action, Observation

from engine.parser.toolcall import ToolCallParser
from engine.parser.conv.message import (
    MessageToolCall,
    ReasoningItemModel,
    RedactedThinkingBlock,
    TextContent,
    ThinkingBlock,
)

from ator.action.builder import ActionDefinition
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class LLMSecurityAnalyzer(SecurityAnalyzerBase):
    def security_risk(self, action: ActionEvent) -> risk.SecurityRisk:
        log.debug(f"Analyzing security risk: {action} -- {action.security_risk}")
        return action.security_risk

class ActionParser:
    @staticmethod
    def parse_tool_call(
        tool_call: MessageToolCall,
        tools_map: dict[str, ActionDefinition],
        llm_response_id: str,
        security_analyzer: Any = None,
        thought: list[TextContent] | None = None,
        reasoning_content: str | None = None,
        thinking_blocks: list[ThinkingBlock | RedactedThinkingBlock] | None = None,
        responses_reasoning_item: ReasoningItemModel | None = None,
    ) -> tuple[ActionEvent, AgentErrorEvent | None]:
        requested_tool_name = tool_call.name
        tool: ActionDefinition | None = None
        normalized_tool_call = tool_call
        arguments: dict[str, object] | None = None
        security_risk: risk.SecurityRisk = risk.SecurityRisk.UNKNOWN

        try:
            arguments = ToolCallParser.parse_arguments(tool_call.arguments)
            tool_name, arguments = ToolCallParser.normalize_tool_call(
                requested_tool_name,
                arguments,
                tools_map.keys(),
            )

            tool = tools_map.get(tool_name, None)
            if tool is None:
                available = list(tools_map.keys())
                err = f"Tool '{tool_name}' not found. Available: {available}"
                return ActionParser._build_error_tuple(
                    err=err,
                    tool_name=tool_name,
                    tool_call=tool_call,
                    llm_response_id=llm_response_id,
                    thought=thought,
                    reasoning_content=reasoning_content,
                    thinking_blocks=thinking_blocks,
                    responses_reasoning_item=responses_reasoning_item,
                )

            arguments = ToolCallParser.fix_malformed_arguments(arguments, tool.action_type)
            normalized_tool_call = tool_call.model_copy(
                update={
                    "name": tool_name,
                    "arguments": json.dumps(arguments),
                }
            )
            
            security_risk = ActionParser._extract_security_risk(
                arguments,
                tool.name,
                tool.annotations.readOnlyHint if tool.annotations else False,
                security_analyzer,
            )
            assert "security_risk" not in arguments, (
                "Unexpected 'security_risk' key found in tool arguments"
            )
            summary = ActionParser.extract_tool_summary(tool.name, arguments, tool=tool)
            action: Action = tool.action_from_arguments(arguments)

        except (ValueError, json.JSONDecodeError, ValidationError) as e:
            err_str = str(e)
            display_tool_name = requested_tool_name
            if "Cannot infer" in err_str:
                match = re.search(r"for tool '([^']+)'", err_str)
                if match:
                    display_tool_name = match.group(1)

            keys = list(arguments.keys()) if isinstance(arguments, dict) else None
            params = (
                f"Parameters provided: {keys}"
                if keys is not None
                else "Arguments: unparseable JSON"
            )
            err = f"Error validating tool '{display_tool_name}': {e}. {params}"
            
            return ActionParser._build_error_tuple(
                err=err,
                tool_name=display_tool_name,
                tool_call=tool_call,
                llm_response_id=llm_response_id,
                thought=thought,
                reasoning_content=reasoning_content,
                thinking_blocks=thinking_blocks,
                responses_reasoning_item=responses_reasoning_item,
            )

        action_event = ActionEvent(
            action=action,
            thought=thought or [],
            reasoning_content=reasoning_content,
            thinking_blocks=thinking_blocks or [],
            responses_reasoning_item=responses_reasoning_item,
            tool_name=tool.name,
            tool_call_id=normalized_tool_call.id,
            tool_call=normalized_tool_call,
            llm_response_id=llm_response_id,
            security_risk=security_risk,
            summary=summary,
        )

        return action_event, None

    @staticmethod
    def _build_error_tuple(
        err: str,
        tool_name: str,
        tool_call: MessageToolCall,
        llm_response_id: str,
        thought: list[TextContent] | None = None,
        reasoning_content: str | None = None,
        thinking_blocks: list[ThinkingBlock | RedactedThinkingBlock] | None = None,
        responses_reasoning_item: ReasoningItemModel | None = None,
    ) -> tuple[ActionEvent, AgentErrorEvent]:
        """에러 발생 시 추적용 빈 ActionEvent와 AgentErrorEvent를 생성하여 반환합니다."""
        shell_event = ActionEvent(
            source="agent",
            thought=thought or [],
            reasoning_content=reasoning_content,
            thinking_blocks=thinking_blocks or [],
            responses_reasoning_item=responses_reasoning_item,
            tool_call=tool_call,
            tool_name=tool_name,
            tool_call_id=tool_call.id,
            llm_response_id=llm_response_id,
            action=None,
        )
        err_event = AgentErrorEvent(
            error=err,
            tool_name=tool_name,
            tool_call_id=tool_call.id,
        )
        return shell_event, err_event

    @staticmethod
    def _extract_security_risk(
        arguments: dict,
        tool_name: str,
        read_only_tool: bool,
        security_analyzer: Any = None,
    ) -> risk.SecurityRisk:
        requires_sr = isinstance(security_analyzer, LLMSecurityAnalyzer)
        raw = arguments.pop("security_risk", None)
        
        if read_only_tool:
            return risk.SecurityRisk.UNKNOWN

        if requires_sr and raw is None:
            raise ValueError(f"Failed to provide security_risk field in tool '{tool_name}'")

        if security_analyzer is None:
            return risk.SecurityRisk.UNKNOWN

        if not requires_sr and raw is None:
            return risk.SecurityRisk.UNKNOWN
        return risk.SecurityRisk(raw)

    @staticmethod
    def extract_action_name(action_event: ActionEvent) -> str:
        """액션 이벤트를 파싱하여 안전하게 액션 이름을 추출합니다."""
        try:
            if action_event.action is not None and hasattr(action_event.action, "kind"):
                return action_event.action.kind
            return action_event.tool_name
        except Exception:
            return "agent.execute_action"

    @staticmethod
    def extract_tool_summary(tool_name: str, arguments: dict, tool: ActionDefinition | None = None) -> str:
        """도구의 실행 인자에서 요약(summary) 문자열을 안전하게 추출합니다."""
        has_summary_param = tool is not None and "summary" in tool.action_type.model_fields
        
        if has_summary_param:
            summary = arguments.get("summary")
            if isinstance(summary, str) and summary.strip():
                return summary.strip()
            return f"{tool_name}: {json.dumps(arguments)}"

        summary = arguments.pop("summary", None)
        if summary is not None and isinstance(summary, str) and summary.strip():
            return summary

        return f"{tool_name}: {json.dumps(arguments)}"


def format_context_exceeded_message(llm_model: str) -> str:
    """The LLM's context window has been exceeded."""
    return (
        "\n" + "=" * 51 + "\n"
        "⚠️  CONTEXT WINDOW EXCEEDED ERROR\n"
        + "=" * 51 + "\n\n"
        "The LLM's context window has been exceeded\n\n"
        + "=" * 51
    )