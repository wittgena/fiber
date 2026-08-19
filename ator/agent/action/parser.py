# ator.agent.action.parser
## @lineage: xor.parser.action
## @lineage: ator.action.parser
from __future__ import annotations

import contextlib
import json
import logging
import re
import shlex
import types
from collections.abc import Collection
from functools import lru_cache
from typing import Annotated, Any, Union, get_args, get_origin

from pydantic import ValidationError

from ator.agent.action.builder import ActionDefinition
from ator.conv.event.llm.action import ActionEvent
from ator.conv.event.llm.observation import AgentErrorEvent
from ator.agent.action.schema.action import Action, Observation
from eco.bound.xor.bridge.security.analyzer import SecurityAnalyzerBase
import eco.bound.xor.bridge.security.eval as risk

from ator.conv.schema.message import (
    MessageToolCall,
    ReasoningItemModel,
    RedactedThinkingBlock,
    TextContent,
    ThinkingBlock,
)
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
logger = logging.getLogger(__name__)


class LLMSecurityAnalyzer(SecurityAnalyzerBase):
    def security_risk(self, action: ActionEvent) -> risk.SecurityRisk:
        log.debug(f"Analyzing security risk: {action} -- {action.security_risk}")
        return action.security_risk


class ToolCallParser:
    """
    @desc: LLM이 반환한 원시 도구 호출 문자열(JSON)과 파라미터를 교정, 파싱, 정규화하는 클래스.
    """
    _CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f]")
    _CTRL_ESCAPE_TABLE: dict[int, str] = {
        0x08: "\\b", 0x09: "\\t", 0x0A: "\\n", 0x0C: "\\f", 0x0D: "\\r",
    }
    
    TOOL_NAME_ALIASES: dict[str, str] = {
        "bash": "terminal",
        "command": "terminal",
        "execute": "terminal",
        "execute_bash": "terminal",
        "str_replace": "file_editor",
        "str_replace_editor": "file_editor",
    }
    _SHELL_TOOL_FALLBACK_COMMANDS = frozenset({"find", "ls", "pwd"})
    _SECURITY_RISK_TYPOS = {"security_rort", "securtiy_risk", "security_riks"}

    @classmethod
    def parse_arguments(cls, raw_arguments: str) -> dict[str, Any]:
        """Raw JSON 문자열을 딕셔너리로 파싱. 실패 시 제어문자를 치환하여 재시도."""
        try:
            parsed = json.loads(raw_arguments)
        except json.JSONDecodeError:
            sanitized_args = cls._sanitize_json_control_chars(raw_arguments)
            parsed = json.loads(sanitized_args)

        result = parsed if isinstance(parsed, dict) else {}
        return cls._normalize_argument_keys(result)

    @classmethod
    def fix_malformed_arguments(cls, arguments: dict[str, Any], action_type: type[Action]) -> dict[str, Any]:
        """Action 스키마(Pydantic)를 기반으로 손상된 인자 타입이나 후행 쓰레기값을 복구."""
        if not isinstance(arguments, dict):
            return arguments

        fixed_args = arguments.copy()
        for field_name, field_info in action_type.model_fields.items():
            data_key = field_info.alias if field_info.alias else field_name
            if data_key not in fixed_args: 
                continue

            value = fixed_args[data_key]
            if not isinstance(value, str): 
                continue

            expected_type = field_info.annotation
            if get_origin(expected_type) is Annotated:
                type_args = get_args(expected_type)
                expected_type = type_args[0] if type_args else expected_type

            origin = get_origin(expected_type)
            if origin is Union or origin is types.UnionType:
                expected_origins = [get_origin(arg) or arg for arg in get_args(expected_type)]
            else:
                expected_origins = [origin or expected_type]

            if any(exp in (list, dict) for exp in expected_origins):
                try:
                    parsed_val = json.loads(value, strict=False)
                    if isinstance(parsed_val, (list, dict)):
                        fixed_args[data_key] = parsed_val
                except (json.JSONDecodeError, ValueError):
                    fixed_args[data_key] = cls._recover_truncated_json(value) or value
        return fixed_args

    @classmethod
    def normalize_tool_call(
        cls, tool_name: str, arguments: dict[str, Any], available_tools: Collection[str]
    ) -> tuple[str, dict[str, Any]]:
        """존재하지 않는 도구를 앨리어스나 터미널 커맨드로 맵핑 및 정규화."""
        norm_name = tool_name
        norm_args = arguments.copy()

        if tool_name not in available_tools:
            if alias := cls.TOOL_NAME_ALIASES.get(tool_name):
                if alias in available_tools:
                    norm_name = alias
            elif "terminal" in available_tools:
                if term_cmd := cls._maybe_rewrite_as_terminal_command(tool_name, norm_args):
                    norm_name = "terminal"
                    norm_args = {k: v for k, v in norm_args.items() if k in {"security_risk", "summary"}}
                    norm_args["command"] = term_cmd

        if norm_name == "file_editor":
            inferred = cls._infer_file_editor_command(norm_args)
            if inferred is not None:
                norm_args = {"command": inferred, **norm_args}
            elif "command" not in norm_args and not cls._has_file_editor_hint(norm_args):
                raise ValueError(f"Cannot infer 'command' for file_editor from {norm_args!r}")

        return norm_name, norm_args

    # --- Private Helpers ---
    @classmethod
    def _sanitize_json_control_chars(cls, raw: str) -> str:
        def replace(m: re.Match[str]) -> str:
            ch = m.group(0)
            return cls._CTRL_ESCAPE_TABLE.get(ord(ch), f"\\u{ord(ch):04x}")
        return cls._CONTROL_CHAR_RE.sub(replace, raw)

    @classmethod
    def _normalize_argument_keys(cls, arguments: dict[str, Any]) -> dict[str, Any]:
        normalized = arguments.copy()
        for typo in cls._SECURITY_RISK_TYPOS:
            if typo in normalized:
                normalized["security_risk"] = normalized.pop(typo)
                break
        return {k: v for k, v in normalized.items() if v is not None}

    @classmethod
    def _recover_truncated_json(cls, value: str) -> dict | list | None:
        for end_char in ("}", "]"):
            idx = value.rfind(end_char)
            if idx != -1:
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    parsed = json.loads(value[: idx + 1], strict=False)
                    if isinstance(parsed, (list, dict)):
                        return parsed
        return None

    @classmethod
    def _infer_file_editor_command(cls, args: dict[str, Any]) -> str | None:
        if "command" in args: return None
        if "old_str" in args: return "str_replace"
        if "insert_line" in args: return "insert"
        if "file_text" in args: return "create"
        if "path" in args: return "view"
        return None

    @classmethod
    def _has_file_editor_hint(cls, args: dict[str, Any]) -> bool:
        hints = {"old_str", "new_str", "insert_line", "file_text", "path", "view_range"}
        return bool(args and any(k in args for k in hints))

    @classmethod
    def _maybe_rewrite_as_terminal_command(cls, name: str, args: dict[str, Any]) -> str | None:
        if name == "grep":
            pattern = args.get("pattern")
            if not isinstance(pattern, str) or not pattern.strip(): return None
            cmd = ["grep", "-RIn"]
            if (inc := args.get("include")) and isinstance(inc, str) and inc.strip():
                cmd.extend(["--include", inc])
            cmd.extend(["--", pattern])
            path = args.get("path")
            cmd.append(path if isinstance(path, str) and path.strip() else ".")
            return shlex.join(cmd)
            
        if args or name not in cls._SHELL_TOOL_FALLBACK_COMMANDS:
            return None
        return name


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