# atoa.agent.conv.parser.toolcall
## @lineage: agent.conv.parser.toolcall
## @lineage: atoa.conv.parser.toolcall
import contextlib
import json
import logging
import re
import shlex
import types
from collections.abc import Collection
from typing import Annotated, Any, Union, get_args, get_origin

from atoa.gov.disc.schema.action import Action, Observation

logger = logging.getLogger(__name__)

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
            if data_key not in fixed_args: continue

            value = fixed_args[data_key]
            if not isinstance(value, str): continue

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