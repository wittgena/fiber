# atoa.topos.terminal.utils
## @lineage: gov.sandbox.engine.terminal.utils
## @lineage: agent.engine.terminal.utils
## @lineage: sandbox.executor.terminal.utils
## @lineage: gov.sandbox.executor.terminal.utils
## @lineage: gov.sandbox.tunnel.utils
import re
import traceback
from typing import Any
import bashlex
from bashlex.errors import ParsingError
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

def split_bash_commands(commands: str) -> list[str]:
    if not commands.strip():
        return [""]
    try:
        parsed = bashlex.parse(commands)
    except (
        ParsingError,
        NotImplementedError,
        TypeError,
        AttributeError,
    ):
        logger.debug(
            f"Failed to parse bash commands\n[input]: {commands}\n[warning]: "
            f"{traceback.format_exc()}\nThe original command will be returned as is."
        )
        return [commands]

    result: list[str] = []
    last_end = 0

    for node in parsed:
        start, end = node.pos

        # Include any text between the last command and this one
        if start > last_end:
            between = commands[last_end:start]
            logger.debug(f"BASH PARSING between: {between}")
            if result:
                result[-1] += between.rstrip()
            elif between.strip():
                # THIS SHOULD NOT HAPPEN
                result.append(between.rstrip())

        # Extract the command, preserving original formatting
        command = commands[start:end].rstrip()
        logger.debug(f"BASH PARSING command: {command}")
        result.append(command)

        last_end = end

    # Add any remaining text after the last command to the last command
    remaining = commands[last_end:].rstrip()
    logger.debug(f"BASH PARSING remaining: {remaining}")
    if last_end < len(commands) and result:
        result[-1] += remaining
        logger.debug(f"BASH PARSING result[-1] += remaining: {result[-1]}")
    elif last_end < len(commands):
        if remaining:
            result.append(remaining)
            logger.debug(f"BASH PARSING result.append(remaining): {result[-1]}")
    return result


def escape_bash_special_chars(command: str) -> str:
    r"""Escapes characters that have different interpretations in bash vs python.
    Specifically handles escape sequences like \;, \|, \&, etc.
    """
    if command.strip() == "":
        return ""

    try:
        parts = []
        last_pos = 0

        def visit_node(node: Any) -> None:
            nonlocal last_pos
            if (
                node.kind == "redirect"
                and hasattr(node, "heredoc")
                and node.heredoc is not None
            ):
                between = command[last_pos : node.pos[0]]
                parts.append(between)
                parts.append(command[node.pos[0] : node.heredoc.pos[0]])
                parts.append(command[node.heredoc.pos[0] : node.heredoc.pos[1]])
                last_pos = node.pos[1]
                return

            if node.kind == "word":
                between = command[last_pos : node.pos[0]]
                word_text = command[node.pos[0] : node.pos[1]]
                between = re.sub(r"\\([;&|><])", r"\\\\\1", between)
                parts.append(between)

                if (
                    (word_text.startswith('"') and word_text.endswith('"'))
                    or (word_text.startswith("'") and word_text.endswith("'"))
                    or (word_text.startswith("$(") and word_text.endswith(")"))
                    or (word_text.startswith("`") and word_text.endswith("`"))
                ):
                    parts.append(word_text)
                else:
                    word_text = re.sub(r"\\([;&|><])", r"\\\\\1", word_text)
                    parts.append(word_text)

                last_pos = node.pos[1]
                return

            # Visit child nodes
            if hasattr(node, "parts"):
                for part in node.parts:
                    visit_node(part)

        # Process all nodes in the AST
        nodes = list(bashlex.parse(command))
        for node in nodes:
            between = command[last_pos : node.pos[0]]
            between = re.sub(r"\\([;&|><])", r"\\\\\1", between)
            parts.append(between)
            last_pos = node.pos[0]
            visit_node(node)

        # Handle any remaining text after the last word
        remaining = command[last_pos:]
        parts.append(remaining)
        return "".join(parts)
    except (ParsingError, NotImplementedError, TypeError, AttributeError):
        logger.debug(
            f"Failed to parse bash commands for special characters escape\n[input]: "
            f"{command}\n[warning]: {traceback.format_exc()}\nThe original command "
            f"will be returned as is."
        )
        return command


_DSR_PATTERN = re.compile(rb"\x1b\[6n")
_OSC_QUERY_PATTERN = re.compile(
    rb"\x1b\]"  # OSC introducer
    rb"\d+"  # Parameter number (10, 11, 4, 12, etc.)
    rb"(?:;[^;\x07\x1b]*)?"  # Optional sub-parameter (e.g., palette index)
    rb";\?"  # Query marker - the key indicator this is a query
    rb"(?:\x07|\x1b\\)"  # BEL or ST terminator
)
_DA_PATTERN = re.compile(rb"\x1b\[0?c")
_DA2_PATTERN = re.compile(rb"\x1b\[>0?c")
_DECRQSS_PATTERN = re.compile(
    rb"\x1bP\$q"  # DCS introducer + DECRQSS
    rb"[^\x1b]*"  # Setting identifier
    rb"\x1b\\"  # ST terminator
)

_INCOMPLETE_ESC_PATTERN = re.compile(
    rb"(?:"
    rb"\x1b$|"  # ESC at end (might be start of any sequence)
    rb"\x1b\[[0-9;>]*$|"  # CSI without command char
    rb"\x1b\][^\x07]*$|"  # OSC without BEL terminator (ST needs \x1b\)
    rb"\x1bP(?:[^\x1b]|\x1b(?!\\))*$"  # DCS without complete ST terminator
    rb")"
)


def _filter_complete_queries(output_bytes: bytes) -> bytes:
    """Filter complete terminal query sequences from output bytes."""
    output_bytes = _DSR_PATTERN.sub(b"", output_bytes)
    output_bytes = _OSC_QUERY_PATTERN.sub(b"", output_bytes)
    output_bytes = _DA_PATTERN.sub(b"", output_bytes)
    output_bytes = _DA2_PATTERN.sub(b"", output_bytes)
    output_bytes = _DECRQSS_PATTERN.sub(b"", output_bytes)
    return output_bytes


class TerminalQueryFilter:
    def __init__(self) -> None:
        self._pending: bytes = b""

    def reset(self) -> None:
        self._pending = b""

    def filter(self, output: str) -> str:
        output_bytes = output.encode("utf-8", errors="surrogateescape")
        if self._pending:
            output_bytes = self._pending + output_bytes
            self._pending = b""

        match = _INCOMPLETE_ESC_PATTERN.search(output_bytes)
        if match:
            self._pending = output_bytes[match.start() :]
            output_bytes = output_bytes[: match.start()]

        output_bytes = _filter_complete_queries(output_bytes)
        return output_bytes.decode("utf-8", errors="surrogateescape")

    def flush(self) -> str:
        if not self._pending:
            return ""
        pending = self._pending
        self._pending = b""
        filtered = _filter_complete_queries(pending)
        return filtered.decode("utf-8", errors="surrogateescape")

def filter_terminal_queries(output: str) -> str:
    temp_filter = TerminalQueryFilter()
    result = temp_filter.filter(output)
    result += temp_filter.flush()
    return result
