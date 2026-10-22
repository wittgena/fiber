# fiber.infra.bound.terminal.polling
## @lineage: fiber.dev.ex.terminal.polling
## @lineage: phase.align.terminal.polling
## @lineage: phase.agent.terminal.polling
import re
import time
import traceback
from typing import TYPE_CHECKING, Any
import bashlex
from bashlex.errors import ParsingError

from fiber.infra.bound.terminal.metadata import (
    CMD_OUTPUT_PS1_END,
    MAX_CMD_OUTPUT_SIZE,
    POLL_INTERVAL,
    TIMEOUT_MESSAGE_TEMPLATE,
    CmdOutputMetadata,
    TerminalCommandStatus,
)
from fiber.infra.bound.terminal.session import (
    ExecutionContext,
    ExecutionEngine,
    TerminalRequest,
    TerminalResult,
)
from xphi.arch.xor.parser.mark import maybe_truncate
from xphi.watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from fiber.infra.bound.terminal.session import TerminalSession

log = get_emitter(__name__)


def split_bash_commands(commands: str) -> list[str]:
    if not commands.strip():
        return [""]
    try:
        parsed = bashlex.parse(commands)
    except (ParsingError, NotImplementedError, TypeError, AttributeError):
        log.debug(f"Failed to parse bash commands: {commands}")
        return [commands]

    result: list[str] = []
    last_end = 0

    for node in parsed:
        start, end = node.pos
        if start > last_end:
            between = commands[last_end:start]
            if result:
                result[-1] += between.rstrip()
            elif between.strip():
                result.append(between.rstrip())

        command = commands[start:end].rstrip()
        result.append(command)
        last_end = end

    remaining = commands[last_end:].rstrip()
    if last_end < len(commands) and result:
        result[-1] += remaining
    elif last_end < len(commands) and remaining:
        result.append(remaining)
    return result


def escape_bash_special_chars(command: str) -> str:
    if command.strip() == "":
        return ""
    try:
        parts = []
        last_pos = 0

        def visit_node(node: Any) -> None:
            nonlocal last_pos
            if node.kind == "redirect" and getattr(node, "heredoc", None) is not None:
                parts.append(command[last_pos : node.pos[0]])
                parts.append(command[node.pos[0] : node.heredoc.pos[0]])
                parts.append(command[node.heredoc.pos[0] : node.heredoc.pos[1]])
                last_pos = node.pos[1]
                return

            if node.kind == "word":
                between = re.sub(r"\\([;&|><])", r"\\\\\1", command[last_pos : node.pos[0]])
                parts.append(between)
                word_text = command[node.pos[0] : node.pos[1]]
                if (
                    (word_text.startswith('"') and word_text.endswith('"'))
                    or (word_text.startswith("'") and word_text.endswith("'"))
                    or (word_text.startswith("$(") and word_text.endswith(")"))
                    or (word_text.startswith("`") and word_text.endswith("`"))
                ):
                    parts.append(word_text)
                else:
                    parts.append(re.sub(r"\\([;&|><])", r"\\\\\1", word_text))
                last_pos = node.pos[1]
                return

            if hasattr(node, "parts"):
                for part in node.parts:
                    visit_node(part)

        for node in list(bashlex.parse(command)):
            between = re.sub(r"\\([;&|><])", r"\\\\\1", command[last_pos : node.pos[0]])
            parts.append(between)
            last_pos = node.pos[0]
            visit_node(node)

        parts.append(command[last_pos:])
        return "".join(parts)
    except (ParsingError, NotImplementedError, TypeError, AttributeError):
        return command


class PollingExecutionEngine(ExecutionEngine):
    def execute(self, context: ExecutionContext) -> TerminalResult:
        session: "TerminalSession" = context.session
        req: TerminalRequest = context.request
        
        if validation_error := self._validate_request(req):
            return validation_error

        if state_error := self._check_previous_running_state(session, req):
            return state_error

        initial_terminal_output = session.terminal.read_screen()
        initial_ps1_matches = CmdOutputMetadata.matches_ps1_metadata(initial_terminal_output)
        initial_ps1_count = len(initial_ps1_matches)

        self._send_command_to_backend(session, req)

        return self._wait_for_completion(
            session=session,
            req=req,
            initial_output=initial_terminal_output,
            initial_ps1_count=initial_ps1_count,
            initial_ps1_matches=initial_ps1_matches,
        )

    def _validate_request(self, req: TerminalRequest) -> TerminalResult | None:
        command = req.command.strip()
        splited_commands = split_bash_commands(command)
        if len(splited_commands) > 1:
            commands_list = "\n".join(f"({i + 1}) {cmd}" for i, cmd in enumerate(splited_commands))
            return TerminalResult(
                command=command,
                output=(
                    "Cannot execute multiple commands at once.\n"
                    "Please run each command separately OR chain them into a single "
                    f"command via && or ;\nProvided commands:\n{commands_list}"
                ),
                is_error=True,
            )
        return None

    def _check_previous_running_state(self, session: "TerminalSession", req: TerminalRequest) -> TerminalResult | None:
        command = req.command.strip()
        if session.prev_status not in {
            TerminalCommandStatus.CONTINUE,
            TerminalCommandStatus.NO_CHANGE_TIMEOUT,
            TerminalCommandStatus.HARD_TIMEOUT,
        }:
            if command == "":
                return TerminalResult(
                    command=command,
                    output="No previous running command to retrieve logs from.",
                    is_error=True,
                )
            if req.is_input:
                return TerminalResult(
                    command=command,
                    output="No previous running command to interact with.",
                    is_error=True,
                )
        return None

    def _send_command_to_backend(self, session: "TerminalSession", req: TerminalRequest) -> None:
        command = req.command.strip()
        if command == "":
            return

        is_special_key = self._is_special_key(command)
        if not req.is_input and not session.terminal.is_powershell():
            command = escape_bash_special_chars(command)
            
        session.terminal.send_keys(command, enter=not is_special_key)

    def _wait_for_completion(
        self,
        session: "TerminalSession",
        req: TerminalRequest,
        initial_output: str,
        initial_ps1_count: int,
        initial_ps1_matches: list[re.Match],
    ) -> TerminalResult:
        command = req.command.strip()
        start_time = time.time()
        last_change_time = start_time
        last_terminal_output = initial_output

        if (
            session.prev_status in {TerminalCommandStatus.HARD_TIMEOUT, TerminalCommandStatus.NO_CHANGE_TIMEOUT}
            and not last_terminal_output.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip())
            and not req.is_input
            and command != ""
        ):
            return self._handle_blocked_command(session, command, last_terminal_output, initial_ps1_matches)

        while True:
            cur_terminal_output = session.terminal.read_screen()
            ps1_matches = CmdOutputMetadata.matches_ps1_metadata(cur_terminal_output)
            current_ps1_count = len(ps1_matches)

            if cur_terminal_output != last_terminal_output:
                last_terminal_output = cur_terminal_output
                last_change_time = time.time()

            if (
                current_ps1_count > initial_ps1_count
                or cur_terminal_output.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip())
            ):
                return self._handle_completed_command(session, command, cur_terminal_output, ps1_matches)

            time_since_last_change = time.time() - last_change_time
            if (
                req.timeout is None
                and session.no_change_timeout_seconds is not None
                and time_since_last_change >= session.no_change_timeout_seconds
            ):
                return self._handle_nochange_timeout_command(session, command, cur_terminal_output, ps1_matches)

            if req.timeout is not None and (time.time() - start_time) >= req.timeout:
                return self._handle_hard_timeout_command(
                    session, command, cur_terminal_output, ps1_matches, req.timeout
                )

            time.sleep(POLL_INTERVAL)

    @staticmethod
    def _is_special_key(command: str) -> bool:
        _command = command.strip()
        return _command.startswith("C-") and len(_command) == 3

    def _handle_completed_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match]
    ) -> TerminalResult:
        is_special_key = self._is_special_key(command)
        metadata = CmdOutputMetadata.from_ps1_match(ps1_matches[-1])
        get_content_before_last_match = bool(len(ps1_matches) == 1)

        if metadata.working_dir != session._cwd and metadata.working_dir:
            session._cwd = metadata.working_dir

        raw_command_output = self._combine_outputs_between_matches(
            terminal_content, ps1_matches, get_content_before_last_match
        )

        if get_content_before_last_match:
            num_lines = len(raw_command_output.splitlines())
            metadata.prefix = f"[Previous command outputs are truncated. Showing the last {num_lines} lines of the output below.]\n"

        metadata.suffix = (
            f"\n[The command completed with exit code {metadata.exit_code}.]"
            if not is_special_key
            else f"\n[The command completed with exit code {metadata.exit_code}. CTRL+{command[-1].upper()} was sent.]"
        )
        
        command_output = self._get_command_output(session, command, raw_command_output, metadata, is_final=True)
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)

        session.prev_status = TerminalCommandStatus.COMPLETED
        session.prev_output = ""
        session._query_filter.reset()
        session.terminal.clear_screen()

        return TerminalResult(
            command=command, output=command_output, metadata=metadata, exit_code=metadata.exit_code
        )

    def _handle_nochange_timeout_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match]
    ) -> TerminalResult:
        session.prev_status = TerminalCommandStatus.NO_CHANGE_TIMEOUT
        raw_command_output = self._combine_outputs_between_matches(terminal_content, ps1_matches)
        
        metadata = CmdOutputMetadata()
        metadata.suffix = f"\n[The command has no new output after {session.no_change_timeout_seconds} seconds. {TIMEOUT_MESSAGE_TEMPLATE}]"
        
        command_output = self._get_command_output(
            session, command, raw_command_output, metadata, continue_prefix="[Below is the output of the previous command.]\n"
        )
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)
        
        return TerminalResult(
            command=command, output=command_output, metadata=metadata, exit_code=metadata.exit_code, timeout=True
        )

    def _handle_hard_timeout_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match], timeout: float
    ) -> TerminalResult:
        session.prev_status = TerminalCommandStatus.HARD_TIMEOUT
        raw_command_output = self._combine_outputs_between_matches(terminal_content, ps1_matches)
        
        metadata = CmdOutputMetadata()
        metadata.suffix = f"\n[The command timed out after {timeout} seconds. {TIMEOUT_MESSAGE_TEMPLATE}]"
        
        command_output = self._get_command_output(
            session, command, raw_command_output, metadata, continue_prefix="[Below is the output of the previous command.]\n"
        )
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)
        
        return TerminalResult(
            command=command, output=command_output, metadata=metadata, exit_code=metadata.exit_code, timeout=True
        )

    def _handle_blocked_command(
        self, session: "TerminalSession", command: str, last_terminal_output: str, initial_ps1_matches: list[re.Match]
    ) -> TerminalResult:
        _ps1_matches = CmdOutputMetadata.matches_ps1_metadata(last_terminal_output)
        current_matches_for_output = _ps1_matches if _ps1_matches else initial_ps1_matches
        
        raw_command_output = self._combine_outputs_between_matches(last_terminal_output, current_matches_for_output)
        metadata = CmdOutputMetadata()
        metadata.suffix = (
            f'\n[Your command "{command}" is NOT executed. The previous command is still running - '
            f'You CANNOT send new commands until the previous command is completed. '
            f'By setting `is_input` to `true`, you can interact with the current process: {TIMEOUT_MESSAGE_TEMPLATE}]'
        )

        command_output = self._get_command_output(
            session, command, raw_command_output, metadata, continue_prefix="[Below is the output of the previous command.]\n"
        )
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)
        
        return TerminalResult(
            command=command, output=command_output, metadata=metadata, exit_code=metadata.exit_code, is_error=True
        )

    def _get_command_output(
        self, session: "TerminalSession", command: str, raw_command_output: str, metadata: CmdOutputMetadata, continue_prefix: str = "", is_final: bool = False
    ) -> str:
        if session.prev_output:
            command_output = raw_command_output.removeprefix(session.prev_output)
            metadata.prefix = continue_prefix
        else:
            command_output = raw_command_output
            
        session.prev_output = raw_command_output
        command_output = command_output.lstrip().removeprefix(command.lstrip()).lstrip()
        command_output = session._query_filter.filter(command_output)
        
        if is_final:
            command_output += session._query_filter.flush()

        return command_output.rstrip()

    def _combine_outputs_between_matches(
        self, terminal_content: str, ps1_matches: list[re.Match], get_content_before_last_match: bool = False
    ) -> str:
        if len(ps1_matches) == 1:
            if get_content_before_last_match:
                return terminal_content[: ps1_matches[0].start()]
            return terminal_content[ps1_matches[0].end() + 1 :]
        elif len(ps1_matches) == 0:
            return terminal_content
            
        combined_output = ""
        for i in range(len(ps1_matches) - 1):
            output_segment = terminal_content[ps1_matches[i].end() + 1 : ps1_matches[i + 1].start()]
            combined_output += output_segment + "\n"
            
        combined_output += terminal_content[ps1_matches[-1].end() + 1 :]
        return combined_output