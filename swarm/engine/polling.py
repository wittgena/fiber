# swarm.engine.polling
## @lineage: swarm.mesh.engine.polling
import re
import time
from typing import TYPE_CHECKING, Any
from arch.xor.bridge.tool.terminal import (
    CMD_OUTPUT_PS1_END,
    MAX_CMD_OUTPUT_SIZE,
    POLL_INTERVAL,
    TIMEOUT_MESSAGE_TEMPLATE,
)
import traceback
import bashlex
from bashlex.errors import ParsingError

from phi.agent.action.tool.terminal import TerminalAction, TerminalObservation
from arch.xor.bridge.tool.terminal import CmdOutputMetadata
from arch.xor.bridge.tool.command.terminal import TerminalCommandStatus
from swarm.engine.executor.context import ExecutionEngine, ExecutionContext

from arch.xor.xe.truncate import maybe_truncate
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from swarm.engine.terminal.session import TerminalSession

log = get_emitter(__name__)

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
        log.debug(
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
            log.debug(f"BASH PARSING between: {between}")
            if result:
                result[-1] += between.rstrip()
            elif between.strip():
                # THIS SHOULD NOT HAPPEN
                result.append(between.rstrip())

        # Extract the command, preserving original formatting
        command = commands[start:end].rstrip()
        log.debug(f"BASH PARSING command: {command}")
        result.append(command)

        last_end = end

    # Add any remaining text after the last command to the last command
    remaining = commands[last_end:].rstrip()
    log.debug(f"BASH PARSING remaining: {remaining}")
    if last_end < len(commands) and result:
        result[-1] += remaining
        log.debug(f"BASH PARSING result[-1] += remaining: {result[-1]}")
    elif last_end < len(commands):
        if remaining:
            result.append(remaining)
            log.debug(f"BASH PARSING result.append(remaining): {result[-1]}")
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
        log.debug(
            f"Failed to parse bash commands for special characters escape\n[input]: "
            f"{command}\n[warning]: {traceback.format_exc()}\nThe original command "
            f"will be returned as is."
        )
        return command

class PollingExecutionEngine(ExecutionEngine):
    """Execution engine that actively polls the terminal screen to detect command completion using PS1 prompt matching"""

    def execute(self, context: ExecutionContext) -> TerminalObservation:
        """Main execution flow delegated from the Middleware Pipeline."""
        session: "TerminalSession" = context.session
        action: TerminalAction = context.action
        
        # 1. Action structure validation
        validation_error = self._validate_action(action)
        if validation_error:
            return validation_error

        # 2. Previous running state check
        state_error = self._check_previous_running_state(session, action)
        if state_error:
            return state_error

        # 3. Initial terminal state capture
        initial_terminal_output = session.terminal.read_screen()
        initial_ps1_matches = CmdOutputMetadata.matches_ps1_metadata(initial_terminal_output)
        initial_ps1_count = len(initial_ps1_matches)

        log.debug(f"Initial PS1 count: {initial_ps1_count}")
        log.debug(f"INITIAL TERMINAL OUTPUT: {initial_terminal_output!r}")

        # 4. Send command to the terminal backend
        self._send_command_to_backend(session, action)

        # 5. Wait for completion and observe results
        return self._wait_for_completion(
            session=session,
            action=action,
            initial_output=initial_terminal_output,
            initial_ps1_count=initial_ps1_count,
            initial_ps1_matches=initial_ps1_matches,
        )

    def _validate_action(self, action: TerminalAction) -> TerminalObservation | None:
        """Check if the action contains invalid command structures."""
        command = action.command.strip()
        
        splited_commands = split_bash_commands(command)
        if len(splited_commands) > 1:
            commands_list = "\n".join(
                f"({i + 1}) {cmd}" for i, cmd in enumerate(splited_commands)
            )
            return TerminalObservation.from_text(
                text=(
                    "Cannot execute multiple commands at once.\n"
                    "Please run each command separately OR chain them into a single "
                    f"command via && or ;\nProvided commands:\n{commands_list}"
                ),
                command=command,
                is_error=True,
            )
        return None

    def _check_previous_running_state(self, session: "TerminalSession", action: TerminalAction) -> TerminalObservation | None:
        """Ensure no conflicting commands are sent while a process is running."""
        command = action.command.strip()
        is_input = action.is_input

        # Handle logs or inputs requested without an active process
        if session.prev_status not in {
            TerminalCommandStatus.CONTINUE,
            TerminalCommandStatus.NO_CHANGE_TIMEOUT,
            TerminalCommandStatus.HARD_TIMEOUT,
        }:
            if command == "":
                return TerminalObservation.from_text(
                    text="No previous running command to retrieve logs from.",
                    command=command,
                    is_error=True,
                )
            if is_input:
                return TerminalObservation.from_text(
                    text="No previous running command to interact with.",
                    command=command,
                    is_error=True,
                )
        return None

    def _send_command_to_backend(self, session: "TerminalSession", action: TerminalAction) -> None:
        """Format and send the actual command to the terminal backend."""
        command = action.command.strip()
        is_input = action.is_input
        if command == "":
            return

        is_special_key = self._is_special_key(command)
        
        if is_input:
            log.debug(f"SENDING INPUT TO RUNNING PROCESS: {command!r}")
            session.terminal.send_keys(command, enter=not is_special_key)
        else:
            if not session.terminal.is_powershell():
                command = escape_bash_special_chars(command)
            log.debug(f"SENDING COMMAND: {command!r}")
            session.terminal.send_keys(command, enter=not is_special_key)

    def _wait_for_completion(
        self,
        session: "TerminalSession",
        action: TerminalAction,
        initial_output: str,
        initial_ps1_count: int,
        initial_ps1_matches: list[re.Match],
    ) -> TerminalObservation:
        """The core polling loop observing the terminal screen."""
        command = action.command.strip()
        start_time = time.time()
        last_change_time = start_time
        last_terminal_output = initial_output

        if (
            session.prev_status in {TerminalCommandStatus.HARD_TIMEOUT, TerminalCommandStatus.NO_CHANGE_TIMEOUT}
            and not last_terminal_output.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip())
            and not action.is_input
            and command != ""
        ):
            return self._handle_blocked_command(session, command, last_terminal_output, initial_ps1_matches)

        while True:
            _poll_start_time = time.time()
            cur_terminal_output = session.terminal.read_screen()

            ps1_matches = CmdOutputMetadata.matches_ps1_metadata(cur_terminal_output)
            current_ps1_count = len(ps1_matches)

            # Detect output changes
            if cur_terminal_output != last_terminal_output:
                last_terminal_output = cur_terminal_output
                last_change_time = time.time()

            # Detect completion condition: new PS1 prompt appears or output ends with PS1
            if (
                current_ps1_count > initial_ps1_count
                or cur_terminal_output.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip())
            ):
                return self._handle_completed_command(session, command, cur_terminal_output, ps1_matches)

            time_since_last_change = time.time() - last_change_time
            is_blocking = action.timeout is not None
            if (
                not is_blocking
                and session.no_change_timeout_seconds is not None
                and time_since_last_change >= session.no_change_timeout_seconds
            ):
                return self._handle_nochange_timeout_command(session, command, cur_terminal_output, ps1_matches)

            if action.timeout is not None:
                time_since_start = time.time() - start_time
                if time_since_start >= action.timeout:
                    return self._handle_hard_timeout_command(
                        session, command, cur_terminal_output, ps1_matches, action.timeout
                    )

            time.sleep(POLL_INTERVAL)

    @staticmethod
    def _is_special_key(command: str) -> bool:
        """Check if the command is a special key (e.g., C-c, C-d)."""
        _command = command.strip()
        # Special keys are typically represented as 3-character strings starting with C-
        return _command.startswith("C-") and len(_command) == 3

    def _handle_completed_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match]
    ) -> TerminalObservation:
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

        return TerminalObservation.from_text(
            command=command, text=command_output, metadata=metadata, exit_code=metadata.exit_code
        )

    def _handle_nochange_timeout_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match]
    ) -> TerminalObservation:
        session.prev_status = TerminalCommandStatus.NO_CHANGE_TIMEOUT
        raw_command_output = self._combine_outputs_between_matches(terminal_content, ps1_matches)
        
        metadata = CmdOutputMetadata()
        metadata.suffix = f"\n[The command has no new output after {session.no_change_timeout_seconds} seconds. {TIMEOUT_MESSAGE_TEMPLATE}]"
        
        command_output = self._get_command_output(
            session, command, raw_command_output, metadata, continue_prefix="[Below is the output of the previous command.]\n"
        )
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)
        
        return TerminalObservation.from_text(
            command=command, text=command_output, metadata=metadata, exit_code=metadata.exit_code
        )

    def _handle_hard_timeout_command(
        self, session: "TerminalSession", command: str, terminal_content: str, ps1_matches: list[re.Match], timeout: float
    ) -> TerminalObservation:
        session.prev_status = TerminalCommandStatus.HARD_TIMEOUT
        raw_command_output = self._combine_outputs_between_matches(terminal_content, ps1_matches)
        
        metadata = CmdOutputMetadata()
        metadata.suffix = f"\n[The command timed out after {timeout} seconds. {TIMEOUT_MESSAGE_TEMPLATE}]"
        
        command_output = self._get_command_output(
            session, command, raw_command_output, metadata, continue_prefix="[Below is the output of the previous command.]\n"
        )
        command_output = maybe_truncate(command_output, truncate_after=MAX_CMD_OUTPUT_SIZE)
        
        return TerminalObservation.from_text(
            command=command, exit_code=metadata.exit_code, text=command_output, metadata=metadata
        )

    def _handle_blocked_command(
        self, session: "TerminalSession", command: str, last_terminal_output: str, initial_ps1_matches: list[re.Match]
    ) -> TerminalObservation:
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
        
        return TerminalObservation.from_text(
            command=command, text=command_output, metadata=metadata, exit_code=metadata.exit_code, is_error=True
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
            else:
                return terminal_content[ps1_matches[0].end() + 1 :]
        elif len(ps1_matches) == 0:
            return terminal_content
            
        combined_output = ""
        for i in range(len(ps1_matches) - 1):
            output_segment = terminal_content[ps1_matches[i].end() + 1 : ps1_matches[i + 1].start()]
            combined_output += output_segment + "\n"
            
        combined_output += terminal_content[ps1_matches[-1].end() + 1 :]
        return combined_output