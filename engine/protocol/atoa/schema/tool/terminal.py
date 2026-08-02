# engine.protocol.atoa.schema.tool.terminal
## @lineage: phi.agent.atoa.schema.tool.terminal
## @lineage: agent.atoa.schema.tool.terminal
## @lineage: atoa.agent.action.tool.terminal
## @lineage: phi.agent.action.tool.terminal
## @lineage: swarm.phi.action.tool.terminal
## @lineage: agent.action.tool.terminal
## @lineage: gov.action.tool.terminal
## @lineage: atoa.disc.action.tool.terminal
## @lineage: atoa.gov.disc.action.tool.terminal
## @lineage: agent.atoa.action.tool.terminal
## @lineage: atoa.call.action.tool.terminal
import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, Optional
from pydantic import Field

if TYPE_CHECKING:
    from agent.state.protocol import ConvStateProtocol
from rich.text import Text
from engine.protocol.atoa.conv.message import ImageContent, TextContent
from engine.protocol.atoa.schema.executor import ActionExecutor
from engine.protocol.atoa.schema.disc.action import Action, Observation
from engine.protocol.action.builder import DeclaredResources, ActionAnnotations, ActionDefinition
from arch.xor.bridge.mark.truncate import maybe_truncate
from arch.xor.bridge.tool.terminal import MAX_CMD_OUTPUT_SIZE, NO_CHANGE_TIMEOUT_SECONDS
from arch.xor.bridge.tool.terminal import CmdOutputMetadata

class TerminalAction(Action):
    """Schema for bash command execution."""
    intent: Literal["explore", "search", "read", "run", "modify", "kill"] | None = Field(
        default=None,
        description="Optional intent: 'explore' (ls/pwd), 'search' (find/grep), 'read' (cat/tail), 'run' (scripts/tests), 'modify' (rm/install), 'kill' (processes)."
    )
    command: str = Field(description="Bash command. Chain with `&&` or `;`. Send empty string to fetch logs. `C-c` interrupts.")
    is_input: bool = Field(default=False, description="True to send STDIN to a running process.")
    timeout: float | None = Field(
        default=None,
        ge=0,
        description=f"Max execution seconds. Unset pauses after {NO_CHANGE_TIMEOUT_SECONDS}s of no output.",
    )
    reset: bool = Field(default=False, description="True to restart unresponsive session. Clears state. Incompatible with is_input.",)

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation with PS1-style bash prompt."""
        content = Text()
        content.append("$ ", style="bold green")
        if self.command:
            content.append(self.command, style="white")
        else:
            content.append("[empty command]", style="italic")

        if self.is_input:
            content.append(" ", style="white")
            content.append("(input to running process)", style="yellow")

        if self.timeout is not None:
            content.append(" ", style="white")
            content.append(f"[timeout: {self.timeout}s]", style="cyan")

        if self.reset:
            content.append(" ", style="white")
            content.append("[reset terminal]", style="red bold")

        return content


class TerminalObservation(Observation):
    """A ToolResult that can be rendered as a CLI output."""

    command: str | None = Field(description="Executed command. Empty if continuing from soft timeout.")
    exit_code: int | None = Field(default=None, description="Exit code. -1 indicates process hit soft timeout and is running.",)
    timeout: bool = Field(default=False, description="True if execution timed out.")
    metadata: CmdOutputMetadata = Field(default_factory=CmdOutputMetadata, description="Metadata captured from PS1 after execution.")
    full_output_save_dir: str | None = Field(default=None, description="Directory for saving full output files.",)

    @property
    def command_id(self) -> int | None:
        return self.metadata.pid

    @property
    def to_llm_content(self) -> Sequence[TextContent | ImageContent]:
        llm_content: list[TextContent | ImageContent] = []

        if self.is_error:
            llm_content.append(TextContent(text=self.ERROR_MESSAGE_HEADER))

        content_text = self.text
        ret = f"{self.metadata.prefix}{content_text}{self.metadata.suffix}"
        
        if self.metadata.working_dir:
            ret += f"\n[Current working directory: {self.metadata.working_dir}]"
        if self.metadata.py_interpreter_path:
            ret += f"\n[Python interpreter: {self.metadata.py_interpreter_path}]"
        if self.metadata.exit_code != -1:
            ret += f"\n[Command finished with exit code {self.metadata.exit_code}]"

        truncated_text = maybe_truncate(
            content=ret,
            truncate_after=MAX_CMD_OUTPUT_SIZE,
            save_dir=self.full_output_save_dir,
            tool_prefix="bash",
        )
        llm_content.append(TextContent(text=truncated_text))

        return llm_content

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation with terminal-style output formatting."""
        text = Text()
        if self.is_error:
            text.append("❌ ", style="red bold")
            text.append(self.ERROR_MESSAGE_HEADER, style="bold red")

        content_text = self.text
        if content_text:
            output_lines = content_text.split("\n")
            for line in output_lines:
                if line.strip():
                    if any(keyword in line.lower() for keyword in ["error", "failed", "exception", "traceback"]):
                        text.append(line, style="red")
                    elif any(keyword in line.lower() for keyword in ["warning", "warn"]):
                        text.append(line, style="yellow")
                    elif line.startswith("+ "):
                        text.append(line, style="cyan")
                    else:
                        text.append(line, style="white")
                text.append("\n")

        if hasattr(self, "metadata") and self.metadata:
            if self.metadata.working_dir:
                text.append("\n📁 ", style="blue")
                text.append(f"Working directory: {self.metadata.working_dir}", style="blue")

            if self.metadata.py_interpreter_path:
                text.append("\n🐍 ", style="green")
                text.append(f"Python interpreter: {self.metadata.py_interpreter_path}", style="green",)

            if (hasattr(self.metadata, "exit_code") and self.metadata.exit_code is not None):
                if self.metadata.exit_code == 0:
                    text.append("\n✅ ", style="green")
                    text.append(f"Exit code: {self.metadata.exit_code}", style="green")
                elif self.metadata.exit_code == -1:
                    text.append("\n⏳ ", style="yellow")
                    text.append("Process still running (soft timeout)", style="yellow")
                else:
                    text.append("\n❌ ", style="red")
                    text.append(f"Exit code: {self.metadata.exit_code}", style="red")

        return text

TOOL_DESCRIPTION = """Execute persistent bash commands.

* Syntax: Chain via `&&` or `;`. Do NOT use `set -e`.
* Long Tasks: Use background (`... &`) or configure `timeout`.
* Soft Timeout (Exit -1): If a command hits timeout, set `is_input=true` and:
  - Send empty `command` to fetch logs.
  - Send STDIN text.
  - Send `C-c` to interrupt.
* Filesystem: Prefer absolute paths. Verify directories.
* Reset: Use `reset=true` ONLY for unresponsive sessions (destroys state).
"""

class TerminalTool(ActionDefinition[TerminalAction, TerminalObservation]):
    """Automatically initializes TerminalExecutor with auto-detection."""
    
    def declared_resources(self, action: Action) -> DeclaredResources:
        if getattr(self.executor, "is_pooled", False):
            return DeclaredResources(keys=(), declared=True)
        return DeclaredResources(keys=("terminal:session",), declared=True)

    @classmethod
    def create(
        cls,
        conv_state: Optional["ConvStateProtocol"] = None, # [핵심 변경] Optional 허용
        username: str | None = None,
        no_change_timeout_seconds: int | None = None,
        terminal_type: Literal["tmux", "subprocess"] | None = None,
        shell_path: str | None = None,
        executor: ActionExecutor | None = None,
    ) -> Sequence["TerminalTool"]:
        
        # 기본 툴 껍데기(Schema) 생성
        tool_instance = cls(
            action_type=TerminalAction,
            observation_type=TerminalObservation,
            description=TOOL_DESCRIPTION,
            annotations=ActionAnnotations(
                title="terminal",
                readOnlyHint=False,
                destructiveHint=True,
                idempotentHint=False,
                openWorldHint=True,
            ),
        )

        # [핵심 변경] conv_state가 없으면 실행기 없이 껍데기만 반환 (Agent 로딩용)
        if conv_state is None:
            return [tool_instance]

        # conv_state가 주어지면 실제 환경 바인딩 (Gov 런타임용)
        from engine.tool.terminal.executor import TerminalExecutor

        working_dir = conv_state.workspace.working_dir
        if not os.path.isdir(working_dir):
            raise ValueError(f"working_dir '{working_dir}' is not a valid directory")

        if executor is None:
            executor = TerminalExecutor(
                working_dir=working_dir,
                username=username,
                no_change_timeout_seconds=no_change_timeout_seconds,
                terminal_type=terminal_type,
                shell_path=shell_path,
                full_output_save_dir=conv_state.env_observation_dir,
            )
        return [tool_instance.set_executor(executor)]