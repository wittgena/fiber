# swarm.mesh.engine.terminal.tmux
## @lineage: swarm.mesh.engine.tmux
import time
import uuid
import libtmux
from swarm.mesh.engine.executor.command import sanitized_env
from arch.gov.tool.terminal import (
    HISTORY_LIMIT,
    TMUX_SESSION_HEIGHT,
    TMUX_SESSION_WIDTH,
    TMUX_SOCKET_NAME,
    CMD_OUTPUT_PS1_END
)
from arch.gov.tool.terminal import CmdOutputMetadata
from swarm.mesh.tool.terminal.interface import TerminalInterface
from watcher.plane.emitter import get_logger

log = get_logger(__name__)

class TmuxTerminal(TerminalInterface):
    PS1: str
    server: libtmux.Server
    session: libtmux.Session
    window: libtmux.Window
    pane: libtmux.Pane

    def __init__(
        self,
        work_dir: str,
        username: str | None = None,
    ):
        super().__init__(work_dir, username)
        self.PS1 = CmdOutputMetadata.to_ps1_prompt()

    def initialize(self) -> None:
        """Initialize the tmux terminal session."""
        if self._initialized:
            return

        env = sanitized_env()
        self.server = libtmux.Server(socket_name=TMUX_SOCKET_NAME, environment=env)
        _shell_command = "/bin/bash"
        if self.username in ["root", "surgent"]:
            _shell_command = f"su {self.username} -"

        window_command = _shell_command
        log.debug(f"Initializing tmux terminal with command: {window_command}")
        session_name = f"surgent-{self.username}-{uuid.uuid4()}"
        self.session = self.server.new_session(
            session_name=session_name,
            start_directory=self.work_dir,
            kill_session=True,
            x=TMUX_SESSION_WIDTH,
            y=TMUX_SESSION_HEIGHT,
        )
        for k, v in env.items():
            self.session.set_environment(k, v)

        # Set history limit to a large number to avoid losing history
        # https://unix.stackexchange.com/questions/43414/unlimited-history-in-tmux
        self.session.set_option("history-limit", str(HISTORY_LIMIT))
        self.session.history_limit = str(HISTORY_LIMIT)

        # Create a new pane because the initial pane's history limit is (default) 2000
        _initial_window = self.session.active_window
        self.window = self.session.new_window(
            window_name="terminal",
            window_shell=window_command,
            start_directory=self.work_dir,
        )
        active_pane = self.window.active_pane
        assert active_pane is not None, "Window should have an active pane"
        self.pane = active_pane
        log.debug(f"pane: {self.pane}; history_limit: {self.session.history_limit}")
        _initial_window.kill()

        self.pane.send_keys(f'set +H; export PROMPT_COMMAND=\'export PS1="{self.PS1}"\'; export PS2=""')
        time.sleep(0.1)  # Wait for command to take effect
        log.debug(f"Tmux terminal initialized with work dir: {self.work_dir}")

        self._initialized: bool = True
        self.clear_screen()

    def close(self) -> None:
        """Clean up the tmux session."""
        if self._closed:
            return
        try:
            if hasattr(self, "session"):
                self.session.kill()
        except Exception as e:
            log.debug(f"Error closing tmux session (may already be dead): {e}")
        self._closed: bool = True

    def send_keys(self, text: str, enter: bool = True) -> None:
        if not self._initialized or not isinstance(self.pane, libtmux.Pane):
            raise RuntimeError("Tmux terminal is not initialized")

        self.pane.send_keys(text, enter=enter)

    def read_screen(self) -> str:
        if not self._initialized or not isinstance(self.pane, libtmux.Pane):
            raise RuntimeError("Tmux terminal is not initialized")

        content = "\n".join(
            map(
                # avoid double newlines
                lambda line: line.rstrip(),
                self.pane.cmd("capture-pane", "-J", "-pS", "-").stdout,
            )
        )
        return content

    def clear_screen(self) -> None:
        if not self._initialized or not isinstance(self.pane, libtmux.Pane):
            raise RuntimeError("Tmux terminal is not initialized")

        self.pane.send_keys("clear", enter=True)
        time.sleep(0.1)
        self.pane.cmd("clear-history")

    def interrupt(self) -> bool:
        if not self._initialized or not isinstance(self.pane, libtmux.Pane):
            return False
        try:
            self.pane.send_keys("C-c", enter=False)
            return True
        except Exception as e:
            log.error(f"Failed to interrupt command: {e}", exc_info=True)
            return False

    def is_running(self) -> bool:
        if not self._initialized:
            return False

        try:
            content = self.read_screen()
            return not content.rstrip().endswith(CMD_OUTPUT_PS1_END.rstrip())
        except Exception:
            return False
