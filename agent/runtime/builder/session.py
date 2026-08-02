# agent.runtime.builder.session
## @lineage: actor.runtime.builder.session
## @lineage: phi.executor.engine.builder.session
## @lineage: phi.engine.builder.session
## @lineage: swarm.engine.builder.session
## @lineage: swarm.mesh.engine.builder.session
## @lineage: agent.factory.session
import platform
import subprocess
import warnings
from typing import Literal

from agent.runtime.executor.command import sanitized_env
from engine.terminal.session import TerminalSession

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

def _is_tmux_available() -> bool:
    """Check if tmux is available on the system."""
    try:
        result = subprocess.run(
            ["tmux", "-V"],
            capture_output=True,
            text=True,
            timeout=5.0,
            env=sanitized_env(),
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

def _is_powershell_available() -> bool:
    """Check if PowerShell is available on the system."""
    if platform.system() == "Windows":
        powershell_cmd = "powershell"
    else:
        powershell_cmd = "pwsh"

    try:
        result = subprocess.run(
            [powershell_cmd, "-Command", "Write-Host 'PowerShell Available'"],
            capture_output=True,
            text=True,
            timeout=5.0,
            env=sanitized_env(),
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def create_terminal_session(
    work_dir: str,
    username: str | None = None,
    no_change_timeout_seconds: int | None = None,
    terminal_type: Literal["tmux", "subprocess"] | None = None,
    shell_path: str | None = None,
) -> TerminalSession:
    from engine.terminal.session import TerminalSession

    if terminal_type:
        # Force specific session type
        if terminal_type == "tmux":
            if not _is_tmux_available():
                raise RuntimeError("Tmux is not available on this system")
            from engine.terminal.tmux import (
                TmuxTerminal,
            )

            log.info("Using forced TmuxTerminal")
            terminal = TmuxTerminal(work_dir, username)
            return TerminalSession(terminal, no_change_timeout_seconds)
        elif terminal_type == "subprocess":
            from engine.terminal.backend import (
                SubprocessTerminal,
            )

            log.info("Using forced SubprocessTerminal")
            terminal = SubprocessTerminal(work_dir, username, shell_path)
            return TerminalSession(terminal, no_change_timeout_seconds)
        else:
            raise ValueError(f"Unknown session type: {terminal_type}")

    # Auto-detect based on system capabilities
    system = platform.system()
    if system == "Windows":
        raise NotImplementedError("Windows is not supported yet")
    else:
        # On Unix-like systems, prefer tmux if available, otherwise use subprocess
        if _is_tmux_available():
            from engine.terminal.tmux import TmuxTerminal

            log.info("Auto-detected: Using TmuxTerminal (tmux available)")
            terminal = TmuxTerminal(work_dir, username)
            return TerminalSession(terminal, no_change_timeout_seconds)
        else:
            from engine.terminal.backend import SubprocessTerminal
            _tmux_warning = (
                "tmux is not installed. Falling back to subprocess-based"
                " terminal, which may be less stable. For best agent"
                " performance, install tmux (e.g. `apt-get install tmux`"
                " or `brew install tmux`)."
            )
            log.warning(_tmux_warning)
            warnings.warn(_tmux_warning, stacklevel=2)
            terminal = SubprocessTerminal(work_dir, username, shell_path)
            return TerminalSession(terminal, no_change_timeout_seconds)
