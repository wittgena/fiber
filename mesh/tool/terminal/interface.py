# mesh.tool.terminal.interface
## @lineage: gov.tool.terminal.interface
## @lineage: eco.gov.tool.terminal.interface
## @lineage: atoa.gov.tool.terminal.interface
## @lineage: agent.gov.tool.terminal.interface
## @lineage: gov.sandbox.engine.tool.terminal.interface
## @lineage: agent.engine.tool.terminal.interface
## @lineage: sandbox.tool.terminal.interface
## @lineage: ops.xor.tool.terminal.interface
## @lineage: meta.xor.tool.terminal.interface
## @lineage: gov.engine.tool.terminal.interface
## @lineage: gov.engine.executor.tool.terminal.interface
"""Abstract interface for terminal backends."""
import os
from abc import ABC, abstractmethod

from arch.gov.tool.terminal import NO_CHANGE_TIMEOUT_SECONDS
from gov.action.tool.terminal import TerminalAction, TerminalObservation

class TerminalInterface(ABC):
    work_dir: str
    username: str | None
    _initialized: bool
    _closed: bool

    def __init__(self, work_dir: str, username: str | None = None):
        self.work_dir = work_dir
        self.username = username
        self._initialized = False
        self._closed = False

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the terminal backend"""

    @abstractmethod
    def close(self) -> None:
        """Clean up the terminal backend"""

    @abstractmethod
    def send_keys(self, text: str, enter: bool = True) -> None:
        """Send text/keys to the terminal"""

    @abstractmethod
    def read_screen(self) -> str:
        """Read the current terminal screen content"""

    @abstractmethod
    def clear_screen(self) -> None:
        """Clear the terminal screen and history"""

    @abstractmethod
    def interrupt(self) -> bool:
        """Send interrupt signal (Ctrl+C) to the terminal"""

    @abstractmethod
    def is_running(self) -> bool:
        """Check if a command is currently running in the terminal"""

    @property
    def initialized(self) -> bool:
        """Check if the terminal is initialized."""
        return self._initialized

    @property
    def closed(self) -> bool:
        """Check if the terminal is closed."""
        return self._closed

    def is_powershell(self) -> bool:
        """Check if this is a PowerShell terminal"""
        return False

class TerminalSessionBase(ABC):
    """Abstract base class for terminal sessions"""
    work_dir: str
    username: str | None
    no_change_timeout_seconds: int
    _initialized: bool
    _closed: bool
    _cwd: str

    def __init__(self, work_dir: str, username: str | None = None, no_change_timeout_seconds: int | None = None):
        self.work_dir = work_dir
        self.username = username
        self.no_change_timeout_seconds = no_change_timeout_seconds or NO_CHANGE_TIMEOUT_SECONDS
        self._initialized = False
        self._closed = False
        self._cwd = os.path.abspath(work_dir)

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the terminal session"""

    @abstractmethod
    def execute(self, action: TerminalAction) -> TerminalObservation:
        """Execute a command in the terminal session"""

    @abstractmethod
    def close(self) -> None:
        """Clean up the terminal session"""

    @abstractmethod
    def interrupt(self) -> bool:
        """Interrupt the currently running command (equivalent to Ctrl+C)"""

    @abstractmethod
    def is_running(self) -> bool:
        """Check if a command is currently running"""

    @property
    def cwd(self) -> str:
        """Get the current working directory."""
        return self._cwd

    def __del__(self) -> None:
        """Ensure the session is closed when the object is destroyed."""
        try:
            self.close()
        except ImportError:
            # Python is shutting down, let the OS handle cleanup
            pass
