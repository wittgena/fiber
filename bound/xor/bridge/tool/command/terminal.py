# bound.xor.bridge.tool.command.terminal
## @lineage: bound.eco.xor.bridge.tool.command.terminal
## @lineage: eco.bound.xor.bridge.tool.command.terminal
## @lineage: engine.xor.bridge.tool.command.terminal
## @lineage: xor.bridge.tool.command.terminal
## @lineage: xor.tool.command.terminal
## @lineage: arch.xor.bridge.tool.command.terminal
## @lineage: arch.gov.tool.command.terminal
## @lineage: sandbox.executor.command.terminal
from enum import Enum

class TerminalCommandStatus(Enum):
    """Status of a terminal command execution."""
    CONTINUE = "continue"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"
    NO_CHANGE_TIMEOUT = "no_change_timeout"
    HARD_TIMEOUT = "hard_timeout"
