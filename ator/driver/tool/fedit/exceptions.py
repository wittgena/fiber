# ator.driver.tool.fedit.exceptions
## @lineage: driver.tool.fedit.exceptions
## @lineage: ator.bound.tool.fedit.exceptions
## @lineage: eco.tool.fedit.exceptions
## @lineage: engine.tool.fedit.exceptions
## @lineage: phi.tool.fedit.exceptions
## @lineage: phi.runtime.tool.fedit.exceptions
## @lineage: swarm.mesh.tool.fedit.exceptions
## @lineage: mesh.tool.fedit.exceptions
## @lineage: gov.tool.fedit.exceptions
## @lineage: eco.gov.tool.fedit.exceptions
## @lineage: atoa.gov.tool.fedit.exceptions
## @lineage: agent.gov.tool.fedit.exceptions
## @lineage: gov.sandbox.engine.tool.fedit.exceptions
## @lineage: agent.engine.tool.fedit.exceptions
## @lineage: sandbox.tool.fedit.exceptions
## @lineage: ops.xor.tool.fedit.exceptions
## @lineage: meta.xor.tool.fedit.exceptions
## @lineage: gov.engine.tool.fedit.exceptions
## @lineage: gov.engine.executor.tool.fedit.exceptions
class ToolError(Exception):
    """Raised when a tool encounters an error."""

    message: str

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

    def __str__(self):
        return self.message


class EditorToolParameterMissingError(ToolError):
    """Raised when a required parameter is missing for a tool command."""

    command: str
    parameter: str

    def __init__(self, command: str, parameter: str):
        self.command = command
        self.parameter = parameter
        self.message: str = (
            f"Parameter `{parameter}` is required for command: {command}."
        )


class EditorToolParameterInvalidError(ToolError):
    """Raised when a parameter is invalid for a tool command."""

    parameter: str
    value: str

    def __init__(self, parameter: str, value: str, hint: str | None = None):
        self.parameter = parameter
        self.value = value
        self.message: str = (
            f"Invalid `{parameter}` parameter: {value}. {hint}"
            if hint
            else f"Invalid `{parameter}` parameter: {value}."
        )


class FileValidationError(ToolError):
    """Raised when a file fails validation checks (size, type, etc.)."""

    path: str
    reason: str

    def __init__(self, path: str, reason: str):
        self.path = path
        self.reason = reason
        self.message: str = f"File validation failed for {path}: {reason}"
        super().__init__(self.message)
