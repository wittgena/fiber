# phi.tool.git.exceptions
## @lineage: phi.runtime.tool.git.exceptions
## @lineage: swarm.mesh.tool.git.exceptions
## @lineage: mesh.tool.git.exceptions
## @lineage: gov.tool.git.exceptions
## @lineage: eco.gov.tool.git.exceptions
## @lineage: atoa.gov.tool.git.exceptions
## @lineage: agent.gov.tool.git.exceptions
## @lineage: gov.sandbox.engine.tool.git.exceptions
## @lineage: agent.engine.tool.git.exceptions
## @lineage: sandbox.tool.git.exceptions
## @lineage: ops.xor.tool.git.exceptions
## @lineage: meta.xor.tool.git.exceptions
## @lineage: gov.engine.tool.git.exceptions
## @lineage: gov.engine.executor.tool.git.exceptions
## @lineage: gov.executor.tool.git.exceptions
## @lineage: gov.executor.git.exceptions

class GitError(Exception):
    """Base exception for git-related errors."""
    pass

class GitRepositoryError(GitError):
    """Exception raised when git repository operations fail."""
    command: str | None
    exit_code: int | None

    def __init__(
        self, message: str, command: str | None = None, exit_code: int | None = None
    ):
        self.command = command
        self.exit_code = exit_code
        super().__init__(message)


class GitCommandError(GitError):
    """Exception raised when git command execution fails."""
    command: list[str]
    exit_code: int
    stderr: str

    def __init__(
        self, message: str, command: list[str], exit_code: int, stderr: str = ""
    ):
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr
        super().__init__(message)


class GitPathError(GitError):
    """Exception raised when git path operations fail."""
    pass
