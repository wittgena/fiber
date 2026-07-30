# phi.agent.disc.workspace
## @lineage: agent.workspace
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Annotated, Any
from pydantic import BeforeValidator, Field

from arch.xor.bridge.tool.command.workspace import CommandResult, FileOperationResult
from arch.xor.bridge.tool.git import GitChange, GitDiff
from arch.bound.surge.disc import DiscMixin

from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

def _convert_path_to_str(v: str | Path) -> str:
    """Convert Path objects to string for working_dir."""
    if isinstance(v, Path):
        return str(v)
    return v

class BaseWorkspace(DiscMixin, ABC):
    working_dir: Annotated[
        str,
        BeforeValidator(_convert_path_to_str),
        Field(
            description=(
                "The working directory for agent operations and tool execution. "
                "Accepts both string paths and Path objects. "
                "Path objects are automatically converted to strings."
            )
        ),
    ]

    def __enter__(self) -> "BaseWorkspace":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    @abstractmethod
    def execute_command(
        self,
        command: str,
        cwd: str | Path | None = None,
        timeout: float = 30.0,
    ) -> CommandResult:
        ...

    @abstractmethod
    def file_upload(
        self,
        source_path: str | Path,
        destination_path: str | Path,
    ) -> FileOperationResult:
        ...

    @abstractmethod
    def file_download(
        self,
        source_path: str | Path,
        destination_path: str | Path,
    ) -> FileOperationResult:
        ...

    @abstractmethod
    def git_changes(self, path: str | Path) -> list[GitChange]:
        pass

    @abstractmethod
    def git_diff(self, path: str | Path) -> GitDiff:
        pass

    def pause(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support pause()")

    def resume(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support resume()")
