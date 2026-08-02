# agent.runtime.space.manager
## @lineage: agent.runtime.space
import shutil
from pathlib import Path
from typing import Any
import urllib.parse
import httpx
import websockets
import json
from typing import Optional

from agent.runtime.space.base import BaseWorkspace
from engine.tool.git.changes import get_git_changes
from engine.tool.git.diff import get_git_diff

from agent.runtime.executor.command import execute_command

from arch.xor.bridge.tool.command.workspace import CommandResult, FileOperationResult
from arch.xor.bridge.tool.git import GitChange, GitDiff
from watcher.tracer.scope import get_current_trace_path
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class SandboxWorkspace(BaseWorkspace):
    def __init__(self, *, working_dir: str | Path, **kwargs: Any):
        super().__init__(working_dir=str(working_dir), **kwargs)

    def execute_command(
        self,
        command: str,
        cwd: str | Path | None = None,
        timeout: float = 30.0,
    ) -> CommandResult:
        logger.debug(f"Executing local bash command: {command} in {cwd}")
        result = execute_command(
            command,
            cwd=str(cwd) if cwd is not None else str(self.working_dir),
            timeout=timeout,
            print_output=True,
        )
        return CommandResult(
            command=command,
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            timeout_occurred=result.returncode == -1,
        )

    def file_upload(
        self,
        source_path: str | Path,
        destination_path: str | Path,
    ) -> FileOperationResult:
        source = Path(source_path)
        destination = Path(destination_path)

        logger.debug(f"Local file upload: {source} -> {destination}")

        try:
            # Ensure destination directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Copy the file with metadata preservation
            shutil.copy2(source, destination)

            return FileOperationResult(
                success=True,
                source_path=str(source),
                destination_path=str(destination),
                file_size=destination.stat().st_size,
            )

        except Exception as e:
            logger.error(f"Local file upload failed: {e}")
            return FileOperationResult(
                success=False,
                source_path=str(source),
                destination_path=str(destination),
                error=str(e),
            )

    def file_download(
        self,
        source_path: str | Path,
        destination_path: str | Path,
    ) -> FileOperationResult:
        source = Path(source_path)
        destination = Path(destination_path)

        logger.debug(f"Local file download: {source} -> {destination}")

        try:
            # Ensure destination directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Copy the file with metadata preservation
            shutil.copy2(source, destination)

            return FileOperationResult(
                success=True,
                source_path=str(source),
                destination_path=str(destination),
                file_size=destination.stat().st_size,
            )

        except Exception as e:
            logger.error(f"Local file download failed: {e}")
            return FileOperationResult(
                success=False,
                source_path=str(source),
                destination_path=str(destination),
                error=str(e),
            )

    def git_changes(self, path: str | Path) -> list[GitChange]:
        path = Path(self.working_dir) / path
        return get_git_changes(path)

    def git_diff(self, path: str | Path) -> GitDiff:
        path = Path(self.working_dir) / path
        return get_git_diff(path)

    def pause(self) -> None:
        logger.debug("pause() called on LocalWorkspace - nothing to do")

    def resume(self) -> None:
        logger.debug("resume() called on LocalWorkspace - nothing to do")

class SandboxProxy:
    def __init__(self, host_url: str, workspace_ref: str = None, session_api_key: Optional[str] = None):
        self.host_url = host_url
        self.workspace_ref = workspace_ref or "default-workspace"
        self.session_api_key = session_api_key
        
        parsed_url = urllib.parse.urlparse(host_url)
        ws_scheme = "wss" if parsed_url.scheme == "https" else "ws"
        self.ws_url = f"{ws_scheme}://{parsed_url.netloc}"
        self._http_client = httpx.AsyncClient(base_url=self.host_url)

    def _build_headers(self, base_headers: Optional[dict] = None) -> dict:
        headers = base_headers or {}
        if self.session_api_key:
            headers["x-session-api-key"] = self.session_api_key
            
        current_trace = get_current_trace_path()
        if current_trace:
            headers["x-trace-path"] = str(current_trace)
            
        return headers

    async def execute_action_http(self, endpoint: str, payload: dict) -> dict:
        headers = self._build_headers()
        response = await self._http_client.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def connect_ws(self, path: str):
        target_url = f"{self.ws_url}{path}"
        headers = self._build_headers()
        return websockets.connect(target_url, additional_headers=headers)

    async def close(self):
        if self._http_client:
            await self._http_client.aclose()