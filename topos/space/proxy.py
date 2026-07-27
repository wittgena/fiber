# topos.space.proxy
## @lineage: gov.workspace
import shutil
from pathlib import Path
from typing import Any
import urllib.parse
import httpx
import websockets
import json
from typing import Optional

from phi.agent.disc.workspace import BaseWorkspace
from swarm.engine.executor.command import execute_command
from swarm.mesh.tool.git.changes import get_git_changes
from swarm.mesh.tool.git.diff import get_git_diff

from arch.gov.tool.command.workspace import CommandResult, FileOperationResult
from arch.gov.tool.git import GitChange, GitDiff

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
        
        # 재사용 비동기 HTTP 클라이언트 풀 초기화
        self._http_client = httpx.AsyncClient(base_url=self.host_url)

    def _build_headers(self, base_headers: Optional[dict] = None) -> dict:
        """기본 인증 스펙과 인프라 관측용 트레이싱 경로를 결합하는 헤더 빌더"""
        headers = base_headers or {}
        if self.session_api_key:
            headers["x-session-api-key"] = self.session_api_key
            
        # [핵심 연동 포인트] 현재 로컬 스코프의 트레이싱 경로를 헤더에 주입하여 서버로 전파
        current_trace = get_current_trace_path()
        if current_trace:
            headers["x-trace-path"] = str(current_trace)
            
        return headers

    async def execute_action_http(self, endpoint: str, payload: dict) -> dict:
        """[HTTP] 동적 추적 수신 구조가 보강된 단발성 제어부"""
        headers = self._build_headers()
        response = await self._http_client.post(endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()

    def connect_ws(self, path: str):
        """[WebSocket] 분산 트레이싱 가시성이 통합된 실시간 채널 프로바이더"""
        target_url = f"{self.ws_url}{path}"
        
        # 웹소켓 오프닝 핸드셰이크 헤더에도 동일하게 분산 트레이싱 컨텍스트 주입
        headers = self._build_headers()
        return websockets.connect(target_url, additional_headers=headers)

    async def close(self):
        """[명시적 자원 해제] 소켓 바인딩 및 커넥션 풀 클리어 파괴자"""
        if self._http_client:
            await self._http_client.aclose()