# agent.runtime.space.manager
import os
import shutil
import asyncio
import io
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import httpx
import websockets
import docker
from docker.errors import NotFound, BuildError

from agent.runtime.space.base import BaseWorkspace
from engine.tool.git.changes import get_git_changes
from engine.tool.git.diff import get_git_diff

# [변경됨] 범용 동기식 execute_command 대신 통합 비동기 실행기와 환경변수 정제기 임포트
from agent.runtime.builder.executor import executor_factory, sanitized_env
from arch.xor.bridge.tool.command.workspace import CommandResult, FileOperationResult
from arch.xor.bridge.tool.git import GitChange, GitDiff

# [Topology & Nodes]
from arch.topos.node.gan import Message, GanNode
from phase.executor.flow.event import WorkspaceReady
from phase.bind.resolver import resolve_path

# [Infra & Tracing]
from watcher.tracer.scope import get_current_trace_path
from watcher.tracer.infra.router import InfraRouter
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

RES_ROOT = resolve_path("res")
SPACE_DIR = RES_ROOT / "space"

CUSTOM_BASE_IMAGE_TAG = "custom-base-image:latest"
LOCAL_WORKSPACE_NAME = "hands_workspace_local"  # 컨테이너 재사용을 위한 고정 이름

PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")

# 쉘 스크립트 파일 쓰기를 대체하는 In-memory Dockerfile
DOCKERFILE_CONTENT = """
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git python3-pip
"""

class SandboxWorkspace(BaseWorkspace):
    def __init__(self, *, working_dir: str | Path, **kwargs: Any):
        super().__init__(working_dir=str(working_dir), **kwargs)

    def execute_command(
        self,
        command: str,
        cwd: str | Path | None = None,
        timeout: float = 30.0,
    ) -> CommandResult:
        target_cwd = str(cwd) if cwd is not None else str(self.working_dir)
        log.debug(f"Executing local bash command: {command} in {target_cwd}")

        # [개선됨] 더미 스레드 생성 없이 OS 차원의 Async IO 통신으로 서브프로세스 관리
        async def _async_exec():
            proc = await asyncio.create_subprocess_shell(
                command,
                cwd=target_cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=sanitized_env()  # 보안을 위한 환경변수 필터링 적용
            )
            stdout, stderr = await proc.communicate()
            return (
                proc.returncode,
                stdout.decode('utf-8', errors='replace'),
                stderr.decode('utf-8', errors='replace')
            )

        # 공유 AsyncExecutor 활용하여 동기 컨텍스트에서도 비동기 함수 안전하게 실행 및 타임아웃 통제
        executor = executor_factory.get_async_executor()
        
        try:
            returncode, stdout_str, stderr_str = executor.run_async(_async_exec, timeout=timeout)
            timeout_occurred = False
        except TimeoutError:
            # run_async 내부의 anyio.fail_after에 의해 타임아웃 발생 시 안전하게 캡처
            returncode = -1
            stdout_str = ""
            stderr_str = f"Command timed out after {timeout} seconds"
            timeout_occurred = True
        except Exception as e:
            # 기타 시스템 에러 및 실행 실패 예외 처리
            log.error(f"Execution error in execute_command: {e}", exc_info=True)
            returncode = -1
            stdout_str = ""
            stderr_str = f"Execution error: {str(e)}"
            timeout_occurred = False

        return CommandResult(
            command=command,
            exit_code=returncode,
            stdout=stdout_str,
            stderr=stderr_str,
            timeout_occurred=timeout_occurred,
        )

    def file_upload(self, source_path: str | Path, destination_path: str | Path) -> FileOperationResult:
        source, destination = Path(source_path), Path(destination_path)
        log.debug(f"Local file upload: {source} -> {destination}")
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            return FileOperationResult(success=True, source_path=str(source), destination_path=str(destination), file_size=destination.stat().st_size)
        except Exception as e:
            log.error(f"Local file upload failed: {e}")
            return FileOperationResult(success=False, source_path=str(source), destination_path=str(destination), error=str(e))

    def file_download(self, source_path: str | Path, destination_path: str | Path) -> FileOperationResult:
        source, destination = Path(source_path), Path(destination_path)
        log.debug(f"Local file download: {source} -> {destination}")
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            return FileOperationResult(success=True, source_path=str(source), destination_path=str(destination), file_size=destination.stat().st_size)
        except Exception as e:
            log.error(f"Local file download failed: {e}")
            return FileOperationResult(success=False, source_path=str(source), destination_path=str(destination), error=str(e))

    def git_changes(self, path: str | Path) -> list[GitChange]:
        return get_git_changes(Path(self.working_dir) / path)

    def git_diff(self, path: str | Path) -> GitDiff:
        return get_git_diff(Path(self.working_dir) / path)

    def pause(self) -> None:
        log.debug("pause() called on LocalWorkspace - nothing to do")

    def resume(self) -> None:
        log.debug("resume() called on LocalWorkspace - nothing to do")

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
        if current_trace := get_current_trace_path():
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


# ============================================================================
# [Space Node Organizer (Integrated)]
# ============================================================================
class SpaceNode(GanNode):
    """
    @desc: Hybrid workspace isolation environment controller.
    @flow: Signal matching -> Deployment routing -> Asset resource teardown.
    """
    def __init__(self, name: str, use_proxy: bool = False, router: Optional[InfraRouter] = None):
        super().__init__(name)
        self.use_proxy = use_proxy
        
        self.router = router or InfraRouter(PROXY_URL, PROXY_API_KEY)
        self.client: Optional[docker.DockerClient] = None
        self.container: Optional[docker.models.containers.Container] = None
        
        self.workspace_ref: Optional[str] = None
        self.remote_http_client: Optional[httpx.AsyncClient] = None

    def _build_image_sync(self):
        log.info(f"[{self.name}] [Local] Target footprint missing. Compiling image via Docker SDK... (This may take 1~3 minutes)")
        f = io.BytesIO(DOCKERFILE_CONTENT.encode('utf-8'))
        
        try:
            # decode=True로 스트림을 받되, 화면에 출력하지 않고 메모리에 캐싱하여 에러 감지에만 사용합니다.
            build_logs = self.client.api.build(
                fileobj=f, 
                rm=True, 
                tag=CUSTOM_BASE_IMAGE_TAG, 
                decode=True
            )
            
            for chunk in build_logs:
                # 에러가 발생한 경우에만 캡처하여 예외를 발생시킵니다.
                if 'error' in chunk:
                    raise BuildError(chunk['error'], build_logs)
                    
            log.info(f"[{self.name}] [Local] ✅ Image compilation completed successfully.")
            
        except BuildError as e:
            log.error(f"[{self.name}] ❌ Docker build failed: {e}")
            raise

    async def _ensure_docker_image(self):
        """@step: Image signature alignment verification"""
        # Event Loop 블로킹을 막기 위해 디렉토리 생성을 별도 스레드에서 처리
        await asyncio.to_thread(SPACE_DIR.mkdir, parents=True, exist_ok=True)
        
        self.client = docker.from_env()
        try:
            await asyncio.to_thread(self.client.images.get, CUSTOM_BASE_IMAGE_TAG)
            log.info(f"[{self.name}] [Local] Baseline image target aligned: {CUSTOM_BASE_IMAGE_TAG}")
        except docker.errors.ImageNotFound:
            # Subprocess 쉘 스크립트 호출을 SDK 기반 인메모리 빌드로 교체
            await asyncio.to_thread(self._build_image_sync)

    async def _start_local_workspace(self):
        """## @flow: Image verification -> Check existing -> container run or attach"""
        log.info(f"[{self.name}] [Local] Checking for existing isolated local sandbox container...")
        await self._ensure_docker_image()
        
        try:
            # 1. 기존 컨테이너가 존재하는지 고정된 이름으로 확인
            self.container = await asyncio.to_thread(self.client.containers.get, LOCAL_WORKSPACE_NAME)
            
            # 2. 존재한다면 상태 확인 후 실행
            if self.container.status != "running":
                log.info(f"[{self.name}] [Local] Existing container found but stopped. Starting it...")
                await asyncio.to_thread(self.container.start)
            else:
                log.info(f"[{self.name}] [Local] Existing running container found. Reusing it.")
                
            self.workspace_ref = self.container.id
            log.info(f"[{self.name}] [Local] 로컬 컨테이너 재사용 성공 (ID: {self.container.short_id})")

        except NotFound:
            # 3. 존재하지 않으면 새로 생성
            log.info(f"[{self.name}] [Local] No existing container found. Provisioning new container...")
            self.container = await asyncio.to_thread(
                self.client.containers.run,
                image=CUSTOM_BASE_IMAGE_TAG,
                name=LOCAL_WORKSPACE_NAME,
                ports={'8011/tcp': 8011},
                detach=True,
                environment={"SANDBOX_USER_ID": os.getuid() if hasattr(os, 'getuid') else 1000},
                working_dir="/source"
            )
            self.workspace_ref = self.container.id
            log.info(f"[{self.name}] [Local] 로컬 컨테이너 신규 구동 성공 (ID: {self.container.short_id})")

    async def _start_remote_workspace(self):
        """## @flow: router absolute uri -> dynamic headers -> httpx payload"""
        log.info(f"[{self.name}] [Proxy] Emitting provisioning payload via InfraRouter.")
        
        provision_url = self.router.get_http_endpoint("provision")
        headers = self.router.build_headers()
        
        self.remote_http_client = httpx.AsyncClient(headers=headers)
        response = await self.remote_http_client.post(
            provision_url, 
            json={
                "image": CUSTOM_BASE_IMAGE_TAG,
                "timeout": 3600
            }
        )
        response.raise_for_status()
        
        data = response.json()
        self.workspace_ref = data.get("workspace_ref")
        if not self.workspace_ref:
            raise ValueError("Topological fault: Missing workspace_ref token in remote residue.")
        log.info(f"[{self.name}] [Proxy] Remote sandbox successfully assigned (Ref: {self.workspace_ref})")

    async def on_start_workspace(self, message: Message):
        """@phase: Isolation Infra Allocation"""
        try:
            if self.use_proxy:
                try:
                    await self._start_remote_workspace()
                except Exception as e:
                    log.warning(f"[{self.name}] ⚠️ Remote deployment fault. Triggering Local Fallback loop: {e}")
                    self.use_proxy = False
            
            if not self.use_proxy:
                await self._start_local_workspace()

            self.post_message(WorkspaceReady(workspace_ref=self.workspace_ref))
        except Exception as e:
            log.error(f"[{self.name}] ❌ Complete breakdown of workspace initialization layers: {e}")
            err_msg = Message("node_error", bubble=True)
            err_msg.source_node = self.name
            err_msg.error = str(e)
            self.post_message(err_msg)

    async def on_shutdown(self, message: Message):
        """@phase: Infra Collapse & Resource Reclaim"""
        log.info(f"[{self.name}] 💤 Deconstructing execution environment allocations...")
        
        # [Proxy Cleanup]
        if self.remote_http_client:
            if self.use_proxy and self.workspace_ref:
                try:
                    teardown_url = self.router.get_http_endpoint("teardown", workspace_ref=self.workspace_ref)
                    log.info(f"[{self.name}] [Proxy] Requesting remote sandbox deletion (Ref: {self.workspace_ref})")
                    await self.remote_http_client.delete(teardown_url)
                except Exception as e:
                    log.error(f"[{self.name}] [Proxy] Reclaim exception: {e}")
            try:
                await self.remote_http_client.aclose()
            except Exception:
                pass

        if not self.use_proxy and self.container:
            try:
                log.info(f"[{self.name}] [Local] Preserving sandbox container ({self.container.short_id}) for future reuse.")
            except Exception as e:
                log.error(f"[{self.name}] [Local] Shutdown exception: {e}")

        if self.client:
            try:
                await asyncio.to_thread(self.client.close)
            except Exception:
                pass

        self._running = False
        self._queue.put_nowait(None)