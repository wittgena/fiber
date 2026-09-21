# fiber.dev.ex.space.manager
## @lineage: theoria.phase.agent.space.manager
import os
import signal
import shutil
import asyncio
import io
import urllib.parse
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Dict, Annotated

import httpx
import docker
from docker.errors import NotFound, BuildError
from docker.models.containers import Container
from pydantic import Field, ConfigDict, BeforeValidator

from fiber.dev.ex.space.git import GitChange, GitDiff
from fiber.dev.ex.space.terminal import CommandResult, FileOperationResult

from xphi.arch.model.surge.disc import DiscMixin
from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.arch.dev.tracer.scope import get_current_trace_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

RES_ROOT = resolve_path("res")
SPACE_DIR = RES_ROOT / "space"
CUSTOM_BASE_IMAGE_TAG = "custom-base-image:latest"

DOCKERFILE_CONTENT = """
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git python3-pip
"""

def _convert_path_to_str(v: str | Path) -> str:
    """Convert Path objects to string for working_dir."""
    if isinstance(v, Path):
        return str(v)
    return v

class BaseWorkspace(DynamicSurgeModel, DiscMixin, ABC):
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


# ==========================================
# 2. Docker & Container Management 영역
# ==========================================
class SessionContainerManager:
    """Manager that provisions Docker containers on-demand per session and handles volume mounting."""
    
    def __init__(self):
        self.client = None
        self._initialized = False

    async def initialize(self):
        if self._initialized: 
            return
        
        try:
            self.client = docker.from_env()
            await asyncio.to_thread(self.client.ping)
        except Exception as e:
            log.error("=========================================================")
            log.error(" 🚨 [Error] Failed to connect to the Docker Daemon!")
            log.error(" -> Please verify that Docker Desktop (or the Docker engine) is running on the host machine.")
            log.error(f" -> Exception details: {str(e)}")
            log.error("=========================================================")
            raise RuntimeError("Docker is not running. Please start Docker Desktop and try again.") from e

        await asyncio.to_thread(SPACE_DIR.mkdir, parents=True, exist_ok=True)
        await self._ensure_image()
        self._initialized = True
        log.info("[SessionContainerManager] Docker environment ready. Using on-demand provisioning.")

    async def _ensure_image(self):
        try:
            await asyncio.to_thread(self.client.images.get, CUSTOM_BASE_IMAGE_TAG)
        except docker.errors.ImageNotFound:
            log.info("[SessionContainerManager] Building custom base image...")
            f = io.BytesIO(DOCKERFILE_CONTENT.encode('utf-8'))
            try:
                build_logs = self.client.api.build(fileobj=f, rm=True, tag=CUSTOM_BASE_IMAGE_TAG, decode=True)
                for chunk in build_logs:
                    if 'error' in chunk: 
                        raise BuildError(chunk['error'], build_logs)
            except BuildError as e:
                log.error(f"[SessionContainerManager] Image build failed: {e}")
                raise

    async def allocate(self, session_id: str, working_dir: Path) -> Container:
        """Lazily creates a session-dedicated container and mounts the host directory."""
        if not self._initialized: 
            await self.initialize()
            
        host_workspace = SPACE_DIR / session_id
        await asyncio.to_thread(host_workspace.mkdir, parents=True, exist_ok=True)

        container_name = f"xphi-space-{session_id}"
        try:
            # Check for an existing session container to reuse (ensures statefulness)
            container = await asyncio.to_thread(self.client.containers.get, container_name)
            if container.status != "running": 
                await asyncio.to_thread(container.start)
            log.info(f"[SessionContainerManager] Reusing existing container: {container_name}")
            return container
        except docker.errors.NotFound:
            pass

        # If not found, spawn a new container and mount the volume
        log.info(f"[SessionContainerManager] Spawning new container for session: {session_id}")
        container = await asyncio.to_thread(
            self.client.containers.run,
            image=CUSTOM_BASE_IMAGE_TAG,
            name=container_name,
            detach=True,
            tty=True,
            working_dir=str(working_dir),
            command="/bin/bash",
            volumes={
                str(host_workspace.absolute()): {'bind': str(working_dir), 'mode': 'rw'}
            }
        )
        return container

    async def release(self, session_id: str, container: Container, destroy: bool = False):
        if destroy:
            log.info(f"[SessionContainerManager] Destroying container {container.name}")
            async def _kill_and_remove():
                try:
                    await asyncio.to_thread(container.remove, force=True)
                except Exception as e:
                    log.warning(f"[SessionContainerManager] Failed to remove container: {e}")
            asyncio.create_task(_kill_and_remove())
        else:
            log.info(f"[SessionContainerManager] Retaining container {container.name} for future reuse.")


# ==========================================
# 3. Workspace Implementation 영역
# ==========================================
class CoreSandboxWorkspace(BaseWorkspace):
    """
    Core Workspace decoupled from external framework dependencies. 
    Accepts environment variables and a custom async executor via injection.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    # 💡 직렬화(Serialization) 에러 해결: 런타임 객체는 exclude=True 로 JSON 변환 방지
    container: Optional[Container] = Field(default=None, exclude=True)
    custom_executor: Any = Field(default=None, exclude=True)
    
    env_vars: Dict[str, str] = Field(default_factory=lambda: os.environ.copy())

    def execute_command(self, command: str, cwd: str | Path | None = None, timeout: float = 30.0) -> CommandResult:
        target_cwd = str(cwd) if cwd is not None else str(self.working_dir)
        
        async def _async_exec():
            if self.container:
                exec_instance = await asyncio.to_thread(
                    self.container.client.api.exec_create,
                    self.container.id, cmd=["/bin/bash", "-c", command],
                    workdir=target_cwd, environment=self.env_vars
                )
                output = await asyncio.to_thread(
                    self.container.client.api.exec_start, exec_instance['Id'], stream=False
                )
                inspect = await asyncio.to_thread(self.container.client.api.exec_inspect, exec_instance['Id'])
                return inspect['ExitCode'], output.decode('utf-8', errors='replace'), ""
            else:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    cwd=target_cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=self.env_vars,
                    preexec_fn=os.setsid if os.name == 'posix' else None # Assign as Process Group Leader
                )
                
                try:
                    async with asyncio.timeout(timeout):
                        stdout, stderr = await proc.communicate()
                        return proc.returncode, stdout.decode('utf-8', errors='replace'), stderr.decode('utf-8', errors='replace')
                except asyncio.TimeoutError:
                    if proc.returncode is None:
                        try:
                            if os.name == 'posix':
                                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                            else:
                                proc.kill()
                        except ProcessLookupError:
                            pass
                    raise TimeoutError(f"Command timed out after {timeout} seconds")

        try:
            # Use the injected custom executor if available
            if self.custom_executor and hasattr(self.custom_executor, 'run_async'):
                returncode, stdout_str, stderr_str = self.custom_executor.run_async(_async_exec, timeout=timeout + 1.0)
            else:
                # Fallback: run the async method synchronously
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If already inside an event loop, create a task (depends on your caller context)
                    raise RuntimeError("execute_command must be used with a custom_executor when inside an active event loop.")
                returncode, stdout_str, stderr_str = loop.run_until_complete(_async_exec())
            
            timeout_occurred = False

        except TimeoutError:
            returncode = -1
            stdout_str = ""
            stderr_str = f"Command timed out after {timeout} seconds"
            timeout_occurred = True
        except Exception as e:
            returncode = -1
            stdout_str = ""
            stderr_str = f"Execution error: {str(e)}"
            timeout_occurred = False

        return CommandResult(
            command=command, exit_code=returncode,
            stdout=stdout_str, stderr=stderr_str, timeout_occurred=timeout_occurred
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
        # Identical logic for local environments
        return self.file_upload(source_path, destination_path)

    def git_changes(self, path: str | Path) -> list[GitChange]:
        return get_git_changes(Path(self.working_dir) / path)

    def git_diff(self, path: str | Path) -> GitDiff:
        return get_git_diff(Path(self.working_dir) / path)


class SandboxProxy:
    """Proxy Logic for remote infrastructure communication."""
    
    def __init__(self, host_url: str, workspace_ref: str = None, session_api_key: Optional[str] = None):
        self.host_url = host_url
        self.workspace_ref = workspace_ref or "default-workspace"
        self.session_api_key = session_api_key
        
        parsed_url = urllib.parse.urlparse(host_url)
        self.ws_url = f"{'wss' if parsed_url.scheme == 'https' else 'ws'}://{parsed_url.netloc}"
        self._http_client = httpx.AsyncClient(base_url=self.host_url)

    def _build_headers(self, base_headers: Optional[dict] = None) -> dict:
        headers = base_headers or {}
        if self.session_api_key: 
            headers["x-session-api-key"] = self.session_api_key
        
        if trace := get_current_trace_path(): 
            headers["x-trace-path"] = str(trace)
            
        return headers

    async def execute_action_http(self, endpoint: str, payload: dict) -> dict:
        response = await self._http_client.post(endpoint, json=payload, headers=self._build_headers())
        response.raise_for_status()
        return response.json()

    async def close(self):
        if self._http_client: 
            await self._http_client.aclose()

class BaseSpaceManager:
    def __init__(self):
        self._local_manager = SessionContainerManager()

    def get_proxy_headers(self) -> dict:
        return {}

    def get_proxy_endpoint(self, action: str, **kwargs) -> str:
        raise NotImplementedError("Subclasses must implement get_proxy_endpoint.")

    async def allocate_workspace(
        self, 
        working_dir: str | Path, 
        use_proxy: bool = False, 
        session_api_key: Optional[str] = None, 
        session_id: str = "shared_workspace"
    ) -> Any:
        
        if use_proxy:
            async with httpx.AsyncClient(headers=self.get_proxy_headers()) as client:
                res = await client.post(
                    self.get_proxy_endpoint("provision"), 
                    json={"image": CUSTOM_BASE_IMAGE_TAG}
                )
                res.raise_for_status()
                ref = res.json().get("workspace_ref")
                
                # Fetch base_url dynamically from the subclass implementation
                base_url = self.get_proxy_endpoint("base_url")
                return SandboxProxy(base_url, ref, session_api_key)
        else:
            container = await self._local_manager.allocate(session_id, Path(working_dir))
            # Returns the decoupled CoreSandboxWorkspace by default
            return CoreSandboxWorkspace(working_dir=working_dir, container=container)

    async def release_workspace(self, workspace: Any, session_id: str = "shared_workspace"):
        if isinstance(workspace, CoreSandboxWorkspace) and getattr(workspace, 'container', None):
            # Retain container state for subsequent reuse
            await self._local_manager.release(session_id, workspace.container, destroy=False)
            
        elif isinstance(workspace, SandboxProxy):
            async with httpx.AsyncClient(headers=self.get_proxy_headers()) as client:
                teardown_endpoint = self.get_proxy_endpoint("teardown", workspace_ref=workspace.workspace_ref)
                await client.delete(teardown_endpoint)
            await workspace.close()