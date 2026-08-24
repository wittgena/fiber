# agent.space.manager
## @lineage: bound.space.manager
## @lineage: bound.adapter.space.manager
import os
import signal
import shutil
import asyncio
import io
import urllib.parse
from pathlib import Path
from typing import Any, Optional, Dict

import httpx
import websockets
import docker
from docker.errors import NotFound, BuildError
from docker.models.containers import Container

from xphi.kernel.space.topos.context.space import BaseWorkspace
from xphi.arch.xor.bridge.git.schema import GitChange, GitDiff
from xphi.arch.xor.bridge.git.changes import get_git_changes
from xphi.arch.xor.bridge.git.diff import get_git_diff
from xphi.arch.xor.bridge.command.workspace import CommandResult, FileOperationResult

from fiber.agent.space.terminal.session.builder import executor_factory, sanitized_env

from xphi.kernel.space.topos.node.gan import Message, GanNode
from xphi.kernel.space.topos.node.event import WorkspaceReady
from xphi.kernel.space.bind.resolver import resolve_path

from xphi.watcher.tracer.scope import get_current_trace_path
from fiber.phase.tracer.router import InfraRouter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

RES_ROOT = resolve_path("res")
SPACE_DIR = RES_ROOT / "space"

CUSTOM_BASE_IMAGE_TAG = "custom-base-image:latest"
PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")

DOCKERFILE_CONTENT = """
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git python3-pip
"""

class HotWarmContainerPool:
    def __init__(self, pool_size: int = 3):
        self.pool_size = pool_size
        self.client = None
        self.ready_queue: asyncio.Queue[Container] = asyncio.Queue(maxsize=pool_size)
        self._initialized = False

    async def initialize(self):
        if self._initialized: return
        
        try:
            self.client = docker.from_env()
            await asyncio.to_thread(self.client.ping)
        except Exception as e:
            log.error("=========================================================")
            log.error(" 🚨 [오류] Docker 데몬(Daemon)에 연결할 수 없습니다!")
            log.error(" -> 호스트 머신에서 Docker Desktop이 실행 중인지 확인해주세요.")
            log.error(f" -> 상세 예외 정보: {str(e)}")
            log.error("=========================================================")
            raise RuntimeError("Docker is not running. Please start Docker Desktop and try again.") from e

        await asyncio.to_thread(SPACE_DIR.mkdir, parents=True, exist_ok=True)
        await self._ensure_image()
        
        log.info(f"[ContainerPool] Pre-provisioning {self.pool_size} hot-warm containers...")
        for _ in range(self.pool_size):
            await self._spawn_and_enqueue()
        self._initialized = True

    async def _ensure_image(self):
        try:
            await asyncio.to_thread(self.client.images.get, CUSTOM_BASE_IMAGE_TAG)
        except docker.errors.ImageNotFound:
            log.info("[ContainerPool] Building custom base image...")
            f = io.BytesIO(DOCKERFILE_CONTENT.encode('utf-8'))
            try:
                build_logs = self.client.api.build(fileobj=f, rm=True, tag=CUSTOM_BASE_IMAGE_TAG, decode=True)
                for chunk in build_logs:
                    if 'error' in chunk: raise BuildError(chunk['error'], build_logs)
            except BuildError as e:
                log.error(f"[ContainerPool] Image build failed: {e}")
                raise

    async def _spawn_and_enqueue(self):
        """새 컨테이너를 구동하여 대기 큐에 삽입"""
        container = await asyncio.to_thread(
            self.client.containers.run,
            image=CUSTOM_BASE_IMAGE_TAG,
            detach=True,
            tty=True,
            working_dir="/source",
            command="/bin/bash" 
        )
        await self.ready_queue.put(container)
        log.debug(f"[ContainerPool] New container {container.short_id} added to pool.")

    async def acquire(self) -> Container:
        """대기 중인 컨테이너 즉시 할당 O(1)"""
        if not self._initialized:
            await self.initialize()
        container = await self.ready_queue.get()
        log.info(f"[ContainerPool] Allocated container {container.short_id}")
        return container

    async def release_and_replenish(self, container: Container):
        log.info(f"[ContainerPool] Destroying used container {container.short_id} (Zero State Contamination)")
        async def _kill_and_remove():
            try:
                await asyncio.to_thread(container.remove, force=True)
            except Exception as e:
                log.warning(f"[ContainerPool] Failed to remove container: {e}")
        asyncio.create_task(_kill_and_remove())
        asyncio.create_task(self._spawn_and_enqueue())

class SandboxWorkspace(BaseWorkspace):
    def __init__(self, *, working_dir: str | Path, container: Optional[Container] = None, **kwargs: Any):
        # ✅ Pydantic 제약 충돌 해결:
        # BaseWorkspace (DynamicSurgeModel 상속)로 위임하여 Pydantic이 안전하게 extra 필드로 수용하게 함.
        # self.container = container 와 같은 편법 할당 우회 로직을 완벽히 제거.
        super().__init__(working_dir=working_dir, container=container, **kwargs)

    def execute_command(self, command: str, cwd: str | Path | None = None, timeout: float = 30.0) -> CommandResult:
        target_cwd = str(cwd) if cwd is not None else str(self.working_dir)
        executor = executor_factory.get_async_executor()
        async def _async_exec():
            # DynamicSurgeModel의 extra 속성에 의해 self.container(또는 self.get('container'))로 안전하게 접근 가능
            if self.get('container'):
                exec_instance = await asyncio.to_thread(
                    self.container.client.api.exec_create,
                    self.container.id, cmd=["/bin/bash", "-c", command],
                    workdir=target_cwd, environment=sanitized_env()
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
                    env=sanitized_env(),
                    preexec_fn=os.setsid if os.name == 'posix' else None # Process Group Leader로 지정
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
            returncode, stdout_str, stderr_str = executor.run_async(_async_exec, timeout=timeout + 1.0)
            timeout_occurred = False
        except TimeoutError:
            returncode, stdout_str, stderr_str = -1, "", f"Command timed out after {timeout} seconds"
            timeout_occurred = True
        except Exception as e:
            returncode, stdout_str, stderr_str = -1, "", f"Execution error: {str(e)}"
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
        return self.file_upload(source_path, destination_path) # 로컬의 경우 동일 로직

    def git_changes(self, path: str | Path) -> list[GitChange]:
        return get_git_changes(Path(self.working_dir) / path)

    def git_diff(self, path: str | Path) -> GitDiff:
        return get_git_diff(Path(self.working_dir) / path)

    def pause(self) -> None: pass
    def resume(self) -> None: pass

class SandboxProxy:
    def __init__(self, host_url: str, workspace_ref: str = None, session_api_key: Optional[str] = None):
        self.host_url, self.workspace_ref = host_url, workspace_ref or "default-workspace"
        self.session_api_key = session_api_key
        parsed_url = urllib.parse.urlparse(host_url)
        self.ws_url = f"{'wss' if parsed_url.scheme == 'https' else 'ws'}://{parsed_url.netloc}"
        self._http_client = httpx.AsyncClient(base_url=self.host_url)

    def _build_headers(self, base_headers: Optional[dict] = None) -> dict:
        headers = base_headers or {}
        if self.session_api_key: headers["x-session-api-key"] = self.session_api_key
        if trace := get_current_trace_path(): headers["x-trace-path"] = str(trace)
        return headers

    async def execute_action_http(self, endpoint: str, payload: dict) -> dict:
        response = await self._http_client.post(endpoint, json=payload, headers=self._build_headers())
        response.raise_for_status()
        return response.json()

    async def close(self):
        if self._http_client: await self._http_client.aclose()

class SpaceManager:
    def __init__(self):
        self._local_pool = HotWarmContainerPool(pool_size=3)
        self.router = InfraRouter(PROXY_URL, PROXY_API_KEY)

    async def allocate_workspace(self, working_dir: str | Path, use_proxy: bool = False, session_api_key: Optional[str] = None) -> Any:
        if use_proxy:
            async with httpx.AsyncClient(headers=self.router.build_headers()) as client:
                res = await client.post(self.router.get_http_endpoint("provision"), json={"image": CUSTOM_BASE_IMAGE_TAG})
                res.raise_for_status()
                ref = res.json().get("workspace_ref")
                return SandboxProxy(PROXY_URL, ref, session_api_key)
        else:
            container = await self._local_pool.acquire()
            return SandboxWorkspace(working_dir=working_dir, container=container)

    async def release_workspace(self, workspace: Any):
        if isinstance(workspace, SandboxWorkspace) and workspace.get('container'):
            await self._local_pool.release_and_replenish(workspace.container)
        elif isinstance(workspace, SandboxProxy):
            async with httpx.AsyncClient(headers=self.router.build_headers()) as client:
                await client.delete(self.router.get_http_endpoint("teardown", workspace_ref=workspace.workspace_ref))
            await workspace.close()

    def create_space_node(self, name: str, use_proxy: bool = False) -> 'SpaceNode':
        return SpaceNode(name=name, provider=self, use_proxy=use_proxy)

space_provider = SpaceManager()

class SpaceNode(GanNode):
    def __init__(self, name: str, provider: SpaceManager, use_proxy: bool = False):
        super().__init__(name)
        self.provider = provider
        self.use_proxy = use_proxy
        self.active_workspace = None

    async def on_start_workspace(self, message: Message):
        try:
            self.active_workspace = await self.provider.allocate_workspace(
                working_dir="/source", use_proxy=self.use_proxy
            )
            # hasattr 및 get 메서드를 통해 컨테이너 참조를 안전하게 획득
            ref = getattr(self.active_workspace, 'workspace_ref', 
                          self.active_workspace.container.short_id if (hasattr(self.active_workspace, 'container') and self.active_workspace.container) else 'local')
            self.post_message(WorkspaceReady(workspace_ref=ref))
        except RuntimeError as e:
            # [개선 3] Docker 연결 오류와 같이 런타임에서 잡힌 치명적 에러를 메시지로 전파
            log.error(f"[{self.name}] ❌ Workspace allocation aborted: {e}")
            err_msg = Message("node_error", bubble=True)
            err_msg.source_node, err_msg.error = self.name, str(e)
            self.post_message(err_msg)
        except Exception as e:
            log.error(f"[{self.name}] ❌ Workspace allocation failed: {e}")
            err_msg = Message("node_error", bubble=True)
            err_msg.source_node, err_msg.error = self.name, str(e)
            self.post_message(err_msg)

    async def on_shutdown(self, message: Message):
        """@phase: Infra Collapse & Resource Reclaim"""
        log.info(f"[{self.name}] 💤 Deconstructing execution environment...")
        if self.active_workspace:
            await self.provider.release_workspace(self.active_workspace)
            self.active_workspace = None
        
        self._running = False
        self._queue.put_nowait(None)