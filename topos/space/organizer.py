# topos.space.organizer
## @lineage: topos.bound.space
import os
import asyncio
import platform
import subprocess
from pathlib import Path
from typing import Optional
import httpx 
import docker

from phase.executor.flow.event import WorkspaceReady
from arch.topos.node.gan import Message, GanNode
from phase.bind.resolver import resolve_path

from watcher.tracer.infra.router import InfraRouter
from watcher.plane.emitter import get_emitter

RES_ROOT = resolve_path("res")
SURGENT_DIR = RES_ROOT / "surgent"
BUILD_SCRIPT_PATH = SURGENT_DIR / "build_custom_image.sh"
CUSTOM_BASE_IMAGE_TAG = "custom-base-image:latest"

PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")

log = get_emitter("node.space")

SCRIPT_CONTENT = """
#!/bin/bash
IMAGE_NAME=$1
echo "Building Docker image: $IMAGE_NAME"
cat <<EOF | docker build -t "$IMAGE_NAME" -
FROM python:3.11-slim
RUN apt-get update && apt-get install -y git python3-pip
EOF
"""

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

    async def _ensure_build_script(self):
        """@step: Self-healing compilation layer layout mapping"""
        if not SURGENT_DIR.exists():
            SURGENT_DIR.mkdir(parents=True, exist_ok=True)
        if not BUILD_SCRIPT_PATH.exists():
            await asyncio.to_thread(BUILD_SCRIPT_PATH.write_text, SCRIPT_CONTENT)
            BUILD_SCRIPT_PATH.chmod(0o755)

    async def _ensure_docker_image(self):
        """@step: Image signature alignment verification"""
        await self._ensure_build_script()
        self.client = docker.from_env()
        try:
            await asyncio.to_thread(self.client.images.get, CUSTOM_BASE_IMAGE_TAG)
            log.info(f"[{self.name}] [Local] Baseline image target aligned: {CUSTOM_BASE_IMAGE_TAG}")
        except docker.errors.ImageNotFound:
            log.warning(f"[{self.name}] [Local] Target footprint missing. Initiating compilation.")
            await asyncio.to_thread(
                subprocess.run, [str(BUILD_SCRIPT_PATH), CUSTOM_BASE_IMAGE_TAG],
                cwd=str(SURGENT_DIR), check=True
            )

    async def _start_local_workspace(self):
        """## @flow: Image verification -> container run configurations -> local binding"""
        log.info(f"[{self.name}] [Local] Provisioning isolated local sandbox container...")
        await self._ensure_docker_image()
        self.container = await asyncio.to_thread(
            self.client.containers.run,
            image=CUSTOM_BASE_IMAGE_TAG,
            name=f"hands_workspace_{int(asyncio.get_event_loop().time())}",
            ports={'8011/tcp': 8011},
            detach=True,
            environment={"SANDBOX_USER_ID": os.getuid() if hasattr(os, 'getuid') else 1000},
            working_dir="/source"
        )
        self.workspace_ref = self.container.id
        log.info(f"[{self.name}] [Local] 로컬 컨테이너 구동 성공 (ID: {self.container.short_id})")

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
        
        ## 원격 프록시 자원 및 클라이언트 정리 (소켓 누수 방지)
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

        ## 로컬 도커 컨테이너 및 클라이언트 정리
        if not self.use_proxy and self.container:
            try:
                log.info(f"[{self.name}] [Local] Terminating standalone sandbox container ({self.container.short_id})")
                await asyncio.to_thread(self.container.remove, force=True)
                log.info(f"[{self.name}] [Local] Standalone local resources successfully reclaimed.")
            except Exception as e:
                log.error(f"[{self.name}] [Local] Reclaim exception: {e}")

        if self.client:
            try:
                await asyncio.to_thread(self.client.close)
            except Exception:
                pass

        self._running = False
        self._queue.put_nowait(None)