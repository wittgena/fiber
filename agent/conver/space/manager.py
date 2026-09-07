# fiber.agent.conver.space.manager
## @lineage: surgent.topos.space.manager
import os
from pathlib import Path
from typing import Any, Optional

from fiber.agent.client.engine.protocol.builder import executor_factory, sanitized_env

from xphi.kernel.space.topos.node.gan import Message, GanNode
from xphi.kernel.space.topos.node.event import WorkspaceReady
from xphi.bound.space.manager import CoreSandboxWorkspace, BaseSpaceManager
from xphi.watcher.tracer.infra.router import InfraRouter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")


class SandboxWorkspace(CoreSandboxWorkspace):
    def __init__(self, **kwargs):
        kwargs['env_vars'] = sanitized_env()
        kwargs['custom_executor'] = executor_factory.get_async_executor()
        super().__init__(**kwargs)

class SpaceManager(BaseSpaceManager):
    def __init__(self):
        super().__init__()
        self.router = InfraRouter(PROXY_URL, PROXY_API_KEY)

    def get_proxy_headers(self) -> dict:
        return self.router.build_headers()

    def get_proxy_endpoint(self, action: str, **kwargs) -> str:
        if action == "base_url": return PROXY_URL
        return self.router.get_http_endpoint(action, **kwargs)

    async def allocate_workspace(self, working_dir: str | Path, use_proxy: bool = False, session_api_key: Optional[str] = None, session_id: str = "shared_workspace") -> Any:
        workspace = await super().allocate_workspace(working_dir, use_proxy, session_api_key, session_id)
        if isinstance(workspace, CoreSandboxWorkspace):
            workspace = SandboxWorkspace(working_dir=working_dir, container=workspace.container)
        return workspace

    def create_space_node(self, name: str, use_proxy: bool = False) -> 'SpaceNode':
        return SpaceNode(name=name, provider=self, use_proxy=use_proxy)

space_provider = SpaceManager()

class SpaceNode(GanNode):
    def __init__(self, name: str, provider: SpaceManager, use_proxy: bool = False):
        super().__init__(name)
        self.provider = provider
        self.use_proxy = use_proxy
        self.active_workspace = None
        self.session_id = "shared_workspace"

    async def on_start_workspace(self, message: Message):
        try:
            self.session_id = getattr(message, 'session_id', self.session_id)
            self.active_workspace = await self.provider.allocate_workspace(
                working_dir="/source", use_proxy=self.use_proxy, session_id=self.session_id
            )
            
            ref = getattr(self.active_workspace, 'workspace_ref', 
                          self.active_workspace.container.short_id if (hasattr(self.active_workspace, 'container') and self.active_workspace.container) else 'local')
            
            self.post_message(WorkspaceReady(workspace_ref=ref))
        except Exception as e:
            log.error(f"[{self.name}] ❌ Workspace allocation failed: {e}")
            err_msg = Message("node_error", bubble=True)
            err_msg.source_node, err_msg.error = self.name, str(e)
            self.post_message(err_msg)

    async def on_shutdown(self, message: Message):
        log.info(f"[{self.name}] 💤 Deconstructing execution environment...")
        if self.active_workspace:
            await self.provider.release_workspace(self.active_workspace, session_id=self.session_id)
            self.active_workspace = None
        
        self._running = False
        self._queue.put_nowait(None)