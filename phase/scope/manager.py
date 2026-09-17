# fiber.phase.scope.manager
import os
import sys
import time
import socket
import asyncio
import threading
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Type, Optional, Callable, Any
from contextlib import asynccontextmanager, AsyncExitStack

import httpx
import redis

from fiber.phase.scope.local.engine import LLMEngine
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.dev.tracer.scope import scope_trace, get_current_trace_path

log_flow = get_emitter("scope.manager")
log_local = get_emitter("surface.local")
log_sandbox = get_emitter("surface.sandbox")
log_proxy = get_emitter("scope.proxy")

# =====================================================================
# [UTILS]
# =====================================================================
def get_free_port(starting_port: int, max_port: int = 8999) -> int:
    for port in range(starting_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free ports available between {starting_port} and {max_port}.")

# =====================================================================
# [CONFIG & BASE]
# =====================================================================
@dataclass
class SurfaceConfig:
    surface_type: str = "local"
    host: str = "0.0.0.0"
    port: int = 8000
    timeout: int = 30
    show_logs: bool = True
    use_proxy: bool = False
    server_url: str = "http://localhost:8000"
    workspace_ref: Optional[str] = None
    session_api_key: Optional[str] = None
    engine_factory: Optional[Callable[..., Any]] = None

class BaseSurface(ABC):
    @abstractmethod
    async def up(self) -> None: 
        pass

    @abstractmethod
    async def down(self) -> None: 
        pass

    @abstractmethod
    def get_engine(self) -> Any: 
        pass

# =====================================================================
# [SURFACE IMPLEMENTATIONS]
# =====================================================================
class LocalSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.engine = LLMEngine()

    async def up(self) -> None:
        log_local.info("[*] Initializing Local Direct Surface...")
        self.engine.ensure_server()
        try:
            await asyncio.sleep(0.3)
        except Exception as e:
            log_local.debug(f"[-] Wait interrupted during Local Surface init: {e}")

    async def down(self) -> None:
        log_local.info("[*] Folding Local Surface...")

    def get_engine(self) -> Any:
        return lambda agent_usage: self.engine

class SandboxSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.process = None
        self._stop_event = threading.Event()
        self.threads = []
        self.llm_engine = LLMEngine()
        
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis = redis.Redis(host=redis_host, decode_responses=True)
        
        self.process_name = "sandbox.surface"
        self._launcher_module = None 
        self.registry_key = "system:sandbox:pids"

    def stream_output(self, pipe, prefix: str):
        try:
            for line in iter(pipe.readline, ""):
                if self._stop_event.is_set():
                    break
                if line:
                    sys.stdout.write(f"[{prefix}] {line}")
                    sys.stdout.flush()
        finally:
            pipe.close()

    async def up(self) -> None:
        if not self._launcher_module:
            raise NotImplementedError("Launcher module must be injected by subclass.")

        self.config.port = get_free_port(self.config.port)
        self.base_url = f"http://{self.config.host}:{self.config.port}"

        log_sandbox.info(f"[*] Booting Sandbox Surface on {self.base_url}...")
        self.llm_engine.ensure_server()

        cmd_str = f"exec -a {self.process_name} {sys.executable} -m {self._launcher_module} --host {self.config.host} --port {self.config.port}"
        cmd = ["bash", "-c", cmd_str]
        
        env = {**os.environ, "LOG_JSON": "true", "PYTHONUNBUFFERED": "1"}
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            stderr=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            text=True, env=env, bufsize=1
        )

        pid = self.process.pid
        try:
            self.redis.sadd(self.registry_key, pid)
            log_sandbox.info(f"[*] Registered Sandbox PID {pid} to {self.registry_key}")
        except Exception as e:
            log_sandbox.warning(f"[-] Failed to register Sandbox PID to Redis: {e}")

        if self.config.show_logs and self.process.stdout and self.process.stderr:
            t1 = threading.Thread(target=self.stream_output, args=(self.process.stdout, "SURFACE:OUT"), daemon=True)
            t2 = threading.Thread(target=self.stream_output, args=(self.process.stderr, "SURFACE:LOG"), daemon=True)
            t1.start()
            t2.start()
            self.threads = [t1, t2]

        start_time = time.time()
        ready = False
        
        # 완전한 비동기 HTTP Polling
        async with httpx.AsyncClient() as client:
            while time.time() - start_time < self.config.timeout:
                if self.process.poll() is not None:
                    raise RuntimeError(f"Server exited with code {self.process.returncode}")
                try:
                    res = await client.get(f"{self.base_url}/ready", timeout=1.0)
                    if res.status_code < 500:
                        ready = True
                        break
                except (httpx.RequestError, httpx.ConnectError):
                    pass
                
                # 블로킹 방지
                await asyncio.sleep(0.3)

        if not ready:
            await self.down()
            raise RuntimeError("Hand failed to stabilize within timeout.")

        log_sandbox.info(f"\n[+] Hand stabilized at {self.base_url}\n")

    async def down(self) -> None:
        if self.process:
            log_sandbox.info("[*] Folding Sandbox Surface (Teardown)...")
            self._stop_event.set()
            self.process.terminate()
            
            try:
                # 동기 wait()를 백그라운드 스레드로 격리하여 이벤트 루프 보호
                await asyncio.wait_for(asyncio.to_thread(self.process.wait), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
            
            try:
                self.redis.srem(self.registry_key, self.process.pid)
                log_sandbox.info(f"[+] Unregistered Sandbox PID {self.process.pid} from {self.registry_key}")
            except Exception:
                pass
            log_sandbox.info("[+] Sandbox Surface process terminated.")

    @abstractmethod
    def get_engine(self) -> Any:
        pass

class ProxySurface(SandboxSurface):
    def __init__(self, config: SurfaceConfig):
        super().__init__(config)
        self.host_url = config.server_url
        self.workspace_ref = config.workspace_ref
        self.session_api_key = config.session_api_key
        self.process_name = "proxy.surface"
        
        self._engine: Optional[Any] = None
        self.engine_factory: Callable[..., Any] = getattr(config, 'engine_factory', None)
        if not self.engine_factory:
            raise ValueError("[ProxySurface] BaseEngine 생성을 위한 engine_factory가 제공되지 않았습니다.")

    def get_engine(self) -> Any:
        if not self._engine:
            self._engine = self.engine_factory(
                host_url=self.host_url, 
                agent_usage="managed_context", 
                workspace_ref=self.workspace_ref,
                session_api_key=self.session_api_key
            )
        return lambda agent_usage: self._engine
        
    async def up(self) -> None:
        log_proxy.info(f"[ProxySurface] Pre-flight checking to remote server at {self.host_url}")
        engine_initializer = self.get_engine()
        engine = engine_initializer(None)
        
        try:
            health_response = await engine.health_check()
            log_proxy.info(f"[ProxySurface] Remote Server Alive: {health_response.get('status', 'OK')}")
        except Exception as e:
            log_proxy.error(f"[ProxySurface] Remote Sandbox Pre-flight connection failed: {str(e)}")
            raise ConnectionError(f"Cannot enter managed_scope. Target host unreachable: {e}")
            
        await super().up()

    async def down(self) -> None:
        log_proxy.info(f"[ProxySurface] Cleaning up workspace communication resources...")
        if self._engine:
            await self._engine.close()
        log_proxy.info(f"[ProxySurface] Disconnected safely from remote server.")
        await super().down()

SURFACE_REGISTRY = {
    "local": LocalSurface,
    "sandbox": SandboxSurface,
    "proxy": ProxySurface
}

def get_surface_class(surface_type: str) -> Type[BaseSurface]:
    surface_class = SURFACE_REGISTRY.get(surface_type)
    if not surface_class:
        raise ValueError(f"Unknown surface type: {surface_type}")
    return surface_class

# =====================================================================
# [MANAGER & CONTEXT]
# =====================================================================
class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log_flow.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
            surface_class = get_surface_class("local")
            
        self.impl = surface_class(config)

    async def up(self):
        # 모든 impl이 비동기로 수정되었으므로, 불필요한 분기문 제거
        await self.impl.up()

    async def down(self):
        await self.impl.down()
    
    def get_engine(self):
        return self.impl.get_engine()

@asynccontextmanager
async def managed_scope(**surface_kwargs):
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log_flow.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log_flow.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log_flow.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await asyncio.sleep(0.1)
            await manager.down()
            log_flow.info("[+] Context Manager closed safely.")