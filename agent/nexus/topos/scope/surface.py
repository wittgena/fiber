# agent.nexus.topos.scope.surface
## @lineage: nexus.agent.topos.scope.surface
## @lineage: meta.agent.topos.scope.surface
## @lineage: topos.scope.surface
## @lineage: bound.space.sandbox.surface
## @lineage: agent.loop.conv.registry
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
import httpx
import redis

from arch.local.llm import LLMEngine
from watcher.plane.emitter import get_emitter

log_local = get_emitter("surface.local")
log_sandbox = get_emitter("surface.sandbox")
log_proxy = get_emitter("scope.proxy")

def get_free_port(starting_port: int, max_port: int = 8999) -> int:
    for port in range(starting_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free ports available between {starting_port} and {max_port}.")

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
    def up(self) -> None: 
        pass

    @abstractmethod
    def down(self) -> None: 
        pass

    @abstractmethod
    def get_engine(self) -> Any: 
        pass

class LocalSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.engine = LLMEngine()

    def up(self) -> None:
        log_local.info("[*] Initializing Local Direct Surface...")
        self.engine.ensure_server()
        start_time = time.time()
        ready = False
        try:
            time.sleep(2) 
            ready = True
        except Exception as e:
            log_local.debug(f"[-] Wait interrupted during Local Surface init: {e}")

        if not ready:
            log_local.warning("[-] Local Engine might not be fully ready, proceeding anyway.")

    def down(self) -> None:
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

    def up(self) -> None:
        if not self._launcher_module:
            raise NotImplementedError("Launcher module must be injected by subclass.")

        # Port collision prevention: scan for an available port
        self.config.port = get_free_port(self.config.port)
        self.base_url = f"http://{self.config.host}:{self.config.port}"

        log_sandbox.info(f"[*] Booting Sandbox Surface on {self.base_url}...")
        self.llm_engine.ensure_server()

        # Use the injected launcher module for dynamic execution
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
        while time.time() - start_time < self.config.timeout:
            if self.process.poll() is not None:
                raise RuntimeError(f"Server exited with code {self.process.returncode}")
            try:
                if httpx.get(f"{self.base_url}/ready", timeout=1.0).status_code < 500:
                    ready = True
                    break
            except (httpx.RequestError, httpx.ConnectError):
                pass
            time.sleep(1)

        if not ready:
            self.down()
            raise RuntimeError("Hand failed to stabilize within timeout.")

        log_sandbox.info(f"\n[+] Hand stabilized at {self.base_url}\n")

    def down(self) -> None:
        if self.process:
            log_sandbox.info("[*] Folding Sandbox Surface (Teardown)...")
            self._stop_event.set()
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            
            # Remove specific PID from registry upon graceful exit
            try:
                self.redis.srem(self.registry_key, self.process.pid)
                log_sandbox.info(f"[+] Unregistered Sandbox PID {self.process.pid} from {self.registry_key}")
            except Exception as e:
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
        
    async def up(self) -> None: # type: ignore
        log_proxy.info(f"[ProxySurface] Pre-flight checking to remote server at {self.host_url}")
        engine_initializer = self.get_engine()
        engine = engine_initializer(None)
        
        try:
            health_response = await engine.health_check()
            log_proxy.info(f"[ProxySurface] Remote Server Alive: {health_response.get('status', 'OK')}")
        except Exception as e:
            log_proxy.error(f"[ProxySurface] Remote Sandbox Pre-flight connection failed: {str(e)}")
            raise ConnectionError(f"Cannot enter managed_scope. Target host unreachable: {e}")
            
        super().up()

    async def down(self) -> None: # type: ignore
        log_proxy.info(f"[ProxySurface] Cleaning up workspace communication resources...")
        if self._engine:
            await self._engine.close()
        log_proxy.info(f"[ProxySurface] Disconnected safely from remote server.")
        super().down()

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