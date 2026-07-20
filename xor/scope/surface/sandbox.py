# xor.scope.surface.sandbox
## @lineage: xphi.scope.surface.sandbox
from abc import abstractmethod
import os
import subprocess
import sys
import time
import threading
import httpx
import redis
from xor.scope.surface.config import BaseSurface, SurfaceConfig, get_free_port
from phase.bind.client.engine.local import LLMEngine
from watcher.plane.emitter import get_emitter

log = get_emitter("surface.sandbox")

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

    def up(self):
        if not self._launcher_module:
            raise NotImplementedError("Launcher module must be injected by subclass.")

        ## Port collision prevention: scan for an available port
        self.config.port = get_free_port(self.config.port)
        self.base_url = f"http://{self.config.host}:{self.config.port}"

        log.info(f"[*] Booting Sandbox Surface on {self.base_url}...")
        self.llm_engine.ensure_server()

        ## Use the injected launcher module for dynamic execution
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
            log.info(f"[*] Registered Sandbox PID {pid} to {self.registry_key}")
        except Exception as e:
            log.warning(f"[-] Failed to register Sandbox PID to Redis: {e}")

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

        log.info(f"\n[+] Hand stabilized at {self.base_url}\n")

    def down(self):
        if self.process:
            log.info("[*] Folding Sandbox Surface (Teardown)...")
            self._stop_event.set()
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            
            ## Remove specific PID from registry upon graceful exit
            try:
                self.redis.srem(self.registry_key, self.process.pid)
                log.info(f"[+] Unregistered Sandbox PID {self.process.pid} from {self.registry_key}")
            except Exception as e:
                pass
            log.info("[+] Sandbox Surface process terminated.")

    @abstractmethod
    def get_engine(self):
        """
        @desc: Forces the higher-layer subclass (e.g., Surgent/Gov) to inject and return 
        - the specific engine implementation (e.g., GraphEngine).
        """
        pass