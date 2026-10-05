# fiber.phase.scope.local.llama
import os
import sys
import time
import subprocess
import requests
import json
import atexit
import argparse
from dataclasses import dataclass

from xphi.arch.bound.event.next import uuid4
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter('local.llama')

MODEL_HF = os.getenv("LLAMA_MODEL_HF", "ggml-org/gemma-3-1b-it-GGUF")
MODEL_NAME = os.getenv("LLAMA_MODEL_NAME", "gemma-3-1b-it-Q4_K_M.gguf")
SERVER_PORT = int(os.getenv("LLAMA_PORT", "8080"))

SERVER_URL = f"http://localhost:{SERVER_PORT}/v1/chat/completions"
HEALTH_URL = f"http://localhost:{SERVER_PORT}/health"

LLAMA_SERVER_CMD = [
    "llama-server",
    "-hf", MODEL_HF,
    "--port", str(SERVER_PORT),
]

@dataclass
class BridgeEvent:
    content: str
    source: str = "agent"
    event_type: str = "message"

class LlamaServer:
    def __init__(self):
        self.model_name = MODEL_NAME
        self.server_url = SERVER_URL
        self.health_url = HEALTH_URL
        self._process = None
        self._owns_process = False  # 자신이 띄운 프로세스인지 추적

        # 1. 메인 프로세스(게이트웨이) 종료 시 무조건 하위 프로세스를 정리하도록 훅 등록
        atexit.register(self.stop)

    def __del__(self):
        # 2. scope.manager에서 LocalSurface 객체가 소멸(GC)될 때 자동 정리 보장
        self.stop()

    def is_alive(self) -> bool:
        try:
            r = requests.get(self.health_url, timeout=1.0)
            return r.status_code == 200
        except Exception:
            return False

    def ensure_server(self):
        """scope.manager에서 동기 호출 시 문제없도록 독립적으로 상태 검증 및 구동"""
        if self.is_alive():
            log.debug(f"[monitor] llama-server already running on port {SERVER_PORT}.")
            return

        log.info("[*] Starting llama-server process...")
        
        # 3. 파이프를 끊어(DEVNULL) OS 리소스 누수 방지
        self._process = subprocess.Popen(
            LLAMA_SERVER_CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._owns_process = True

        # 4. 루프 블로킹을 최소화하기 위해 인터벌을 0.5초로 줄이고 최대 60번(30초) 대기
        for _ in range(60): 
            if self.is_alive():
                log.info("[+] llama-server is online and ready.")
                return
            time.sleep(0.5)

        self.stop()
        raise RuntimeError("llama-server failed to start within timeout.")

    def stop(self):
        """자신이 생성한 프로세스일 경우에만 안전하게 Kill (다른 인스턴스 보호)"""
        if self._process and self._owns_process:
            if self._process.poll() is None:  # 아직 살아있는 경우만
                log.info(f"[*] Terminating managed llama-server (PID: {self._process.pid})...")
                self._process.terminate()
                try:
                    self._process.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    self._process.kill()
            self._process = None
            self._owns_process = False

    def ask(self, prompt: str, callback: callable = None, req_id: str = None) -> str:
        req_id = req_id or str(uuid4())[:8]
        log.debug(f"[Engine-{req_id}] 🌊 ask (stream) START | model={self.model_name}")
        
        self.ensure_server()
        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True
        }

        full_text = ""
        try:
            log.debug(f"[Engine-{req_id}] 📡 Sending Streaming POST to {self.server_url}")
            with requests.post(self.server_url, json=payload, stream=True, timeout=60) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    if not line: continue
                    
                    line_str = line.decode('utf-8')
                    if line_str.startswith("data: "):
                        content = line_str[6:]
                        if content == "[DONE]": break
                        
                        try:
                            chunk = json.loads(content)["choices"][0]["delta"].get("content", "")
                            if chunk:
                                full_text += chunk
                                if callback:
                                    callback(BridgeEvent(source="agent", content=full_text))
                        except Exception as e:
                            log.error(f"[Engine-{req_id}] SSE JSON parsing error: {e}, Payload: {content}")
                            continue
            log.debug(f"[Engine-{req_id}] ✅ ask SUCCESS | total_length={len(full_text)}")
        except Exception as e:
            log.error(f"[Engine-{req_id}] 🚨 Failed during LLM ask request: {e}", exc_info=True)
            
        return full_text

    def chat(self, system_prompt: str, user_prompt: str, timeout: int = 30, req_id: str = None) -> str:
        req_id = req_id or str(uuid4())[:8]
        log.debug(f"[Engine-{req_id}] ⚡ chat START | timeout={timeout}s, model={self.model_name}")
        
        self.ensure_server()
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        try:
            log.debug(f"[Engine-{req_id}] 📡 Sending POST to {self.server_url}")
            r = requests.post(self.server_url, json=payload, timeout=timeout)
            r.raise_for_status()

            data = r.json()
            content = data["choices"][0]["message"]["content"]
            log.debug(f"[Engine-{req_id}] ✅ chat SUCCESS | response_len={len(content)}")
            return content
        except Exception as e:
            log.error(f"[Engine-{req_id}] 🚨 chat FAILED: {e}", exc_info=True)
            raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local Llama Server Manager")
    parser.add_argument(
        "action", 
        nargs="?", 
        choices=["start", "test", "stop"], 
        default="test",
        help="start: 서버를 띄우고 유지 / test: 구동 후 짧은 응답 테스트 / stop: 현재 포트를 점유 중인 서버 강제 종료"
    )
    args = parser.parse_args()
    if args.action == "stop":
        log.info(f"[*] Attempting to kill llama-server on port {SERVER_PORT}...")
        exit_code = os.system(f"lsof -ti:{SERVER_PORT} | xargs kill -9 2>/dev/null")
        if exit_code == 0:
            log.info(f"[+] Successfully terminated processes on port {SERVER_PORT}.")
        else:
            log.info(f"[-] No running server found on port {SERVER_PORT}.")
        sys.exit(0)

    client = LlamaServer()

    if args.action == "start":
        log.info(f"[*] Starting server on port {SERVER_PORT}. Press Ctrl+C to terminate.")
        try:
            client.ensure_server()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("\n[*] Received shutdown signal (Ctrl+C). Terminating...")
        finally:
            client.stop()

    elif args.action == "test":
        try:
            log.info("[*] Running standalone LLM Client test...")
            system_msg = "You are a concise assistant."
            user_msg = "Hello, tell me a short joke about robots."
            log.info(f"\n[Requesting to {MODEL_NAME} on port {SERVER_PORT}...]")
            
            response = client.chat(system_msg, user_msg)
            log.info(f"[+] Response:\n{response}")
        except KeyboardInterrupt:
            log.info("Stopped by user.")
        except Exception as e:
            log.error(f"[-] Test failed: {e}")
        finally:
            client.stop()