# fiber.llm.driver.local.engine
## @lineage: xphi.arch.local.llm
## @lineage: arch.local.llm
## @lineage: ator.client.local.llm
import os
import time
import subprocess
import requests
import json

from fiber.dphi.model.resonance import BridgeEvent
from xphi.arch.event.next import uuid4
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter('local.engine')
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

class LLMEngine:
    """
    Infra layer:
    - llama-server lifecycle
    - HTTP transport
    - no structural logic
    """
    def __init__(self):
        self.model_name = MODEL_NAME
        self.server_url = SERVER_URL
        self.health_url = HEALTH_URL
        self._process = None

    def is_alive(self) -> bool:
        try:
            r = requests.get(self.health_url, timeout=2)
            return r.status_code == 200
        except Exception:
            log.debug('[monitor] llama-server not ready yet...')
            return False

    def ensure_server(self):
        if self.is_alive():
            return

        log.info("[*] Starting llama-server process...")
        self._process = subprocess.Popen(
            LLAMA_SERVER_CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        for _ in range(30): # 대기 시간을 좀 더 넉넉히(15->30초) 부여
            if self.is_alive():
                log.info("[+] llama-server is online.")
                return
            time.sleep(1)

        self._process.terminate()
        raise RuntimeError("llama-server failed to start within timeout.")

    def ask(self, prompt: str, callback: callable = None, req_id: str = None) -> str:
        """기존 chat을 대체/확장: 스트리밍 및 루처(Rupture) 지원"""
        req_id = req_id or str(uuid4())[:8]
        log.debug(f"[Engine-{req_id}] 🌊 ask (stream) START | model={self.model_name}")
        
        self.ensure_server()
        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True # 스트리밍 활성화
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
    client = LLMEngine()
    try:
        log.info("Starting LLM Client test...")
        system_msg = "You are a concise assistant."
        user_msg = "Hello, tell me a short joke about robots."
        log.info(f"\n[Requesting to {MODEL_NAME}...]")
        
        response = client.chat(system_msg, user_msg)
        log.info(f"Response:\n{response}")
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error(f"Test failed: {e}")
    finally:
        if client._process:
            log.info("Terminating llama-server...")
            client._process.terminate()