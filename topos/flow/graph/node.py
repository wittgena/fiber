# topos.flow.graph.node
import os
from pydantic import SecretStr

from topos.scope.config import AgentConfig
from phi.driver.llm.model import LLMModel
from agent.atoa.schema.disc.tool import Tool

from phase.executor.flow.event import AgentConfigured

from arch.topos.node.gan import Message, GanNode
from watcher.plane.emitter import get_emitter

log = get_emitter("node.engine")

LOCAL_MODEL = os.getenv("LLAMA_MODEL_NAME", "openai/gemma-3-1b-it-Q4_K_M.gguf")
LOCAL_PORT = os.getenv("LLAMA_PORT", "8080")
LOCAL_URL = os.getenv("LLM_BASE_URL", f"http://localhost:{LOCAL_PORT}/v1")

PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_WORKSPACE = os.getenv("SANDBOX_WORKSPACE_REF", "container-edge-123")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")

class EngineNode(GanNode):
    """
    @desc: Hybrid runtime specification resolver.
    @flow: Config detection -> Polymorphic payload generation -> Quorum dispatch.
    """
    def __init__(self, name: str, use_proxy: bool = False, target_model: str = None):
        super().__init__(name)
        self.settings = None
        self.use_proxy = use_proxy
        self.target_model = target_model

    def _init_local_llm_payload(self) -> dict:
        model_to_use = self.target_model or LOCAL_MODEL
        log.info(f"[{self.name}] 🔌 [Local] Binding engine to model: {model_to_use}")
        is_external = "gemini" in model_to_use or "openai/" in model_to_use
        base_url = None if is_external else LOCAL_URL
        api_key_val = None if is_external else "not-needed"

        llm_obj = LLMModel(
            model=model_to_use,
            base_url=base_url,
            api_key=SecretStr(api_key_val) if api_key_val else None,
        )
        return llm_obj.model_dump() if hasattr(llm_obj, "model_dump") else llm_obj.dict()

    def _init_proxy_llm_payload(self) -> dict:
        """@flow: Remote parameter extraction -> Ephemeral routing DTO synthesis"""
        log.info(f"[{self.name}] 🌐 [Proxy] Structuring remote connection profile: {PROXY_URL}")
        return {
            "model": self.target_model or LOCAL_MODEL,
            "server_url": PROXY_URL,
            "workspace_ref": PROXY_WORKSPACE,
            "session_api_key": PROXY_API_KEY,
            "is_proxy": True
        }

    async def on_boot(self, message: Message):
        """## @phase: Engine Assetization Initialization"""
        log.info(f"[{self.name}] ⚙️ Initializing engine allocation (Proxy Mode: {self.use_proxy})")

        try:
            llm_payload = None
            if self.use_proxy:
                try:
                    ## @step: Attempt priority remote proxy topology wiring
                    llm_payload = self._init_proxy_llm_payload()
                except Exception as e:
                    ## @step: Local standalone fallback on remote connectivity failure
                    log.warning(f"[{self.name}] ⚠️ Proxy allocation failed. Collapsing to Local Fallback: {e}")
                    self.use_proxy = False  
            
            if not self.use_proxy or llm_payload is None:
                ## @step: Resolve baseline local model specs
                llm_payload = self._init_local_llm_payload()

            ## @step: Construct deterministic capability descriptors (DTO)
            tools_dto = [
                Tool(name="terminal"), 
                Tool(name="file_editor")
            ]
            
            ## @step: Secure Pydantic boundary validation
            self.settings = AgentConfig(llm=llm_payload, tools=tools_dto)
            
            mode_str = "Proxy" if self.use_proxy else "Local"
            log.info(f"[{self.name}] ✓ 인프라 자산 구성 완료 ({mode_str} Mode).")
            
            self.post_message(AgentConfigured(settings=self.settings))
        except Exception as e:
            log.error(f"[{self.name}] ❌ Fatal exception during asset allocation: {e}")
            self.post_message(Message("shutdown", bubble=True))

    async def on_shutdown(self, message: Message):
        """## @phase: Sub-manifold Reclaim"""
        mode = "Proxy" if getattr(self, "use_proxy", False) else "Local"
        log.info(f"[{self.name}] 💤 Purging configuration engine ({mode}).")
        self._running = False
        self._queue.put_nowait(None)