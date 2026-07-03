# anchor.provider.dsp.local
## @lineage: xphi.xor.dsp.llm.local
## @lineage: anchor.model.dsp.llm.local
import asyncio
import functools
from typing import Any

from anchor.provider.dsp.base import BaseLM

from phase.gov.proto.gate import uuid4
from phase.bind.client.engine.local import LLMEngine
from watcher.plane.emitter import get_emitter

log = get_emitter("llm.local")

class LocalLM(BaseLM):
    """@desc: LiteLLM 델리게이터나 복잡한 프로바이더 추론을 거치지 않는 순수 로컬 엔진 어댑터"""
    def __init__(self, model="local-gemma-3", **kwargs):
        super().__init__(model=model, **kwargs) 
        self.client = LLMEngine()

    def _prepare_prompt(self, prompt: str | None, messages: list[dict[str, Any]] | None) -> tuple[str, str]:
        system_prompt = ""
        user_prompt = ""

        if messages:
            for m in messages:
                if m["role"] == "system":
                    system_prompt += str(m["content"]) + "\n"
                elif m["role"] == "user":
                    user_prompt += str(m["content"]) + "\n"
        else:
            user_prompt = prompt or ""
            
        return system_prompt, user_prompt

    def forward(self, prompt: str | None = None, messages: list[dict[str, Any]] | None = None, **kwargs) -> list[str]:
        """동기 호출 처리 (BaseLM.forward 오버라이드)"""
        req_id = str(uuid4())[:8]
        log.debug(f"[LocalLM-{req_id}] 🚀 forward START | model={self.model}")
        
        system_prompt, user_prompt = self._prepare_prompt(prompt, messages)
        response_text = self.client.chat(system_prompt, user_prompt, req_id=req_id)
        log.debug(f"[LocalLM-{req_id}] 🏁 forward END | response_length={len(response_text)}")
        return response_text

    async def aforward(self, prompt: str | None = None, messages: list[dict[str, Any]] | None = None, **kwargs) -> list[str]:
        """비동기 호출 처리 (BaseLM.aforward 오버라이드)"""
        req_id = str(uuid4())[:8]
        log.debug(f"[LocalLM-{req_id}] ⚡ aforward START | model={self.model}")
        
        system_prompt, user_prompt = self._prepare_prompt(prompt, messages)
        loop = asyncio.get_running_loop()
        
        chat_func = functools.partial(self.client.chat, system_prompt, user_prompt, req_id=req_id)
        response_text = await loop.run_in_executor(None, chat_func)
        
        log.debug(f"[LocalLM-{req_id}] 🏁 aforward END | response_length={len(response_text)}")
        return response_text