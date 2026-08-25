# agent.tenant
## @lineage: phase.agent.tenant
## @lineage: agent.nexus.tenant
## @lineage: nexus.agent.tenant
## @lineage: meta.agent.tenant
## @lineage: ops.chat.adapter.tenant
from typing import AsyncGenerator, Protocol, Any
from fiber.llm.entry import acompletion
from fiber.llm.param import ModelResponse

class SimpleMessage(dict):
    role: str
    content: str

class LLMProvider(Protocol):
    async def stream_response(self, messages: list[dict[str, Any]], model: Any) -> AsyncGenerator[str, None]:
        ...

class CompletionAdapter(LLMProvider):
    async def stream_response(self, messages: list[dict[str, Any]], model: Any) -> AsyncGenerator[str, None]:
        try:
            response = await acompletion(
                messages=messages,
                stream=True,
                model=model.name,
                temperature=model.temperature,
                max_retries=model.max_retries,
                api_key=model.api_key.get_secret_value() if model.api_key else None,
                api_base=model.api_base.unicode_string() if model.api_base else None,
            )
            
            async for chunk in response:
                chunk_content = chunk.choices[0].delta.content
                if isinstance(chunk_content, str):
                    yield chunk_content
                else:
                    break
        except Exception as e:
            raise e