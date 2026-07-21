# eco.switch.chat.adapter
## @lineage: adapter.switch.chat.adapter
## @lineage: bound.adapter.switch.chat.adapter
## @lineage: anchor.bind.switch.chat.adapter
## @lineage: bound.surface.switch.chat.adapter
## @lineage: anchor.surface.switch.chat.adapter
## @lineage: bound.channel.switch.chat.adapter
## @lineage: bound.channel.compat.switch.chat.adapter
## @lineage: anchor.channel.compat.switch.chat.adapter
## @lineage: anchor.channel.switch.chat.adapter
## @lineage: anchor.switch.chat.adapter
from typing import AsyncGenerator, Protocol, Any
from eco.legacy.action.completion import acompletion
from eco.switch.params import ModelResponse

class SimpleMessage(dict):
    role: str
    content: str

class LLMProvider(Protocol):
    """TUI 로직에서 바라볼 LLM 인터페이스"""
    async def stream_response(self, messages: list[dict[str, Any]], model: Any) -> AsyncGenerator[str, None]:
        ...

class LiteLLMAdapter(LLMProvider):
    """litellm을 사용하는 구체적 구현체"""
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
            # 여기서 litellm 특화 에러를 앱의 공통 에러로 변환해서 던질 수도 있습니다.
            raise e