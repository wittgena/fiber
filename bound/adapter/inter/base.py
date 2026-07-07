# bound.adapter.inter.base
## @lineage: bound.adapter.provider.base
from typing import Any, Dict, List, Optional, Union
from bound.surface.switch.params import ModelResponse
from bound.channel.action.preprocessor import CompletionContext
from bound.surface.legacy.completor import CompletionHandler
from bound.transport.stream.wrapper import CustomStreamWrapper
from xphi.xor.secret.manager import get_secret_bool
from bound.surface.legacy.openai.completion import OpenAIChatCompletion

from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.base")

class BaseProviderAdapter:
    def execute(self, ctx: CompletionContext) -> Union[ModelResponse, CustomStreamWrapper]:
        raise NotImplementedError()

class OpenAICompatibleAdapter(BaseProviderAdapter):
    def __init__(self):
        self.completion_handler = CompletionHandler()
        self.openai_chat = OpenAIChatCompletion()

    def execute(self, ctx: CompletionContext):
        use_base_llm = get_secret_bool("EXPERIMENTAL_OPENAI_BASE_LLM_HTTP_HANDLER")
        if use_base_llm:
            return self.completion_handler.completion(
                model=ctx.model, messages=ctx.messages, api_base=ctx.api_base, api_key=ctx.api_key,
                custom_llm_provider=ctx.custom_llm_provider, model_response=ctx.model_response, 
                optional_params=ctx.optional_params, litellm_params=ctx.litellm_params, logging_obj=ctx.logging_obj,
                timeout=ctx.timeout, shared_session=ctx.shared_session, acompletion=ctx.acompletion, 
                stream=ctx.stream, headers=ctx.headers, client=ctx.client_instance, encoding=None
            )
        else:
            context_obj = self.openai_chat.create_context(
                model=ctx.model, messages=ctx.messages, api_base=ctx.api_base, api_key=ctx.api_key,
                custom_llm_provider=ctx.custom_llm_provider, model_response=ctx.model_response,
                optional_params=ctx.optional_params, litellm_params=ctx.litellm_params, logging_obj=ctx.logging_obj,
                timeout=ctx.timeout, shared_session=ctx.shared_session, acompletion=ctx.acompletion,
                headers=ctx.headers, client=ctx.client_instance, organization=ctx.original_kwargs.get("organization")
            )
            return self.openai_chat.completion(context_obj, ctx.model_response)

class GenericHTTPAdapter(BaseProviderAdapter):
    def __init__(self):
        self.completion_handler = CompletionHandler()

    def execute(self, ctx: CompletionContext):
        if ctx.custom_llm_provider == "ollama" and ctx.api_key and "Authorization" not in ctx.headers:
            ctx.headers["Authorization"] = f"Bearer {ctx.api_key}"
            
        return self.completion_handler.completion(
            model=ctx.model, messages=ctx.messages, api_base=ctx.api_base, api_key=ctx.api_key,
            custom_llm_provider=ctx.custom_llm_provider, model_response=ctx.model_response,
            optional_params=ctx.optional_params, litellm_params=ctx.litellm_params, logging_obj=ctx.logging_obj,
            timeout=ctx.timeout, shared_session=ctx.shared_session, acompletion=ctx.acompletion,
            stream=ctx.stream, headers=ctx.headers, client=ctx.client_instance, encoding=None 
        )