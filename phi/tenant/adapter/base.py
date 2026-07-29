# phi.tenant.adapter.base
from typing import Any, Dict, List, Optional, Union

from atoa.secure.secret.manager import get_secret_bool
from bound.stream.wrapper import StreamWrapper
from tenant.switch.params import ModelResponse

from phi.runtime.executor.pre import CompletionContext
from phi.tenant.client.handler import CompletionHandler

from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.base")

class BaseProviderAdapter:
    def execute(self, ctx: CompletionContext) -> Union[ModelResponse, StreamWrapper]:
        raise NotImplementedError()

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