# bound.agent.llm.gemini
## @lineage: ext.router.llm.gemini
## @lineage: router.llm.gemini
import asyncio
import inspect
import functools
import os
from importlib.metadata import PackageNotFoundError, version
import typing
from typing import (
    TYPE_CHECKING,
    cast,
    Any,
    AsyncGenerator,
    Dict,
    Generator,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    Callable,
    Literal,
)
from bound.agent.llm.handle.converter import chat_to_completion_decorator, achat_to_completion_decorator, stream_chat_to_completion_decorator, astream_chat_to_completion_decorator
from bound.agent.llm.model.types.block import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    LLMMetadata,
    MessageRole,
    ToolCallBlock,
)
from bound.agent.adapter.pydantic import BaseModel, Field, PrivateAttr
from bound.agent.adapter.callback.manager import CallbackManager
from bound.agent.adapter.constants import DEFAULT_TEMPERATURE, DEFAULT_NUM_OUTPUTS
from bound.agent.adapter.callback.manager import llm_chat_callback, llm_completion_callback
from bound.agent.llm.model.funcall import FunctionCallingLLM
from bound.agent.llm.model.llm import ToolSelection, Model
from bound.agent.llm.handle.template import PromptTemplate
from bound.agent.llm.model.flex import FlexibleModel, create_flexible_model
from bound.agent.adapter.types.base import PydanticProgramMode
from bound.agent.llm.handle.gemini import (
    chat_from_gemini_response,
    chat_message_to_gemini,
    convert_schema_to_function_declaration,
    prepare_chat_params,
    handle_streaming_flexible_model,
    create_retry_decorator,
    adelete_uploaded_files,
    delete_uploaded_files,
)

import google.genai
import google.auth
import google.genai.types as types
from bound.agent.adapter.callback.dispatcher import dispatcher

DEFAULT_MODEL = "gemini-3-flash-preview"

if TYPE_CHECKING:
    from bound.agent.llm.model.types.tool import BaseTool

from arch.model.phase.gate import uuid4 
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class VertexAIConfig(typing.TypedDict, total=False):
    credentials: Optional[google.auth.credentials.Credentials]
    project: Optional[str]
    location: Optional[str]


def llm_retry_decorator(f: Callable[..., Any]) -> Callable[..., Any]:
    if inspect.iscoroutinefunction(f):

        @functools.wraps(f)
        async def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            max_retries = getattr(self, "max_retries", 0)
            if max_retries <= 0:
                return await f(self, *args, **kwargs)
            retry = create_retry_decorator(
                max_retries=max_retries,
                random_exponential=True,
                stop_after_delay_seconds=60,
                min_seconds=1,
                max_seconds=20,
            )
            return await retry(f)(self, *args, **kwargs)
    else:

        @functools.wraps(f)
        def wrapper(self, *args: Any, **kwargs: Any) -> Any:
            max_retries = getattr(self, "max_retries", 0)
            if max_retries <= 0:
                return f(self, *args, **kwargs)
            retry = create_retry_decorator(
                max_retries=max_retries,
                random_exponential=True,
                stop_after_delay_seconds=60,
                min_seconds=1,
                max_seconds=20,
            )
            return retry(f)(self, *args, **kwargs)

    return wrapper


class GoogleGenAI(FunctionCallingLLM):
    model: str = Field(default=DEFAULT_MODEL, description="The Gemini model to use.")
    temperature: float = Field(
        default=DEFAULT_TEMPERATURE,
        description="The temperature to use during generation.",
        ge=0.0,
        le=2.0,
    )
    context_window: Optional[int] = Field(
        default=None,
        description="The context window of the model. If not provided, the default context window 200000 will be used.",
    )
    max_retries: int = Field(
        default=3,
        description="The maximum number of API retries.",
        ge=0,
    )
    is_function_calling_model: bool = Field(
        default=True, description="Whether the model is a function calling model."
    )
    cached_content: Optional[str] = Field(
        default=None,
        description="Cached content to use for the model.",
    )
    built_in_tool: Optional[types.Tool] = Field(
        default=None,
        description="Google GenAI tool to use for the model to augment responses.",
    )
    file_mode: Literal["inline", "fileapi", "hybrid"] = Field(
        default="hybrid",
        description="Whether to use inline-only, FileAPI-only or both for handling files.",
    )

    _max_tokens: int = PrivateAttr()
    _client: google.genai.Client = PrivateAttr()
    _generation_config: types.GenerateContentConfigDict = PrivateAttr()
    _model_meta: types.Model = PrivateAttr()

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        api_key: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        context_window: Optional[int] = None,
        max_retries: int = 3,
        vertexai_config: Optional[VertexAIConfig] = None,
        http_options: Optional[types.HttpOptions] = None,
        debug_config: Optional[google.genai.client.DebugConfig] = None,
        generation_config: Optional[types.GenerateContentConfig] = None,
        callback_manager: Optional[CallbackManager] = None,
        is_function_calling_model: bool = True,
        cached_content: Optional[str] = None,
        built_in_tool: Optional[types.Tool] = None,
        file_mode: Literal["inline", "fileapi", "hybrid"] = "hybrid",
        **kwargs: Any,
    ):
        if temperature is None:
            if "gemini-3" in model:
                temperature = 1.0
            else:
                temperature = DEFAULT_TEMPERATURE
        # API keys are optional. The API can be authorised via OAuth (detected
        # environmentally) or by the GOOGLE_API_KEY environment variable.
        api_key = api_key or os.getenv("GOOGLE_API_KEY", None)
        vertexai = (
            vertexai_config is not None
            or os.getenv("GOOGLE_GENAI_USE_VERTEXAI", "false") != "false"
        )
        project = (vertexai_config or {}).get("project") or os.getenv(
            "GOOGLE_CLOUD_PROJECT", None
        )
        location = (vertexai_config or {}).get("location") or os.getenv(
            "GOOGLE_CLOUD_LOCATION", None
        )

        config_params: Dict[str, Any] = {
            "api_key": api_key,
        }

        if vertexai_config is not None:
            config_params.update(vertexai_config)
            config_params["api_key"] = None
            config_params["vertexai"] = True
        elif vertexai:
            config_params["project"] = project
            config_params["location"] = location
            config_params["api_key"] = None
            config_params["vertexai"] = True

        try:
            package_v = version("llama-index-llms-google-genai")
        except PackageNotFoundError:
            package_v = "0.0.0"
        client_hdr = {"x-goog-api-client": f"llamaindex/{package_v}"}

        if isinstance(http_options, dict):
            http_opts = http_options
        elif isinstance(http_options, types.HttpOptions):
            http_opts = http_options.to_json_dict()
        else:
            http_opts = {}
        http_opts["headers"] = http_opts.get("headers", {}) | client_hdr

        config_params["http_options"] = types.HttpOptions(**http_opts)

        if debug_config:
            config_params["debug_config"] = debug_config

        client = google.genai.Client(**config_params)

        # only get model meta data if max_tokens or context_window is not specified
        if max_tokens is None or context_window is None:
            model_meta = client.models.get(model=model)
        else:
            model_meta = None

        super().__init__(
            model=model,
            temperature=temperature,
            context_window=context_window,
            callback_manager=callback_manager,
            is_function_calling_model=is_function_calling_model,
            max_retries=max_retries,
            cached_content=cached_content,
            built_in_tool=built_in_tool,
            file_mode=file_mode,
            **kwargs,
        )

        self.model = model
        self._client = client
        self._model_meta = model_meta
        # store this as a dict and not as a pydantic model so we can more easily
        # merge it later
        if generation_config:
            self._generation_config = generation_config.model_dump()
            if cached_content:
                self._generation_config.setdefault("cached_content", cached_content)
            if built_in_tool is not None:
                if self._generation_config.get("tools") is None:
                    self._generation_config["tools"] = []
                if isinstance(self._generation_config["tools"], list):
                    if len(self._generation_config["tools"]) > 0:
                        raise ValueError(
                            "Providing multiple Google GenAI tools or mixing with custom tools is not supported."
                        )
                self._generation_config["tools"].append(built_in_tool)
        else:
            config_kwargs = {
                "temperature": temperature,
                "max_output_tokens": max_tokens,
                "cached_content": cached_content,
            }
            if built_in_tool:
                config_kwargs["tools"] = [built_in_tool]

            self._generation_config = types.GenerateContentConfig(
                **config_kwargs
            ).model_dump()
        self._max_tokens = (
            max_tokens or model_meta.output_token_limit or DEFAULT_NUM_OUTPUTS
        )

    @classmethod
    def class_name(cls) -> str:
        return "GenAI"

    @property
    def metadata(self) -> LLMMetadata:
        if self.context_window is None:
            base = self._model_meta.input_token_limit or 200000
            total_tokens = base + self._max_tokens
        else:
            total_tokens = self.context_window

        return LLMMetadata(
            context_window=total_tokens,
            num_output=self._max_tokens,
            model_name=self.model,
            is_chat_model=True,
            is_function_calling_model=self.is_function_calling_model,
        )

    @llm_completion_callback()
    def complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        chat_fn = chat_to_completion_decorator(self._chat)
        return chat_fn(prompt, **kwargs)

    @llm_completion_callback()
    async def acomplete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponse:
        chat_fn = achat_to_completion_decorator(self._achat)
        return await chat_fn(prompt, **kwargs)

    @llm_completion_callback()
    def stream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseGen:
        chat_fn = stream_chat_to_completion_decorator(self._stream_chat)
        return chat_fn(prompt, **kwargs)

    @llm_completion_callback()
    async def astream_complete(
        self, prompt: str, formatted: bool = False, **kwargs: Any
    ) -> CompletionResponseAsyncGen:
        chat_fn = astream_chat_to_completion_decorator(self.astream_chat)
        return await chat_fn(prompt, **kwargs)

    @llm_retry_decorator
    def _chat(self, messages: Sequence[ChatMessage], **kwargs: Any):
        req_id = str(uuid4())[:8]
        log.debug(f"[GenAI-{req_id}] 🚀 _chat START | model={self.model}, messages_count={len(messages)}")

        generation_config = {
            **(self._generation_config or {}),
            **kwargs.pop("generation_config", {}),
        }
        params = {**kwargs, "generation_config": generation_config}
        next_msg, chat_kwargs, file_api_names = asyncio.run(
            prepare_chat_params(
                self.model, messages, self.file_mode, self._client, **params
            )
        )
        chat = self._client.chats.create(**chat_kwargs)
        try:
            response = chat.send_message(
                next_msg.parts if isinstance(next_msg, types.Content) else next_msg
            )
            log.debug(f"[GenAI-{req_id}] ✅ _chat SUCCESS")
        finally:
            if self.file_mode in ("fileapi", "hybrid"):
                delete_uploaded_files(file_api_names, self._client)

        return chat_from_gemini_response(response, [])

    @llm_retry_decorator
    async def _achat(self, messages: Sequence[ChatMessage], **kwargs: Any):
        req_id = str(uuid4())[:8]
        log.debug(f"[GenAI-{req_id}] ⚡ _achat START | model={self.model}, messages_count={len(messages)}")

        generation_config = {
            **(self._generation_config or {}),
            **kwargs.pop("generation_config", {}),
        }
        params = {**kwargs, "generation_config": generation_config}
        next_msg, chat_kwargs, file_api_names = await prepare_chat_params(
            self.model, messages, self.file_mode, self._client, **params
        )
        chat = self._client.aio.chats.create(**chat_kwargs)
        try:
            response = await chat.send_message(
                next_msg.parts if isinstance(next_msg, types.Content) else next_msg
            )
            log.debug(f"[GenAI-{req_id}] ✅ _achat SUCCESS")
        finally:
            if self.file_mode in ("fileapi", "hybrid"):
                await adelete_uploaded_files(file_api_names, self._client)

        return chat_from_gemini_response(response, [])

    @llm_chat_callback()
    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return self._chat(messages, **kwargs)

    @llm_chat_callback()
    async def achat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponse:
        return await self._achat(messages, **kwargs)

    def _stream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseGen:
        req_id = str(uuid4())[:8]
        log.debug(f"[GenAI-{req_id}] 🌊 _stream_chat START | model={self.model}, messages_count={len(messages)}")

        generation_config = {
            **(self._generation_config or {}),
            **kwargs.pop("generation_config", {}),
        }
        params = {**kwargs, "generation_config": generation_config}
        next_msg, chat_kwargs, file_api_names = asyncio.run(
            prepare_chat_params(
                self.model, messages, self.file_mode, self._client, **params
            )
        )
        chat = self._client.chats.create(**chat_kwargs)
        response = chat.send_message_stream(
            next_msg.parts if isinstance(next_msg, types.Content) else next_msg
        )

        def gen() -> ChatResponseGen:
            try:
                content = []
                thought_signatures = []
                for r in response:
                    if candidates := r.candidates:
                        if not candidates:
                            continue

                        top_candidate = candidates[0]
                        if response_content := top_candidate.content:
                            if parts := response_content.parts:
                                # Only use non-thought text parts for the delta
                                content_delta = "".join(
                                    part.text
                                    for part in parts
                                    if part.text and not part.thought
                                )

                                llama_resp = chat_from_gemini_response(
                                    r,
                                    existing_content=content,
                                    thought_signatures=thought_signatures,
                                )
                                llama_resp.delta = (
                                    llama_resp.delta or content_delta or ""
                                )

                                yield llama_resp
            finally:
                if self.file_mode in ("fileapi", "hybrid"):
                    delete_uploaded_files(file_api_names, self._client)

        return gen()

    @llm_chat_callback()
    def stream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseGen:
        return self._stream_chat(messages, **kwargs)

    async def _astream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        req_id = str(uuid4())[:8]
        log.debug(f"[GenAI-{req_id}] 🌊⚡ _astream_chat START | model={self.model}, messages_count={len(messages)}")

        generation_config = {
            **(self._generation_config or {}),
            **kwargs.pop("generation_config", {}),
        }
        params = {**kwargs, "generation_config": generation_config}
        next_msg, chat_kwargs, file_api_names = await prepare_chat_params(
            self.model, messages, self.file_mode, self._client, **params
        )
        chat = self._client.aio.chats.create(**chat_kwargs)

        async def gen() -> ChatResponseAsyncGen:
            try:
                content = []
                thought_signatures = []
                async for r in await chat.send_message_stream(
                    next_msg.parts if isinstance(next_msg, types.Content) else next_msg
                ):
                    if candidates := r.candidates:
                        if not candidates:
                            continue

                        top_candidate = candidates[0]
                        if response_content := top_candidate.content:
                            if parts := response_content.parts:
                                # Only use non-thought text parts for the delta
                                content_delta = "".join(
                                    part.text
                                    for part in parts
                                    if part.text and not part.thought
                                )

                                llama_resp = chat_from_gemini_response(
                                    r,
                                    existing_content=content,
                                    thought_signatures=thought_signatures,
                                )
                                llama_resp.delta = (
                                    llama_resp.delta or content_delta or ""
                                )

                                yield llama_resp
            finally:
                if self.file_mode in ("fileapi", "hybrid"):
                    await adelete_uploaded_files(file_api_names, self._client)

        return gen()

    @llm_chat_callback()
    async def astream_chat(
        self, messages: Sequence[ChatMessage], **kwargs: Any
    ) -> ChatResponseAsyncGen:
        return await self._astream_chat(messages, **kwargs)

    def _prepare_chat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        tool_choice: Optional[Union[str, dict]] = None,
        strict: Optional[bool] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Predict and call the tool."""
        if tool_choice is None:
            tool_choice = "any" if tool_required else "auto"

        if tool_choice == "auto":
            tool_mode = types.FunctionCallingConfigMode.AUTO
        elif tool_choice == "none":
            tool_mode = types.FunctionCallingConfigMode.NONE
        else:
            tool_mode = types.FunctionCallingConfigMode.ANY

        function_calling_config = types.FunctionCallingConfig(mode=tool_mode)

        if tool_choice not in ["auto", "none"]:
            if isinstance(tool_choice, dict):
                raise ValueError("Gemini does not support tool_choice as a dict")

            # assume that the user wants a tool call to be made
            # if the tool choice is not in the list of tools, then we will make a tool call to all tools
            # otherwise, we will make a tool call to the tool choice
            tool_names = [tool.metadata.name for tool in tools if tool.metadata.name]
            if tool_choice not in tool_names:
                function_calling_config.allowed_function_names = tool_names
            else:
                function_calling_config.allowed_function_names = [tool_choice]

        tool_config = types.ToolConfig(
            function_calling_config=function_calling_config,
        )

        tool_declarations = []
        for tool in tools:
            if tool.metadata.fn_schema:
                function_declaration = convert_schema_to_function_declaration(
                    self._client, tool
                )
                tool_declarations.append(function_declaration)

        if isinstance(user_msg, str):
            user_msg = ChatMessage(role=MessageRole.USER, content=user_msg)

        messages = chat_history or []
        if user_msg:
            messages.append(user_msg)

        return {
            "messages": messages,
            "tools": (
                [types.Tool(function_declarations=tool_declarations)]
                if tool_declarations
                else None
            ),
            "tool_config": tool_config,
            **kwargs,
        }

    def get_tool_calls_from_response(
        self,
        response: ChatResponse,
        error_on_no_tool_call: bool = True,
        **kwargs: Any,
    ) -> List[ToolSelection]:
        """Predict and call the tool."""
        tool_calls = [
            block
            for block in response.message.blocks
            if isinstance(block, ToolCallBlock)
        ]

        if len(tool_calls) < 1:
            if error_on_no_tool_call:
                raise ValueError(
                    f"Expected at least one tool call, but got {len(tool_calls)} tool calls."
                )
            else:
                return []

        tool_selections = []
        for tool_call in tool_calls:
            tool_selections.append(
                ToolSelection(
                    tool_id=tool_call.tool_name,
                    tool_name=tool_call.tool_name,
                    tool_kwargs=cast(Dict[str, Any], tool_call.tool_kwargs),
                )
            )

        return tool_selections