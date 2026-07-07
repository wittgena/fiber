# bound.surface.hf.llm.api
## @lineage: anchor.surface.hf.llm.api
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Sequence, Union
from bound.adapter.llama.base.llms.generic_utils import (
    completion_response_to_chat_response,
    stream_completion_response_to_chat_response,
    astream_completion_response_to_chat_response,
    chat_response_to_completion_response,
    stream_chat_response_to_completion_response,
    astream_chat_response_to_completion_response,
)
from bound.adapter.llama.base.llms.types import (
    ChatMessage, ChatResponse, ChatResponseAsyncGen, ChatResponseGen,
    CompletionResponse, CompletionResponseAsyncGen, CompletionResponseGen,
    LLMMetadata, MessageRole,
)
from bound.adapter.llama.bridge.pydantic import Field, PrivateAttr
from bound.adapter.llama.constants import DEFAULT_CONTEXT_WINDOW, DEFAULT_NUM_OUTPUTS
from bound.adapter.llama.tools.types import BaseTool
from bound.bridge.adapter.hf import HFInferenceBridge
from xphi.loop.flow.llm.llm import ToolSelection
from xphi.loop.flow.llm.function_calling import FunctionCallingLLM

logger = logging.getLogger(__name__)

class HuggingFaceInferenceAPI(FunctionCallingLLM):
    @classmethod
    def class_name(cls) -> str:
        return "HuggingFaceInferenceAPI"

    model: Optional[str] = Field(default=None)
    model_name: Optional[str] = Field(default=None)
    provider: str = Field(default="auto")
    token: Union[str, bool, None] = Field(default=None)
    timeout: Optional[float] = Field(default=None)
    headers: Dict[str, str] = Field(default=None)
    cookies: Dict[str, str] = Field(default=None)
    task: Optional[str] = Field(default=None)

    # 💡 모든 HF 클라이언트 속성을 Bridge 단일 속성으로 통합
    _bridge: Any = PrivateAttr()

    context_window: int = Field(default=DEFAULT_CONTEXT_WINDOW)
    num_output: int = Field(default=DEFAULT_NUM_OUTPUTS)
    temperature: float = Field(default=0.1)
    is_chat_model: bool = Field(default=True)
    is_function_calling_model: bool = Field(default=False)

    def __init__(self, **kwargs: Any) -> None:
        model_name = kwargs.get("model_name") or kwargs.get("model")
        if model_name is None:
            task = kwargs.get("task", "")
            kwargs["model_name"] = HFInferenceBridge.get_recommended_model(task=task)
            logger.debug(
                f"Using Hugging Face's recommended model {kwargs['model_name']} given task {task}."
            )

        if kwargs.get("task") is None:
            kwargs["task"] = "conversational"
        else:
            kwargs["task"] = kwargs["task"].lower()

        if kwargs.get("is_function_calling_model", False):
            print("Function calling is currently not supported for Hugging Face Inference API, setting to False")
            kwargs["is_function_calling_model"] = False

        super().__init__(**kwargs)

        # 💡 런타임에 Bridge 초기화
        self._bridge = HFInferenceBridge(**self._get_inference_client_kwargs())

        try:
            info = self._bridge.get_endpoint_info()
            if "max_input_tokens" in info and kwargs.get("context_window") is None:
                self.context_window = info["max_input_tokens"]
        except Exception:
            pass

    def _get_inference_client_kwargs(self) -> Dict[str, Any]:
        return {
            "model": self.model_name or self.model,
            "provider": self.provider,
            "token": self.token,
            "timeout": self.timeout,
            "headers": self.headers,
            "cookies": self.cookies,
        }

    def _get_model_kwargs(self, **kwargs: Any) -> Dict[str, Any]:
        base_kwargs = {
            "model": self.model_name or self.model,
            "max_tokens": self.num_output,
            "temperature": self.temperature,
        }
        return {**base_kwargs, **kwargs}

    def _to_huggingface_messages(self, messages: Sequence[ChatMessage]) -> List[Dict[str, Any]]:
        hf_dicts = []
        for m in messages:
            hf_dicts.append({"role": m.role.value, "content": m.content if m.content else ""})
            if m.additional_kwargs.get("tool_calls", []):
                tool_call_dicts = []
                for tool_call in m.additional_kwargs["tool_calls"]:
                    function_dict = {
                        "name": tool_call.id,
                        "arguments": tool_call.function.arguments,
                    }
                    tool_call_dicts.append({"type": "function", "function": function_dict})
                hf_dicts[-1]["tool_calls"] = tool_call_dicts

            if m.role == MessageRole.TOOL:
                hf_dicts[-1]["name"] = m.additional_kwargs.get("tool_call_id")
        return hf_dicts

    def _parse_streaming_tool_calls(self, tool_call_strs: List[str]) -> List[Union[ToolSelection, str]]:
        tool_calls = []
        for tool_call_str in tool_call_strs:
            try:
                tool_call_dict = json.loads(tool_call_str)
                args = tool_call_dict["function"]
                name = args.pop("_name")
                # 💡 ToolCall 객체 생성을 Bridge에 위임
                tool_calls.append(self._bridge.create_tool_call_object(name, args))
            except Exception:
                tool_calls.append(tool_call_str)
        return tool_calls

    def get_model_info(self, **kwargs: Any) -> Any:
        return self._bridge.get_model_info(self.model_name or self.model, **kwargs)

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(
            context_window=self.context_window,
            num_output=self.num_output,
            is_chat_model=self.is_chat_model,
            is_function_calling_model=self.is_function_calling_model,
            model_name=self.model_name or self.model,
        )

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        if self.task == "conversational" or self.task is None:
            model_kwargs = self._get_model_kwargs(**kwargs)
            # 💡 Bridge를 통한 호출 (반환 타입 Any로 오리 타이핑)
            output = self._bridge.chat_completion(
                messages=self._to_huggingface_messages(messages),
                **model_kwargs,
            )
            content = output.choices[0].message.content or ""
            tool_calls = output.choices[0].message.tool_calls or []
            additional_kwargs = {"tool_calls": tool_calls} if tool_calls else {}

            return ChatResponse(
                message=ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content=content,
                    additional_kwargs=additional_kwargs,
                ),
                raw=output,
            )
        else:
            prompt = self.messages_to_prompt(messages)
            completion = self.complete(prompt, formatted=True, **kwargs)
            return completion_response_to_chat_response(completion)

    def complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponse:
        if self.task == "conversational":
            chat_resp = self.chat(messages=[ChatMessage(role=MessageRole.USER, content=prompt)], **kwargs)
            return chat_response_to_completion_response(chat_resp)

        model_kwargs = self._get_model_kwargs(**kwargs)
        model_kwargs["max_new_tokens"] = model_kwargs.pop("max_tokens", None)

        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return CompletionResponse(
            text=self._bridge.text_generation(prompt, **model_kwargs)
        )

    def stream_chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponseGen:
        if self.task == "conversational" or self.task is None:
            model_kwargs = self._get_model_kwargs(**kwargs)

            def gen() -> ChatResponseGen:
                response = ""
                tool_call_strs = []
                cur_index = -1
                for chunk in self._bridge.chat_completion(
                    messages=self._to_huggingface_messages(messages),
                    stream=True,
                    **model_kwargs,
                ):
                    delta = chunk.choices[0].delta.content or ""
                    response += delta
                    tool_call_delta = chunk.choices[0].delta.tool_calls
                    if tool_call_delta:
                        if tool_call_delta.index != cur_index:
                            cur_index = tool_call_delta.index
                            tool_call_strs.append(tool_call_delta.function.arguments)
                        else:
                            tool_call_strs[cur_index] += tool_call_delta.function.arguments

                    tool_calls = self._parse_streaming_tool_calls(tool_call_strs)
                    additional_kwargs = {"tool_calls": tool_calls} if tool_calls else {}
                    yield ChatResponse(
                        message=ChatMessage(
                            role=MessageRole.ASSISTANT,
                            content=response,
                            additional_kwargs=additional_kwargs,
                        ),
                        delta=delta,
                        raw=chunk,
                    )
            return gen()
        else:
            prompt = self.messages_to_prompt(messages)
            completion_stream = self.stream_complete(prompt, formatted=True, **kwargs)
            return stream_completion_response_to_chat_response(completion_stream)

    def stream_complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponseGen:
        if self.task == "conversational":
            chat_gen = self.stream_chat(messages=[ChatMessage(role=MessageRole.USER, content=prompt)], **kwargs)
            return stream_chat_response_to_completion_response(chat_gen)

        model_kwargs = self._get_model_kwargs(**kwargs)
        model_kwargs["max_new_tokens"] = model_kwargs.pop("max_tokens", None)

        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        def gen() -> CompletionResponseGen:
            response = ""
            for delta in self._bridge.text_generation(prompt, stream=True, **model_kwargs):
                response += delta
                yield CompletionResponse(text=response, delta=delta)

        return gen()

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        if self.task == "conversational" or self.task is None:
            model_kwargs = self._get_model_kwargs(**kwargs)
            output = await self._bridge.achat_completion(
                messages=self._to_huggingface_messages(messages),
                **model_kwargs,
            )
            content = output.choices[0].message.content or ""
            tool_calls = output.choices[0].message.tool_calls or []
            additional_kwargs = {"tool_calls": tool_calls} if tool_calls else {}

            return ChatResponse(
                message=ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content=content,
                    additional_kwargs=additional_kwargs,
                ),
                raw=output,
            )
        else:
            prompt = self.messages_to_prompt(messages)
            completion = await self.acomplete(prompt, formatted=True, **kwargs)
            return completion_response_to_chat_response(completion)

    async def acomplete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponse:
        if self.task == "conversational":
            chat_resp = await self.achat(messages=[ChatMessage(role=MessageRole.USER, content=prompt)], **kwargs)
            return chat_response_to_completion_response(chat_resp)

        model_kwargs = self._get_model_kwargs(**kwargs)
        model_kwargs["max_new_tokens"] = model_kwargs.pop("max_tokens", None)

        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        return CompletionResponse(
            text=await self._bridge.atext_generation(prompt, **model_kwargs)
        )

    async def astream_chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponseAsyncGen:
        if self.task == "conversational" or self.task is None:
            model_kwargs = self._get_model_kwargs(**kwargs)

            async def gen() -> ChatResponseAsyncGen:
                response = ""
                tool_call_strs = []
                cur_index = -1
                async for chunk in await self._bridge.achat_completion(
                    messages=self._to_huggingface_messages(messages),
                    stream=True,
                    **model_kwargs,
                ):
                    if chunk.choices[0].finish_reason is not None:
                        break

                    delta = chunk.choices[0].delta.content or ""
                    response += delta
                    tool_call_delta = chunk.choices[0].delta.tool_calls
                    if tool_call_delta:
                        if tool_call_delta.index != cur_index:
                            cur_index = tool_call_delta.index
                            tool_call_strs.append(tool_call_delta.function.arguments)
                        else:
                            tool_call_strs[cur_index] += tool_call_delta.function.arguments

                    tool_calls = self._parse_streaming_tool_calls(tool_call_strs)
                    additional_kwargs = {"tool_calls": tool_calls} if tool_calls else {}

                    yield ChatResponse(
                        message=ChatMessage(
                            role=MessageRole.ASSISTANT,
                            content=response,
                            additional_kwargs=additional_kwargs,
                        ),
                        delta=delta,
                        raw=chunk,
                    )
                await self._bridge.close_async()
            return gen()
        else:
            prompt = self.messages_to_prompt(messages)
            completion_stream = await self.astream_complete(prompt, formatted=True, **kwargs)
            return astream_completion_response_to_chat_response(completion_stream)

    async def astream_complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponseAsyncGen:
        if self.task == "conversational":
            chat_gen = await self.astream_chat(messages=[ChatMessage(role=MessageRole.USER, content=prompt)], **kwargs)
            return astream_chat_response_to_completion_response(chat_gen)

        model_kwargs = self._get_model_kwargs(**kwargs)
        model_kwargs["max_new_tokens"] = model_kwargs.pop("max_tokens", None)

        if not formatted:
            prompt = self.completion_to_prompt(prompt)

        async def gen() -> CompletionResponseAsyncGen:
            response = ""
            async for delta in await self._bridge.atext_generation(prompt, stream=True, **model_kwargs):
                response += delta
                yield CompletionResponse(text=response, delta=delta)
            await self._bridge.close_async()
        return gen()

    def _prepare_chat_with_tools(self, tools: List["BaseTool"], user_msg: Optional[Union[str, ChatMessage]] = None, chat_history: Optional[List[ChatMessage]] = None, verbose: bool = False, allow_parallel_tool_calls: bool = False, tool_required: bool = False, **kwargs: Any) -> Dict[str, Any]:
        tool_specs = [tool.metadata.to_openai_tool(skip_length_check=True) for tool in tools]
        if isinstance(user_msg, str):
            user_msg = ChatMessage(role=MessageRole.USER, content=user_msg)
        messages = chat_history or []
        if user_msg:
            messages.append(user_msg)
        return {
            "messages": messages,
            "tools": tool_specs or None,
            "tool_choice": "required" if tool_required else "auto",
        }

    def _validate_chat_with_tools_response(self, response: ChatResponse, tools: List["BaseTool"], allow_parallel_tool_calls: bool = False, **kwargs: Any) -> ChatResponse:
        if not allow_parallel_tool_calls and response.message.additional_kwargs.get("tool_calls", []):
            response.additional_kwargs["tool_calls"] = response.message.additional_kwargs["tool_calls"][0]
        return response

    def get_tool_calls_from_response(self, response: "ChatResponse", error_on_no_tool_call: bool = True) -> List[ToolSelection]:
        tool_calls = response.message.additional_kwargs.get("tool_calls", [])
        if len(tool_calls) < 1:
            if error_on_no_tool_call:
                raise ValueError(f"Expected at least one tool call, but got {len(tool_calls)} tool calls.")
            else:
                return []
        tool_selections = []
        for tool_call in tool_calls:
            if isinstance(tool_call, str):
                continue
            tool_selections.append(
                ToolSelection(
                    tool_id=tool_call.id,
                    tool_name=tool_call.function.name,
                    tool_kwargs=tool_call.function.arguments,
                )
            )
        return tool_selections