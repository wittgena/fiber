# router.llm.model.llm
## @lineage: engine.router.llm.model.llm
## @lineage: engine.eco.llm.model.llm
## @lineage: runtime.engine.eco.llm.model.llm
## @lineage: eco.llms.model.llm
from collections import ChainMap
from typing import (
    Any,
    Dict,
    List,
    Generator,
    AsyncGenerator,
    Optional,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
    Type,
)
from typing_extensions import Annotated

from ext.router.adapter.parser import asyncio_run
from ext.router.llm.model.types.block import (
    ChatMessage,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    MessageRole,
)
from ext.router.adapter.pydantic import (
    BaseModel,
    WithJsonSchema,
    Field,
    field_validator,
    model_validator,
    ValidationError,
)
from ext.router.adapter.callback.event import CBEventType, EventPayload
from ext.router.llm.model.base import BaseLLM
from ext.router.llm.handle.template import default_messages_to_prompt as generic_messages_to_prompt
from ext.router.llm.handle.template import BasePromptTemplate, PromptTemplate
from ext.router.adapter.types.base import (
    BaseOutputParser,
    PydanticProgramMode,
    TokenAsyncGen,
    TokenGen,
    Model,
)
from ext.router.adapter.callback.event import (
    LLMPredictEndEvent,
    LLMPredictStartEvent,
    LLMStructuredPredictInProgressEvent,
    LLMStructuredPredictEndEvent,
    LLMStructuredPredictStartEvent,
)
from ext.router.adapter.callback.dispatcher import dispatcher
from ext.router.llm.model.types.block import ChatMessage

if TYPE_CHECKING:
    from ext.router.adapter.chat_engine.types import AgentChatResponse
    from ext.router.llm.model.flex import FlexibleModel
    from ext.router.llm.model.types.tool import BaseTool

class ToolSelection(BaseModel):
    tool_id: str = Field(description="Tool ID to select.")
    tool_name: str = Field(description="Tool name to select.")
    tool_kwargs: Dict[str, Any] = Field(description="Keyword arguments for the tool.")

    @field_validator("tool_kwargs", mode="wrap")
    @classmethod
    def ignore_non_dict_arguments(cls, v: Any, handler: Any) -> Dict[str, Any]:
        try:
            return handler(v)
        except ValidationError:
            return handler({})

@runtime_checkable
class MessagesToPromptType(Protocol):
    def __call__(self, messages: Sequence[ChatMessage]) -> str:
        pass

@runtime_checkable
class CompletionToPromptType(Protocol):
    def __call__(self, prompt: str) -> str:
        pass

def stream_completion_response_to_tokens(
    completion_response_gen: CompletionResponseGen,
) -> TokenGen:
    def gen() -> TokenGen:
        for response in completion_response_gen:
            yield response.delta or ""

    return gen()


def stream_chat_response_to_tokens(
    chat_response_gen: ChatResponseGen,
) -> TokenGen:
    def gen() -> TokenGen:
        for response in chat_response_gen:
            yield response.delta or ""
    return gen()

async def astream_completion_response_to_tokens(
    completion_response_gen: CompletionResponseAsyncGen,
) -> TokenAsyncGen:
    async def gen() -> TokenAsyncGen:
        async for response in completion_response_gen:
            yield response.delta or ""

    return gen()

async def astream_chat_response_to_tokens(
    chat_response_gen: ChatResponseAsyncGen,
) -> TokenAsyncGen:
    async def gen() -> TokenAsyncGen:
        async for response in chat_response_gen:
            yield response.delta or ""

    return gen()


def default_completion_to_prompt(prompt: str) -> str:
    return prompt


MessagesToPromptCallable = Annotated[
    Optional[MessagesToPromptType],
    WithJsonSchema({"type": "string"}),
]

CompletionToPromptCallable = Annotated[
    Optional[CompletionToPromptType],
    WithJsonSchema({"type": "string"}),
]

class LLM(BaseLLM):
    system_prompt: Optional[str] = Field(default=None, description="System prompt for LLM calls.")
    messages_to_prompt: MessagesToPromptCallable = Field(description="Function to convert a list of messages to an LLM prompt.",default=None,exclude=True,)
    completion_to_prompt: CompletionToPromptCallable = Field(
        description="Function to convert a completion to an LLM prompt.",
        default=None,
        exclude=True,
    )
    output_parser: Optional[BaseOutputParser] = Field(
        description="Output parser to parse, validate, and correct errors programmatically.",
        default=None,
        exclude=True,
    )
    pydantic_program_mode: PydanticProgramMode = PydanticProgramMode.DEFAULT

    # deprecated
    query_wrapper_prompt: Optional[BasePromptTemplate] = Field(
        description="Query wrapper prompt for LLM calls.",
        default=None,
        exclude=True,
    )

    @field_validator("messages_to_prompt")
    @classmethod
    def set_messages_to_prompt(
        cls, messages_to_prompt: Optional[MessagesToPromptType]
    ) -> MessagesToPromptType:
        return messages_to_prompt or generic_messages_to_prompt

    @field_validator("completion_to_prompt")
    @classmethod
    def set_completion_to_prompt(
        cls, completion_to_prompt: Optional[CompletionToPromptType]
    ) -> CompletionToPromptType:
        return completion_to_prompt or default_completion_to_prompt

    @model_validator(mode="after")
    def check_prompts(self) -> "LLM":
        if self.completion_to_prompt is None:
            self.completion_to_prompt = default_completion_to_prompt
        if self.messages_to_prompt is None:
            self.messages_to_prompt = generic_messages_to_prompt
        return self

    def _log_template_data(
        self, prompt: BasePromptTemplate, **prompt_args: Any
    ) -> None:
        template_vars = {
            k: v
            for k, v in ChainMap(prompt.kwargs, prompt_args).items()
            if k in prompt.template_vars
        }
        with self.callback_manager.event(
            CBEventType.TEMPLATING,
            payload={
                EventPayload.TEMPLATE: prompt.get_template(llm=self),
                EventPayload.TEMPLATE_VARS: template_vars,
                EventPayload.SYSTEM_PROMPT: self.system_prompt,
                EventPayload.QUERY_WRAPPER_PROMPT: self.query_wrapper_prompt,
            },
        ):
            pass

    def _get_prompt(self, prompt: BasePromptTemplate, **prompt_args: Any) -> str:
        formatted_prompt = prompt.format(
            llm=self,
            messages_to_prompt=self.messages_to_prompt,
            completion_to_prompt=self.completion_to_prompt,
            **prompt_args,
        )
        if self.output_parser is not None:
            formatted_prompt = self.output_parser.format(formatted_prompt)
        return self._extend_prompt(formatted_prompt)

    def _get_messages(
        self, prompt: BasePromptTemplate, **prompt_args: Any
    ) -> List[ChatMessage]:
        messages = prompt.format_messages(llm=self, **prompt_args)
        if self.output_parser is not None:
            messages = self.output_parser.format_messages(messages)
        return self._extend_messages(messages)

    def _parse_output(self, output: str) -> str:
        if self.output_parser is not None:
            return str(self.output_parser.parse(output))

        return output

    def _extend_prompt(
        self,
        formatted_prompt: str,
    ) -> str:
        """Add system and query wrapper prompts to base prompt."""
        extended_prompt = formatted_prompt

        if self.system_prompt:
            extended_prompt = self.system_prompt + "\n\n" + extended_prompt

        if self.query_wrapper_prompt:
            extended_prompt = self.query_wrapper_prompt.format(
                query_str=extended_prompt
            )

        return extended_prompt

    def _extend_messages(self, messages: List[ChatMessage]) -> List[ChatMessage]:
        """Add system prompt to chat message list."""
        if self.system_prompt:
            messages = [
                ChatMessage(role=MessageRole.SYSTEM, content=self.system_prompt),
                *messages,
            ]
        return messages

    @dispatcher.span
    def predict(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> str:
        dispatcher.event(
            LLMPredictStartEvent(template=prompt, template_args=prompt_args)
        )
        self._log_template_data(prompt, **prompt_args)

        if self.metadata.is_chat_model:
            messages = self._get_messages(prompt, **prompt_args)
            chat_response = self.chat(messages)
            output = chat_response.message.content or ""
        else:
            formatted_prompt = self._get_prompt(prompt, **prompt_args)
            response = self.complete(formatted_prompt, formatted=True)
            output = response.text
        parsed_output = self._parse_output(output)
        dispatcher.event(LLMPredictEndEvent(output=parsed_output))
        return parsed_output

    @dispatcher.span
    def stream(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> TokenGen:
        self._log_template_data(prompt, **prompt_args)

        dispatcher.event(
            LLMPredictStartEvent(template=prompt, template_args=prompt_args)
        )
        if self.metadata.is_chat_model:
            messages = self._get_messages(prompt, **prompt_args)
            chat_response = self.stream_chat(messages)
            stream_tokens = stream_chat_response_to_tokens(chat_response)
        else:
            formatted_prompt = self._get_prompt(prompt, **prompt_args)
            stream_response = self.stream_complete(formatted_prompt, formatted=True)
            stream_tokens = stream_completion_response_to_tokens(stream_response)

        if prompt.output_parser is not None or self.output_parser is not None:
            raise NotImplementedError("Output parser is not supported for streaming.")

        return stream_tokens

    @dispatcher.span
    async def apredict(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> str:
        dispatcher.event(
            LLMPredictStartEvent(template=prompt, template_args=prompt_args)
        )
        self._log_template_data(prompt, **prompt_args)

        if self.metadata.is_chat_model:
            messages = self._get_messages(prompt, **prompt_args)
            chat_response = await self.achat(messages)
            output = chat_response.message.content or ""
        else:
            formatted_prompt = self._get_prompt(prompt, **prompt_args)
            response = await self.acomplete(formatted_prompt, formatted=True)
            output = response.text

        parsed_output = self._parse_output(output)
        dispatcher.event(LLMPredictEndEvent(output=parsed_output))
        return parsed_output

    @dispatcher.span
    async def astream(
        self,
        prompt: BasePromptTemplate,
        **prompt_args: Any,
    ) -> TokenAsyncGen:
        self._log_template_data(prompt, **prompt_args)
        dispatcher.event(
            LLMPredictStartEvent(template=prompt, template_args=prompt_args)
        )
        if self.metadata.is_chat_model:
            messages = self._get_messages(prompt, **prompt_args)
            chat_response = await self.astream_chat(messages)
            stream_tokens = await astream_chat_response_to_tokens(chat_response)
        else:
            formatted_prompt = self._get_prompt(prompt, **prompt_args)
            stream_response = await self.astream_complete(
                formatted_prompt, formatted=True
            )
            stream_tokens = await astream_completion_response_to_tokens(stream_response)

        if prompt.output_parser is not None or self.output_parser is not None:
            raise NotImplementedError("Output parser is not supported for streaming.")

        return stream_tokens

    @dispatcher.span
    def predict_and_call(
        self,
        tools: List["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        from ext.router.adapter.agent.workflow import ReActAgent
        from ext.router.adapter.agent.workflow.agent_context import SimpleAgentContext
        from ext.router.adapter.chat_engine.types import AgentChatResponse
        from ext.router.adapter.memory import Memory
        from ext.router.adapter.tools import adapt_to_async_tool
        from ext.router.adapter.tools.calling import call_tool_with_selection

        agent = ReActAgent(
            tools=tools,
            llm=self,
            verbose=verbose,
            formatter=kwargs.get("react_chat_formatter"),
            output_parser=kwargs.get("output_parser"),
            tool_retriever=kwargs.get("tool_retriever"),
        )

        memory = kwargs.get("memory", Memory.from_defaults())

        if isinstance(user_msg, ChatMessage) and isinstance(user_msg.content, str):
            pass
        elif isinstance(user_msg, str):
            user_msg = ChatMessage(content=user_msg, role=MessageRole.USER)

        llm_input = []
        if chat_history:
            llm_input.extend(chat_history)
        if user_msg:
            llm_input.append(user_msg)

        ctx = SimpleAgentContext()
        async_tools = [adapt_to_async_tool(t) for t in (tools or [])]

        try:
            resp = asyncio_run(
                agent.take_step(
                    ctx=ctx, llm_input=llm_input, tools=async_tools, memory=memory
                )
            )
            tool_outputs = []
            for tool_call in resp.tool_calls:
                tool_output = call_tool_with_selection(
                    tool_call=tool_call,
                    tools=tools or [],
                    verbose=verbose,
                )
                tool_outputs.append(tool_output)
            output_text = "\n\n".join(
                [tool_output.content for tool_output in tool_outputs]
            )
            return AgentChatResponse(
                response=output_text,
                sources=tool_outputs,
            )
        except Exception as e:
            output = AgentChatResponse(
                response="An error occurred while running the tool: " + str(e),
                sources=[],
            )

        return output

    @dispatcher.span
    async def apredict_and_call(
        self,
        tools: List["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        """Predict and call the tool."""
        from ext.router.adapter.agent.workflow import ReActAgent
        from ext.router.adapter.agent.workflow.agent_context import SimpleAgentContext
        from ext.router.adapter.chat_engine.types import AgentChatResponse
        from ext.router.adapter.memory import Memory
        from ext.router.adapter.tools import adapt_to_async_tool
        from ext.router.adapter.tools.calling import acall_tool_with_selection

        agent = ReActAgent(
            tools=tools,
            llm=self,
            verbose=verbose,
            formatter=kwargs.get("react_chat_formatter"),
            output_parser=kwargs.get("output_parser"),
            tool_retriever=kwargs.get("tool_retriever"),
        )

        memory = kwargs.get("memory", Memory.from_defaults())

        if isinstance(user_msg, ChatMessage) and isinstance(user_msg.content, str):
            pass
        elif isinstance(user_msg, str):
            user_msg = ChatMessage(content=user_msg, role=MessageRole.USER)

        llm_input = []
        if chat_history:
            llm_input.extend(chat_history)
        if user_msg:
            llm_input.append(user_msg)

        ctx = SimpleAgentContext()
        async_tools = [adapt_to_async_tool(t) for t in (tools or [])]

        try:
            resp = await agent.take_step(
                ctx=ctx, llm_input=llm_input, tools=async_tools, memory=memory
            )
            tool_outputs = []
            for tool_call in resp.tool_calls:
                tool_output = await acall_tool_with_selection(
                    tool_call=tool_call,
                    tools=tools or [],
                    verbose=verbose,
                )
                tool_outputs.append(tool_output)

            output_text = "\n\n".join(
                [tool_output.content for tool_output in tool_outputs]
            )
            return AgentChatResponse(
                response=output_text,
                sources=tool_outputs,
            )
        except Exception as e:
            output = AgentChatResponse(
                response="An error occurred while running the tool: " + str(e),
                sources=[],
            )

        return output