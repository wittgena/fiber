# fiber.gateway.llm.inter.base
import asyncio
import logging
from collections import ChainMap
from typing import Any, Dict, List, Optional, Sequence, Union, TYPE_CHECKING

from fiber.llm.router.util import asyncio_run

from xphi.watcher.observer.span import observe
from xphi.watcher.plane.emitter import get_emitter

from fiber.llm.types.inter.response import ChatMessage, ChatResponse, ChatResponseAsyncGen, ChatResponseGen
from fiber.llm.types.inter.block import MessageRole
from fiber.llm.types.inter.llm import LLMBase
from fiber.llm.types.inter.base import (
    BaseOutputParser,
    PydanticProgramMode,
    TokenAsyncGen,
    TokenGen,
)

from fiber.gateway.llm.state.pydantic import (
    Field,
    field_validator,
    model_validator,
)

from fiber.gateway.llm.handler.template import default_messages_to_prompt as generic_messages_to_prompt
from fiber.gateway.llm.handler.template import BasePromptTemplate

from fiber.gateway.llm.context.cbevent import CBEventType, EventPayload
from fiber.llm.router.util import (
    ToolSelection,
    MessagesToPromptType,
    CompletionToPromptType,
    MessagesToPromptCallable,
    CompletionToPromptCallable,
    stream_completion_response_to_tokens,
    stream_chat_response_to_tokens,
    astream_completion_response_to_tokens,
    astream_chat_response_to_tokens,
    default_completion_to_prompt,
    _supports_tool_required,
)
from fiber.llm.types.inter.response import AgentChatResponse

if TYPE_CHECKING:
    from fiber.llm.types.inter.tool import BaseTool

logger = logging.getLogger(__name__)

# [추가됨] 시스템 표준 텔레메트리 로거 할당
log = get_emitter(__name__, phase="LLM_INTER")

class InterLLM(LLMBase):
    """
    Integrates generic generation, templating, and native function calling.
    Automatically falls back to ReAct agent if native function calling is unsupported.
    """
    system_prompt: Optional[str] = Field(default=None, description="System prompt for LLM calls.")
    messages_to_prompt: MessagesToPromptCallable = Field(
        description="Function to convert a list of messages to an LLM prompt.",
        default=None,
        exclude=True
    )
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

    # ------------------------------------------
    # Prompt & Event Utilities
    # ------------------------------------------
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

    def _extend_prompt(self, formatted_prompt: str) -> str:
        """Add system and query wrapper prompts to base prompt."""
        extended_prompt = formatted_prompt
        if self.system_prompt:
            extended_prompt = self.system_prompt + "\n\n" + extended_prompt
        if self.query_wrapper_prompt:
            extended_prompt = self.query_wrapper_prompt.format(query_str=extended_prompt)
        return extended_prompt

    def _extend_messages(self, messages: List[ChatMessage]) -> List[ChatMessage]:
        """Add system prompt to chat message list."""
        if self.system_prompt:
            messages = [
                ChatMessage(role=MessageRole.SYSTEM, content=self.system_prompt),
                *messages,
            ]
        return messages

    # ------------------------------------------
    # Standard Prediction Methods
    # ------------------------------------------
    @observe(name="llm.inter.predict")
    def predict(self, prompt: BasePromptTemplate, **prompt_args: Any) -> str:
        # [수정됨] 비대화 방지 - 거대한 텍스트 대신 메타데이터(Key)만 로깅
        log.info("Starting LLM predict", context={"template_args_keys": list(prompt_args.keys())})
        
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
        
        # [수정됨] 완료 이벤트 명시
        log.debug("LLM predict complete", context={"output_length": len(parsed_output)})
        return parsed_output

    @observe(name="llm.inter.stream")
    def stream(self, prompt: BasePromptTemplate, **prompt_args: Any) -> TokenGen:
        log.info("Starting LLM stream", context={"template_args_keys": list(prompt_args.keys())})
        
        self._log_template_data(prompt, **prompt_args)
        
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
            
        log.debug("LLM stream setup complete")
        return stream_tokens

    @observe(name="llm.inter.apredict")
    async def apredict(self, prompt: BasePromptTemplate, **prompt_args: Any) -> str:
        log.info("Starting async LLM predict", context={"template_args_keys": list(prompt_args.keys())})
        
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
        
        log.debug("Async LLM predict complete", context={"output_length": len(parsed_output)})
        return parsed_output

    @observe(name="llm.inter.astream")
    async def astream(self, prompt: BasePromptTemplate, **prompt_args: Any) -> TokenAsyncGen:
        log.info("Starting async LLM stream", context={"template_args_keys": list(prompt_args.keys())})
        
        self._log_template_data(prompt, **prompt_args)
        
        if self.metadata.is_chat_model:
            messages = self._get_messages(prompt, **prompt_args)
            chat_response = await self.astream_chat(messages)
            stream_tokens = await astream_chat_response_to_tokens(chat_response)
        else:
            formatted_prompt = self._get_prompt(prompt, **prompt_args)
            stream_response = await self.astream_complete(formatted_prompt, formatted=True)
            stream_tokens = await astream_completion_response_to_tokens(stream_response)

        if prompt.output_parser is not None or self.output_parser is not None:
            raise NotImplementedError("Output parser is not supported for streaming.")
            
        log.debug("Async LLM stream setup complete")
        return stream_tokens

    # ------------------------------------------
    # Function Calling Core Utilities
    # ------------------------------------------
    def chat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> ChatResponse:
        chat_kwargs = self._prepare_chat_with_tools_compat(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, tool_required=tool_required, **kwargs
        )
        response = self.chat(**chat_kwargs)
        return self._validate_chat_with_tools_response(
            response, tools, allow_parallel_tool_calls=allow_parallel_tool_calls, **kwargs
        )

    async def achat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> ChatResponse:
        chat_kwargs = self._prepare_chat_with_tools_compat(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, tool_required=tool_required, **kwargs
        )
        response = await self.achat(**chat_kwargs)
        return self._validate_chat_with_tools_response(
            response, tools, allow_parallel_tool_calls=allow_parallel_tool_calls, **kwargs
        )

    def stream_chat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> ChatResponseGen:
        chat_kwargs = self._prepare_chat_with_tools_compat(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, tool_required=tool_required, **kwargs
        )
        return self.stream_chat(**chat_kwargs)

    async def astream_chat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> ChatResponseAsyncGen:
        chat_kwargs = self._prepare_chat_with_tools_compat(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, tool_required=tool_required, **kwargs
        )
        return await self.astream_chat(**chat_kwargs)

    def _prepare_chat_with_tools_compat(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Prepare the arguments needed to let the LLM chat with tools."""
        if not _supports_tool_required(self.__class__, tool_required):
            return self._prepare_chat_with_tools(
                tools=tools, user_msg=user_msg, chat_history=chat_history,
                verbose=verbose, allow_parallel_tool_calls=allow_parallel_tool_calls, **kwargs
            )
        return self._prepare_chat_with_tools(
            tools=tools, user_msg=user_msg, chat_history=chat_history,
            verbose=verbose, allow_parallel_tool_calls=allow_parallel_tool_calls, 
            tool_required=tool_required, **kwargs
        )

    def _prepare_chat_with_tools(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        tool_required: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Subclasses supporting native FC must override this."""
        raise NotImplementedError(
            f"{self.__class__.__name__} claims to support function calling "
            "but has not implemented `_prepare_chat_with_tools`."
        )

    def _validate_chat_with_tools_response(
        self,
        response: ChatResponse,
        tools: Sequence["BaseTool"],
        allow_parallel_tool_calls: bool = False,
        **kwargs: Any,
    ) -> ChatResponse:
        return response

    def get_tool_calls_from_response(
        self,
        response: ChatResponse,
        error_on_no_tool_call: bool = True,
        **kwargs: Any,
    ) -> List[ToolSelection]:
        raise NotImplementedError(
            f"{self.__class__.__name__} claims to support function calling "
            "but has not implemented `get_tool_calls_from_response`."
        )

    # ------------------------------------------
    # Predict & Call (Native FC or ReAct Fallback)
    # ------------------------------------------
    @observe(name="llm.inter.predict_and_call")
    def predict_and_call(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        error_on_no_tool_call: bool = True,
        error_on_tool_error: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        """
        Predict and call the tool.
        Routes to native Function Calling if supported, otherwise falls back to ReAct Agent.
        """
        from fiber.llm.router.chat_engine.types import AgentChatResponse
        from fiber.llm.router.tools.calling import call_tool_with_selection

        # [추가됨] 동작 맥락(보유 툴 개수/이름) 로깅
        tool_names = [getattr(t, "metadata", t).name for t in tools] if tools else []
        log.info("Starting predict_and_call", context={"available_tools": tool_names})

        # Fallback to ReAct Agent if native function calling is not supported
        if not getattr(self.metadata, "is_function_calling_model", False):
            log.debug("Native function calling unsupported, falling back to ReAct.")
            return self._predict_and_call_with_react(
                tools=list(tools), user_msg=user_msg, chat_history=chat_history, verbose=verbose, **kwargs
            )

        # Native Function Calling Execution
        response = self.chat_with_tools(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, **kwargs
        )
        tool_calls = self.get_tool_calls_from_response(
            response, error_on_no_tool_call=error_on_no_tool_call
        )
        
        log.debug("Tool selections parsed", context={"num_calls": len(tool_calls)})
        
        tool_outputs = [
            call_tool_with_selection(tool_call, tools, verbose=verbose)
            for tool_call in tool_calls
        ]
        
        tool_outputs_with_error = [t_out for t_out in tool_outputs if t_out.is_error]
        
        if error_on_tool_error and len(tool_outputs_with_error) > 0:
            error_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            log.error("Tool execution resulted in errors", context={"errors": error_text})
            raise ValueError(error_text)
            
        elif allow_parallel_tool_calls:
            output_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            return AgentChatResponse(response=output_text, sources=tool_outputs)
            
        else:
            if len(tool_outputs) > 1:
                raise ValueError("Multiple tool outputs returned when parallel calls are disabled.")
            elif len(tool_outputs) == 0:
                return AgentChatResponse(
                    response=response.message.content or "", sources=tool_outputs
                )
            return AgentChatResponse(response=tool_outputs[0].content, sources=tool_outputs)

    @observe(name="llm.inter.apredict_and_call")
    async def apredict_and_call(
        self,
        tools: Sequence["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        allow_parallel_tool_calls: bool = False,
        error_on_no_tool_call: bool = True,
        error_on_tool_error: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        """Async predict and call."""
        from fiber.llm.router.chat_engine.types import AgentChatResponse
        from fiber.llm.router.tools.calling import acall_tool_with_selection

        tool_names = [getattr(t, "metadata", t).name for t in tools] if tools else []
        log.info("Starting async apredict_and_call", context={"available_tools": tool_names})

        if not getattr(self.metadata, "is_function_calling_model", False):
            log.debug("Native function calling unsupported, falling back to ReAct.")
            return await self._apredict_and_call_with_react(
                tools=list(tools), user_msg=user_msg, chat_history=chat_history, verbose=verbose, **kwargs
            )

        response = await self.achat_with_tools(
            tools, user_msg=user_msg, chat_history=chat_history, verbose=verbose,
            allow_parallel_tool_calls=allow_parallel_tool_calls, **kwargs
        )

        tool_calls = self.get_tool_calls_from_response(
            response, error_on_no_tool_call=error_on_no_tool_call
        )
        
        log.debug("Async tool selections parsed", context={"num_calls": len(tool_calls)})
        
        tool_tasks = [
            acall_tool_with_selection(tool_call, tools, verbose=verbose)
            for tool_call in tool_calls
        ]
        tool_outputs = await asyncio.gather(*tool_tasks)
        
        tool_outputs_with_error = [t_out for t_out in tool_outputs if t_out.is_error]
        
        if error_on_tool_error and len(tool_outputs_with_error) > 0:
            error_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            log.error("Async tool execution resulted in errors", context={"errors": error_text})
            raise ValueError(error_text)
            
        elif allow_parallel_tool_calls:
            output_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            return AgentChatResponse(response=output_text, sources=tool_outputs)
            
        else:
            if len(tool_outputs) > 1:
                raise ValueError("Multiple tool outputs returned when parallel calls are disabled.")
            elif len(tool_outputs) == 0:
                return AgentChatResponse(
                    response=response.message.content or "", sources=tool_outputs
                )
            return AgentChatResponse(response=tool_outputs[0].content, sources=tool_outputs)

    # ------------------------------------------
    # ReAct Fallback Private Methods
    # ------------------------------------------
    def _predict_and_call_with_react(
        self,
        tools: List["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        from fiber.llm.router.agent.workflow import ReActAgent
        from fiber.llm.router.agent.workflow.agent_context import SimpleAgentContext
        from fiber.llm.router.chat_engine.types import AgentChatResponse
        from fiber.llm.router.memory import Memory
        from fiber.llm.router.tools import adapt_to_async_tool
        from fiber.llm.router.tools.calling import call_tool_with_selection

        agent = ReActAgent(
            tools=tools, llm=self, verbose=verbose,
            formatter=kwargs.get("react_chat_formatter"),
            output_parser=kwargs.get("output_parser"),
            tool_retriever=kwargs.get("tool_retriever"),
        )
        memory = kwargs.get("memory", Memory.from_defaults())

        if isinstance(user_msg, str):
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
                agent.take_step(ctx=ctx, llm_input=llm_input, tools=async_tools, memory=memory)
            )
            tool_outputs = []
            for tool_call in resp.tool_calls:
                tool_output = call_tool_with_selection(
                    tool_call=tool_call, tools=tools or [], verbose=verbose,
                )
                tool_outputs.append(tool_output)
                
            output_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            return AgentChatResponse(response=output_text, sources=tool_outputs)
            
        except Exception as e:
            log.error("ReAct fallback execution failed", context={"error": str(e)})
            return AgentChatResponse(
                response=f"An error occurred while running the tool via ReAct fallback: {str(e)}",
                sources=[],
            )

    async def _apredict_and_call_with_react(
        self,
        tools: List["BaseTool"],
        user_msg: Optional[Union[str, ChatMessage]] = None,
        chat_history: Optional[List[ChatMessage]] = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> "AgentChatResponse":
        from fiber.llm.router.agent.workflow import ReActAgent
        from fiber.llm.router.agent.workflow.agent_context import SimpleAgentContext
        from fiber.llm.router.chat_engine.types import AgentChatResponse
        from fiber.llm.router.memory import Memory
        from fiber.llm.router.tools import adapt_to_async_tool
        from fiber.llm.router.tools.calling import acall_tool_with_selection

        agent = ReActAgent(
            tools=tools, llm=self, verbose=verbose,
            formatter=kwargs.get("react_chat_formatter"),
            output_parser=kwargs.get("output_parser"),
            tool_retriever=kwargs.get("tool_retriever"),
        )
        memory = kwargs.get("memory", Memory.from_defaults())

        if isinstance(user_msg, str):
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
                    tool_call=tool_call, tools=tools or [], verbose=verbose,
                )
                tool_outputs.append(tool_output)

            output_text = "\n\n".join([t_out.content for t_out in tool_outputs])
            return AgentChatResponse(response=output_text, sources=tool_outputs)
            
        except Exception as e:
            log.error("Async ReAct fallback execution failed", context={"error": str(e)})
            return AgentChatResponse(
                response=f"An error occurred while running the tool via ReAct fallback: {str(e)}",
                sources=[],
            )