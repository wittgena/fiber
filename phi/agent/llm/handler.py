# phi.agent.llm.handler
## @lineage: phi.executor.graph.loop
import json
from atoa.event.llm.action import ActionEvent
from atoa.event.llm.message import MessageEvent
from atoa.exception.types import FunctionCallValidationError, LLMContextWindowExceedError, LLMMalformedConversationHistoryError

from phi.agent.loop.step import StepHandler
from swarm.mesh.conv.parser.builder import MessageBuilder, LLMFacade
from atoa.conv.message import Message, TextContent

from watcher.plane.emitter import get_emitter
logger = get_emitter(__name__)

class FinishSignal:
    is_finish_signal = True

class LLMInvocationHandler(StepHandler):
    async def handle_async(self, activator, snapshot, on_event, context) -> bool:
        logger.debug(f"[LLMInvocationHandler]")
        
        _messages = MessageBuilder.prepare_llm_messages(snapshot.events, llm=activator.llm)
        logger.debug(f"[LLMInvocationHandler] Sending {len(_messages)} messages to LLM")

        try:
            llm_response = LLMFacade.make_completion(
                llm=activator.llm,
                messages=_messages,
                tools=list(activator.tools_map.values()),
                on_token=None, 
            )
            context.llm_response = llm_response 
            return False
            
        except FunctionCallValidationError as e:
            logger.warning(f"LLM generated malformed function call: {e}")
            error_message = MessageEvent(
                source="environment",
                llm_message=Message(role="user", content=[TextContent(text=str(e))]),
            )
            await on_event(error_message)
            return True
        except LLMMalformedConversationHistoryError as e:
            raise e
        except LLMContextWindowExceedError as e:
            activator._log_context_window_exceeded_warning()
            raise e

class ToolCallHandler(StepHandler):
    async def handle_async(self, activator, snapshot, on_event, context) -> bool:
        logger.debug(f"[ToolCallHandler]")
        llm_response = context.llm_response
        message: Message = llm_response.message
        
        if not message.tool_calls or len(message.tool_calls) == 0:
            return False

        thought_content = [c for c in message.content if isinstance(c, TextContent)]
        action_events: list[ActionEvent] = []
        
        for i, tool_call in enumerate(message.tool_calls):
            action_event, error_event = activator._get_action_event(
                tool_call=tool_call,
                llm_response_id=llm_response.id,
                snapshot=snapshot,
                security_analyzer=None,
                thought=thought_content if i == 0 else [],
                reasoning_content=message.reasoning_content if i == 0 else None,
                thinking_blocks=list(message.thinking_blocks) if i == 0 else [],
                responses_reasoning_item=message.responses_reasoning_item if i == 0 else None,
            )
            
            if error_event:
                await on_event(error_event)
            if action_event:
                action_events.append(action_event)

        for action in action_events:
            await on_event(action)

        token_event = activator._maybe_emit_vllm_tokens(llm_response)
        if token_event:
            await on_event(token_event)
            
        return True

class TextResponseHandler(StepHandler):
    async def handle_async(self, activator, snapshot, on_event, context) -> bool:
        logger.debug(f"[TextResponseHandler]")
        llm_response = context.llm_response
        message: Message = llm_response.message
        
        has_reasoning = (
            message.responses_reasoning_item is not None
            or message.reasoning_content is not None
            or (message.thinking_blocks and len(message.thinking_blocks) > 0)
        )
        has_content = any(isinstance(c, TextContent) and c.text.strip() for c in message.content)

        msg_event = MessageEvent(
            source="activator",
            llm_message=message,
            llm_response_id=llm_response.id,
        )

        if activator.evaluator is not None and getattr(activator, "reflector", None) and activator.reflector.mode == "finish_and_message":
            reflector_result = activator.evaluator.evaluate(snapshot, msg_event)
            if reflector_result is not None:
                msg_event = msg_event.model_copy(update={"reflector_result": reflector_result})
                
        await on_event(msg_event)
        
        token_event = activator._maybe_emit_vllm_tokens(llm_response)
        if token_event:
            await on_event(token_event)

        if has_content:
            await on_event(FinishSignal()) 
            return True

        if not has_content and not has_reasoning:
            nudge = MessageEvent(
                source="user",
                llm_message=Message(
                    role="user",
                    content=[TextContent(text="Your last response did not include a function call or a message.")]
                ),
            )
            await on_event(nudge)
        return True