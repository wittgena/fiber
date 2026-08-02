# phi.driver.llm.handler
## @lineage: phi.loop.llm.handler
import json
from agent.atoa.event.llm.action import ActionEvent
from agent.atoa.event.llm.message import MessageEvent
from mesh.bound.exception.types import FunctionCallValidationError, LLMContextWindowExceedError, LLMMalformedConversationHistoryError

from phi.loop.step import StepHandler
from phi.driver.llm.facade import MessageBuilder, LLMFacade
from agent.atoa.conv.message import Message, TextContent

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
            llm_response = await LLMFacade.make_completion(
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

        # [안전 보강] message.content가 None일 경우 빈 리스트로 처리하여 순회 에러(TypeError) 방지
        safe_content = message.content if isinstance(message.content, list) else []
        thought_content = [c for c in safe_content if isinstance(c, TextContent)]
        
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

        # 레거시 이벤트 순서 보장을 위해 기존 위치 유지
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
        
        # [안전 보강] Iterable 보장
        safe_content = message.content if isinstance(message.content, list) else []
        has_content = any(isinstance(c, TextContent) and c.text.strip() for c in safe_content)

        # 레거시 위상(Topology) 보장을 위해 빈 메시지도 무조건 emit
        msg_event = MessageEvent(
            source="activator",
            llm_message=message,
            llm_response_id=llm_response.id,
        )

        await on_event(msg_event)
        
        # 레거시 이벤트 순서 보장
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