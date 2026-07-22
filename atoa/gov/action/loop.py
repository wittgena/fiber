# atoa.gov.action.loop
import json
from atoa.agent.disc.status import ConverStatus
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.agent.disc.event.llm.message import MessageEvent
from atoa.exception.types import FunctionCallValidationError, LLMContextWindowExceedError, LLMMalformedConversationHistoryError

from atoa.gov.action.step import StepHandler
from atoa.gov.context.message.parser.builder import MessageBuilder, LLMFacade
from atoa.gov.context.state import ConversationState
from eco.call.action.message import Message, TextContent
from atoa.gov.context.command import TransitionStatus

from watcher.plane.emitter import get_emitter
logger = get_emitter(__name__)

class PendingActionHandler(StepHandler):
    async def handle_async(self, activator, snapshot, on_event, context) -> bool:
        logger.debug(f"[PendingActionHandler]")
        
        # [핵심 변경] Agent는 툴을 실행할 권한이 없습니다.
        # Gov가 툴을 실행하지 않고 Agent에게 Pending 상태로 넘겼다면, 
        # 이는 Gov의 처리를 대기해야 하거나 비정상 상태입니다. 즉시 제어권을 넘깁니다.
        pending_actions = ConversationState.get_unmatched_actions(snapshot.events)
        if pending_actions:
            logger.info("Gov node is currently processing pending actions. Yielding control.")
            return True

        return False

class LLMInvocationHandler(StepHandler):
    async def handle_async(self, activator, snapshot, on_event, context) -> bool:
        logger.debug(f"[LLMInvocationHandler]")
        
        _messages = MessageBuilder.prepare_llm_messages(snapshot.events, llm=activator.llm)
        logger.debug(f"[LLMInvocationHandler] Sending messages to LLM: {json.dumps([m.model_dump() for m in _messages[1:]], indent=2)}")

        try:
            # (LLMFacade가 동기 함수라면 asyncio.to_thread 등으로 감쌀 수 있으나, 호환을 위해 그대로 둠)
            llm_response = LLMFacade.make_completion(
                llm=activator.llm,
                messages=_messages,
                tools=list(activator.tools_map.values()),
                on_token=None, # 스트리밍은 구조상 비활성화 또는 별도 처리
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
            # _get_action_event는 이제 실행 및 상태 평가 없이 파싱만 하여 반환합니다. (이전 단계에서 리팩토링됨)
            action_event, error_event = activator._get_action_event(
                tool_call,
                llm_response_id=llm_response.id,
                security_analyzer=None, # 보안 검사는 Gov에서 수행
                thought=thought_content if i == 0 else [],
                reasoning_content=message.reasoning_content if i == 0 else None,
                thinking_blocks=list(message.thinking_blocks) if i == 0 else [],
                responses_reasoning_item=message.responses_reasoning_item if i == 0 else None,
            )
            
            if error_event:
                await on_event(error_event)
            if action_event:
                action_events.append(action_event)

        # [핵심 변경] 실행 권한 및 보안 검사를 Gov로 완전히 이관
        # 1. activator._requires_user_confirmation 제거
        # 2. activator._execute_actions 제거
        for action in action_events:
            await on_event(action) # Gov 측 워커가 이를 수신하고 Tool을 실행함

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
            logger.debug("LLM produced a message response - emits FINISHED signal to Gov")
            # [핵심 변경] 상태 직접 조작 불가. Gov로 상태 변이 명령(TransitionStatus)을 Emit
            await on_event(TransitionStatus(new_status=ConverStatus.FINISHED, reason="LLM produced a direct text response"))
            return True

        if not has_content and not has_reasoning:
            logger.warning("LLM response contained no tool call and no content - sending corrective feedback")
            nudge = MessageEvent(
                source="user",
                llm_message=Message(
                    role="user",
                    content=[TextContent(text="Your last response did not include a function call or a message. Please use a tool to proceed with the task.")]
                ),
            )
            await on_event(nudge)
        return True