# atoa.gov.action.loop
## @lineage: gov.action.loop
## @lineage: gov.engine.action.loop
import json

from atoa.disc.status import ConverStatus
from atoa.disc.event.llm.action import ActionEvent
from atoa.disc.event.llm.message import MessageEvent
from atoa.exception.types import (
    FunctionCallValidationError,
    LLMContextWindowExceedError,
    LLMMalformedConversationHistoryError,
)

from atoa.gov.action.step import StepHandler
from atoa.context.gov.message.parser.builder import MessageBuilder, LLMFacade
from atoa.context.gov.state import ConversationState
from eco.call.action.message import Message, TextContent
from atoa.context.gov.command import TransitionStatus

from watcher.plane.emitter import get_emitter
logger = get_emitter(__name__)

class PendingActionHandler(StepHandler):
    def handle(self, activator, conversation, on_event, on_token, context) -> bool:
        logger.debug(f"[PendingActionHandler]")
        state = conversation.state
        
        pending_actions = ConversationState.get_unmatched_actions(state.events)
        if pending_actions:
            logger.info("Confirmation mode: Executing %d pending action(s)", len(pending_actions))
            activator._execute_actions(conversation, pending_actions, on_event)
            return True

        if state.last_user_message_id is not None:
            reason = state.pop_blocked_message(state.last_user_message_id)
            if reason is not None:
                logger.info(f"User message blocked by hook: {reason}")
                
                # [개선됨] 직접 할당 제거 및 Command 패턴 적용
                state.apply(
                    TransitionStatus(
                        new_status=ConverStatus.FINISHED, 
                        reason=f"Hook blocked user message (Reason: {reason})"
                    )
                )
                return True
        elif state.blocked_messages:
            logger.debug("Blocked messages exist but last_user_message_id is None; skipping hook check.")
            
        return False


class LLMInvocationHandler(StepHandler):
    """컨텍스트 준비 및 LLM API 호출 (예외 처리 통합)"""
    def handle(self, activator, conversation, on_event, on_token, context) -> bool:
        logger.debug(f"[LLMInvocationHandler]")
        state = conversation.state
        
        # MessageBuilder를 통해 안정적이고 규격화된 프롬프트 메시지 리스트 생성
        _messages = MessageBuilder.prepare_llm_messages(state.events, llm=activator.llm)

        logger.debug(f"[LLMInvocationHandler] Sending messages to LLM: {json.dumps([m.model_dump() for m in _messages[1:]], indent=2)}")

        try:
            # LLMFacade를 통해 통합된 파라미터 컨벤션으로 LLM API 호출
            llm_response = LLMFacade.make_completion(
                llm=activator.llm,
                messages=_messages,
                tools=list(activator.tools_map.values()),
                on_token=on_token,
            )
            context.llm_response = llm_response  # 성공 시 다음 단계로 응답 전달
            return False
            
        except FunctionCallValidationError as e:
            logger.warning(f"LLM generated malformed function call: {e}")
            error_message = MessageEvent(
                source="environment",
                llm_message=Message(role="user", content=[TextContent(text=str(e))]),
            )
            on_event(error_message)
            return True
        except LLMMalformedConversationHistoryError as e:
            logger.warning(f"Malformed history error: {e}")
            raise e
        except LLMContextWindowExceedError as e:
            activator._log_context_window_exceeded_warning()
            raise e


class ToolCallHandler(StepHandler):
    """LLM 응답 중 Tool Call 파싱 및 실행"""
    def handle(self, activator, conversation, on_event, on_token, context) -> bool:
        logger.debug(f"[ToolCallHandler]")
        llm_response = context.llm_response
        message: Message = llm_response.message
        
        if not message.tool_calls or len(message.tool_calls) == 0:
            return False # 툴 호출이 없으면 Text Response 핸들러로 위임

        if not all(isinstance(c, TextContent) for c in message.content):
            logger.warning("LLM returned tool calls but message content is not all TextContent")

        thought_content = [c for c in message.content if isinstance(c, TextContent)]
        action_events: list[ActionEvent] = []
        
        for i, tool_call in enumerate(message.tool_calls):
            action_event = activator._get_action_event(
                tool_call,
                conversation=conversation,
                llm_response_id=llm_response.id,
                on_event=on_event,
                security_analyzer=conversation.state.security_analyzer,
                thought=thought_content if i == 0 else [],
                reasoning_content=message.reasoning_content if i == 0 else None,
                thinking_blocks=list(message.thinking_blocks) if i == 0 else [],
                responses_reasoning_item=message.responses_reasoning_item if i == 0 else None,
            )
            if action_event is not None:
                action_events.append(action_event)

        if activator._requires_user_confirmation(conversation.state, action_events):
            return True

        if action_events:
            activator._execute_actions(conversation, action_events, on_event)

        activator._maybe_emit_vllm_tokens(llm_response, on_event)
        return True


class TextResponseHandler(StepHandler):
    """일반 텍스트(또는 빈 응답), Nudge 피드백 처리 및 상태 종료"""
    def handle(self, activator, conversation, on_event, on_token, context) -> bool:
        logger.debug(f"[TextResponseHandler]")
        llm_response = context.llm_response
        message: Message = llm_response.message
        
        has_reasoning = (
            message.responses_reasoning_item is not None
            or message.reasoning_content is not None
            or (message.thinking_blocks and len(message.thinking_blocks) > 0)
        )
        has_content = any(isinstance(c, TextContent) and c.text.strip() for c in message.content)

        if not has_reasoning and not has_content:
            logger.warning("LLM produced empty response - continuing loop")

        msg_event = MessageEvent(
            source="activator",
            llm_message=message,
            llm_response_id=llm_response.id,
        )

        if activator.reflector is not None and activator.reflector.mode == "finish_and_message":
            reflector_result = activator._evaluate_with_reflector(conversation, msg_event)
            if reflector_result is not None:
                msg_event = msg_event.model_copy(update={"reflector_result": reflector_result})
                
        on_event(msg_event)
        activator._maybe_emit_vllm_tokens(llm_response, on_event)

        if has_content:
            logger.debug("LLM produced a message response - awaits user input")
            
            # [개선됨] 직접 할당 제거 및 Command 패턴 적용
            conversation.state.apply(
                TransitionStatus(
                    new_status=ConverStatus.FINISHED, 
                    reason="LLM produced a direct text response"
                )
            )
            return True

        if not has_content:
            logger.warning("LLM response contained no tool call and no content - sending corrective feedback")
            nudge = MessageEvent(
                source="user",
                llm_message=Message(
                    role="user",
                    content=[TextContent(text="Your last response did not include a function call or a message. Please use a tool to proceed with the task.")]
                ),
            )
            on_event(nudge)
        return True