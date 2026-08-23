# agent.loop.organizer
from typing import Any, Callable, Awaitable

# --- Messages & Events ---
from agent.space.action.message import Message, TextContent
from agent.llm.driver.event.message import MessageEvent
from agent.llm.driver.event.action import ActionEvent
from agent.llm.driver.event.observation import ObservationEvent, AgentErrorEvent, UserRejectObservation
from agent.loop.runtime.exception.types import (
    FunctionCallValidationError, 
    LLMContextWindowExceedError, 
    LLMMalformedConversationHistoryError
)

from agent.loop.conv.command import TransitionStatus
from agent.loop.runtime.protocol.step import StepHandler, StepContext, ActivatorProtocol
from agent.llm.driver.facade import MessageBuilder, LLMFacade

from arch.topos.context.status import ConverStatus
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)


class FinishSignal:
    """루프 종료를 알리는 내부 시그널 객체"""
    is_finish_signal = True


# ==========================================
# 1. EvalReflector: 궤적 평가 및 인지 오버레이
# ==========================================
class EvalReflector(StepHandler):
    REFLECTION_IDENTIFIER = "[SYSTEM_OVERLAY: EVALUATE_TRAJECTORY]"

    async def handle_async(
        self, 
        agent: ActivatorProtocol, 
        snapshot: Any, 
        on_event: Callable[[Any], Awaitable[None]], 
        context: StepContext
    ) -> bool:
        logger.debug("## @phase.evaluate: Transiting through EvalReflector Manifold")
        events = snapshot.events
        
        if not self._should_trigger_reflection(events):
            return False
            
        logger.info("[OVERLAY] Topological anomalies validated. Injecting cognitive reflection overlay.")
        reflection_overlay = MessageEvent(
            source="user",
            llm_message=Message(
                role="assistant",
                content=[TextContent(text=(
                    f"{self.REFLECTION_IDENTIFIER} "
                    "Evaluate the current topological state against the terminal objective. "
                    "If convergence is achieved, initiate graceful termination. "
                    "If the trajectory is fractured or local entropy is rising, project an alternative paradigm."
                ))]
            ),
        )
        await on_event(reflection_overlay)
        return False

    def _should_trigger_reflection(self, events: list) -> bool:
        if not events:
            return False
            
        last_event = events[-1]
        is_valid_obs_type = isinstance(last_event, (ObservationEvent, AgentErrorEvent, UserRejectObservation))
        if not is_valid_obs_type:
            return False

        if isinstance(last_event, (AgentErrorEvent, UserRejectObservation)):
            return True

        recent_action = None
        for event in reversed(events[:-1]):
            if isinstance(event, MessageEvent):
                text_contents = [c.text for c in event.llm_message.content if isinstance(c, TextContent)]
                if any(self.REFLECTION_IDENTIFIER in t for t in text_contents):
                    return False
            if isinstance(event, ActionEvent):
                recent_action = event
                break

        obs_obj = getattr(last_event, "observation", None)
        if obs_obj:
            obs_text = getattr(obs_obj, "result", str(obs_obj)).lower()
            if "error:" in obs_text or "failed to" in obs_text or "exception:" in obs_text:
                return True

        if recent_action and recent_action.action:
            tension_level = getattr(recent_action.action, "tension_level", 0)
            if isinstance(tension_level, int) and tension_level >= 3:
                return True

        return False


# ==========================================
# 2. TensionHandler: 무한 루프 및 교착 상태 방지
# ==========================================
class TensionHandler(StepHandler):
    async def handle_async(
        self, 
        agent: ActivatorProtocol, 
        snapshot: Any, 
        on_event: Callable[[Any], Awaitable[None]], 
        context: StepContext
    ) -> bool:
        logger.debug("[TensionHandler]")
        events = snapshot.events
        is_stuck = getattr(snapshot, "is_stuck", False)
        
        action_events = [e for e in events if isinstance(e, ActionEvent)]
        duplicate_detected = False
        tension = 0
        intent = ""

        if len(action_events) >= 2:
            curr_action = action_events[-1].action
            prev_action = action_events[-2].action

            if curr_action is not None:
                tension = getattr(curr_action, "tension_level", 0)
                raw_intent = getattr(curr_action, "intent", None)
                intent = raw_intent.lower() if isinstance(raw_intent, str) else ""
            
            if curr_action is not None and prev_action is not None:
                curr_kind = getattr(curr_action, "kind", getattr(action_events[-1], "tool_name", ""))
                prev_kind = getattr(prev_action, "kind", getattr(action_events[-2], "tool_name", ""))
                
                if curr_kind and (curr_kind == prev_kind):
                    curr_dump = curr_action.model_dump()
                    prev_dump = prev_action.model_dump()
                    ignore_keys = ["id", "timestamp", "tension_level", "intent", "thought", "reasoning"]
                    for key in ignore_keys:
                        curr_dump.pop(key, None)
                        prev_dump.pop(key, None)
                    
                    if curr_dump == prev_dump:
                        duplicate_detected = True
                        logger.warning("🔄 Duplicate action detected")

        if is_stuck or duplicate_detected or (isinstance(tension, int) and tension >= 4) or intent == "replan":
            reason = "Infinite loop (stuck) detected" if (is_stuck or duplicate_detected) else f"Tension threshold reached (Tension: {tension}/5)"
            logger.error(f"🚨 {reason}. Yielding control and requesting state replan from Gov node.")
            
            error_event = AgentErrorEvent(
                source="agent", 
                error=reason,
                tool_name="system_monitor",
                tool_call_id="tension_halt"
            )
            
            error_event = error_event.model_copy(update={"is_finish_signal": True})
            await on_event(error_event)

            is_graph_mode = getattr(agent, "is_graph_mode", False)
            new_status = ConverStatus.NEEDS_REPLAN if is_graph_mode else ConverStatus.FINISHED
            await on_event(TransitionStatus(new_status=new_status, reason=reason))
            return True
            
        return False


# ==========================================
# 3. LLMInvocationHandler: LLM 추론 요청
# ==========================================
class LLMInvocationHandler(StepHandler):
    async def handle_async(
        self, 
        activator: ActivatorProtocol,  # 의존성 역전: Protocol 주입
        snapshot: Any, 
        on_event: Callable[[Any], Awaitable[None]], 
        context: StepContext
    ) -> bool:
        logger.debug("[LLMInvocationHandler]")
        
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
            if hasattr(activator, "_log_context_window_exceeded_warning"):
                activator._log_context_window_exceeded_warning()
            raise e


# ==========================================
# 4. ToolCallHandler: 도구 호출 파싱 및 이벤트 방출
# ==========================================
class ToolCallHandler(StepHandler):
    async def handle_async(
        self, 
        activator: ActivatorProtocol, 
        snapshot: Any, 
        on_event: Callable[[Any], Awaitable[None]], 
        context: StepContext
    ) -> bool:
        logger.debug("[ToolCallHandler]")
        llm_response = context.llm_response
        
        if not llm_response:
            return False
            
        message: Message = llm_response.message
        
        if not message.tool_calls or len(message.tool_calls) == 0:
            return False

        # [안전 보강] message.content가 None일 경우 빈 리스트로 처리하여 순회 에러 방지
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


# ==========================================
# 5. TextResponseHandler: 텍스트 응답 처리 및 Nudge
# ==========================================
class TextResponseHandler(StepHandler):
    async def handle_async(
        self, 
        activator: ActivatorProtocol, 
        snapshot: Any, 
        on_event: Callable[[Any], Awaitable[None]], 
        context: StepContext
    ) -> bool:
        logger.debug("[TextResponseHandler]")
        llm_response = context.llm_response
        
        if not llm_response:
            return False
            
        message: Message = llm_response.message
        
        has_reasoning = (
            message.responses_reasoning_item is not None
            or message.reasoning_content is not None
            or (message.thinking_blocks and len(message.thinking_blocks) > 0)
        )
        
        safe_content = message.content if isinstance(message.content, list) else []
        has_content = any(isinstance(c, TextContent) and c.text.strip() for c in safe_content)

        # 레거시 위상 보장을 위해 빈 메시지도 무조건 emit
        msg_event = MessageEvent(
            source="activator",
            llm_message=message,
            llm_response_id=llm_response.id,
        )

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