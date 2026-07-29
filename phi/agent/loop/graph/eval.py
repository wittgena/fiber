# phi.agent.loop.graph.eval
## @lineage: phi.executor.graph.eval
from atoa.event.llm.action import ActionEvent
from atoa.event.llm.message import MessageEvent
from atoa.event.llm.observation import ObservationEvent, AgentErrorEvent, UserRejectObservation
from phi.agent.loop.step import StepHandler
from atoa.conv.message import Message, TextContent
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class EvalReflector(StepHandler):
    REFLECTION_IDENTIFIER = "[SYSTEM_OVERLAY: EVALUATE_TRAJECTORY]"

    async def handle_async(self, agent, snapshot, on_event, context) -> bool:
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
        # 기존 로직과 동일 (이벤트 검사는 상태 비의존적이므로 그대로 유지)
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