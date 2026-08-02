# agent.protocol.tension
## @lineage: engine.protocol.tension
## @lineage: phi.agent.tension
## @lineage: phi.loop.tension
from engine.atoa.event.llm.action import ActionEvent
from engine.atoa.event.llm.observation import AgentErrorEvent
from agent.state.context.status import ConverStatus
from agent.protocol.step import StepHandler
from agent.state.command import TransitionStatus
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class TensionHandler(StepHandler):
    async def handle_async(self, agent, snapshot, on_event, context) -> bool:
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