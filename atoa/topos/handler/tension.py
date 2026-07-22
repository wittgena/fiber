# atoa.topos.handler.tension
## @lineage: atoa.agent.action.tension
from atoa.agent.disc.status import ConverStatus
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.topos.handler.step import StepHandler
from atoa.conv.context.command import TransitionStatus
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class TensionHandler(StepHandler):
    async def handle_async(self, agent, snapshot, on_event, context) -> bool:
        logger.debug(f"[CognitiveTensionHandler]")
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
                    _ = curr_dump.pop("id", None), prev_dump.pop("id", None)
                    _ = curr_dump.pop("timestamp", None), prev_dump.pop("timestamp", None)
                    
                    if curr_dump == prev_dump:
                        duplicate_detected = True
                        logger.warning(f"🔄 동일한 행동 반복 감지")

        if is_stuck or duplicate_detected or (isinstance(tension, int) and tension >= 4) or intent == "replan":
            reason = "무한 루프(Stuck) 감지" if (is_stuck or duplicate_detected) else f"텐션 임계점 도달 (Tension: {tension}/5)"
            logger.error(f"🚨 {reason}. 제어권을 반납하고 Gov 노드에 상태 재조정을 요청합니다.")
            
            is_graph_mode = getattr(agent, "is_graph_mode", False)
            new_status = ConverStatus.NEEDS_REPLAN if is_graph_mode else ConverStatus.FINISHED
            await on_event(TransitionStatus(new_status=new_status, reason=reason))
            return True 
            
        return False