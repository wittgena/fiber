# atoa.agent.handler.tension
## @lineage: agent.topos.handler.tension
## @lineage: atoa.topos.handler.tension
from atoa.disc.status import ConverStatus
from atoa.disc.event.llm.action import ActionEvent
from atoa.disc.event.llm.observation import AgentErrorEvent
from atoa.agent.handler.step import StepHandler
from eco.gov.atoa.conv.command import TransitionStatus
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
                    ignore_keys = ["id", "timestamp", "tension_level", "intent", "thought", "reasoning"]
                    for key in ignore_keys:
                        curr_dump.pop(key, None)
                        prev_dump.pop(key, None)
                    
                    if curr_dump == prev_dump:
                        duplicate_detected = True
                        logger.warning(f"🔄 동일한 행동 반복 감지")

        if is_stuck or duplicate_detected or (isinstance(tension, int) and tension >= 4) or intent == "replan":
            reason = "무한 루프(Stuck) 감지" if (is_stuck or duplicate_detected) else f"텐션 임계점 도달 (Tension: {tension}/5)"
            logger.error(f"🚨 {reason}. 제어권을 반납하고 Gov 노드에 상태 재조정을 요청합니다.")
            error_event = AgentErrorEvent(
                source="agent", 
                error=reason,
                tool_name="system_monitor",
                tool_call_id="tension_halt"
            )
            
            ## Activator가 이 이벤트를 catch하여 Gov로 "finish" 시그널을 보내도록 유도
            setattr(error_event, "is_finish_signal", True)
            await on_event(error_event)
            
            ## 기존의 로컬 상태 전이 커맨드 유지
            is_graph_mode = getattr(agent, "is_graph_mode", False)
            new_status = ConverStatus.NEEDS_REPLAN if is_graph_mode else ConverStatus.FINISHED
            await on_event(TransitionStatus(new_status=new_status, reason=reason))
            return True
            
        return False