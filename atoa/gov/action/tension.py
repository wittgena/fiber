# atoa.gov.action.tension
## @lineage: gov.action.tension
## @lineage: gov.engine.action.tension
from abc import ABC, abstractmethod
import json
from typing import TYPE_CHECKING, Any
from dataclasses import dataclass, field

from atoa.agent.disc.status import ConverStatus
from atoa.agent.disc.event.llm.action import ActionEvent
from eco.call.disc.action import Action

from atoa.gov.action.step import StepHandler
import atoa.gov.security.analyzer as analyzer
import atoa.gov.security.eval as risk

from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class TensionHandler(StepHandler):
    """에이전트의 메타 인지적 텐션(Tension)과 무한 루프(Stuck) 상태를 모니터링"""
    def handle(self, agent, conversation, on_event, on_token, context) -> bool:
        logger.debug(f"[CognitiveTensionHandler]")
        state = conversation.state
        events = state.events
        
        ## 프레임워크 내장 Stuck 감지기 연동
        is_stuck = getattr(state, "is_stuck", False)
        
        ## MessageEvent(Reflection 등)나 병렬 Observation 순서 꼬임을 무시 -> 에이전트가 수행한 '행동'의 순서만 보장
        action_events = [e for e in events if isinstance(e, ActionEvent)]
        
        duplicate_detected = False
        tension = 0
        intent = ""

        ## 최소 2번 이상의 Action이 수행되었을 때만 분석
        if len(action_events) >= 2:
            curr_action_event = action_events[-1]
            prev_action_event = action_events[-2]
            
            curr_action = curr_action_event.action
            prev_action = prev_action_event.action

            ## 텐션 레벨 및 Intent 확인 (가장 최근 Action 기준)
            if curr_action is not None:
                tension = getattr(curr_action, "tension_level", 0)
                raw_intent = getattr(curr_action, "intent", None)
                intent = raw_intent.lower() if isinstance(raw_intent, str) else ""
            
            ## 동일 도구 반복 호출(무한 루프) 직접 감지 로직
            if curr_action is not None and prev_action is not None:
                ## 도구의 종류(kind) 또는 이름(tool_name)을 우선 비교
                curr_kind = getattr(curr_action, "kind", getattr(curr_action_event, "tool_name", ""))
                prev_kind = getattr(prev_action, "kind", getattr(prev_action_event, "tool_name", ""))
                
                if curr_kind and (curr_kind == prev_kind):
                    curr_dump = curr_action.model_dump()
                    prev_dump = prev_action.model_dump()
                    _ = curr_dump.pop("id", None), prev_dump.pop("id", None)
                    _ = curr_dump.pop("timestamp", None), prev_dump.pop("timestamp", None)
                    
                    if curr_dump == prev_dump:
                        duplicate_detected = True
                        logger.warning(f"🔄 동일한 행동 반복 감지: {curr_action}")

        ## 루프 파괴(Break) 조건 통합 판단
        if is_stuck or duplicate_detected or (isinstance(tension, int) and tension >= 4) or intent == "replan":
            ## 로그 메시지 분기 처리
            if is_stuck or duplicate_detected:
                reason = "무한 루프(Stuck) 감지"
            else:
                reason = f"텐션 임계점 도달 (Tension: {tension}/5, Intent: {intent})"
                
            logger.error(f"🚨 {reason}. 선형 루프를 강제 중단합니다.")
            
            ## Graph Mode 여부 안전하게 확인
            is_graph_mode = False
            if hasattr(agent, "is_running_in_graph_mode"):
                attr = agent.is_running_in_graph_mode
                is_graph_mode = attr() if callable(attr) else attr

            if is_graph_mode:
                logger.info("Graph Orchestrator에게 위상(Topology) 재구성 시그널 전송")
                state.execution_status = ConverStatus.NEEDS_REPLAN
            else:
                ## 선형 모드에서 무한 헛도는 것을 방지하기 위해 대화를 강제 종료
                logger.warning("Graph Mode가 아닙니다. 에이전트의 폭주를 막기 위해 대화를 FINISHED 상태로 강제 종료합니다.")
                state.execution_status = ConverStatus.FINISHED
            return True # 파이프라인 즉시 중단 (return True)
        return False # 문제없으면 다음 핸들러로 진행