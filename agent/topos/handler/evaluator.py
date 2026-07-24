# agent.topos.handler.evaluator
## @lineage: atoa.topos.handler.evaluator
## @lineage: atoa.agent.action.evaluator
from __future__ import annotations
from typing import TYPE_CHECKING
from agent.atoa.disc.schema.reflect import ReflectorResult, ReflectorBase
from agent.eco.event.base import LLMConvertibleEvent
from agent.eco.action.message import Message, TextContent
from agent.atoa.disc.event.llm.action import ActionEvent
from agent.atoa.disc.event.llm.message import MessageEvent
from watcher.plane.emitter import get_emitter
from agent.atoa.action.factory import CoreAction

if TYPE_CHECKING:
    from agent.topos.activator import AgentStateSnapshot

logger = get_emitter(__name__)
ITERATIVE_REFINEMENT_ITERATION_KEY = "iterative_refinement_iteration"

class ActionEvaluator:
    def __init__(self, reflector: ReflectorBase):
        self.reflector = reflector

    def should_evaluate(self, tool_name: str) -> bool:
        if self.reflector.mode == "all_actions":
            return True
        if tool_name == CoreAction.FINISH:
            return True
        return False

    def evaluate(self, snapshot: "AgentStateSnapshot", event: ActionEvent | MessageEvent) -> ReflectorResult | None:
        try:
            # 상태 객체 대신, 전달받은 읽기 전용 이벤트 배열 활용
            events = list(snapshot.events) + [event]
            llm_convertible_events = [e for e in events if isinstance(e, LLMConvertibleEvent)]
            
            result = self.reflector.evaluate(events=llm_convertible_events, git_patch=None)
            logger.info(f"✓ evaluation: score={result.score:.3f}, success={result.success}")
            return result
        except Exception as e:
            logger.error(f"✗ evaluation failed: {e}", exc_info=True)
            return None

    def check_iterative_refinement(
        self, snapshot: "AgentStateSnapshot", action_event: ActionEvent
    ) -> MessageEvent | None:
        """
        @desc: 반복 개선(Refinement)이 필요한 경우, 
               LLM에게 피드백을 주기 위한 User MessageEvent를 반환합니다.
               개선이 불필요하거나 최대치에 도달하면 None을 반환합니다.
        """
        if not self.reflector.iterative_refinement:
            return None

        config = self.reflector.iterative_refinement
        iteration = getattr(snapshot, "iteration", 0)
        
        if iteration >= config.max_iterations:
            logger.info(f"Iterative refinement: max iterations ({config.max_iterations}) reached")
            return None

        result = action_event.reflector_result
        if not result or result.score >= config.success_threshold:
            return None

        logger.info(f"Iterative refinement: retrying {iteration + 1}/{config.max_iterations}")
        followup_text = self.reflector.get_followup_prompt(result, iteration + 1)
        
        return MessageEvent(
            source="user",
            llm_message=Message(
                role="user",
                content=[TextContent(text=followup_text)]
            ),
        )