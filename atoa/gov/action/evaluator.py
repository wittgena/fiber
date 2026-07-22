# atoa.gov.action.evaluator
from __future__ import annotations
from typing import TYPE_CHECKING
from atoa.agent.disc.schema.reflect import ReflectorResult, ReflectorBase
from eco.call.event.base import LLMConvertibleEvent
from atoa.agent.disc.event.llm.action import ActionEvent
from atoa.agent.disc.event.llm.message import MessageEvent
from watcher.plane.emitter import get_logger
from atoa.call.action.factory import CoreAction

if TYPE_CHECKING:
    from atoa.activator import AgentStateSnapshot

logger = get_logger(__name__)
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
    ) -> tuple[bool, str | None]:
        if not self.reflector.iterative_refinement:
            return False, None

        config = self.reflector.iterative_refinement
        # Agent 내부 상태가 아닌, Gov가 관리하여 넘겨준 iteration 정보를 활용
        iteration = getattr(snapshot, "iteration", 0)
        
        if iteration >= config.max_iterations:
            logger.info(f"Iterative refinement: max iterations ({config.max_iterations}) reached")
            return False, None

        result = action_event.reflector_result
        if not result or result.score >= config.success_threshold:
            return False, None

        logger.info(f"Iterative refinement: retrying {iteration + 1}/{config.max_iterations}")
        followup = self.reflector.get_followup_prompt(result, iteration + 1)
        return True, followup