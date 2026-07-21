# atoa.gov.action.evaluator
## @lineage: gov.action.evaluator
## @lineage: gov.engine.action.evaluator
from __future__ import annotations
from typing import TYPE_CHECKING
from atoa.disc.schema.reflect import ReflectorResult, ReflectorBase
from eco.call.event.base import LLMConvertibleEvent
from atoa.disc.event.llm.action import ActionEvent
from atoa.disc.event.llm.message import MessageEvent
from watcher.plane.emitter import get_logger
from atoa.call.action.factory import CoreAction

if TYPE_CHECKING:
    from atoa.disc.base.conv import ProtoConv

logger = get_logger(__name__)
ITERATIVE_REFINEMENT_ITERATION_KEY = "iterative_refinement_iteration"

class ActionEvaluator:
    """
    @desc: Agent 내부에서 평가 및 반복 개선(Iterative Refinement) 로직을 위임받아 처리하는 컴포넌트
    """
    def __init__(self, reflector: ReflectorBase):
        self.reflector = reflector

    def should_evaluate(self, tool_name: str) -> bool:
        if self.reflector.mode == "all_actions":
            return True
        if tool_name == CoreAction.FINISH:
            return True
        return False

    def evaluate(self, conversation: "ProtoConv", event: ActionEvent | MessageEvent) -> ReflectorResult | None:
        try:
            events = list(conversation.state.events) + [event]
            llm_convertible_events = [e for e in events if isinstance(e, LLMConvertibleEvent)]
            
            result = self.reflector.evaluate(events=llm_convertible_events, git_patch=None)
            logger.info(f"✓ evaluation: score={result.score:.3f}, success={result.success}")
            return result
        except Exception as e:
            logger.error(f"✗ evaluation failed: {e}", exc_info=True)
            return None

    def check_iterative_refinement(
        self, conversation: "ProtoConv", action_event: ActionEvent
    ) -> tuple[bool, str | None]:
        if not self.reflector.iterative_refinement:
            return False, None

        config = self.reflector.iterative_refinement
        state = conversation.state
        iteration = state.agent_state.get(ITERATIVE_REFINEMENT_ITERATION_KEY, 0)
        
        if iteration >= config.max_iterations:
            logger.info(f"Iterative refinement: max iterations ({config.max_iterations}) reached")
            return False, None

        result = action_event.reflector_result
        if not result:
            logger.warning("Iterative refinement: no reflector result on ActionEvent")
            return False, None

        if result.score >= config.success_threshold:
            logger.info(f"Iterative refinement: success threshold met (score: {result.score:.3f})")
            return False, None

        new_iteration = iteration + 1
        state.agent_state = {**state.agent_state, ITERATIVE_REFINEMENT_ITERATION_KEY: new_iteration}
        
        logger.info(f"Iterative refinement: retrying {new_iteration}/{config.max_iterations}")
        followup = self.reflector.get_followup_prompt(result, new_iteration)
        return True, followup