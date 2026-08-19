# ator.runtime.action.batch
## @lineage: ator.agent.action.batch
## @lineage: ator.action.batch
## @lineage: engine.atoa.action.batch
from __future__ import annotations
from collections.abc import Callable
from dataclasses import dataclass, field

from ator.conv.schema.types import ConversationCallbackType
from ator.conv.schema.event import Event
from ator.conv.schema.message import Message, TextContent

from ator.conv.event.llm.action import ActionEvent
from ator.conv.event.llm.message import MessageEvent
from ator.conv.event.llm.observation import UserRejectObservation
from ator.conv.protocol.state import ConvStateProtocol
from ator.driver.tool.builder.executor import BatchExecutorProtocol
from ator.runtime.action.builder import ActionDefinition
from ator.runtime.action.factory import CoreAction

from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

@dataclass(frozen=True, slots=True)
class ActionBatch:
    action_events: list[ActionEvent]
    has_finish: bool
    blocked_reasons: dict[str, str] = field(default_factory=dict)
    results_by_id: dict[str, list[Event]] = field(default_factory=dict)

    @staticmethod
    def _truncate_at_finish(action_events: list[ActionEvent]) -> tuple[list[ActionEvent], bool]:
        """Finish 액션 이후에 등장하는 무의미한 액션들을 잘라냅니다(Truncate)."""
        finish_idx = next(
            (
                i
                for i, ae in enumerate(action_events)
                if ae.tool_name == CoreAction.FINISH
            ),
            None,
        )
        if finish_idx is None:
            return action_events, False

        discarded = action_events[finish_idx + 1 :]
        if discarded:
            names = [ae.tool_name for ae in discarded]
            logger.warning(
                f"Discarding {len(discarded)} tool call(s) "
                f"after FinishTool: {', '.join(names)}"
            )
        return action_events[: finish_idx + 1], True

    @classmethod
    def prepare(
        cls,
        action_events: list[ActionEvent],
        state: ConvStateProtocol,
        # [개선 1 적용] 이제 병렬/동기 실행기 상관없이 프로토콜을 따르는 어떤 인스턴스든 주입받을 수 있습니다.
        executor: BatchExecutorProtocol,
        tool_runner: Callable[[ActionEvent], list[Event]],
        tools: dict[str, ActionDefinition] | None = None,
    ) -> ActionBatch:
        """Truncate, partition blocked actions, execute the rest, return the batch."""
        action_events, has_finish = cls._truncate_at_finish(action_events)

        blocked_reasons: dict[str, str] = {}
        executable: list[ActionEvent] = []
        
        for ae in action_events:
            reason = state.pop_blocked_action(ae.id)
            if reason is not None:
                blocked_reasons[ae.id] = reason
            else:
                executable.append(ae)

        executed_results = executor.execute_batch(executable, tool_runner, tools)
        if len(executable) != len(executed_results):
            logger.error(f"Executor mismatch: {len(executable)} actions but {len(executed_results)} results returned.")

        results_by_id = {
            ae.id: res for ae, res in zip(executable, executed_results)
        }

        return cls(
            action_events=action_events,
            has_finish=has_finish,
            blocked_reasons=blocked_reasons,
            results_by_id=results_by_id,
        )

    def emit(self, on_event: ConversationCallbackType) -> None:
        """Emit all events in original action order."""
        for ae in self.action_events:
            reason = self.blocked_reasons.get(ae.id)
            if reason is not None:
                logger.info(f"Action '{ae.tool_name}' blocked by hook: {reason}")
                on_event(
                    UserRejectObservation(
                        action_id=ae.id,
                        tool_name=ae.tool_name,
                        tool_call_id=ae.tool_call_id,
                        rejection_reason=reason,
                        rejection_source="hook",
                    )
                )
            else:
                for event in self.results_by_id.get(ae.id, []):
                    on_event(event)

    def finalize(
        self,
        on_event: ConversationCallbackType,
        check_iterative_refinement: Callable[[ActionEvent], tuple[bool, str | None]],
        mark_finished: Callable[[], None],
    ) -> None:
        """Finish 액션 도달 시 반복 정제(Iterative Refinement) 또는 종료를 처리합니다."""
        if not self.has_finish or self.action_events[-1].id in self.blocked_reasons:
            return

        should_continue, followup = check_iterative_refinement(self.action_events[-1])
        if should_continue and followup:
            on_event(
                MessageEvent(
                    source="user",
                    llm_message=Message(
                        role="user",
                        content=[TextContent(text=followup)],
                    ),
                )
            )
        else:
            mark_finished()