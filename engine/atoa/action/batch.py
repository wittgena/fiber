# engine.atoa.action.batch
from __future__ import annotations
from collections.abc import Callable
from dataclasses import dataclass, field

from engine.atoa.conv.types import ConversationCallbackType
from engine.atoa.conv.event import Event
from engine.atoa.event.llm.action import ActionEvent
from engine.atoa.event.llm.message import MessageEvent
from engine.atoa.event.llm.observation import UserRejectObservation
from agent.state.protocol import ConvStateProtocol

# [개선 1] 구체적인 ParallelExecutor 대신 추상화된 프로토콜(인터페이스)을 임포트
from agent.runtime.builder.executor import BatchExecutorProtocol
from engine.atoa.conv.message import Message, TextContent
from engine.atoa.action.builder import ActionDefinition
from engine.atoa.action.factory import CoreAction

# [개선 3] 다른 시스템 모듈과의 일관성을 위해 get_logger 사용 (필요시 get_emitter 유지 가능)
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

@dataclass(frozen=True, slots=True)
class ActionBatch:
    """
    @desc: 다수의 Tool Call(Action)을 일괄 실행하고 결과를 조율(Orchestration)하는 배치 컨테이너.
    """
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

        # 주입받은 다형성 기반 실행기(Protocol)에 실행 위임
        executed_results = executor.execute_batch(executable, tool_runner, tools)
        
        # [방어 로직] 실행기가 규약을 어기고 다른 개수의 결과를 반환할 경우 에러 트래킹
        if len(executable) != len(executed_results):
            logger.error(f"Executor mismatch: {len(executable)} actions but {len(executed_results)} results returned.")

        # [개선 2] 성능 및 가독성이 좋은 딕셔너리 컴프리헨션으로 변경
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