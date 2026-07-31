# topos.state.parser.view
## @lineage: agent.conver.state.parser.view
## @lineage: phi.conver.state.parser.view
## @lineage: swarm.mesh.parser.view
## @lineage: swarm.mesh.conv.parser.view
## @lineage: swarm.mesh.engine.conv.parser.view
## @lineage: mesh.engine.conv.parser.view
## @lineage: gov.conv.parser.view
## @lineage: gov.atoa.parser.conv.view
## @lineage: bound.parser.atoa.conv.view
## @lineage: agent.conv.view
## @lineage: atoa.agent.conv.view
## @lineage: atoa.conv.view
## @lineage: atoa.gov.context.message.view
from __future__ import annotations
from abc import ABC, abstractmethod
from itertools import pairwise
from collections import defaultdict
from collections.abc import Sequence
from typing import overload
from pydantic import BaseModel, Field

from agent.atoa.conv.event import Event, LLMConvertibleEvent
from agent.atoa.conv.event import EventID, ToolCallID
from agent.atoa.event.llm.action import ActionEvent
from agent.atoa.event.llm.observation import ObservationBaseEvent

from watcher.plane.emitter import get_logger

log = get_logger(__name__)

class ViewIndex(set[int]):
    def find_next(self, threshold: int) -> int:
        valid_indices = {idx for idx in self if idx >= threshold}
        if not valid_indices:
            raise ValueError(f"No manipulation index found >= {threshold}.")

        return min(valid_indices)

    @staticmethod
    def complete(events: list[LLMConvertibleEvent]) -> ViewIndex:
        indices = ViewIndex()
        indices.update(range(0, len(events)))
        indices.add(len(events))
        return indices

class ViewPropertyBase(ABC):
    @abstractmethod
    def enforce(
        self,
        current_view_events: list[LLMConvertibleEvent],
        all_events: Sequence[Event],
    ) -> set[EventID]:
        ...

    @abstractmethod
    def viewindex(
        self,
        current_view_events: list[LLMConvertibleEvent],
    ) -> ViewIndex:
        ...

class BatchAtomic(ViewPropertyBase):
    def enforce(
        self,
        current_view_events: list[LLMConvertibleEvent],
        all_events: Sequence[Event],
    ) -> set[EventID]:
        all_batches = self._build_batches(all_events)
        events_to_remove: set[EventID] = set()

        for llm_response_id, view_batch_ids in self._build_batches(
            current_view_events
        ).items():
            if view_batch_ids != all_batches[llm_response_id]:
                events_to_remove.update(view_batch_ids)

        return events_to_remove

    def viewindex(self, current_view_events: list[LLMConvertibleEvent]) -> ViewIndex:
        viewindex: ViewIndex = ViewIndex.complete(current_view_events)
        for index, (left, right) in enumerate(pairwise(current_view_events)):
            if (
                isinstance(left, ActionEvent)
                and isinstance(right, ActionEvent)
                and left.llm_response_id == right.llm_response_id
            ):
                viewindex.remove(index + 1)
        return viewindex

    def _build_batches(self, events: Sequence[Event]) -> dict[EventID, set[EventID]]:
        batches: dict[EventID, set[EventID]] = defaultdict(set)
        for event in events:
            if isinstance(event, ActionEvent):
                batches[event.llm_response_id].add(event.id)

        return batches

class ToolLoopAtomic(ViewPropertyBase):
    def _tool_loops(self, events: Sequence[Event]) -> list[set[EventID]]:
        tool_loops: list[set[EventID]] = []
        current_tool_loop: set[EventID] | None = None
        for event in events:
            match event:
                case ActionEvent() if event.thinking_blocks:
                    if current_tool_loop is not None:
                        tool_loops.append(current_tool_loop)
                    current_tool_loop = {event.id}
                case ActionEvent() | ObservationBaseEvent():
                    if current_tool_loop is not None:
                        current_tool_loop.add(event.id)
                case _:
                    if current_tool_loop is not None:
                        tool_loops.append(current_tool_loop)
                        current_tool_loop = None

        if current_tool_loop is not None:
            tool_loops.append(current_tool_loop)
        return tool_loops

    def enforce(
        self,
        current_view_events: list[LLMConvertibleEvent],
        all_events: Sequence[Event],
    ) -> set[EventID]:
        all_tool_loops: list[set[EventID]] = self._tool_loops(all_events)
        view_event_ids: set[EventID] = {event.id for event in current_view_events}
        events_to_remove: set[EventID] = set()

        for event in current_view_events:
            if event.id in events_to_remove:
                continue

            for tool_loop in all_tool_loops:
                if event.id in tool_loop:
                    if not tool_loop.issubset(view_event_ids):
                        events_to_remove.update(view_event_ids & tool_loop)
                    break

        return events_to_remove

    def viewindex(
        self,
        current_view_events: list[LLMConvertibleEvent],
    ) -> ViewIndex:
        viewindex: ViewIndex = ViewIndex.complete(current_view_events)
        in_tool_loop: bool = False
        for index, event in enumerate(current_view_events):
            match event:
                case ActionEvent() if event.thinking_blocks:
                    in_tool_loop = True

                case ActionEvent() | ObservationBaseEvent():
                    if in_tool_loop:
                        viewindex.remove(index)

                case _:
                    in_tool_loop = False

        return viewindex

class ToolCallMatching(ViewPropertyBase):
    def enforce(
        self,
        current_view_events: list[LLMConvertibleEvent],
        all_events: Sequence[Event],  # noqa: ARG002
    ) -> set[EventID]:
        action_tool_call_ids: set[ToolCallID] = set()
        observation_tool_call_ids: set[ToolCallID] = set()

        for event in current_view_events:
            match event:
                case ActionEvent():
                    action_tool_call_ids.add(event.tool_call_id)
                case ObservationBaseEvent():
                    observation_tool_call_ids.add(event.tool_call_id)

        events_to_remove: set[EventID] = set()
        for event in current_view_events:
            match event:
                case ActionEvent():
                    if event.tool_call_id not in observation_tool_call_ids:
                        events_to_remove.add(event.id)
                case ObservationBaseEvent():
                    if event.tool_call_id not in action_tool_call_ids:
                        events_to_remove.add(event.id)

        return events_to_remove

    def viewindex(
        self,
        current_view_events: list[LLMConvertibleEvent],
    ) -> ViewIndex:
        viewindex: ViewIndex = ViewIndex.complete(current_view_events)
        pending_tool_call_ids: set[ToolCallID] = set()
        for index, event in enumerate(current_view_events):
            match event:
                case ActionEvent():
                    pending_tool_call_ids.add(event.tool_call_id)
                case ObservationBaseEvent():
                    pending_tool_call_ids.remove(event.tool_call_id)

            if pending_tool_call_ids:
                viewindex.remove(index + 1)

        return viewindex

ALL_PROPERTIES: list[ViewPropertyBase] = [BatchAtomic(), ToolCallMatching(), ToolLoopAtomic()]

class View(BaseModel):
    """Linearly ordered view of events"""
    events: list[LLMConvertibleEvent] = Field(default_factory=list)

    def __len__(self) -> int:
        return len(self.events)

    @property
    def viewindex(self) -> ViewIndex:
        results: ViewIndex = ViewIndex.complete(self.events)
        for property in ALL_PROPERTIES:
            results &= property.viewindex(self.events)
        return results

    @overload
    def __getitem__(self, key: slice) -> list[LLMConvertibleEvent]: ...

    @overload
    def __getitem__(self, key: int) -> LLMConvertibleEvent: ...

    def __getitem__(
        self, key: int | slice
    ) -> LLMConvertibleEvent | list[LLMConvertibleEvent]:
        if isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            return [self[i] for i in range(start, stop, step)]
        elif isinstance(key, int):
            return self.events[key]
        else:
            raise ValueError(f"Invalid key type: {type(key)}")

    def enforce_properties(
        self,
        all_events: Sequence[Event],
    ) -> None:
        for property in ALL_PROPERTIES:
            events_to_forget = property.enforce(self.events, all_events)
            if events_to_forget:
                log.warning(
                    f"Property {property.__class__} enforced, "
                    f"{len(events_to_forget)} events dropped."
                )

                self.events = [
                    event for event in self.events if event.id not in events_to_forget
                ]
                break

        # If we get all the way through the loop without hitting a break, that means no
        # properties needed to be enforced and we can keep the view as-is.
        else:
            return

        # If we did hit a break in the loop, a property applied and now we need to check
        # all the properties again to see if any are unblocked.
        self.enforce_properties(all_events)

    def append_event(self, event: Event) -> None:
        match event:
            case LLMConvertibleEvent():
                self.events.append(event)
            case _:
                log.debug(f"Skipping non-LLMConvertibleEvent of type {type(event)} in View.append_event")

    @staticmethod
    def from_events(events: Sequence[Event]) -> View:
        result: View = View()
        for event in events:
            result.append_event(event)

        result.enforce_properties(events)
        return result