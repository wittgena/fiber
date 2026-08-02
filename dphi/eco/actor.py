# dphi.eco.actor
## @lineage: dphi.topos.actor
## @lineage: agent.topos.conver.actor
import json
import asyncio
from pathlib import Path
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from engine.atoa.context.state import ConversationState
from engine.driver.disc.action import Action, Observation
from engine.atoa.event.llm.message import MessageEvent
from engine.atoa.event.llm.action import ActionEvent
from engine.atoa.event.llm.observation import ObservationEvent, UserRejectObservation, AgentErrorEvent
from engine.atoa.event.llm.system import SystemPromptEvent
from engine.atoa.event.conv import ConversationErrorEvent, PauseEvent
from agent.state.context.status import ConverStatus
from engine.atoa.action.factory import CoreAction

from mesh.bound.exception.types import ConversationRunError
from agent.state.command import TransitionStatus
from agent.state.store.log import LogStore

from arch.xor.store.file import LocalFileStore
from arch.model.payload import StreamPayloadAdapter
from arch.topos.tunnel.factory import TunnelFactory
from watcher.plane.observer.span import observe
from watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from agent.state.context.conv import ConvContext
    convType = ConvContext | Any
else:
    convType = Any

log = get_emitter("conver.actor")
MAX_ABSOLUTE_ITERATIONS = 7
MAX_CONSECUTIVE_TIMEOUTS = 3  # 연속 타임아웃 허용치


class Actor:
    def __init__(self, conversation: convType):
        self.conv = conversation

    @observe(name="conver.run")
    async def run(self) -> None:
        """Event-driven async control loop."""
        if self.conv.state.execution_status in [
            ConverStatus.IDLE, ConverStatus.PAUSED,
            ConverStatus.ERROR, ConverStatus.STUCK,
        ]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Engine loop started"))

        tunnel = await TunnelFactory.get_default()
        conv_id_str = str(self.conv.id)
        agent_task_topic = f"agent:tasks:{conv_id_str}"
        agent_response_topic = f"agent:responses:{conv_id_str}"
        consumer_group = "gov_orchestrator"

        iteration = 0
        consecutive_timeouts = 0  # [개선 1] 타임아웃 방어용 카운터

        try:
            while True:
                log.debug(f"[ConvRunner] Execution iteration: {iteration}")
                if self._check_halt_conditions(iteration):
                    break

                # [Phase 1] Construct and Produce State Snapshot
                current_topo = len(self.conv.state.events)
                is_rupture = self.conv.state.execution_status == ConverStatus.NEEDS_REPLAN

                step_payload = {
                    "conversation_id": conv_id_str,
                    "iteration": iteration,
                    "events": [e.model_dump(mode="json") for e in self.conv.state.events],
                    "_telemetry": {
                        "topo": current_topo,
                        "press": 0,
                        "rupture": is_rupture,
                        "tick": iteration
                    }
                }
                
                await tunnel.stream_produce(
                    topic=agent_task_topic, 
                    payload=StreamPayloadAdapter.encode(step_payload)
                )
                log.debug(f"[ConvRunner] Step request produced to {agent_task_topic}")

                # [Phase 2] Await Agent Response with Timeout Handling
                stream_results = []
                try:
                    stream_results = await tunnel.stream_consume(
                        topic=agent_response_topic, 
                        group=consumer_group, 
                        consumer="conver_worker_1", 
                        count=1, 
                        block=30000 
                    )
                    consecutive_timeouts = 0  # 성공 시 카운터 초기화
                except (TimeoutError, ConnectionError) as te:
                    # 소켓 레벨의 타임아웃/연결 예외를 삼켜 크래시 방지
                    log.warning(f"[ConvRunner] Redis stream consume timed out (Socket Level): {te}")
                    consecutive_timeouts += 1
                except Exception as e:
                    if "Timeout" in type(e).__name__:
                        log.warning(f"[ConvRunner] Redis stream consume timed out (Library Level): {e}")
                        consecutive_timeouts += 1
                    else:
                        raise e

                if not stream_results:
                    if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                        self._halt_execution("AgentTimeout", f"에이전트 응답 없음 ({MAX_CONSECUTIVE_TIMEOUTS}회 연속 타임아웃)")
                        break
                    log.warning("[ConvRunner] Agent response timeout. Retrying loop...")
                    continue

                # [Phase 3] Consume Decision & Execute Physical Action
                for stream_name, messages in stream_results:
                    for message_id, message_data in messages:
                        try:
                            parsed_decision = StreamPayloadAdapter.decode(message_data)
                            await self._process_agent_decision(parsed_decision)
                        finally:
                            await tunnel.stream_ack(agent_response_topic, consumer_group, message_id)

                iteration += 1
                
                # Terminate loop if state exits RUNNING mode
                if self.conv.state.execution_status in [
                    ConverStatus.WAITING_FOR_USER, ConverStatus.FINISHED,
                    ConverStatus.STUCK, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
                ]:
                    break
                    
        except Exception as e:
            if not isinstance(e, ConversationRunError):
                self.conv.state.apply(TransitionStatus(new_status=ConverStatus.ERROR, reason=f"Unhandled exception: {e}"))
                self.conv._on_event(ConversationErrorEvent(source="environment", code=e.__class__.__name__, detail=str(e)))
                raise ConversationRunError(self.conv.state.id, e, persistence_dir=self.conv.state.persistence_dir) from e
            raise

    async def _process_agent_decision(self, data: dict) -> None:
        """Parses the Agent's intent, updates the state, and executes tools."""
        decision_type = data.get("type")
        telemetry = data.get("_telemetry", {})
        
        if decision_type == "action":
            action_event = ActionEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(action_event)
            tool_name = action_event.tool_name
            action_obj = action_event.action
            
            try:
                obs = self.execute_tool(tool_name, action_obj)
                if obs is not None:
                    obs_event = ObservationEvent(
                        observation=obs, 
                        tool_name=tool_name, 
                        action_id=action_event.id,
                        tool_call_id=action_event.tool_call_id 
                    )
                    self.conv._on_event(obs_event)
                    
            except Exception as e:
                log.error(f"[ConvRunner] Tool execution failed: {e}")
                error_event = AgentErrorEvent(
                    source="environment",
                    error=str(e),
                    tool_name=tool_name,
                    tool_call_id=action_event.tool_call_id
                )
                self.conv._on_event(error_event)
                
        elif decision_type == "message":
            msg_event = MessageEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(msg_event)
            
        elif decision_type == "system_prompt":
            sys_event = SystemPromptEvent.model_validate(data.get("event_payload"))
            self.conv._on_event(sys_event)
            
        elif decision_type == "error":
            try:
                error_event = AgentErrorEvent.model_validate(data.get("event_payload", {}))
                self.conv._on_event(error_event)
            except Exception as e:
                log.error(f"[ConvRunner] Failed to parse agent error event: {e}")

            new_status = ConverStatus.NEEDS_REPLAN if telemetry.get("rupture") else ConverStatus.STUCK
            self.conv.state.apply(TransitionStatus(new_status=new_status, reason="Agent reported an error."))
            
        elif decision_type == "finish":
            if telemetry.get("rupture"):
                payload = data.get("event_payload", {})
                reason = payload.get("error", "Agent execution forcibly ruptured (e.g., Livelock/Tension).") if isinstance(payload, dict) else str(payload)
                
                log.warning(f"[ConvRunner] Received RUPTURE finish signal: {reason}")
                
                # 강제 종료에 의한 것이므로 재조정(NEEDS_REPLAN) 상태로 전이
                self.conv.state.apply(TransitionStatus(new_status=ConverStatus.NEEDS_REPLAN, reason=reason))
                
                # 페이로드가 AgentErrorEvent 데이터라면 추가 기록
                if isinstance(payload, dict) and "error" in payload:
                    try:
                        self.conv._on_event(AgentErrorEvent.model_validate(payload))
                    except:
                        pass
            else:
                self.conv.state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Agent finalized turn"))

    def execute_tool(self, tool_name: str, action: Action) -> Observation | None:
        """
        Executes the tool. 
        Safely bypasses cognitive tools (e.g., think, finish, bridge) without raising execution exceptions.
        """
        local_tools_map = getattr(self.conv, "tools", {})
        tool = local_tools_map.get(tool_name)
        
        if tool is None:
            available_tools = list(local_tools_map.keys())
            raise KeyError(f"Tool '{tool_name}' not found in Gov Environment. Available tools: {available_tools}")

        if not getattr(tool, "executor", None):
            if CoreAction.is_safe_cognitive(tool_name) or tool_name in ["finish", "bridge", "signal", "lang"]:
                log.debug(f"[ConvRunner] Cognitive action '{tool_name}' processed without physical execution.")
                return None
            else:
                raise NotImplementedError(f"Physical Tool '{tool_name}' has no configured executor in Gov Env.")
        
        return tool(action, self.conv)

    def _check_halt_conditions(self, iteration: int) -> bool:
        if iteration >= MAX_ABSOLUTE_ITERATIONS:
            self._halt_execution(
                "AbsoluteMaxIterationsReached",
                f"Topological Rupture: Absolute system iterations limit ({MAX_ABSOLUTE_ITERATIONS}) reached."
            )
            return True

        if self.conv.state.execution_status in [
            ConverStatus.PAUSED, ConverStatus.STUCK, 
            ConverStatus.FINISHED, ConverStatus.ERROR, ConverStatus.NEEDS_REPLAN
        ]:
            return True

        if getattr(self.conv, "_stuck_detector", None) and self.conv._stuck_detector.is_stuck():
            self._halt_execution("AgentStuck", "Cognitive Livelock (Stuck pattern) detected.")
            return True
        
        if getattr(self.conv, "_drift_detector", None) and self.conv._drift_detector.is_drifting():
            self._halt_execution("AgentDrift", "Topological Drift detected: Agent is wandering without progress.")
            return True

        if self.conv.state.execution_status == ConverStatus.WAITING_FOR_USER:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.RUNNING, reason="Resuming from user wait"))
            
        max_iter = getattr(self.conv, "max_iteration_per_run", MAX_ABSOLUTE_ITERATIONS)
        if iteration >= max_iter:
            self._halt_execution("MaxIterationsReached", "User-defined iterations limit reached.")
            return True
            
        return False

    def _halt_execution(self, code: str, detail: str) -> None:
        log.error(detail)
        new_status = ConverStatus.STUCK if "Stuck" in code or "Drift" in code or "Timeout" in code else ConverStatus.ERROR
        self.conv.state.apply(TransitionStatus(new_status=new_status, reason=detail))
        self.conv._on_event(ConversationErrorEvent(source="environment", code=code, detail=detail))

    def pause(self) -> None:
        if self.conv.state.execution_status == ConverStatus.PAUSED:
            return
            
        if self.conv.state.execution_status in [ConverStatus.IDLE, ConverStatus.RUNNING]:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.PAUSED, reason="Agent execution pause requested"))
            self.conv._on_event(PauseEvent())
            log.info("Agent execution pause requested")

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        pending_actions = ConversationState.get_unmatched_actions(self.conv.state.events)
        
        if self.conv.state.execution_status == ConverStatus.WAITING_FOR_USER:
            self.conv.state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User rejected pending action(s)"))

        if not pending_actions:
            log.warning("No pending actions to reject")
            return

        for action_event in pending_actions:
            rejection_event = UserRejectObservation(
                action_id=action_event.id,
                tool_name=action_event.tool_name,
                tool_call_id=action_event.tool_call_id,
                rejection_reason=reason,
            )
            self.conv._on_event(rejection_event)
            log.info(f"Rejected pending action: {action_event.tool_name} - {reason}")

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        rerun_log: LogStore | None = None
        if rerun_log_path is not None:
            log_dir = Path(rerun_log_path)
            log_dir.mkdir(parents=True, exist_ok=True)
            file_store = LocalFileStore(str(log_dir))
            rerun_log = LogStore(file_store, dir_path="events")

        local_tools_map = getattr(self.conv, "tools", {})
        action_count = 0
        
        for event in self.conv.state.events:
            if not isinstance(event, ActionEvent) or event.action is None:
                continue

            action_count += 1
            tool_name = event.tool_name
            tool = local_tools_map.get(tool_name)
            
            if tool is None:
                raise KeyError(f"Tool '{tool_name}' not found during rerun.")
            if not getattr(tool, "executor", None):
                log.warning(f"Skipping action {action_count}: tool '{tool_name}' has no physical executor")
                continue

            try:
                log.info(f"Rerunning action {action_count}: {tool_name}")
                observation = tool(event.action, self.conv)

                if rerun_log is not None and observation is not None:
                    rerun_log.append(event)
                    obs_event = ObservationEvent(
                        source="environment",
                        tool_name=tool_name,
                        tool_call_id=event.tool_call_id,
                        observation=observation,
                        action_id=event.id,
                    )
                    rerun_log.append(obs_event)
            except Exception as e:
                log.error(f"Action {action_count} ({tool_name}) failed during rerun: {e}")
                return False

        log.info(f"Rerun complete: {action_count} actions processed successfully")
        return True

    def close_executors(self) -> None:
        try:
            local_tools_map = getattr(self.conv, "tools", {})
        except (AttributeError, RuntimeError):
            return
            
        for tool in local_tools_map.values():
            try:
                if hasattr(tool, "as_executable"):
                    executable_tool = tool.as_executable()
                    executable_tool.executor.close()
            except NotImplementedError:
                continue
            except Exception as e:
                log.warning(f"Error closing executor for tool '{tool.name}': {e}")