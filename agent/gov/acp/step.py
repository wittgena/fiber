# agent.gov.acp.step
## @lineage: atoa.gov.acp.step
## @lineage: gov.acp.step
## @lineage: gov.policy.acp.step
from __future__ import annotations
import asyncio
import json
import time
import uuid
from typing import TYPE_CHECKING, Any

from acp.exceptions import RequestError as ACPRequestError
from bound.resolver.acp.schema import PromptResponse

from acp.helpers import text_block
from watcher.plane.observer.span import observe, unified_flow_span
from watcher.plane.emitter import get_emitter

from agent.atoa.disc.event.acp import ACPToolCallEvent
from agent.atoa.disc.event.llm.message import MessageEvent
from agent.atoa.disc.event.llm.action import ActionEvent
from agent.atoa.disc.event.llm.observation import ObservationEvent
from agent.atoa.disc.event.conv.error import ConversationErrorEvent

from agent.atoa.disc.status import ConverStatus
from agent.eco.action.message import Message, MessageToolCall, TextContent
from agent.atoa.action.factory import CoreAction

from agent.gov.acp.support import (
    _USAGE_UPDATE_TIMEOUT,
    _ACP_PROMPT_MAX_RETRIES,
    _ACP_PROMPT_RETRY_DELAYS,
    _RETRIABLE_CONNECTION_ERRORS,
    _RETRIABLE_SERVER_ERROR_CODES,
)

if TYPE_CHECKING:
    from agent.atoa.types import ConversationCallbackType, ConversationTokenCallbackType
    from agent.atoa.disc.base.conv import EngineContextProtocol

log = get_emitter(name="acp.step", phase="agent_execution")

class ACPTrajectory:
    """
    @desc: Executional Manifold / Step Trajectory Injector
    @role: Orchestrates isolated topological steps, merging external ACP resonance into the internal space.
    """
    @observe(name="acp_agent.step", ignore_inputs=["conversation", "on_event"])
    def step(
        self,
        conversation: "EngineContextProtocol",
        on_event: ConversationCallbackType,
        on_token: ConversationTokenCallbackType | None = None,
    ) -> None:
        state = conversation.state

        with unified_flow_span(
            session_id=self._session_id,
            agent_name=self._agent_name,
            agent_version=self._agent_version
        ):
            log.debug("## @phase.execution: Initiating step traversal in ACP Manifold")

            ## @phase.extraction: Isolate the terminal user stimulus from the event topology
            user_message = self._extract_terminal_stimulus(state.events)
            if not user_message:
                log.warning("[TERMINAL] No operational stimulus found; collapsing execution space.")
                state.execution_status = ConverStatus.FINISHED
                return

            ## @phase.reset: Flush accumulators for a clean trajectory
            self._client.reset()
            self._client.on_token = on_token
            self._client.on_activity = self._on_activity

            t0 = time.monotonic()
            try:
                log.info(f"## @phase.resonance: Projecting stimulus to ACP Server (Timeout: {self.acp_prompt_timeout}s)")
                
                ## Project into the execution space via resilient retry manifold
                response = self._execute_with_retries(user_message, on_token)
                elapsed = time.monotonic() - t0
                
                log.info(f"[CONVERGED] Prompt resonance achieved in {elapsed:.1f}s")

                ## @phase.synchronize: Record metrics and structural resource consumption
                session_id = self._session_id or ""
                usage_update = self._client.pop_turn_usage_update(session_id)
                self._record_usage(response, session_id, elapsed=elapsed, usage_update=usage_update)

                ## @phase.mapping: Translate internal ACP tool nodes into local system events
                self._emit_tool_call_events(on_event)

                ## @phase.terminate: Package final output and gracefully sever the topological loop
                # 동적 도구 추출을 위해 conversation 객체를 넘겨줌
                self._emit_terminal_events(on_event, conversation)
                state.execution_status = ConverStatus.FINISHED

            except TimeoutError:
                self._handle_timeout_rupture(t0, on_event, state)
            except Exception as e:
                self._handle_catastrophic_rupture(e, on_event, state)

    # --------------------------------------------------------------------------
    # Sub-Routines for Structural Isolation (Internal Mechanics)
    # --------------------------------------------------------------------------

    def _extract_terminal_stimulus(self, events: list) -> str | None:
        """@desc: Traverse the event topology backwards to locate the latest user injection."""
        for event in reversed(events):
            if isinstance(event, MessageEvent) and event.source == "user":
                for content in event.llm_message.content:
                    if isinstance(content, TextContent) and content.text.strip():
                        return content.text
        return None

    async def _async_prompt(self, user_message: str) -> PromptResponse:
        """@desc: Core asynchronous communication vector for the ACP Prompt."""
        session_id = self._session_id or ""
        usage_sync = self._client.prepare_usage_sync(session_id)
        
        response = await self._conn.prompt(
            [text_block(user_message)],
            session_id,
        )
        
        # Verify telemetry alignment
        if self._client.get_turn_usage_update(session_id) is None:
            try:
                await asyncio.wait_for(usage_sync.wait(), timeout=_USAGE_UPDATE_TIMEOUT)
            except TimeoutError:
                log.warning(f"[LATENCY] UsageUpdate sync failed within {_USAGE_UPDATE_TIMEOUT}s.")
                
        return response

    def _execute_with_retries(self, user_message: str, on_token: Any) -> PromptResponse:
        """@desc: Resilient loop evaluating network and structural faults before projecting."""
        max_retries = _ACP_PROMPT_MAX_RETRIES

        # Safe closure ensuring compatibility with AsyncExecutor signatures
        async def _prompt() -> PromptResponse:
            return await self._async_prompt(user_message)
        
        for attempt in range(max_retries + 1):
            try:
                return self._executor.run_async(_prompt, timeout=self.acp_prompt_timeout)
            except TimeoutError:
                raise
            except (_RETRIABLE_CONNECTION_ERRORS, ACPRequestError) as e:
                # Consolidate overlapping retry logic for Network and Protocol errors
                is_retriable = True
                fault_type = "Connection Fractured"
                
                if isinstance(e, ACPRequestError):
                    if e.code not in _RETRIABLE_SERVER_ERROR_CODES:
                        is_retriable = False
                    fault_type = f"Server Topology Fault [{e.code}]"
                    
                if is_retriable and attempt < max_retries:
                    delay = _ACP_PROMPT_RETRY_DELAYS[min(attempt, len(_ACP_PROMPT_RETRY_DELAYS) - 1)]
                    log.warning(f"[RETRY] {fault_type}. Re-initiating in {delay}s (Attempt {attempt+1}/{max_retries+1}): {e}")
                    
                    time.sleep(delay)
                    self._client.reset()
                    self._client.on_token = on_token
                else:
                    raise
        
        raise RuntimeError("[FATAL] Retry manifold exhausted without achieving resonance.")

    def _emit_tool_call_events(self, on_event: ConversationCallbackType) -> None:
        """@desc: Emit localized events translated from external ACP structural nodes."""
        for tc in self._client.accumulated_tool_calls:
            tc_event = ACPToolCallEvent(
                tool_call_id=tc["tool_call_id"],
                title=tc["title"],
                status=tc.get("status"),
                tool_kind=tc.get("tool_kind"),
                raw_input=tc.get("raw_input"),
                raw_output=tc.get("raw_output"),
                content=tc.get("content"),
                is_error=tc.get("status") == "failed",
            )
            on_event(tc_event)

    def _emit_terminal_events(
        self, 
        on_event: ConversationCallbackType, 
        conversation: "EngineContextProtocol"
    ) -> None:
        """@desc: Conclude the topological step and emit final convergence vectors."""
        response_text = "".join(self._client.accumulated_text) or "(No response from ACP server)"
        thought_text = "".join(self._client.accumulated_thoughts)

        message = Message(
            role="assistant",
            content=[TextContent(text=response_text)],
            reasoning_content=thought_text if thought_text else None,
        )
        on_event(MessageEvent(source="agent", llm_message=message))
        
        tc_id = str(uuid.uuid4())
        
        # ----------------------------------------------------------------------
        # 에이전트에 바인딩된 실제 동적 도구 추출 (ActionProxy 패턴 호환)
        # ----------------------------------------------------------------------
        finish_tool = conversation.ator.tools_map.get(CoreAction.FINISH)
        
        if not finish_tool:
            log.warning(f"Tool '{CoreAction.FINISH}' not found in tools_map. Falling back to generic schema.")
            from agent.atoa.disc.schema.action import Action, Observation
            class DynamicFinishAction(Action): summary: str
            class DynamicFinishObservation(Observation): pass
        else:
            # ActionProxy 인스턴스(_action_type) 또는 ActionDefinition 타입(action_type) 양쪽 모두 지원
            DynamicFinishAction = getattr(finish_tool, "action_type", getattr(finish_tool, "_action_type", None))
            DynamicFinishObservation = getattr(finish_tool, "observation_type", getattr(finish_tool, "_obs_type", None))
            
            if not DynamicFinishAction or not DynamicFinishObservation:
                raise RuntimeError(f"Could not resolve schema for dynamic tool: {CoreAction.FINISH}")

        action_event = ActionEvent(
            source="agent",
            thought=[],
            action=DynamicFinishAction(summary=response_text),  
            tool_name=CoreAction.FINISH,
            tool_call_id=tc_id,
            tool_call=MessageToolCall(
                id=tc_id,
                name=CoreAction.FINISH,
                arguments=json.dumps({"summary": response_text}),
                origin="completion",
            ),
            llm_response_id=str(uuid.uuid4()),
        )
        on_event(action_event)
        
        # 동적 합성된 Observation이 from_text를 지원하면 사용, 아니면 기본 생성
        obs_instance = (
            DynamicFinishObservation.from_text(text=response_text) 
            if hasattr(DynamicFinishObservation, 'from_text') 
            else DynamicFinishObservation()
        )
        
        on_event(ObservationEvent(
            observation=obs_instance,
            action_id=action_event.id,
            tool_name=CoreAction.FINISH,
            tool_call_id=tc_id,
        ))

    def _handle_timeout_rupture(self, start_time: float, on_event: ConversationCallbackType, state: Any) -> None:
        """@desc: Handle temporal decoupling (Timeout) from the ACP Sub-manifold."""
        elapsed = time.monotonic() - start_time
        log.error(f"[RUPTURE] ACP execution timed out after {elapsed:.1f}s. Sub-manifold decoupled.")
        
        error_message = Message(
            role="assistant",
            content=[TextContent(text=f"ACP prompt timed out after {elapsed:.0f}s. Signal lost.")],
        )
        on_event(MessageEvent(source="agent", llm_message=error_message))
        state.execution_status = ConverStatus.ERROR

    def _handle_catastrophic_rupture(self, e: Exception, on_event: ConversationCallbackType, state: Any) -> None:
        """@desc: Handle absolute structural collapse during the execution cycle."""
        log.error(f"[RUPTURE] Catastrophic collapse in ACP trajectory: {e}", exc_info=True)
        error_str = str(e)
        
        on_event(MessageEvent(
            source="agent", 
            llm_message=Message(role="assistant", content=[TextContent(text=f"ACP error: {e}")])
        ))
        
        is_aup = "usage policy" in error_str.lower() or "content policy" in error_str.lower()
        on_event(ConversationErrorEvent(
            source="agent",
            code="UsagePolicyRefusal" if is_aup else "ACPPromptError",
            detail=error_str[:500],
        ))
        state.execution_status = ConverStatus.ERROR
        raise