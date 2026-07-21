# atoa.activator
## @lineage: gov.activator
## @lineage: gov.engine.activator
from __future__ import annotations
from abc import ABC, abstractmethod
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Any, Optional
from pathlib import Path
from pydantic import Field, ValidationError, model_validator

from eco.call.event.base import Event
from eco.call.disc.action import Action, Observation
from eco.call.action.message import Message, MessageToolCall, ReasoningItemModel, RedactedThinkingBlock, TextContent, ThinkingBlock

from atoa.context.parser import format_context_exceeded_message, ActionParser
from atoa.disc.ator import Ator
from atoa.disc.event.llm.action import ActionEvent
from atoa.disc.event.llm.message import MessageEvent
from atoa.disc.event.llm.system import SystemPromptEvent, TokenEvent
from atoa.disc.event.llm.observation import ObservationEvent, UserRejectObservation, AgentErrorEvent
from atoa.disc.event.batch.action import ActionBatch
from atoa.disc.status import ConverStatus
from atoa.disc.base.conv import ProtoConv
from atoa.call.types import ConversationCallbackType, ConversationTokenCallbackType
from atoa.call.response import LLMResponse

from atoa.gov.action.step import StepHandler, StepContext
from atoa.gov.action.loop import PendingActionHandler, LLMInvocationHandler, ToolCallHandler, TextResponseHandler
from atoa.gov.action.tension import TensionHandler
from atoa.gov.action.eval import EvalReflector
from atoa.gov.action.evaluator import ActionEvaluator 

from atoa.context.gov.protocol import ConvStateProtocol
from atoa.context.gov.command import TransitionStatus
from atoa.gov.organizer import DagOrganizer

import atoa.security.analyzer as analyzer
import atoa.security.eval as risk
from atoa.call.action.factory import CoreAction

from xor.executor.parallel import ParallelExecutor

from arch.gov.state.compiler import StateCompiler
from arch.gov.state.projector import StateProjector
from arch.gov.state.schema import FragmentSig
from watcher.plane.observer.span import observe, should_enable_observability
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
INIT_STATE_PREFIX_SCAN_WINDOW = 3

class Activator(Ator):
    parallel_executor: ParallelExecutor = Field(default_factory=ParallelExecutor, exclude=True)
    step_handlers: list[StepHandler] = Field(default_factory=list, exclude=True)
    is_graph_mode: bool = Field(default=False, exclude=True)
    dag_materials: Dict[str, Any] = Field(default_factory=dict, exclude=True)
    evaluator: ActionEvaluator | None = Field(default=None, exclude=True)

    def model_post_init(self, __context: object) -> None:
        super().model_post_init(__context)
        self.parallel_executor = ParallelExecutor(max_workers=self.tool_concurrency_limit)
        self.step_handlers = self._build_default_handlers()
        if self.reflector:
            self.evaluator = ActionEvaluator(self.reflector)

    def _build_default_handlers(self) -> list[StepHandler]:
        """@desc: Synthesize baseline linear handlers."""
        return [
            PendingActionHandler(),
            TensionHandler(),
            EvalReflector(),
            LLMInvocationHandler(),
            ToolCallHandler(),
            TextResponseHandler(),
        ]
    
    def set_step_handlers(self, handlers: list[StepHandler]) -> None:
        """@desc: Inject custom traversal manifolds."""
        self.step_handlers = handlers
        self.is_graph_mode = False
    
    def mount_graph_topology(
        self, 
        runtime_specs: dict, 
        entry_point: str, 
        telemetry_path: Optional[Path] = None
    ) -> None:
        """@desc: Shift to Graph (DAG) orchestration."""
        log.info(f"[EVOLUTION] Mounting Graph Topology. Anchor: {entry_point}")
        graph_handler = DagOrganizer(
            runtime_specs=runtime_specs,
            entry_point=entry_point,
            telemetry_path=telemetry_path
        )
        self.step_handlers = [graph_handler]
        self.is_graph_mode = True

    def is_running_in_graph_mode(self) -> bool:
        return self.is_graph_mode
    
    def harvest_and_evolve_graph(self, raw_schema: Dict[str, Any], telemetry_path: Optional[Path] = None) -> bool:
        """@desc: Compile IR and evolve topology dynamically."""
        log.info("[EVOLUTION] Synthesizing graph substrate...")
        try:
            compiler = StateCompiler()
            ir_sig: FragmentSig = compiler.compile_from_schema(raw_schema)
            
            projector = StateProjector()
            runtime_specs = projector.project(ir_sig)
            
            self.dag_materials.update({
                "ir_signature": ir_sig,
                "runtime_specs": runtime_specs,
                "evolution_status": "success"
            })
            
            self.mount_graph_topology(runtime_specs, ir_sig.entry_point, telemetry_path)
            return True
        except Exception as e:
            log.error(f"[EVOLUTION:RUPTURE] Topos collapse: {e}")
            self.dag_materials.update({
                "evolution_status": "failed",
                "failure_fragments": str(e),
                "raw_schema_dump": raw_schema
            })
            return False

    def dump_evolution_materials(self) -> Dict[str, Any]:
        """@desc: Extract residual traces for diagnostics."""
        materials = self.dag_materials.copy()
        if self.is_graph_mode and self.step_handlers:
            organizer = self.step_handlers[0]
            if hasattr(organizer, 'session_traces'):
                materials["runtime_traces"] = organizer.session_traces
        return materials

    @model_validator(mode="before")
    @classmethod
    def _enforce_security_prompt(cls, data: Any) -> Any:
        """@desc: Enforce security validation rules via AgentContext."""
        if not isinstance(data, dict):
            return data
        
        ctx = data.setdefault("agent_context", {})
        if isinstance(ctx, dict):
            kwargs = ctx.setdefault("system_prompt_kwargs", {})
            kwargs.setdefault("llm_security_analyzer", True)
        elif hasattr(ctx, "system_prompt_kwargs"):
            ctx.system_prompt_kwargs.setdefault("llm_security_analyzer", True)
        return data

    # [개선점] 구체 클래스 대신 ConvStateProtocol 사용
    def init_state(self, state: ConvStateProtocol, on_event: ConversationCallbackType) -> None:
        """@desc: Bootstrap state and inject system prompts via AgentContext."""
        super().init_state(state, on_event=on_event)

        prefix_events = state.events[:INIT_STATE_PREFIX_SCAN_WINDOW] if state.events else []
        has_sys = any(isinstance(e, SystemPromptEvent) for e in prefix_events)
        
        if has_sys:
            log.debug(f"[BOOTSTRAP] Baseline anchored. Skip conv_id={state.id}.")
            return

        if any(isinstance(e, MessageEvent) and e.source == "user" for e in prefix_events):
            raise AssertionError(f"User stimulus prior to SystemPromptEvent. conv_id={state.id}")

        has_browser = any(t.name == "browser" for t in self.tools)
        canonical_name = getattr(self.llm, "model_canonical_name", None)

        static_msg = self.agent_context.get_static_system_message(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            has_browser_tool=has_browser
        )
        
        dynamic_msg = self.agent_context.get_system_message_suffix(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            additional_secret_infos=state.secret_registry.get_secret_infos(),
        )

        event = SystemPromptEvent(
            source="agent",
            system_prompt=TextContent(text=static_msg),
            tools=list(self.tools_map.values()),
            dynamic_context=TextContent(text=dynamic_msg) if dynamic_msg else None,
        )
        on_event(event)

    def _execute_actions(self, conversation: ProtoConv, action_events: list[ActionEvent], on_event: ConversationCallbackType) -> None:
        """@desc: Resolve concurrent action nodes."""
        state = conversation.state
        
        def check_refinement(ae: ActionEvent) -> tuple[bool, str | None]:
            if self.evaluator:
                return self.evaluator.check_iterative_refinement(conversation, ae)
            return False, None

        batch = ActionBatch.prepare(
            action_events,
            state=state,
            executor=self.parallel_executor,
            tool_runner=lambda ae: self._execute_action_event(conversation, ae),
            tools=self.tools_map,
        )
        batch.emit(on_event)
        
        # [개선점] setattr 우회 기법 제거 및 명시적인 Command 패턴(apply) 사용
        batch.finalize(
            on_event=on_event,
            check_iterative_refinement=check_refinement,
            mark_finished=lambda: state.apply(
                TransitionStatus(new_status=ConverStatus.FINISHED, reason="Action batch execution finalized")
            ),
        )
    
    @observe(name="agent.step", ignore_inputs=["state", "on_event"])
    def step(
        self,
        conversation: ProtoConv,
        on_event: ConversationCallbackType,
        on_token: ConversationTokenCallbackType | None = None,
    ) -> None:
        """@desc: Traverse step handlers."""
        log.info("[PHASE] Initiating step sequence.")
        context = StepContext()
        for handler in self.step_handlers:
            if handler.handle(self, conversation, on_event, on_token, context):
                break

    def _requires_user_confirmation(self, state: ConvStateProtocol, action_events: list[ActionEvent]) -> bool:
        """@desc: Evaluate execution risks."""
        if not action_events or (len(action_events) == 1 and CoreAction.is_safe_cognitive(action_events[0].tool_name)):
            return False

        risks = (
            [risk for _, risk in state.security_analyzer.analyze_pending_actions(action_events)]
            if state.security_analyzer else [risk.SecurityRisk.UNKNOWN] * len(action_events)
        )

        if any(state.confirmation_policy.should_confirm(r) for r in risks):
            # [개선점] 직접 할당 금지, Command 패턴 강제
            state.apply(TransitionStatus(new_status=ConverStatus.WAITING_FOR_USER, reason="Action exceeds security confirmation threshold"))
            return True
        return False

    def _get_action_event(
        self,
        tool_call: MessageToolCall,
        conversation: ProtoConv,
        llm_response_id: str,
        on_event: ConversationCallbackType,
        security_analyzer: analyzer.SecurityAnalyzerBase | None = None,
        thought: list[TextContent] | None = None,
        reasoning_content: str | None = None,
        thinking_blocks: list[ThinkingBlock | RedactedThinkingBlock] | None = None,
        responses_reasoning_item: ReasoningItemModel | None = None,
    ) -> ActionEvent | None:
        """@desc: Parse tools into Action Events."""
        action_event, error_event = ActionParser.parse_tool_call(
            tool_call=tool_call,
            tools_map=self.tools_map,
            llm_response_id=llm_response_id,
            security_analyzer=security_analyzer,
            thought=thought,
            reasoning_content=reasoning_content,
            thinking_blocks=thinking_blocks,
            responses_reasoning_item=responses_reasoning_item,
        )

        if error_event:
            on_event(action_event)
            on_event(error_event)
            return None

        if self.evaluator and self.evaluator.should_evaluate(action_event.tool_name):
            reflector_result = self.evaluator.evaluate(conversation, action_event)
            if reflector_result:
                action_event = action_event.model_copy(update={"reflector_result": reflector_result})

        on_event(action_event)
        return action_event

    @observe()
    def _execute_action_event(self, conversation: ProtoConv, action_event: ActionEvent) -> list[Event]:
        """@desc: Resolve localized tool logic."""
        tool = self.tools_map.get(action_event.tool_name)
        if not tool:
            raise RuntimeError(f"[FATAL] Node '{action_event.tool_name}' missing.")

        try:
            if should_enable_observability():
                tool_name = ActionParser.extract_action_name(action_event)
                observation = observe(name=tool_name, span_type="TOOL")(tool)(action_event.action, conversation)
            else:
                observation = tool(action_event.action, conversation)
            assert isinstance(observation, Observation), "Tool must yield valid Observation."
        except ValueError as e:
            err = f"Structural fault in '{tool.name}': {e}"
            log.warning(f"[RUPTURE] {err}")
            return [AgentErrorEvent(error=err, tool_name=tool.name, tool_call_id=action_event.tool_call.id)]

        return [ObservationEvent(
            observation=observation,
            action_id=action_event.id,
            tool_name=tool.name,
            tool_call_id=action_event.tool_call.id,
        )]

    def _maybe_emit_vllm_tokens(self, llm_response: LLMResponse, on_event: ConversationCallbackType) -> None:
        """@desc: Broadcast localized vLLM token metrics."""
        if self.llm.brane_extra_body.get("return_token_ids"):
            on_event(TokenEvent(
                source="agent",
                prompt_token_ids=llm_response.raw_response["prompt_token_ids"],
                response_token_ids=llm_response.raw_response["choices"][0]["provider_specific_fields"]["token_ids"],
            ))

    def _log_context_window_exceeded_warning(self) -> None:
        """@desc: Warn if context bounds exceeded."""
        warning_msg = format_context_exceeded_message(self.llm.model)
        log.warning(f"[BOUND] Topos volume exceeded: {warning_msg}")