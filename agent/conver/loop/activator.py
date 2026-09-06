# fiber.agent.conver.loop.activator
## @lineage: surgent.engine.loop.activator
from __future__ import annotations

import asyncio
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Generator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Any, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from fiber.llm.driver.model import LLMModel
from fiber.llm.driver.config.agent import PromptContext
from fiber.llm.model.message import (
    Message, MessageToolCall, ReasoningItemModel, 
    RedactedThinkingBlock, TextContent, ThinkingBlock
)
from fiber.llm.response import LLMResponse

# Surgent Schemas & Events
from fiber.agent.client.tool.schema.action import Action, Observation
from fiber.agent.client.tool.schema.builder import ActionDefinition
from fiber.agent.client.event.action import ActionEvent
from fiber.agent.client.event.message import MessageEvent
from fiber.agent.client.event.system import SystemPromptEvent, TokenEvent
from fiber.agent.client.event.observation import ObservationEvent, UserRejectObservation, AgentErrorEvent
from fiber.agent.conver.status import ConverStatus

# Surgent Core Actions & Engines
from fiber.agent.client.tool.action.factory import CoreAction
from fiber.agent.client.tool.action.resolver import ActionResolver
from fiber.agent.client.tool.action.parser import format_context_exceeded_message, ActionParser
from fiber.agent.conver.loop.step import StepHandler, StepContext
from fiber.agent.client.tool.mcp.factory import MCPClient, create_mcp_tools, MCPExecutor

# Xphi Arch & Kernel
from xphi.arch.model.conv.tool import Tool
from xphi.arch.model.conv.event import Event
from xphi.arch.model.payload import StreamPayloadAdapter
from xphi.arch.model.surge.disc import DiscMixin
from xphi.kernel.space.topos.node.state.compiler import StateCompiler
from xphi.kernel.space.topos.node.state.projector import StateProjector
from xphi.kernel.space.topos.node.state.schema import FragmentSig
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.watcher.plane.observer.span import observe, should_enable_observability
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("agent.activator")
INIT_STATE_PREFIX_SCAN_WINDOW = 3

class AgentStateSnapshot:
    def __init__(self, task_payload: dict):
        self.conversation_id = task_payload.get("conversation_id")
        self.iteration = task_payload.get("iteration", 0)
        raw_events = task_payload.get("events", [])
        self.events = []
        for e in raw_events:
            parsed = self._parse_event(e)
            if parsed:
                self.events.append(parsed)

    def _parse_event(self, data: dict) -> Event | None:
        """Robust event reconstruction parser"""
        try:
            if "system_prompt" in data:
                return SystemPromptEvent.model_validate(data)
            elif "action" in data and "tool_name" in data and "parameters" in data.get("action", {}):
                return ActionEvent.model_validate(data)
            elif "observation" in data and "tool_name" in data:
                return ObservationEvent.model_validate(data)
            elif "llm_message" in data:
                return MessageEvent.model_validate(data)
            elif "rejection_reason" in data:
                return UserRejectObservation.model_validate(data)
            elif "error" in data and "tool_name" in data:
                return AgentErrorEvent.model_validate(data)
            return Event.model_validate(data)
        except Exception as e:
            log.warning(f"Failed to parse event dict into Pydantic model: {e}")
            return None


# =====================================================================
# 2. Base Ator Interface
# =====================================================================
class Ator(DiscMixin, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    llm: LLMModel = Field(..., description="LLM configuration for the agent.")
    actions: list[Tool] = Field(
        default_factory=lambda: [Tool(name=action.value, params={}) for action in CoreAction],
        description="List of core cognitive and system control actions (e.g., finish, bridge, think).",
    )
    tools: list[Tool] = Field(
        default_factory=list,
        description="List of external capabilities schemas to load for the LLM.",
    )
    mcp_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional MCP configuration dictionary to create MCP tools (Agent-side managed).",
    )
    filter_tools_regex: str | None = Field(
        default=None,
        description="Optional regex to filter the external tools available to the agent by name. Core actions are immune.",
    )
    prompt_context: PromptContext = Field(
        default_factory=PromptContext,
        description="PromptContext to manage prompts, secrets, and environment.",
    )
    tool_concurrency_limit: int = Field(
        default=1,
        ge=1,
        description="Maximum number of tool calls to execute concurrently within a single agent step.",
    )
    runtime_tools: dict[str, ActionDefinition] = Field(default_factory=dict, exclude=True)
    is_initialized: bool = Field(default=False, exclude=True)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def initialize(self) -> None:
        if self.is_initialized:
            log.warning(f"[{self.name}] Agent already initialized; skipping re-initialization.")
            return

        resolved_defs: list[ActionDefinition] = []
        unique_specs: dict[str, Tool] = {}
        for spec in (self.actions + self.tools):
            unique_specs[spec.name] = spec

        combined_specs = self.actions + self.tools
        loop = asyncio.get_running_loop()
        
        def _resolve_sync():
            local_defs = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for spec in combined_specs:
                    futures.append(executor.submit(ActionResolver.resolve, spec, None))

                if self.mcp_config:
                    futures.append(executor.submit(create_mcp_tools, self.mcp_config, 30))

                for future in futures:
                    local_defs.extend(future.result())
            return local_defs

        resolved_defs = await loop.run_in_executor(None, _resolve_sync)

        if self.filter_tools_regex:
            pattern = re.compile(self.filter_tools_regex)
            resolved_defs = [
                tool for tool in resolved_defs 
                if CoreAction.is_safe_cognitive(tool.name) or pattern.match(tool.name)
            ]
            log.info(f"[{self.name}] Filtered tools: {[t.name for t in resolved_defs]}")

        for tool in resolved_defs:
            if not isinstance(tool, ActionDefinition):
                raise ValueError(f"Tool {tool} is not an instance of 'ActionDefinition'.")

        tool_names = [tool.name for tool in resolved_defs]
        if len(tool_names) != len(set(tool_names)):
            duplicates = set(name for name in tool_names if tool_names.count(name) > 1)
            raise ValueError(f"Duplicate capability names found: {duplicates}")

        self.runtime_tools = {tool.name: tool for tool in resolved_defs}
        self.is_initialized = True
        log.info(f"[{self.name}] Successfully initialized with capabilities: {list(self.runtime_tools.keys())}")

    async def run_worker(self, tunnel: UniversalFacade, conversation_id: str) -> None:
        if not self.is_initialized:
            await self.initialize()

        task_topic = f"agent:tasks:{conversation_id}"
        response_topic = f"agent:responses:{conversation_id}"
        group_name = f"agent_worker_group_{conversation_id}"
        consumer_name = f"worker_{id(self)}"

        log.info(f"[{self.name}] Worker started. Listening on {task_topic}")

        while True:
            try:
                results = await tunnel.stream_consume(
                    topic=task_topic, 
                    group=group_name, 
                    consumer=consumer_name, 
                    count=1, 
                    block=5000
                )
                
                if not results:
                    continue

                for stream_name, messages in results:
                    for message_id, message_data in messages:
                        log.debug(f"[{self.name}] Received task message: {message_id}")
                        try:
                            parsed_task = StreamPayloadAdapter.decode(message_data)
                            await self.process_task(parsed_task, tunnel, response_topic)
                        finally:
                            await tunnel.stream_ack(task_topic, group_name, message_id)
            except Exception as e:
                log.error(f"[{self.name}] Critical error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    @abstractmethod
    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        pass

    def get_active_mcp_clients(self) -> set[MCPClient]:
        clients: set[MCPClient] = set()
        for tool in self.runtime_tools.values():
            if hasattr(tool, 'executor') and isinstance(tool.executor, MCPExecutor):
                client = tool.executor.client
                clients.add(client)
        return clients

    def close(self) -> None:
        mcp_clients = self.get_active_mcp_clients()
        for client in mcp_clients:
            try:
                if not getattr(client, "is_closed", False):
                    log.debug(f"[{self.name}] Closing MCP Client explicitly...")
                    client.sync_close()
            except Exception as e:
                log.warning(f"Error while closing MCP client: {e}", exc_info=True)

    def verify(self, persisted: Ator, events: Sequence[Any] | None = None) -> Ator:
        if persisted.__class__ is not self.__class__:
            raise ValueError(
                f"Cannot load from persisted: persisted agent is of type {persisted.__class__.__name__}, "
                f"but self is of type {self.__class__.__name__}."
            )

        runtime_names = {t.name for t in self.actions + self.tools}
        persisted_names = {t.name for t in persisted.actions + persisted.tools}

        missing_in_runtime = persisted_names - runtime_names
        if missing_in_runtime:
            raise ValueError(f"Cannot resume conversation: capabilities were removed mid-conversation (removed: {sorted(missing_in_runtime)}).")
        return self

    def model_dump_succint(self, **kwargs):
        if "exclude_none" not in kwargs:
            kwargs["exclude_none"] = True
        dumped = super().model_dump(**kwargs)
        if "tools" in dumped and isinstance(dumped["tools"], dict):
            dumped["tools"] = list(dumped["tools"].keys())
        if "actions" in dumped and isinstance(dumped["actions"], dict):
            dumped["actions"] = list(dumped["actions"].keys())
        return dumped

    def get_all_llms(self) -> Generator[LLMModel]:
        yielded_ids: set[int] = set()
        visited: set[int] = set()

        def _walk(obj: object) -> Iterable[LLMModel]:
            oid = id(obj)
            if oid in visited:
                return ()
            visited.add(oid)

            if isinstance(obj, LLMModel):
                llm_out: list[LLMModel] = []
                if type(obj) is LLMModel and oid not in yielded_ids:
                    yielded_ids.add(oid)
                    llm_out.append(obj)
                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    llm_out.extend(_walk(val))
                return llm_out

            if isinstance(obj, BaseModel):
                model_out: list[LLMModel] = []
                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    model_out.extend(_walk(val))
                return model_out

            if isinstance(obj, dict):
                dict_out: list[LLMModel] = []
                for k, v in obj.items():
                    dict_out.extend(_walk(k))
                    dict_out.extend(_walk(v))
                return dict_out

            if isinstance(obj, (list, tuple, set, frozenset)):
                container_out: list[LLMModel] = []
                for item in obj:
                    container_out.extend(_walk(item))
                return container_out

            return ()
        yield from _walk(self)

    @property
    def tools_map(self) -> dict[str, ActionDefinition]:
        if not self.is_initialized:
            raise RuntimeError("Agent not initialized; call initialize() before use")
        return self.runtime_tools

    def ask(self, question: str) -> str | None:
        log.warning("Ator.ask() is deprecated in decoupled architecture.")
        return None


# =====================================================================
# 3. Activator Implementation
# =====================================================================
class Activator(Ator):
    """
    @desc: DAG and dynamic topology-based cognitive orchestrator.
    """
    step_handlers: list[StepHandler] = Field(default_factory=list, exclude=True)
    is_graph_mode: bool = Field(default=False, exclude=True)
    dag_materials: Dict[str, Any] = Field(default_factory=dict, exclude=True)

    def model_post_init(self, __context: object) -> None:
        super().model_post_init(__context)

    def set_step_handlers(self, handlers: list[StepHandler], is_graph_mode: bool = False) -> None:
        self.step_handlers = handlers
        self.is_graph_mode = is_graph_mode
    
    def is_running_in_graph_mode(self) -> bool:
        return self.is_graph_mode
    
    def harvest_and_evolve_graph(self, raw_schema: Dict[str, Any]) -> tuple[bool, Optional[dict], Optional[str]]:
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
            
            return True, runtime_specs, ir_sig.entry_point
        except Exception as e:
            log.error(f"[EVOLUTION:RUPTURE] Topos collapse: {e}")
            self.dag_materials.update({
                "evolution_status": "failed",
                "failure_fragments": str(e),
                "raw_schema_dump": raw_schema
            })
            return False, None, None

    @model_validator(mode="before")
    @classmethod
    def _enforce_security_prompt(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        ctx = data.setdefault("prompt_context", {})
        if isinstance(ctx, dict):
            kwargs = ctx.setdefault("system_prompt_kwargs", {})
            kwargs.setdefault("llm_security_analyzer", True)
        elif hasattr(ctx, "system_prompt_kwargs"):
            ctx.system_prompt_kwargs.setdefault("llm_security_analyzer", True)
        return data

    def _generate_system_prompt_event(self, secret_infos: list) -> SystemPromptEvent:
        has_browser = any(t.name == "browser" for t in self.tools)
        canonical_name = getattr(self.llm, "model_canonical_name", None)

        static_msg = self.prompt_context.get_static_system_message(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            has_browser_tool=has_browser
        )
        dynamic_msg = self.prompt_context.get_system_message_suffix(
            llm_model=self.llm.model,
            llm_model_canonical=canonical_name,
            additional_secret_infos=secret_infos,
        )

        return SystemPromptEvent(
            source="agent",
            system_prompt=TextContent(text=static_msg),
            tools=list(self.tools_map.values()),
            dynamic_context=TextContent(text=dynamic_msg) if dynamic_msg else None,
        )

    @observe(name="activator.process_task")
    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        log.info(f"[{self.name}] Initiating async step sequence for task iteration {task_payload.get('iteration')}")
        
        snapshot = AgentStateSnapshot(task_payload)
        context = StepContext()

        if snapshot.iteration == 0:
            sys_event = self._generate_system_prompt_event(secret_infos=task_payload.get("secret_infos", []))
            snapshot.events.insert(0, sys_event)
            await self._emit_event_to_gov(sys_event, tunnel, response_topic, snapshot)

        async def async_on_event(event: Any):
            if isinstance(event, Event):
                snapshot.events.append(event)
            await self._emit_event_to_gov(event, tunnel, response_topic, snapshot)

        for handler in self.step_handlers:
            if hasattr(handler, "handle_async"):
                handled = await handler.handle_async(self, snapshot, async_on_event, context)
            else:
                pending_tasks = []
                
                def _sync_on_event_wrapper(e):
                    task = asyncio.create_task(async_on_event(e))
                    pending_tasks.append(task)
                    return task
                
                handled = handler.handle(self, snapshot, _sync_on_event_wrapper, None, context)
                
                if pending_tasks:
                    await asyncio.gather(*pending_tasks)
                
            if handled:
                break

    async def _emit_event_to_gov(self, event: Any, tunnel: UniversalFacade, response_topic: str, snapshot: Optional[AgentStateSnapshot] = None) -> None:
        payload_raw = None
        current_topo = len(snapshot.events) if snapshot else 0
        current_tick = snapshot.iteration if snapshot else 0
        
        if isinstance(event, ActionEvent):
            current_press = getattr(event, "completion_tokens", 50) 
            payload_raw = {
                "type": "action",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": current_press, "rupture": False, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted Action: {event.tool_name}")
        elif isinstance(event, MessageEvent):
            current_press = getattr(event, "completion_tokens", 20)
            payload_raw = {
                "type": "message",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": current_press, "rupture": False, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted Message Event")
        elif isinstance(event, SystemPromptEvent):
            payload_raw = {
                "type": "system_prompt",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": 0, "rupture": False, "tick": current_tick}
            }
        elif isinstance(event, AgentErrorEvent):
            payload_raw = {
                "type": "error",
                "event_payload": event.model_dump(mode="json"),
                "_telemetry": {"topo": current_topo, "press": 0, "rupture": True, "tick": current_tick}
            }
            log.debug(f"[{self.name}] Emitted AgentErrorEvent")

        if getattr(event, "is_finish_signal", False) or type(event).__name__ == "TransitionStatus":
            payload_data = event.model_dump(mode="json") if hasattr(event, "model_dump") else str(event)
            payload_raw = {
                "type": "finish", 
                "event_payload": payload_data,
                "_telemetry": {
                    "topo": current_topo, 
                    "press": 0, 
                    "rupture": True,
                    "tick": current_tick
                }
            }
            log.debug(f"[{self.name}] Emitted Finish/Transition Signal (Loop Breaker)")

        if payload_raw:
            await tunnel.stream_produce(
                response_topic, 
                payload=StreamPayloadAdapter.encode(payload_raw)
            )

    def _get_action_event(
        self,
        tool_call: MessageToolCall,
        llm_response_id: str,
        snapshot: AgentStateSnapshot,
        security_analyzer: Any = None,
        thought: list[TextContent] | None = None,
        reasoning_content: str | None = None,
        thinking_blocks: list[ThinkingBlock | RedactedThinkingBlock] | None = None,
        responses_reasoning_item: ReasoningItemModel | None = None,
    ) -> tuple[ActionEvent | None, Event | None]:
        
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
            return None, error_event
        return action_event, None

    def _maybe_emit_vllm_tokens(self, llm_response: LLMResponse) -> TokenEvent | None:
        if self.llm.brane_extra_body.get("return_token_ids"):
            return TokenEvent(
                source="agent",
                prompt_token_ids=llm_response.raw_response["prompt_token_ids"],
                response_token_ids=llm_response.raw_response["choices"][0]["provider_specific_fields"]["token_ids"],
            )
        return None

    def _log_context_window_exceeded_warning(self) -> None:
        warning_msg = format_context_exceeded_message(self.llm.model)
        log.warning(f"[BOUND] Topos volume exceeded: {warning_msg}")