# ator.runtime.loop.executor
## @lineage: ator.agent.loop.executor
## @lineage: ator.topos.loop.executor
## @lineage: agent.runtime.loop.executor
import asyncio
import json
from typing import Optional, Dict, Any, List, Callable

from ator.runtime.space.manager import SandboxWorkspace, SandboxProxy 
from ator.conv.protocol.tool.terminal import TerminalTool
from ator.runtime.activator import Activator
from ator.conv.context.prompt import BlueprintCompiler

from ator.conv.schema.event import LLMConvertibleEvent
from ator.conv.context.adapter import AgentCommunicator, ExecutionController
from ator.conv.state import ConversationState
from eco.bound.xor.visual.context import ConversationVisualizer
from ator.runtime.action.factory import CoreAction
from ator.runtime.action.resolver import ActionResolver
from ator.driver.schema.tool import Tool

from arch.contract.model.graph import EntryNode
from arch.topos.node.gan import Message, GanNode
from arch.contract.event.next import next_id
from arch.topos.tunnel.factory import TunnelFactory
from arch.topos.node.event import AgentConfigured, LLMEventMessage, TaskCompletedMessage
from kernel.bind.resolver import resolve_path
from watcher.tracer.phase.router import InfraRouter
from watcher.plane.emitter import get_emitter

log = get_emitter("loop.executor")
WORKSPACE_ROOT = resolve_path("workspace")

CUSTOM_SECURITY_POLICY = (
    "# Security Risk Policy\n"
    "Assess the safety risk of your actions:\n"
    "- **LOW**: Read-only actions (viewing files, documentation).\n"
    "- **MEDIUM**: Modifications (file edits, package installs).\n"
    "- **HIGH**: Dangerous actions (network access, system changes).\n\n"
    "Always prioritize data integrity and user intent."
)

class LocalGovContext:
    """Lightweight runtime wrapper connecting ConversationState and Gov Engine."""
    def __init__(self, state: ConversationState, tools: dict, callbacks: list[Callable] = None):
        self.state = state
        self.tools = tools
        self.id = state.id
        self.conversation_stats = state.stats
        self._cleanup_initiated = False
        self.ator = None 
        self.llm_registry = None 
        self._callbacks = callbacks or []

    def _on_event(self, event: Any) -> None:
        self.state.events.append(event)
        for cb in self._callbacks:
            if cb:
                try:
                    cb(event)
                except Exception as e:
                    log.warning(f"Error executing callback {cb}: {e}")
        
    def _end_observability_span(self):
        pass

class LocalExecutionEngine:
    def __init__(self, instruction: str, settings: Any, on_stream: Callable[[Any], None]):
        self.instruction = instruction
        self.settings = settings
        self.on_stream = on_stream
        self.conv_id = next_id()
        self.visualizer = ConversationVisualizer()

    async def execute(self) -> float:
        tunnel = await TunnelFactory.get_default()
        
        # 1. Setup External Tools & Activator
        real_tools = self._prepare_external_tools()
        activator = Activator(llm=self.settings.llm, tools=real_tools)
        await activator.initialize()

        # 2. Setup Gov Environment (Workspace & State)
        workspace = SandboxWorkspace(working_dir=str(WORKSPACE_ROOT))
        conv_state = ConversationState.create(id=self.conv_id, workspace=workspace, agent_id=activator.name)
        
        # 3. Prepare Gov Tools
        resolved_gov_tools = self._prepare_gov_tools(real_tools, conv_state)

        # 4. Bind Callbacks and Controllers
        gov_context = LocalGovContext(
            state=conv_state, 
            tools=resolved_gov_tools, 
            callbacks=[self._dispatch_visual]
        )
        communicator = AgentCommunicator(gov_context)
        controller = ExecutionController(gov_context)

        # 5. Run Orchestration Loop
        return await self._run_orchestrator(activator, tunnel, conv_state, communicator, controller)

    def _prepare_external_tools(self) -> List[Tool]:
        real_tools = []
        ActionResolver.register("terminal", lambda params, state: TerminalTool.create(conv_state=state, **(params or {})))
        real_tools.append(Tool(name="terminal", params={}))
        
        core_action_names = {action.value for action in CoreAction}
        for tool_name in ActionResolver.list_routes():
            if tool_name not in core_action_names:
                real_tools.append(Tool(name=tool_name, params={}))
                
        log.debug(f"[Local Strategy] Provisioned External Tools: {[t.name for t in real_tools]}")
        return real_tools

    def _prepare_gov_tools(self, real_tools: List[Tool], conv_state: ConversationState) -> Dict[str, Any]:
        resolved_gov_tools = {}
        all_gov_specs = real_tools.copy()
        
        for ca in CoreAction:
            all_gov_specs.append(Tool(name=ca.value, params={}))
            
        for spec in all_gov_specs:
            resolved_defs = ActionResolver.resolve(spec, conv_state)
            for r in resolved_defs:
                resolved_gov_tools[r.name] = r
                
        return resolved_gov_tools

    def _dispatch_visual(self, e: Any):
        self.visualizer(e)
        if self.on_stream:
            content = getattr(e, 'content', None)
            if content:
                self.on_stream(content)
            elif hasattr(e, 'llm_message') and getattr(e, 'source', None) == 'activator':
                for c in getattr(e.llm_message, 'content', []):
                    if getattr(c, 'type', '') == 'text':
                        self.on_stream(getattr(c, 'text', ''))

    async def _run_orchestrator(self, activator, tunnel, conv_state, communicator, controller) -> float:
        # Initial state rendering
        for e in conv_state.events:
            self.visualizer(e)
            
        # Start background worker
        worker_task = asyncio.create_task(activator.run_worker(tunnel, str(self.conv_id)))
        communicator.send_message(self.instruction)
        
        try:
            await controller.run()
        finally:
            worker_task.cancel()
            await controller.close()
        
        stats = conv_state.stats
        metrics = stats.get_combined_metrics() if stats else None
        return metrics.accumulated_cost if metrics else 0.0


class ProxyExecutionEngine:
    """Delegates execution to a remote Proxy Engine (Integrated via WebSocket)."""
    
    def __init__(self, instruction: str, proxy_config: Dict[str, Any], on_stream: Callable[[str], None]):
        self.instruction = instruction
        self.proxy_config = proxy_config
        self.on_stream = on_stream

    async def execute(self) -> float:
        host_url = self.proxy_config.get("server_url")
        conv_id = self.proxy_config.get("workspace_ref")
        session_api_key = self.proxy_config.get("session_api_key")
        router = InfraRouter(host_url, session_api_key)
        workspace = SandboxProxy(
            host_url=host_url, 
            workspace_ref=conv_id,
            session_api_key=session_api_key
        )

        ws_path = router.get_ws_endpoint("events", conversation_id=conv_id)
        log.info(f"[ProxyExecutionEngine] Connecting to remote proxy: {ws_path}")
        try:
            async with workspace.connect_ws(ws_path) as ws:
                # 1. 원격 프록시에 프롬프트 전송
                request_msg = {"role": "user", "content": self.instruction}
                await ws.send(json.dumps(request_msg))

                # 2. 스트리밍 응답 수신 루프
                async for response_str in ws:
                    event_data: Dict[str, Any] = json.loads(response_str)

                    # 에러 처리
                    if "code" in event_data and "detail" in event_data:
                        error_msg = f"[Error {event_data['code']}] {event_data['detail']}"
                        log.error(error_msg)
                        self.on_stream(error_msg)
                        break

                    # 기존 NotificationAtor가 담당하던 엣지 디바이스/보안 로깅을 심플하게 대체
                    if event_data.get("status") == "need_approval" or "tool_name" in event_data:
                        tool_name = event_data.get("tool_name", "Unknown")
                        risk = event_data.get("security_risk", "High")
                        log.warning(f"⚠️ [Proxy Guard] Action required/blocked -> Tool: {tool_name}, Risk: {risk}")

                    content = event_data.get("content", "")
                    if content:
                        self.on_stream(content)
                        
                    if event_data.get("event_type") == "conversation_ended":
                        break
                        
        except Exception as e:
            error_msg = f"Proxy Connection Error: {str(e)}"
            log.error(error_msg)
            self.on_stream(error_msg)

        return 0.0

class LoopExecutor(GanNode):
    def __init__(self, name: str):
        super().__init__(name)
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.current_context: Optional[EntryNode] = None

    async def on_boot(self, message: Message):
        self.main_loop = asyncio.get_running_loop()
        log.info(f"[{self.name}] Policy context loaded in-memory successfully.")
        self.post_message(AgentConfigured())

    async def on_set_context(self, message: Message):
        self.current_context = getattr(message, 'entry_node', None)
        if self.current_context:
            log.info(f"[{self.name}] 🧩 Context locked: [{self.current_context.entry}] (Focus: {self.current_context.focus})")

    async def on_execute_events(self, message: Message):
        events: List[Any] = getattr(message, 'events', []) 
        settings = getattr(message, 'settings', None)
        sys_inst = getattr(message, 'system_instructions', "")
        
        if not settings or not settings.llm:
            log.error(f"[{self.name}] Execution failed: Missing configured primitives.")
            return self.post_message(TaskCompletedMessage(0.0))
            
        if not events:
            log.warning(f"[{self.name}] No nodes provided in blueprint. Bypassing execution.")
            return self.post_message(TaskCompletedMessage(0.0))

        combined_instructions = f"{sys_inst}\n\n{CUSTOM_SECURITY_POLICY}".strip()
        master_instruction = BlueprintCompiler.compile(self.current_context, events, combined_instructions)
        
        await self._route_execution(master_instruction, settings)

    async def on_run_conversation(self, message: Message):
        instruction = getattr(message, 'instruction', "")
        settings = getattr(message, 'settings', None)
        if not settings or not settings.llm:
            log.error(f"[{self.name}] Execution failed: Missing configured primitives.")
            return

        combined_instruction = f"{CUSTOM_SECURITY_POLICY}\n\n{instruction}".strip()
        await self._route_execution(combined_instruction, settings)

    async def _route_execution(self, instruction: str, settings: Any):
        """@desc: Routes the compiled instruction to the appropriate execution strategy engine."""
        is_proxy = isinstance(settings.llm, dict) and settings.llm.get("is_proxy") is True
        def emit_stream(chunk: str):
            self.main_loop.call_soon_threadsafe(self.post_message, LLMEventMessage(chunk))
            
        try:
            if is_proxy:
                log.info(f"[{self.name}] 🌐 [Proxy Mode] Delegating execution to remote proxy.")
                engine = ProxyExecutionEngine(instruction, settings.llm, emit_stream)
            else:
                log.info(f"[{self.name}] 💻 [Local Mode] Deploying isolated async engine loop.")
                engine = LocalExecutionEngine(instruction, settings, emit_stream)
            cost = await engine.execute()
            log.info(f"[{self.name}] Execution converged (Cost: {cost}).")
            self.post_message(TaskCompletedMessage(cost))
            
        except Exception as e:
            error_msg = str(e)
            if "iterations limit reached" in error_msg.lower() or "stuck" in error_msg.lower():
                log.warning(f"[{self.name}] Agent execution halted (Task Failed): {error_msg}")
                self.post_message(TaskCompletedMessage(0.0))
            else:
                log.error(f"[{self.name}] System Execution disrupted: {e}", exc_info=True)
                self.post_message(Message("shutdown", bubble=True))

    async def on_shutdown(self, message: Message):
        log.info(f"[{self.name}] Shutting down PolicyNode...")
        self._running = False
        self._queue.put_nowait(None)