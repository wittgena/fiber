# fiber.agent.client.engine.executor
## @lineage: surgent.engine.executor
import asyncio
import json
from typing import Dict, Any, List, Callable

from fiber.agent.client.engine.adapter import AgentCommunicator, ExecutionController
from fiber.agent.conver.loop.activator import Activator
from fiber.agent.conver.state import ConversationState
from fiber.agent.conver.conv.visual import ConversationVisualizer
from fiber.agent.conver.space.manager import SandboxWorkspace
from fiber.agent.conver.loop.organizer import EvalReflector, TensionHandler, LLMInvocationHandler, ToolCallHandler, TextResponseHandler
from fiber.agent.client.tool.schema.terminal import TerminalTool
from fiber.agent.client.tool.action.factory import CoreAction
from fiber.agent.client.tool.action.resolver import ActionResolver

from xphi.bound.space.manager import SandboxProxy 
from xphi.arch.model.conv.tool import Tool
from xphi.arch.event.next import next_id
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.tracer.infra.router import InfraRouter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("engine.executor")
WORKSPACE_ROOT = resolve_path("workspace")

DEFAULT_SECURITY_POLICY = (
    "# Security Risk Policy\n"
    "Assess the safety risk of your actions:\n"
    "- **LOW**: Read-only actions (viewing files, documentation).\n"
    "- **MEDIUM**: Modifications (file edits, package installs).\n"
    "- **HIGH**: Dangerous actions (network access, system changes).\n\n"
    "Always prioritize data integrity and user intent."
)

class LocalGovContext:
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
    def __init__(self, agent_name: str, instruction: str, settings: Any, on_stream: Callable[[Any], None]):
        self.agent_name = agent_name
        self.instruction = instruction
        self.settings = settings
        self.on_stream = on_stream
        
        self.conv_id = getattr(settings, "shared_conv_id", next_id())
        self.allowed_tools = getattr(settings, "allowed_tools", None)
        self.visualizer = ConversationVisualizer()

    async def execute(self) -> float:
        tunnel = await TunnelFactory.get_default()
        real_tools = self._prepare_external_tools()
        
        activator = Activator(llm=self.settings.llm, tools=real_tools)
        await activator.initialize()
        default_handlers = [
            TensionHandler(), EvalReflector(), LLMInvocationHandler(),
            ToolCallHandler(), TextResponseHandler()
        ]
        activator.set_step_handlers(default_handlers)

        workspace = SandboxWorkspace(working_dir=str(WORKSPACE_ROOT))
        conv_state = ConversationState.create(id=self.conv_id, workspace=workspace, agent_id=self.agent_name)
        
        resolved_gov_tools = self._prepare_gov_tools(real_tools, conv_state)

        gov_context = LocalGovContext(state=conv_state, tools=resolved_gov_tools, callbacks=[self._dispatch_visual])
        communicator = AgentCommunicator(gov_context)
        controller = ExecutionController(gov_context)
        
        return await self._run_orchestrator(activator, tunnel, conv_state, communicator, controller)

    def _prepare_external_tools(self) -> List[Tool]:
        real_tools = []
        ActionResolver.register("terminal", lambda params, state: TerminalTool.create(conv_state=state, **(params or {})))
        
        all_possible_tools = [Tool(name="terminal", params={})]
        core_action_names = {action.value for action in CoreAction}
        for tool_name in ActionResolver.list_routes():
            if tool_name not in core_action_names:
                all_possible_tools.append(Tool(name=tool_name, params={}))
                
        if self.allowed_tools is not None:
            real_tools = [t for t in all_possible_tools if t.name in self.allowed_tools]
        else:
            real_tools = all_possible_tools
            
        log.debug(f"[{self.agent_name} - Local] Provisioned External Tools: {[t.name for t in real_tools]}")
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
        for e in conv_state.events:
            self.visualizer(e)
            
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
    def __init__(self, agent_name: str, instruction: str, proxy_config: Dict[str, Any], on_stream: Callable[[str], None]):
        self.agent_name = agent_name
        self.instruction = instruction
        self.proxy_config = proxy_config
        self.on_stream = on_stream
        self.conv_id = self.proxy_config.get("shared_conv_id", self.proxy_config.get("workspace_ref"))

    async def execute(self) -> float:
        host_url = self.proxy_config.get("server_url")
        session_api_key = self.proxy_config.get("session_api_key")
        router = InfraRouter(host_url, session_api_key)
        
        workspace = SandboxProxy(host_url=host_url, workspace_ref=self.conv_id, session_api_key=session_api_key)
        ws_path = router.get_ws_endpoint("events", conversation_id=self.conv_id)
        
        log.info(f"[{self.agent_name} - Proxy] Connecting to remote proxy: {ws_path}")
        try:
            async with workspace.connect_ws(ws_path) as ws:
                request_msg = {"role": "user", "content": self.instruction}
                await ws.send(json.dumps(request_msg))

                async for response_str in ws:
                    event_data: Dict[str, Any] = json.loads(response_str)

                    if "code" in event_data and "detail" in event_data:
                        error_msg = f"[Error {event_data['code']}] {event_data['detail']}"
                        log.error(f"[{self.agent_name}] {error_msg}")
                        self.on_stream(error_msg)
                        break

                    if event_data.get("status") == "need_approval" or "tool_name" in event_data:
                        tool_name = event_data.get("tool_name", "Unknown")
                        risk = event_data.get("security_risk", "High")
                        log.warning(f"⚠️ [{self.agent_name} Guard] Action required/blocked -> Tool: {tool_name}, Risk: {risk}")

                    if content := event_data.get("content", ""):
                        self.on_stream(content)
                        
                    if event_data.get("event_type") == "conversation_ended":
                        break
                        
        except Exception as e:
            error_msg = f"Proxy Connection Error: {str(e)}"
            log.error(f"[{self.agent_name}] {error_msg}")
            self.on_stream(error_msg)

        return 0.0