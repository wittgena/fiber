# fiber.agent.loop.executor
## @lineage: fiber.agent.conver.loop.executor
## @lineage: surgent.topos.node.executor
import asyncio
from typing import Optional, Any, List

from fiber.agent.engine.executor import LocalExecutionEngine, ProxyExecutionEngine, DEFAULT_SECURITY_POLICY
from xphi.arch.model.dphi.graph import EntryNode
from xphi.arch.model.conv.event import LLMConvertibleEvent
from xphi.kernel.space.topos.node.gan import Message, GanNode
from xphi.kernel.space.topos.node.event import AgentConfigured, LLMEventMessage, TaskCompletedMessage
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("loop.executor")


BLUEPRINT_TEMPLATE = """\
{context_block}
{directives_block}
## Execution Blueprint (Execute the following strictly in sequence):
{steps_block}

*Instructions: Process all above nodes step-by-step using the designated tools. Maintain context integrity and report final completion using the 'finish' tool.*
"""

class BlueprintCompiler:
    @staticmethod
    def compile(context_node: Optional[EntryNode], nodes: List[Any], system_instructions: str = "") -> str:
        context_block = ""
        if context_node:
            relations = ", ".join(context_node.relations) if getattr(context_node, 'relations', None) else "None"
            context_block = (
                f"## System Context: {context_node.entry}\n"
                f"- **Focus**: {context_node.focus}\n"
                f"- **Depth Limit**: {context_node.depth}\n"
                f"- **Relations Constraint**: {relations}\n---\n"
            )

        directives_block = f"## Core Directives:\n{system_instructions.strip()}\n---\n" if system_instructions else ""
        
        steps = []
        for idx, node in enumerate(nodes, 1):
            action_name = getattr(node, 'action', 'terminal').upper()
            intent = getattr(node, 'intent', '')
            desc = getattr(node, 'description', '')
            
            step_line = f"{idx}. [{action_name}] {f'({intent.upper()}) ' if intent else ''}{desc}"
            steps.append(step_line)
            
            if params := getattr(node, 'params_template', None):
                steps.append(f"   > Required Tool Params: {params}")

        return BLUEPRINT_TEMPLATE.format(
            context_block=context_block,
            directives_block=directives_block,
            steps_block="\n".join(steps)
        ).strip()

class NodeExecutor(GanNode):
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

        custom_policy = getattr(settings, "security_policy", DEFAULT_SECURITY_POLICY)
        combined_instructions = f"{sys_inst}\n\n{custom_policy}".strip()
        
        master_instruction = BlueprintCompiler.compile(self.current_context, events, combined_instructions)
        await self._route_execution(master_instruction, settings)

    async def on_run_conversation(self, message: Message):
        instruction = getattr(message, 'instruction', "")
        settings = getattr(message, 'settings', None)
        if not settings or not settings.llm:
            log.error(f"[{self.name}] Execution failed: Missing configured primitives.")
            return

        custom_policy = getattr(settings, "security_policy", DEFAULT_SECURITY_POLICY)
        combined_instruction = f"{custom_policy}\n\n{instruction}".strip()
        await self._route_execution(combined_instruction, settings)

    async def _route_execution(self, instruction: str, settings: Any):
        """@desc: Delegates the compiled instruction to the isolated Engine module."""
        is_proxy = isinstance(settings.llm, dict) and settings.llm.get("is_proxy") is True
        
        def emit_stream(chunk: str):
            evt_msg = LLMEventMessage(chunk)
            evt_msg.sender_id = self.name
            self.main_loop.call_soon_threadsafe(self.post_message, evt_msg)
            
        try:
            # 💡 런타임 제어권이 engine.py 모듈로 완벽히 위임됨
            if is_proxy:
                log.info(f"[{self.name}] 🌐 [Proxy Mode] Delegating execution to remote proxy.")
                engine = ProxyExecutionEngine(self.name, instruction, settings.llm, emit_stream)
            else:
                log.info(f"[{self.name}] 💻 [Local Mode] Deploying isolated async engine loop.")
                engine = LocalExecutionEngine(self.name, instruction, settings, emit_stream)
                
            cost = await engine.execute()
            log.info(f"[{self.name}] Execution converged (Cost: {cost}).")
            
            completed_msg = TaskCompletedMessage(cost)
            completed_msg.sender_id = self.name
            self.post_message(completed_msg)
            
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