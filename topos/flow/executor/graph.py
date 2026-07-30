# topos.flow.executor.graph
## @lineage: phi.executor.topos.graph
## @lineage: topos.ops.scope.execution.graph
## @lineage: ops.scope.execution.graph
## @lineage: meta.scope.execution.graph
## @lineage: topos.scope.execution.graph
## @lineage: gov.sandbox.topos.execution.graph
"""
@desc: Execution layer mapping logical safety policies into polymorphic runtime streams.
@flow: Token context stream -> Structural validation -> Async/Sync dual execution.
"""
import asyncio
from typing import Optional, Dict, Any, List

from topos.flow.executor.compiler import BlueprintCompiler
from topos.flow.executor.strategy import LocalExecutionStrategy, ProxyExecutionStrategy
from phase.executor.flow.event import AgentConfigured, LLMEventMessage, TaskCompletedMessage

from arch.contract.schema.graph import EntryNode
from arch.topos.node.gan import Message, GanNode
from watcher.plane.emitter import get_emitter

log = get_emitter("node.policy")

CUSTOM_SECURITY_POLICY = (
    "# Security Risk Policy\n"
    "Assess the safety risk of your actions:\n"
    "- **LOW**: Read-only actions (viewing files, documentation).\n"
    "- **MEDIUM**: Modifications (file edits, package installs).\n"
    "- **HIGH**: Dangerous actions (network access, system changes).\n\n"
    "Always prioritize data integrity and user intent."
)

class PolicyNode(GanNode):
    """
    @desc: Polymorphic conversation safety mediator.
    @flow: Asset injection -> Blueprint Compilation -> Strategy Routing.
    """
    def __init__(self, name: str):
        super().__init__(name)
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.current_context: Optional[EntryNode] = None

    async def on_boot(self, message: Message):
        self.main_loop = asyncio.get_running_loop()
        # 파일 I/O 로직 제거됨. 메모리 상에서 정책을 관리합니다.
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

        # 시스템 지시문과 보안 정책을 인-메모리에서 결합
        combined_instructions = f"{sys_inst}\n\n{CUSTOM_SECURITY_POLICY}".strip()

        master_instruction = BlueprintCompiler.compile(self.current_context, events, combined_instructions)
        await self._route_execution(master_instruction, settings)

    async def on_run_conversation(self, message: Message):
        instruction = getattr(message, 'instruction', "")
        settings = getattr(message, 'settings', None)
        
        if not settings or not settings.llm:
            log.error(f"[{self.name}] Execution failed: Missing configured primitives.")
            return

        # 단일 프롬프트 실행 모드일 때도 보안 정책을 함께 주입
        combined_instruction = f"{CUSTOM_SECURITY_POLICY}\n\n{instruction}".strip()
        await self._route_execution(combined_instruction, settings)

    async def _route_execution(self, instruction: str, settings: Any):
        """@desc: Routes the compiled instruction to the appropriate execution strategy."""
        is_proxy = isinstance(settings.llm, dict) and settings.llm.get("is_proxy") is True
        
        def emit_stream(chunk: str):
            self.main_loop.call_soon_threadsafe(self.post_message, LLMEventMessage(chunk))
            
        try:
            if is_proxy:
                log.info(f"[{self.name}] 🌐 [Proxy Mode] Delegating execution to remote proxy.")
                cost = await ProxyExecutionStrategy.execute_async(instruction, settings.llm, emit_stream)
            else:
                log.info(f"[{self.name}] 💻 [Local Mode] Deploying isolated async engine loop.")
                
                # [핵심 변경] asyncio.to_thread 래핑 제거 및 execute_async 직접 호출
                # LocalExecutionStrategy.execute_async 내부에서 백그라운드 워커와 오케스트레이터가 비동기로 동작합니다.
                cost = await LocalExecutionStrategy.execute_async(
                    instruction, settings, emit_stream
                )
            
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