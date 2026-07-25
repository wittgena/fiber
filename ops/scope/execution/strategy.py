# ops.scope.execution.strategy
## @lineage: meta.scope.execution.strategy
## @lineage: topos.scope.execution.strategy
## @lineage: gov.sandbox.topos.execution.strategy
import asyncio
from typing import Optional, Dict, Any, List, Callable

from eco.tenant.conv.event import LLMConvertibleEvent
from gov.conv.state import ConversationState
from mesh.engine.conv.adapter import AgentCommunicator, ExecutionController, EngineContextAdapter
from gov.conv.visualizer import ConversationVisualizer

from agent.activator import Activator
from void.extime.logst.topos.proxy import ProxyEngine
from gov.factory.action import CoreAction
from gov.action.resolver import ActionResolver
from gov.action.tool.terminal import TerminalTool

from agent.disc.tool import Tool

from arch.topos.bound.tunnel import TunnelFactory
from arch.contract.event.next import next_id
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

WORKSPACE_ROOT = resolve_path("workspace")
log = get_emitter("execution.strategy")

class LocalGovContext:
    """순수 데이터 객체인 ConversationState와 Gov 엔진을 연결하는 가벼운 런타임 래퍼"""
    def __init__(self, state: ConversationState, tools: dict, callbacks: list[Callable] = None):
        self.state = state
        self.tools = tools
        self.id = state.id
        self.conversation_stats = state.stats
        self._cleanup_initiated = False
        self.ator = None 
        self.llm_registry = None 
        # 콜백(시각화 객체 등) 체인 등록
        self._callbacks = callbacks or []

    def _on_event(self, event: Any) -> None:
        self.state.events.append(event)
        # 이벤트 발생 시마다 콜백(Visualizer) 훅 트리거
        for cb in self._callbacks:
            if cb:
                try:
                    cb(event)
                except Exception as e:
                    log.warning(f"Error executing callback {cb}: {e}")
        
    def _end_observability_span(self):
        pass

class LocalExecutionStrategy:
    """@desc: Orchestrates the local agent lifecycle, tools, and execution loop."""
    
    @staticmethod
    async def execute_async(
        instruction: str, 
        settings: Any, 
        on_stream: Callable[[Any], None]
    ) -> float:
        tunnel = await TunnelFactory.get_default()
        
        # 64-bit Snowflake 문자열
        conv_id = next_id()

        # 1. 외부 도구 레지스트리 준비 (Gov & Agent 공통)
        real_tools: List[Tool] = []
        ActionResolver.register("terminal", lambda params, state: TerminalTool.create(conv_state=state, **(params or {})))
        real_tools.append(Tool(name="terminal", params={}))
        
        core_action_names = {action.value for action in CoreAction}
        for tool_name in ActionResolver.list_routes():
            if tool_name not in core_action_names:
                real_tools.append(Tool(name=tool_name, params={}))
            
        log.debug(f"[Local Strategy] Provisioned External Tools: {[t.name for t in real_tools]}")
        
        # 2. Agent(Activator) 인스턴스화 및 스키마 초기화 
        # (Activator는 내부적으로 CoreAction을 자동 탑재합니다)
        activator = Activator(llm=settings.llm, tools=real_tools)
        await activator.initialize()

        # 3. Gov 측의 물리 환경(Workspace) 준비
        from gov.workspace import SandboxWorkspace
        workspace = SandboxWorkspace(working_dir=str(WORKSPACE_ROOT))
        
        conv_state = ConversationState.create(
            id=conv_id,
            workspace=workspace,
            agent_id=activator.name,
        )
        
        # 4. Gov 실행용 툴 레지스트리 생성 
        resolved_gov_tools = {}
        
        # [핵심 수정] Gov 노드도 Agent가 뱉어내는 CoreAction(think, finish 등)을 
        # 인식하고 처리할 수 있도록 모든 스펙을 합쳐서 Resolve 합니다.
        all_gov_specs = real_tools.copy()
        for ca in CoreAction:
            all_gov_specs.append(Tool(name=ca.value, params={}))
            
        for spec in all_gov_specs:
            resolved_defs = ActionResolver.resolve(spec, conv_state)
            for r in resolved_defs:
                resolved_gov_tools[r.name] = r
                
        # 5. 시각화 뷰어 및 어댑터 연결
        visualizer = ConversationVisualizer()
        
        def _dispatch_visual(e):
            visualizer(e)
            if on_stream:
                content = getattr(e, 'content', None)
                if content:
                    on_stream(content)
                elif hasattr(e, 'llm_message') and getattr(e, 'source', None) == 'activator':
                    for c in getattr(e.llm_message, 'content', []):
                        if getattr(c, 'type', '') == 'text':
                            on_stream(getattr(c, 'text', ''))

        gov_context = LocalGovContext(
            state=conv_state, 
            tools=resolved_gov_tools, 
            callbacks=[_dispatch_visual]
        )
        communicator = AgentCommunicator(gov_context)
        controller = ExecutionController(gov_context)

        # 6. 분산 환경 시뮬레이션: Agent 워커 루프를 백그라운드로 실행
        worker_task = asyncio.create_task(activator.run_worker(tunnel, str(conv_id)))

        # 7. 시작 명령 하달 및 제어 루프 진입
        # 초기 상태 렌더링
        for e in conv_state.events:
            visualizer(e)
            
        communicator.send_message(instruction)
        
        try:
            await controller.run()
        finally:
            worker_task.cancel()
            await controller.close()
        
        stats = conv_state.stats
        metrics = stats.get_combined_metrics() if stats else None
        return metrics.accumulated_cost if metrics else 0.0


class ProxyExecutionStrategy:
    """@desc: Delegates execution to a remote Proxy Engine."""
    @staticmethod
    async def execute_async(
        instruction: str, 
        proxy_config: Dict[str, Any], 
        on_stream: Callable[[str], None]
    ) -> float:
        engine = ProxyEngine(
            host_url=proxy_config.get("server_url"),
            agent_usage="policy_node_remote",
            workspace_ref=proxy_config.get("workspace_ref"),
            session_api_key=proxy_config.get("session_api_key")
        )
        
        def proxy_event_callback(event: Any):
            content = getattr(event, 'content', str(event))
            on_stream(content)

        await engine._async_ask(instruction, callback=proxy_event_callback)
        return 0.0