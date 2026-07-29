# phi.agent.loop.graph.organizer
## @lineage: phi.executor.graph.organizer
## @lineage: swarm.phi.handler.graph.organizer
import time
import json
from pathlib import Path
from typing import Dict, Any, Set

from atoa.conv.message import Message, TextContent

from atoa.event.llm.message import MessageEvent
from swarm.conver.protocol import ProtoConv

from phi.agent.llm.handler import (
    LLMInvocationHandler, 
    ToolCallHandler, 
    TextResponseHandler, 
)
from phi.agent.loop.step import StepHandler, StepContext
from phi.agent.loop.tension import TensionHandler
from phi.agent.loop.graph.eval import EvalReflector

from arch.gov.state.vocab import SigType, SpecKey
from watcher.plane.observer.span import unified_flow_span
from watcher.plane.emitter import get_emitter

logger = get_emitter("dag.organizer")

class DagOrganizer(StepHandler):
    """
    컴파일러가 생성한 DAG(Runtime Specs)를 해석하고, 
    Laminar 관측망(unified_flow_span) 하에서 에이전트의 사고를 정렬하는 메타 핸들러.
    """
    def __init__(self, runtime_specs: dict, entry_point: str, telemetry_path: Path | None = None):
        self.specs = runtime_specs
        self.entry_point = entry_point
        self.telemetry_path = telemetry_path or Path("./telemetry_traces.json")
        
        self.session_traces: Dict[str, Any] = {"nodes": {}}
        self._load_existing_telemetry()
        self.registry = {
            SigType.PROJECTOR.value: LLMInvocationHandler(),
            SigType.TENSION.value: TensionHandler(),
            SigType.OPERSIG.value: ToolCallHandler(),
            SigType.ACT.value: TextResponseHandler(),
            SigType.REFLECT.value: EvalReflector(),
        }

    def handle(self, agent, conversation: ProtoConv, on_event, on_token, context: StepContext) -> bool:
        state = conversation.state
        if getattr(state, "graph_cursor", None) is None:
            state.graph_cursor = self.entry_point
            
        current_node_id = state.graph_cursor
        
        if not hasattr(context, "produced_aspects"):
            context.produced_aspects = set()
            
        while current_node_id and current_node_id != SigType.END.value:
            node_spec = self.specs.get(current_node_id)
            if not node_spec:
                logger.error(f"Graph execution failed: Node '{current_node_id}' not found.")
                break

            if self._is_node_fatigued(current_node_id, node_spec):
                logger.error(f"[GraphEngine] 위상 붕괴 위험: 노드 '{current_node_id}' 강제 종료.")
                state.graph_cursor = SigType.END.value
                self._flush_telemetry()
                return False

            node_type = node_spec.get(SpecKey.TYPE)
            logger.info(f"[GraphEngine] Executing Node: {current_node_id} ({node_type})")

            # 1. 라우터(조건 분기) 노드 처리
            if node_type == SigType.ROUTER.value:
                current_node_id = self._evaluate_router(node_spec, context)
                continue

            # 2. 컨텍스트 동적 주입 (SpecKey 참조)
            context.node_attributes = node_spec.get(SpecKey.ATTRIBUTES, {})
            self._apply_node_pressure(agent, conversation, on_event, context.node_attributes, current_node_id)

            # 3. Base Handler 위임
            base_handler = self.registry.get(node_type)
            if not base_handler:
                logger.error(f"Handler for {node_type} is not mapped.")
                break

            start_time = time.time()
            
            try:
                # [PHASE & THEORIA] Laminar 통합 트레이싱 Span 생성
                with unified_flow_span(name=f"Node:{current_node_id}", phase=node_type) as flow_ctx:
                    
                    # 베이스 핸들러 실행 (loop.handler)
                    should_break = base_handler.handle(agent, conversation, on_event, on_token, context)
                    
                    # 실행 결과물에서 동적 Aspect 추출 및 성공 지표 누적
                    self._extract_dynamic_aspects(context)
                    self._record_trace(current_node_id, success=True, duration=time.time() - start_time)
                    
                    # 다음 진행할 노드 계산
                    next_node = node_spec.get(SpecKey.NEXT, SigType.END.value)
                    
                    # [통합 포인트 5] 상태 전이(Control Flow) 보정
                    if should_break:
                        # loop.handler가 제어권 반환을 요청함 (루프 종료 요청)
                        # 중요: 현재 노드가 아닌 '다음 노드'로 커서를 갱신해 두어야 다음 틱에서 정확히 이어서 실행됨.
                        state.graph_cursor = next_node
                        self._flush_telemetry()
                        return True 
                    else:
                        # loop.handler가 동기적으로 즉시 다음 스텝 진행을 허용함
                        current_node_id = next_node
                        state.graph_cursor = current_node_id

            except Exception as e:
                duration = time.time() - start_time
                self._record_trace(current_node_id, success=False, duration=duration)
                
                # otel_log_interceptor가 ERROR 로그를 낚아채어 Span 상태를 자동 전환
                logger.error(f"Node '{current_node_id}' failed with error: {e}")
                
                # [통합 포인트 6] SpecKey.FALLBACK을 통한 침묵의 방어선 작동
                fallback_node = node_spec.get(SpecKey.FALLBACK)
                if fallback_node and fallback_node in self.specs:
                    logger.warning(f"[GraphEngine] 예외 복구 작동 -> {fallback_node}")
                    context.produced_aspects.add("execution_failure")
                    current_node_id = fallback_node
                    state.graph_cursor = current_node_id
                    self._flush_telemetry()
                    continue
                else:
                    state.graph_cursor = SigType.END.value
                    self._flush_telemetry()
                    raise e

        # 그래프 순회 정상 종료
        state.graph_cursor = SigType.END.value
        self._flush_telemetry()
        return False

    def _evaluate_router(self, node_spec: dict, context: StepContext) -> str:
        """라우터 노드의 분기 조건을 평가하여 다음 노드를 결정합니다."""
        produced_aspects: Set[str] = getattr(context, "produced_aspects", set())
        
        for rule in node_spec.get(SpecKey.RULES, []):
            req_aspect = rule.get(SpecKey.IF_COND, {}).get(SpecKey.ASPECT)
            if req_aspect in produced_aspects:
                logger.debug(f"[Router] Aspect '{req_aspect}' matched. Routing to '{rule.get(SpecKey.NEXT)}'.")
                return rule.get(SpecKey.NEXT)
                
        fallback_node = node_spec.get(SpecKey.DEFAULT_NEXT, SigType.END.value)
        logger.debug(f"[Router] No aspects matched. Falling back to '{fallback_node}'.")
        return fallback_node

    def _apply_node_pressure(self, agent, conversation: ProtoConv, on_event, attributes: dict, node_id: str):
        state = conversation.state
        injected_nodes: Set[str] = getattr(state, "injected_nodes", set())
        if node_id in injected_nodes:
            return  

        instructions = attributes.get("instructions")
        pressure = attributes.get("pressure", 0.0)

        if instructions:
            urgency_prefix = "[URGENT] " if pressure > 0.7 else ""
            msg_text = f"{urgency_prefix}System Instruction for current phase: {instructions}"
            logger.info(f"[Injector] Injecting instructions for {node_id} (Pressure: {pressure})")
            
            nudge_event = MessageEvent(
                source="hook", 
                llm_message=Message(role="user", content=[TextContent(text=msg_text)])
            )
            on_event(nudge_event)
            injected_nodes.add(node_id)
            setattr(state, "injected_nodes", injected_nodes)

    # ... 이하 (동적 메트릭 처리 및 텔레메트리 메서드)는 Conv 참조가 없으므로 동일 ...
    def _extract_dynamic_aspects(self, context: StepContext):
        if not context.llm_response:
            return
        response_text = str(getattr(context.llm_response, "content", ""))
        if "RETRY_REQUIRED" in response_text:
            context.produced_aspects.add("retry_flag")
        if "HIGH_ENTROPY" in response_text:
            context.produced_aspects.add("high_entropy")

    def _is_node_fatigued(self, node_id: str, node_spec: dict) -> bool:
        trace = self.session_traces["nodes"].get(node_id)
        if not trace:
            return False
        max_allowed_failures = node_spec.get(SpecKey.MAX_FAILURES, 3)
        return trace["failure_count"] >= max_allowed_failures

    def _record_trace(self, node_id: str, success: bool, duration: float):
        if node_id not in self.session_traces["nodes"]:
            self.session_traces["nodes"][node_id] = {
                "execution_count": 0, "failure_count": 0, 
                "failure_rate": 0.0, "total_duration": 0.0
            }
        trace = self.session_traces["nodes"][node_id]
        trace["execution_count"] += 1
        trace["total_duration"] += duration
        if not success:
            trace["failure_count"] += 1
        trace["failure_rate"] = trace["failure_count"] / trace["execution_count"]

    def _load_existing_telemetry(self):
        if self.telemetry_path.exists():
            try:
                with open(self.telemetry_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if "nodes" in data:
                        self.session_traces = data
            except Exception as e:
                logger.warning(f"Failed to load existing telemetry: {e}")

    def _flush_telemetry(self):
        try:
            self.telemetry_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.telemetry_path, "w", encoding="utf-8") as f:
                json.dump(self.session_traces, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to flush telemetry to {self.telemetry_path}: {e}")