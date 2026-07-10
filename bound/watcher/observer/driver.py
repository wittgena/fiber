import time
import traceback
import warnings
from enum import IntEnum
from collections import defaultdict
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

from anchor.bind.switch.params import ResponseAPIUsage, ResponsesAPIResponse, ModelResponse
from bound.watcher.metrics.snapshot import Metrics
from phase.gov.proto.gate import uuid4
from watcher.plane.emitter import get_emitter, _flow_context

emitter = get_emitter("observer.driver")

# 1. 위상적 생명주기 정의
class TopoPhase(IntEnum):
    BOUND = 1       # 경계 확정
    ANCHOR = 2      # 맥락 결속
    PROJECTION = 3  # 외부 투영

@dataclass
class ParsedUsage:
    prompt: int = 0
    completion: int = 0
    cache_read: int = 0
    cache_write: int = 0
    reasoning: int = 0
    is_meaningful: bool = False

@dataclass
class TopologyState:
    """위상적 흐름을 관통하며 상태가 결속되는 컨텍스트 객체 (Stateless flow)"""
    response: Any
    start_time: float
    latency_sec: float
    telemetry_ctx: dict
    model_name: str
    is_internal_call: bool = False
    
    # 궤적을 지나며 채워지는 속성들
    response_id: str = field(init=False)
    parsed_usage: ParsedUsage = field(default_factory=ParsedUsage)
    cost: float = 0.0

    def __post_init__(self):
        self.response_id = getattr(self.response, "id", uuid4().hex)


class ObserverNode:
    """위상 노드의 기본 인터페이스"""
    def on_flow(self, state: TopologyState) -> None:
        pass
    def on_rupture(self, error: BaseException, state: TopologyState) -> None:
        pass


class DriverObserver:
    """
    @desc: Unified Observability Dispatcher based on Topological Flow.
    @role: Orchestrates the Bound -> Anchor -> Projection lifecycle without holding state.
    """
    def __init__(self, model_name: str, config: dict[str, Any], initial_metrics: Metrics | None = None):
        self.model_name = model_name
        self.log_enabled = config.get("log_completions", False)
        
        self._metrics = initial_metrics or Metrics(model_name=model_name)
        self._nodes: dict[TopoPhase, list[ObserverNode]] = defaultdict(list)

    @property
    def metrics(self) -> Metrics:
        return self._metrics

    def restore_metrics(self, metrics: Metrics) -> None:
        self._metrics = metrics

    def reset_metrics(self) -> None:
        self._metrics = None

    def register_node(self, phase: TopoPhase, node: ObserverNode) -> None:
        """위상 흐름의 특정 단계에 노드를 결속합니다."""
        self._nodes[phase].append(node)

    def on_request(self, telemetry_ctx: dict | None = None) -> float:
        return time.time()

    def track_success(
        self,
        resp: ModelResponse | ResponsesAPIResponse,
        start_time: float,
        telemetry_ctx: dict | None = None,
    ) -> Metrics:
        """Bound -> Anchor -> Projection 흐름을 순차적으로 통과시킵니다."""
        ctx = _flow_context.get()
        state = TopologyState(
            response=resp,
            start_time=start_time,
            latency_sec=time.time() - start_time,
            telemetry_ctx=telemetry_ctx or {},
            model_name=self.model_name,
            is_internal_call=ctx.get("is_internal_call", False)
        )

        # 위상적 흐름 순회
        for phase in sorted(TopoPhase):
            for node in self._nodes[phase]:
                try:
                    node.on_flow(state)
                except Exception as e:
                    emitter.debug(f"[{phase.name}] Topological Node {node.__class__.__name__} failed: {e}")

        # 로컬 Metrics 결속 (흐름 통과 후 최종 갱신)
        if self._metrics and not state.is_internal_call:
            self._metrics.add_response_latency(state.latency_sec, state.response_id)
            if state.cost > 0:
                self._metrics.add_cost(state.cost)
            if state.parsed_usage.is_meaningful:
                self._metrics.add_token_usage(
                    prompt_tokens=state.parsed_usage.prompt,
                    completion_tokens=state.parsed_usage.completion,
                    cache_read_tokens=state.parsed_usage.cache_read,
                    cache_write_tokens=state.parsed_usage.cache_write,
                    reasoning_tokens=state.parsed_usage.reasoning,
                    context_window=state.telemetry_ctx.get("context_window", 0),
                    response_id=state.response_id,
                )

        return self._metrics.deep_copy() if self._metrics else Metrics(model_name=self.model_name)

    def track_rupture(
        self, 
        error: BaseException, 
        start_time: float | None = None, 
        telemetry_ctx: dict | None = None
    ) -> None:
        """흐름 파탄(Rupture) 시, Bound/Anchor를 건너뛰고 즉시 Projection으로 투영합니다."""
        ctx = _flow_context.get()
        if ctx.get("is_internal_call"):
            emitter.debug("Internal sub-call failed. Skipping telemetry error log.")
            return

        state = TopologyState(
            response=None,
            start_time=start_time or time.time(),
            latency_sec=(time.time() - start_time) if start_time else 0.0,
            telemetry_ctx=telemetry_ctx or {},
            model_name=self.model_name
        )

        # 파탄(Rupture) 상황에서는 PROJECTION 단계의 노드들에게만 알림
        for node in self._nodes[TopoPhase.PROJECTION]:
            try:
                node.on_rupture(error, state)
            except Exception as e:
                warnings.warn(f"Critical: Rupture projection failed: {e}", RuntimeWarning)