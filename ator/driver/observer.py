# ator.driver.observer
## @lineage: driver.observer
## @lineage: engine.driver.observer
## @lineage: engine.driver.observer.flow
## @lineage: phi.driver.observer.flow
## @lineage: agent.llm.driver.observer
import time
import warnings
from typing import Any
from dataclasses import dataclass, field
from enum import IntEnum
from collections import defaultdict

from bound.xor.model.metric import Metrics
from ator.client.model.param import ModelResponse

from arch.model.phase.gate import uuid4
from watcher.plane.emitter import get_emitter, _flow_context

log = get_emitter("driver.observer")

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
        resp: ModelResponse,
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
                    log.debug(f"[{phase.name}] Topological Node {node.__class__.__name__} failed: {e}")

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
            log.debug("Internal sub-call failed. Skipping telemetry error log.")
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


class UsageParserNode(ObserverNode):
    """
    [Phase: BOUND]
    LLM 응답 객체(ModelResponse)로부터 사용량(Token Usage) 정보를 추출하여 
    위상 상태(TopologyState)의 `parsed_usage` 객체에 바인딩합니다.
    """
    def on_flow(self, state: TopologyState) -> None:
        if not state.response:
            return
            
        usage = getattr(state.response, "usage", None)
        if not usage:
            return

        # dict 타입이든 Pydantic/Object 타입이든 안전하게 속성을 가져오는 헬퍼
        def _get_val(obj: Any, key: str, default: int = 0) -> int:
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        prompt = _get_val(usage, "prompt_tokens", 0)
        completion = _get_val(usage, "completion_tokens", 0)
        
        # 최신 LLM API(예: OpenAI)의 캐시 및 추론 토큰 세부 정보 추출
        prompt_details = _get_val(usage, "prompt_tokens_details", {})
        cache_read = _get_val(prompt_details, "cached_tokens", 0)
        
        completion_details = _get_val(usage, "completion_tokens_details", {})
        reasoning = _get_val(completion_details, "reasoning_tokens", 0)

        # 상태에 결속 (is_meaningful 설정)
        state.parsed_usage = ParsedUsage(
            prompt=prompt,
            completion=completion,
            cache_read=cache_read,
            cache_write=0,  # 필요한 경우 매니페스트나 헤더에서 추가 파싱
            reasoning=reasoning,
            is_meaningful=(prompt + completion > 0)
        )


class ManifestCostNode(ObserverNode):
    """
    [Phase: ANCHOR]
    추출된 토큰 사용량과 주입된 Cost Manifest(요금표)를 바탕으로 
    이번 호출의 비용(Cost)을 계산하여 상태에 결속시킵니다.
    """
    def __init__(self, cost_manifest: dict | None = None):
        self.manifest = cost_manifest or {}

    def on_flow(self, state: TopologyState) -> None:
        if not state.parsed_usage.is_meaningful:
            return
            
        # 매니페스트에서 모델 요금 정보 조회
        model_rates = self.manifest.get(state.model_name, {})
        if not model_rates:
            return
            
        # 요금표 (일반적으로 1토큰 당 가격으로 환산되어 있다고 가정)
        prompt_rate = model_rates.get("prompt_token_cost", 0.0)
        completion_rate = model_rates.get("completion_token_cost", 0.0)
        
        # 캐시 히트 등 복잡한 요금 할인이 있다면 이 부분에서 추가 로직 적용
        state.cost = (
            state.parsed_usage.prompt * prompt_rate + 
            state.parsed_usage.completion * completion_rate
        )


class CentralEmitterNode(ObserverNode):
    def on_flow(self, state: TopologyState) -> None:
        if state.is_internal_call:
            return  # 내부 서브콜은 메인 텔레메트리 로그에서 제외
            
        log.info(
            f"🟢 [FLOW: SUCCESS] Model: {state.model_name} | "
            f"Latency: {state.latency_sec:.3f}s | "
            f"Cost: ${state.cost:.6f} | "
            f"Tokens: [P:{state.parsed_usage.prompt} / C:{state.parsed_usage.completion} / R:{state.parsed_usage.reasoning}]"
        )

    def on_rupture(self, error: BaseException, state: TopologyState) -> None:
        # 에러 발생 시 즉각적으로 로깅
        log.error(
            f"🔴 [FLOW: RUPTURE] Model: {state.model_name} | "
            f"Failed after {state.latency_sec:.3f}s | "
            f"Error: {type(error).__name__} - {str(error)}"
        )


def create_topological_observer(model_name: str, config: dict, initial_metrics: Metrics | None = None) -> DriverObserver:
    """위상 노드들이 완벽히 결속된 DriverObserver를 반환합니다."""
    observer = DriverObserver(model_name=model_name, config=config, initial_metrics=initial_metrics)
    observer.register_node(TopoPhase.BOUND, UsageParserNode())
    observer.register_node(TopoPhase.ANCHOR, ManifestCostNode(config.get("cost_manifest")))
    observer.register_node(TopoPhase.PROJECTION, CentralEmitterNode())
    
    return observer