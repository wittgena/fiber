# xphi.watcher.sphere.prom.adapter
## @lineage: bound.watcher.sphere.prom.adapter
"""
@desc: Prometheus-to-Phase Adapters with Declarative Traversal
@flow: 
  [LLM Rule] ↦ DeclarativePromAdapter 
  ↦ IPromAdapter.translate() (via StateTraverser) ↦ Universal Payload
"""
from typing import Dict, Any, Protocol, List
from abc import abstractmethod

class StateTraverser:
    """@role: Safe JSON Path Extractor (No KeyError/IndexError)"""
    @staticmethod
    def resolve(obj: Any, path: str, default: Any = None) -> Any:
        if not path or obj is None:
            return default
        keys = path.split('.')
        current = obj
        for k in keys:
            if current is None: return default
            if isinstance(current, dict):
                current = current.get(k)
            elif isinstance(current, (list, tuple)):
                try: current = current[int(k)]
                except (IndexError, ValueError): return default
            else:
                current = getattr(current, k, None)
        return current if current is not None else default

class IPromAdapter(Protocol):
    """@role: Base interface for metric translation"""
    @property
    def query(self) -> str: ...
        
    def translate(self, raw_item: Dict[str, Any]) -> Dict[str, Any]: ...

class DeclarativePromAdapter(IPromAdapter):
    """
    @role: LLM-Driven Adapter
    @desc: LLM이 생성한 선언적 룰(JSON Dict)을 주입받아 StateTraverser로 데이터를 파싱합니다.
    """
    def __init__(self, rule_spec: Dict[str, Any]):
        self.rule = rule_spec
        self._query = self.rule.get("promql", "")
        self.resource_path = self.rule.get("resource_path", "metric.instance")
        self.value_path = self.rule.get("value_path", "value.1")
        self.tension_multiplier = float(self.rule.get("tension_multiplier", 1.0))
        self.fallback_resource_id = self.rule.get("fallback_resource_id", "unknown_target")

    @property
    def query(self) -> str:
        return self._query

    def translate(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        """@flow: Traverse ↦ Transform ↦ Universal Payload"""
        # 1. Traverser를 통한 안전한 데이터 추출
        raw_val = StateTraverser.resolve(raw_item, self.value_path, default=0.0)
        resource_id = StateTraverser.resolve(raw_item, self.resource_path, default=self.fallback_resource_id)
        
        # 2. 형변환 및 가중치 계산
        try:
            val = float(raw_val) * self.tension_multiplier
        except (ValueError, TypeError):
            val = 0.0

        metadata = raw_item.get("metric", {})

        return {
            "resource_id": resource_id,
            "metadata": metadata,
            "target_scale": 1,
            "actual_scale": 1,
            "error_weight": val, 
            "is_locked": False
        }

class JvmMemoryFallbackAdapter(IPromAdapter):
    """
    @role: Imperative Fallback Adapter (2차 방어선)
    @desc: LLM의 선언적 규칙만으로 해결할 수 없는 비선형 연산 등 복잡한 도메인에 직접 코딩된 어댑터
    """
    @property
    def query(self) -> str:
        return 'jvm_memory_bytes_used{area="heap"} / jvm_memory_bytes_max{area="heap"}'
        
    def translate(self, raw_item: Dict[str, Any]) -> Dict[str, Any]:
        usage_ratio = float(raw_item["value"][1])
        tension = (usage_ratio ** 2) * 10.0 if usage_ratio > 0.8 else usage_ratio
        
        return {
            "resource_id": raw_item.get("metric", {}).get("instance", "unknown_jvm"),
            "metadata": raw_item.get("metric", {}),
            "target_scale": 1,
            "actual_scale": 1,
            "error_weight": tension,
            "is_locked": False
        }