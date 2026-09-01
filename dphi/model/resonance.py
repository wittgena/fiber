# xphi.arch.contract.model.resonance
## @lineage: arch.contract.model.resonance
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Set, Optional

@dataclass
class NodeRelation:
    """노드 간의 결합(Edge) 상태"""
    target: str
    rel: str = "coupled"  # 관계 타입 (coupled, flows_into 등)
    strength: int = 1

@dataclass
class ResonanceNode:
    """매니폴드 내의 단일 위상 노드"""
    id: str
    intensity: int
    is_invariant: bool
    boundaries: Dict[str, int]
    support_manifold: List[str]
    relations: List[NodeRelation] = field(default_factory=list)

@dataclass
class ResonanceGraph:
    """전체 시스템의 위상 지도 (Model Manifold Projection)"""
    invariants: List[str]
    nodes: Dict[str, ResonanceNode]
    version: str = "2.1"
    type: str = "topos.network"

    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class BridgeEvent:
    """엔진에 상관없이 워크플로우가 수신할 공통 이벤트 규격"""
    content: str
    source: str = "agent"
    event_type: str = "message"