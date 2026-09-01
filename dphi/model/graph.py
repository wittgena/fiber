# xphi.arch.contract.model.graph
## @lineage: arch.contract.model.graph
from typing import TypedDict, List, Dict, Any
from dataclasses import dataclass, asdict, field

class MetaModel(TypedDict):
    total_modules: int
    total_dependencies: int
    cycles_detected: int
    layer_distribution: Dict[str, int]
    absorbable_count: int      # 즉시 분리/재사용 가능한 독립 모듈 수
    phase_stability: float     # 위상 안정성 점수 (100점 만점)

class NodeData(TypedDict):
    id: str
    file_path: str
    layer: str                 # L0 ~ L4
    is_topos: bool             # 내부 런타임/루프 존재 여부
    degree: int
    betweenness: float

class EdgeData(TypedDict):
    source: str
    target: str
    keyword: str
    linenos: List[int]

class LoopEdgeData(TypedDict):
    source: str
    target: str
    type: str

@dataclass
class GraphSchema:
    meta: MetaModel
    nodes: List[NodeData]
    topos_edges: List[EdgeData]
    loop_edges: List[LoopEdgeData]
    cycles: List[List[str]]

@dataclass
class EntryNode:
    entry: str
    focus: str = "CLI Observation"
    depth: int = 1
    relations: List[str] = field(default_factory=lambda: ["coupled"])

    @property
    def valid_relations(self) -> set:
        return set(self.relations)

@dataclass
class RenderingData:
    """마크다운 렌더링에 주입되는 템플릿 데이터 모델"""
    entry_point: str
    focus: str
    depth: str
    relations_list: str
    fragments: str
    relations: str

def _extract_rel_attr(rel: Any, key: str, default: Any = None) -> Any:
    """dict 또는 객체 형태의 relation에서 속성을 안전하게 추출"""
    if isinstance(rel, dict):
        return rel.get(key, default)
    return getattr(rel, key, default)

class EntryTemplate:
    MARKDOWN = """# Entry: {entry_point}

> **Phase**: Φ′ → Ψ (Contextual Subgraph) → Φs
> **Role**: Dynamic Perspective Projection

## @focus.context
- **Focus**: `{focus}`
- **Depth**: {depth}
- **Relations**: {relations_list}

---

## @local.topology (Selected Fragments)
{fragments}

---

## @expansion (Paths)
{relations}

---
*Projected via `EntryProjector` (Dynamic Boundary)*
"""