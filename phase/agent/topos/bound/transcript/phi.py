# phase.agent.topos.bound.transcript.phi
## @lineage: agent.nexus.topos.bound.transcript.phi
## @lineage: nexus.agent.topos.bound.transcript.phi
## @lineage: meta.agent.topos.bound.transcript.phi
## @lineage: topos.bound.transcript.phi
## @lineage: kernel.bind.transcript.phi
"""
@module: kernel.bind.transcript.phi
@desc: Ψ → Φ transformer (transcription + translation boundary)
"""
import asyncio
import json
import inspect
import ast
import re
import yaml
from abc import abstractmethod
from typing import Any, Dict, List, Tuple

from xphi.arch.model.phase.flow import PhaseFlow, FlowState, Transduction
from xphi.arch.contract.registry.unified import contract, registry
from xphi.arch.xor.parser.lang.md import MdAstParser
from xphi.arch.xor.parser.block.extractor import BlockExtractor
from xphi.watcher.plane.emitter import get_logger

log = get_logger("transcript.phi")

class TranscriptBase(Transduction):
    """@flow: Ψ → Φ transformer (transcription + translation boundary)"""
    def __init__(self, base_node: Any = None):
        self.base_node = base_node
        self.manifold = getattr(base_node, "local_manifold", {})

        self.role = "transcript.base"
        self.node_context = {
            "instruction": "System Materialization Kernel",
            "role": self.role
        }

    def transduce(self, flow: PhaseFlow, ator_node: Any) -> PhaseFlow:
        """@phase: Projection (Ψ_reflect)"""
        file_path = flow.payload
        log.info(f"  [Projection] Reflecting source: {file_path}")
        projected_topology = self._reflect_source(file_path)
        return self._close(projected_topology, flow, ator_node)

    def _execute_transformation(self, topology: Dict[str, Any], instruction: str) -> Dict[str, Any]:
        """Translation (Ψ → Φ_materialized)"""
        log.info("    [Kernel] Materializing Topology into Node Instances")
        runtime_nodes = {}
        
        ## 위상 구조 호환성 계층 (Compatibility Layer)
        nodes_topology = topology.get("nodes", topology) if isinstance(topology, dict) else topology

        ## 추출된 노드 맵을 순회
        for node_id, config in nodes_topology.items():
            if not isinstance(config, dict) or "type" not in config:
                continue
                
            node_type = config["type"]
            spec = config["spec"]
            
            if node_type not in self.manifold:
                raise ValueError(f"Unknown node type '{node_type}'")
            
            # [개선됨] self.manifold는 이제 NodeMeta가 아닌 Class 자체(Type)를 반환합니다.
            # (혹시 모를 레거시 NodeMeta 잔재가 들어올 경우를 대비한 하위 호환성 방어 로직 추가)
            NodeClass = self.manifold[node_type]
            if hasattr(NodeClass, "node_class"):
                NodeClass = NodeClass.node_class
                
            node_instance = NodeClass(spec)
            
            target_operator = spec.get("operator")
            if target_operator:
                # [개선됨] 레거시 카테고리 인자("ator") 삭제. 오직 고유 식별자(type)만으로 팩토리 호출
                operator_instance = registry.create_component({"type": target_operator})
                node_instance.bound_operator = operator_instance

            runtime_nodes[node_id] = node_instance
            
        return runtime_nodes

    @abstractmethod
    def _reflect_source(self, file_path: str) -> Dict[str, Any]:
        """소스를 해석하여 Dict(Topology)를 반환하는 메서드 (Subclass must implement)"""
        pass

@contract.ator("transcript.phi", role="phase_node")
class TranscriptPhi(TranscriptBase):
    """@flow: Ψ → Φ transformer (transcription + translation boundary)"""
    def __init__(self, base_node: Any):
        super().__init__(base_node)
        self.role = "transcript.phi"

    def _reflect_source(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == 'XPHI':
                        return ast.literal_eval(node.value)
        raise ValueError(f"XPHI not found in {file_path}")