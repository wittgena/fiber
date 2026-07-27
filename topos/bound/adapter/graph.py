# topos.bound.adapter.graph
## @lineage: arch.topos.node.graph
import json
import argparse
import networkx as nx
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Tuple

from arch.gov.state.vocab import SigType, SpecKey
from arch.gov.state.schema import FragmentSig
from arch.contract.schema.graph import EntryNode
from arch.contract.schema.resonance import ResonanceGraph, ResonanceNode, NodeRelation
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_logger

log = get_logger("graph.builder")

@dataclass
class DagTestReport:
    """Execution test report returned to the agent pipeline."""
    is_valid: bool
    alignment_errors: List[str] = field(default_factory=list)
    simulation_errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metabolic_cost: float = 0.0 
    
    def to_dict(self) -> dict:
        return asdict(self)

class SubgraphExtractor:
    @staticmethod
    def select(graph_data: dict, entry_ctx: EntryNode) -> ResonanceGraph:
        """Extracts a bounded topological subgraph starting from the entry node using BFS."""
        full_nodes = {}
        for k, v in graph_data.get("nodes", {}).items():
            raw_rels = v.get("relations", [])
            v["relations"] = [NodeRelation(**r) if isinstance(r, dict) else r for r in raw_rels]
            full_nodes[k] = ResonanceNode(**v)
        
        entry_id = entry_ctx.entry
        if entry_id not in full_nodes:
            return ResonanceGraph(invariants=[], nodes={})

        selected_ids = {entry_id}
        current_level = {entry_id}
        
        for _ in range(entry_ctx.depth):
            next_level = set()
            for n_id in current_level:
                for rel in full_nodes[n_id].relations:
                    if rel.rel in entry_ctx.valid_relations and rel.target in full_nodes:
                        next_level.add(rel.target)
            selected_ids.update(next_level)
            current_level = next_level

        sub_nodes = {n_id: full_nodes[n_id] for n_id in selected_ids}
        return ResonanceGraph(invariants=[], nodes=sub_nodes)

class AlignTester:
    @staticmethod
    def verify(ir_sig: FragmentSig, codebase_graph: ResonanceGraph) -> List[str]:
        """Verifies if the generated DAG targets valid nodes within the bounded codebase graph."""
        errors = []
        valid_targets = set(codebase_graph.nodes.keys())
        
        for frag_id, frag in ir_sig.nodes.items():
            target_file = frag.attributes.extras.get("file_path")
            if target_file and target_file not in valid_targets:
                errors.append(
                    f"Hallucination Detected: Node '{frag_id}' targets '{target_file}', "
                    f"but this file is outside the bounded context."
                )
        return errors

class DryRunSimulator:
    @staticmethod
    def simulate(runtime_specs: Dict[str, Dict[str, Any]], entry_point: str, max_ticks: int = 100) -> Tuple[List[str], int]:
        """Simulates DAG execution to detect logical flaws. Returns (errors, consumed_ticks)."""
        errors = []
        visited_counts: Dict[str, int] = {}
        current_node = entry_point
        ticks = 0
        
        while current_node != SigType.END.value:
            if ticks > max_ticks:
                errors.append(f"Timeout Exceeded: Potential infinite loop detected. Ticks exceeded {max_ticks}.")
                break
                
            if current_node not in runtime_specs:
                errors.append(f"Broken Link: Node '{current_node}' does not exist in specs.")
                break
                
            spec = runtime_specs[current_node]
            visited_counts[current_node] = visited_counts.get(current_node, 0) + 1
            
            max_fails = spec.get(SpecKey.MAX_FAILURES, 3)
            if visited_counts[current_node] > (max_fails * 2): 
                errors.append(f"Cycle Detected: Node '{current_node}' trapped the execution in an infinite cycle.")
                break
                
            if spec.get(SpecKey.TYPE) == SigType.ROUTER.value:
                current_node = spec.get(SpecKey.DEFAULT_NEXT, SigType.END.value)
            else:
                current_node = spec.get(SpecKey.NEXT, SigType.END.value)
                
            ticks += 1
            
        return errors, ticks