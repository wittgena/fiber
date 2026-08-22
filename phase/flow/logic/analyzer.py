# phase.flow.logic.analyzer
## @lineage: nexus.flow.logic.analyzer
## @lineage: meta.flow.logic.analyzer
## @lineage: meta.flow.trans.logic.analyzer
## @lineage: meta.trans.logic.analyzer
## @lineage: meta.model.trans.logic.analyzer
## @lineage: arch.model.trans.logic.analyzer
## @lineage: arch.contract.trans.logic.analyzer
## @lineage: arch.gov.trans.logic.analyzer
## @lineage: arch.topos.trans.logic.analyzer
## @lineage: arch.topos.node.trans.logic.analyzer
## @lineage: arch.topos.bound.trans.logic.analyzer
## @lineage: arch.bound.trans.logic.analyzer
## @lineage: arch.topic.logic.analyzer
## @lineage: arch.proto.task.logic.analyzer
## @lineage: arch.task.logic.analyzer
import ast
import json
import argparse
import sys
import networkx as nx
from pathlib import Path
from collections import Counter
from typing import TypedDict, List, Dict, Any
from dataclasses import dataclass, asdict
from arch.contract.model.graph import MetaModel, NodeData, EdgeData, LoopEdgeData, GraphSchema
from watcher.plane.emitter import get_emitter
from kernel.bind.resolver import resolve_path
from phase.flow.logic.transformer import LogicTransformer

CODE_ROOT = resolve_path("code")
log = get_emitter("logic.analyzer")

class LogicAnalyzer:
    """@flow: Ψ_fragments → bind(edges) → construct(Φ_graph) → measure(curvature / centrality) → classify(phere) → Φ_dissolved"""
    def __init__(self):
        self.g = nx.DiGraph()
        self.g_loop = nx.DiGraph()
        self.runtime_hints = {}

    def build_structure(self, module_index: Dict[str, Path], root: Path):
        for mod, path in module_index.items():
            ## Φ_loop: containment structure (filesystem topos)
            rel = path.relative_to(root)
            parent = rel.parent.as_posix() if rel.parent.parts else root.name
            self.g_loop.add_edge(parent, rel.as_posix(), type="contain")

            ## Φ_topos: logical dependency graph
            self.g.add_node(mod)
            parsed = LogicTransformer.parse_module(path)
            self.runtime_hints[mod] = parsed["is_topos"]
            
            ## bind Ψ_import into Φ graph
            for imp in parsed["imports"]:
                target = self._resolve(imp["target"], module_index)
                if target and mod != target:
                    self._add_edge(mod, target, imp["lineno"])

    def _resolve(self, target: str, index: Dict[str, Path]) -> str:
        ## Φ_resolution: map symbolic import → internal module
        if target in index: return target
        for mod in index:
            if mod.startswith(target + ".") or target.startswith(mod + "."):
                return mod
        return None

    def _add_edge(self, src: str, tgt: str, line: int):
        if self.g.has_edge(src, tgt):
            if line and line not in self.g[src][tgt]["linenos"]:
                self.g[src][tgt]["linenos"].append(line)
        else:
            self.g.add_edge(src, tgt, keyword="import", linenos=[line] if line else [])

    def get_dissolve_schema(self, module_index: Dict[str, Path], root: Path) -> GraphSchema:
        ## curvature metrics (∂Φ_measure)
        degree = dict(self.g.degree)
        in_degree = dict(self.g.in_degree)
        out_degree = dict(self.g.out_degree)
        betweenness = nx.betweenness_centrality(self.g) if self.g.nodes else {}
        
        try: cycles = list(nx.simple_cycles(self.g))
        except: cycles = []

        nodes = []
        phere_counter = Counter()
        absorbable_count = 0

        ## Φ_classification: assign structural roles
        for n in self.g.nodes:
            deg = degree.get(n, 0)
            in_d = in_degree.get(n, 0)
            out_d = out_degree.get(n, 0)
            bc = betweenness.get(n, 0)
            is_topos = self.runtime_hints.get(n, False)

            if is_topos and deg > 0:
                phere = "L3:LocalTopos"
            elif bc > 0.05:
                phere = "L4:Orchestrator"
            elif out_d > 2 and in_d <= 1:
                phere = "L2:BoundHandler"
            elif in_d > 0 and out_d == 0:
                phere = "L1:Transformer"
            else:
                phere = "L0:Primitive"
                if deg == 0:
                    absorbable_count += 1

            phere_counter[phere] += 1
            nodes.append(NodeData(
                id=n,
                file_path=module_index[n].relative_to(root).as_posix() if n in module_index else "",
                phere=phere,
                is_topos=is_topos,
                degree=deg,
                betweenness=round(bc, 4)
            ))

        penalty = min(len(cycles) * 5, 50)
        base_score = 100 - penalty
        phase_stability = max(0.0, round(base_score, 1))
        meta = MetaModel(
            total_modules=len(self.g.nodes),
            total_dependencies=len(self.g.edges),
            cycles_detected=len(cycles),
            phere_distribution=dict(phere_counter),
            absorbable_count=absorbable_count,
            phase_stability=phase_stability
        )
        edges = [EdgeData(source=u, target=v, **d) for u, v, d in self.g.edges(data=True)]
        loop_edges = [{"source": u, "target": v, "type": d["type"]} for u, v, d in self.g_loop.edges(data=True)]
        return GraphSchema(meta=meta, nodes=nodes, topos_edges=edges, loop_edges=loop_edges, cycles=cycles)
