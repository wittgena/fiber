# topos.bound.adapter.sandbox
## @lineage: topos.tester.agent.dag
import json
import time
import asyncio
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional, List, Tuple

from arch.gov.state.vocab import SigType, SpecKey
from arch.gov.state.schema import FragmentSig
from arch.contract.schema.resonance import ResonanceGraph, ResonanceNode, NodeRelation

from arch.gov.state.compiler import StateCompiler
from arch.gov.state.projector import StateProjector
from arch.gov.trans.logic.analyzer import LogicAnalyzer

from arch.contract.schema.graph import EntryNode, GraphSchema
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_logger

CODE_ROOT = resolve_path("code")
log = get_logger("dag.sandbox")

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

@dataclass
class MetabolicProfile:
    max_threads: int = 2               # Maximum concurrent threads allowed
    max_compute_time: float = 3.0      # Hard timeout limit in seconds
    max_node_capacity: int = 50        # Maximum allowed nodes in the DAG
    max_simulation_ticks: int = 200    # Maximum permitted simulation iterations

class RegulatedSandbox:
    def __init__(self, profile: MetabolicProfile = MetabolicProfile(), fixed_graph_path: Optional[str] = None):
        self.profile = profile
        self.compiler = StateCompiler()
        self.projector = StateProjector()
        
        # Thread pool isolated for asynchronous execution constraints
        self._executor = ThreadPoolExecutor(max_workers=self.profile.max_threads, thread_name_prefix="Sandbox_Core")
        self.fixed_graph_path = Path(fixed_graph_path or "workspace/xor/node/model.bound.json") 
        self.codebase_graph_data = self._load_graph()

    def _load_graph(self) -> dict:
        if self.fixed_graph_path.exists():
            with open(self.fixed_graph_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"nodes": {}}

    async def evaluate_safely(self, raw_schema: Dict[str, Any], entry_ctx: EntryNode) -> DagTestReport:
        """
        Asynchronously evaluates the DAG schema within the defined resource limits and returns the execution report.
        """
        start_time = time.time()
        
        node_count = len(raw_schema.get("nodes", []))
        if node_count > self.profile.max_node_capacity:
            return DagTestReport(
                is_valid=False, 
                simulation_errors=[f"Schema size ({node_count}) exceeds capacity limits ({self.profile.max_node_capacity})."],
                metabolic_cost=1.0 # Baseline cost for capacity rejection
            )

        try:
            loop = asyncio.get_running_loop()
            report = await asyncio.wait_for(
                loop.run_in_executor(self._executor, self._sync_evaluation, raw_schema, entry_ctx),
                timeout=self.profile.max_compute_time
            )
            
            execution_time = time.time() - start_time
            # Calculate resource consumption metric
            report.metabolic_cost += (execution_time * 2.0) 
            return report
            
        except asyncio.TimeoutError:
            log.warning("[Sandbox] Execution timeout detected. Terminating runaway process.")
            return DagTestReport(
                is_valid=False, 
                simulation_errors=["Evaluation timed out. Graph is too complex or caught in an infinite loop."],
                metabolic_cost=10.0 # Penalty cost for timeout
            )

    def _sync_evaluation(self, raw_schema: Dict[str, Any], entry_context: EntryNode) -> DagTestReport:
        """Synchronous validation logic executed within the isolated thread."""
        report = DagTestReport(is_valid=True)
        
        try:
            ir_sig = self.compiler.compile_from_schema(raw_schema)
            runtime_specs = self.projector.project(ir_sig)
        except Exception as e:
            report.is_valid = False
            report.simulation_errors.append(f"Compilation Failed: {e}")
            report.metabolic_cost = 0.5
            return report

        subgraph = SubgraphExtractor.select(self.codebase_graph_data, entry_context)
        align_errors = AlignTester.verify(ir_sig, subgraph)
        if align_errors:
            report.is_valid = False
            report.alignment_errors.extend(align_errors)

        sim_errors, consumed_ticks = DryRunSimulator.simulate(
            runtime_specs, ir_sig.entry_point, max_ticks=self.profile.max_simulation_ticks
        )
        
        report.metabolic_cost = consumed_ticks * 0.1 
        if sim_errors:
            report.is_valid = False
            report.simulation_errors.extend(sim_errors)

        return report