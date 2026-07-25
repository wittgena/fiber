# topos.tester.agent.dag
## @lineage: void.topos.tester.agent.dag
## @lineage: topos.audit.tester.agent.dag
## @lineage: gov.audit.tester.agent.dag
## @lineage: audit.tester.agent.dag
## @lineage: ops.tester.agent.dag
## @lineage: ops.tester.sandbox
import json
import time
import asyncio
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional

from arch.gov.state.compiler import StateCompiler
from arch.gov.state.projector import StateProjector
from arch.topos.node.graph import DagTestReport, SubgraphExtractor, TopologyAlignmentTester, DryRunSimulator
from arch.gov.trans.logic.analyzer import LogicAnalyzer

from arch.contract.schema.graph import EntryNode, GraphSchema
from watcher.plane.emitter import get_logger
from phase.bind.resolver import resolve_path

CODE_ROOT = resolve_path("code")
log = get_logger("dag.sandbox")

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
        align_errors = TopologyAlignmentTester.verify(ir_sig, subgraph)
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