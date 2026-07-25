# topos.gateway.logic.folding
## @lineage: void.topos.gateway.logic.folding
## @lineage: topos.edge.gateway.logic.folding
## @lineage: edge.gateway.logic.folding
## @lineage: fiber.gateway.logic.folding
## @lineage: meta.logic.gate.folding
## @lineage: logict.flow.gate.folding
"""@flow: AUG(Φ_declared) → reflect → Ψ → materialize → Φ_materialized → entry(anchor)"""
import asyncio
import json
import inspect
import ast
from typing import Any, Dict, List, Tuple
from watcher.plane.emitter import get_logger
from arch.gov.flow import PhaseFlow, FlowState, Transduction
from arch.contract.registry.unified import contract, registry
from arch.contract.discovery import discover_modules
from phase.runtime.node import NodeRuntime
from phase.ator.runtime import AtorRuntime
from phase.bind.resolver import find_current_self, resolve_path

log = get_logger("gate.folding")
PAYLOAD = {
  "source_path": inspect.getsourcefile(lambda: None),
  "task": {
      "task_id": "init-trigger",
      "requirement": "..."
  }
}

@contract.ator("gate.folding")
class GateFolding(Transduction):
    def __init__(self):
      self.manifold = None

    def bind(self, manifold: Any):
        self.manifold = manifold

    def transduce(self, flow: PhaseFlow, ator_node: Any) -> PhaseFlow:
        topology = flow.payload
        runtime_nodes = {}
        for node_id, config in topology.items():
            node_type = config["type"]
            spec = config["spec"]

            if node_type not in self.manifold:
                raise ValueError(f"Unknown node type '{node_type}'")

            NodeClass = self.manifold[node_type].node_class
            node_instance = NodeClass(spec)
            target_operator = spec.get("operator")
            if target_operator:
                operator_instance = registry.create_component(
                    "ator", {"type": target_operator}
                )
                node_instance.bound_operator = operator_instance
            runtime_nodes[node_id] = node_instance
        return self._close(runtime_nodes, flow, ator_node)
