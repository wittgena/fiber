# phase.node.ator.reflector
import ast
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict

from arch.model.phase.flow import PhaseFlow, FlowState, Align, Resonance, Transduction
from arch.contract.registry.unified import contract
from phase.node.ator.runtime import AtorRuntime
from watcher.plane.emitter import get_logger

log = get_logger('ator.reflector')

@contract.ator("ator.reflector")
class AtorReflector(Transduction):
    def transduce(self, flow: PhaseFlow, ator_node: Any) -> PhaseFlow:
        # 1. Penetrate the cellular membrane (raw_input)
        raw = flow.payload.get("raw_input", {})
        file_path = raw.get("source_path")
        task_data = raw.get("task")

        if not file_path:
            raise KeyError("Inversion Point (source_path) missing in raw_input")

        log.info(f"  [Reflect] Extracting Topological DNA from source: {file_path}")

        # 2. Meta-Reflection: Parse own AST to extract intent (PHI/XPHI)
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        topology = self._extract_phi(tree)

        # 3. [WASM-Enhanced Morphogenesis]
        # Translate the static Python Dict into WASM EvolutionContext possibilities.
        # Nodes begin as SYMLINKs, waiting for the WASM kernel to collapse them into COREs.
        evolution_ctx = {
            "phase_root": {
                "name": "ator_bootstrap_root",
                "kind": "CORE",
                "children": {
                    k: {
                        "name": k, 
                        "kind": "SYMLINK", 
                        "ref_target": v.get("type", "unknown")
                    }
                    for k, v in topology.items()
                }
            },
            "external_rules": []
        }

        # The folded mRNA seed ready for the AtorRuntime (Ribosome)
        materialization_seed = {
            "evolution_ctx": evolution_ctx,
            "topology": topology,       # Preserved for fallback/legacy mapping
            "task": task_data,
            "meta_context": flow.payload 
        }

        return self._close(materialization_seed, flow, ator_node)

    def _extract_phi(self, tree: ast.AST) -> Dict[str, Any]:
        """Scans the AST for the explicit declaration of PHI or XPHI."""
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in ("PHI", "XPHI"):
                        return ast.literal_eval(node.value)
        raise ValueError("Topological DNA (PHI or XPHI) not found in source")


@contract.ator("runtime.aligner")
class RuntimeAligner(Align):
    def align(self, flow: PhaseFlow, spec: Dict[str, Any]) -> Dict[str, Any]:
        runtime_nodes = flow.payload

        if not isinstance(runtime_nodes, dict) or not runtime_nodes:
            return {
                "status": "error",
                "payload": flow.payload,
                "state": {"alignment_status": "failed", "reason": "invalid_nodes"}
            }

        try:
            # Anchor to the physical engine space
            runtime_node = getattr(self, "base_node", None)
            if runtime_node is None:
                return {
                    "status": "error",
                    "payload": flow.payload,
                    "state": {"alignment_status": "failed", "reason": "missing_runtime"}
                }

            entry = next(iter(runtime_nodes))
            flow_controller = AtorRuntime(
                entry=entry,
                nodes=runtime_nodes,
                runtime_node=runtime_node
            )
            flow_controller.attach()
            return {
                "status": "stable",
                "payload": {
                    "entry": entry,
                    "node_count": len(runtime_nodes)
                },
                "state": {
                    "alignment_status": "success"
                }
            }
        except Exception as e:
            return {
                "status": "fractured",
                "payload": flow.payload,
                "state": {
                    "alignment_status": "failed",
                    "error": str(e)
                }
            }