# phase.flow.logic.transformer
## @lineage: nexus.flow.logic.transformer
## @lineage: meta.flow.logic.transformer
## @lineage: meta.flow.trans.logic.transformer
## @lineage: meta.trans.logic.transformer
## @lineage: meta.model.trans.logic.transformer
## @lineage: arch.model.trans.logic.transformer
## @lineage: arch.contract.trans.logic.transformer
## @lineage: arch.gov.trans.logic.transformer
## @lineage: arch.topos.trans.logic.transformer
## @lineage: arch.topos.node.trans.logic.transformer
## @lineage: arch.topos.bound.trans.logic.transformer
## @lineage: arch.bound.trans.logic.transformer
## @lineage: arch.topic.logic.transformer
## @lineage: arch.code.logic.transformer
## @lineage: arch.model.code.logic.transformer
## @lineage: arch.project.logic.transformer
## @lineage: xphi.code.logic.transformer
## @lineage: topos.arch.code.logic.transformer
import ast
import json
import argparse
import sys
import networkx as nx
from pathlib import Path
from collections import Counter
from typing import TypedDict, List, Dict, Any
from dataclasses import dataclass, asdict
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

class LogicTransformer:
    """
    @phi: Logic Orbit (Pure AST Transformation)
    @flow: source(code) → parse(AST) → extract(import relations) → detect(runtime signals) → Ψ_struct fragments
    """
    RUNTIME_KEYWORDS = {"while", "asyncio", "Queue", "yield", "run_forever"}

    @staticmethod
    def parse_module(path: Path) -> Dict[str, Any]:
        try:
            content = path.read_text(encoding="utf-8")
            tree = ast.parse(content)
            
            ## ∂Φ_runtime: detect potential local topos (loop / async / feedback signals)
            is_topos = any(kw in content for kw in LogicTransformer.RUNTIME_KEYWORDS)
            
            ## Ψ_import: extract dependency edges from AST
            imports = []
            for node in ast.walk(tree):
                lineno = getattr(node, 'lineno', None)
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.append({"target": alias.name, "lineno": lineno})
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0: # 상대경로 보정
                        prefix = "." * node.level
                        target = f"{prefix}{node.module}" if node.module else prefix
                        imports.append({"target": target, "lineno": lineno})
                    elif node.module:
                        imports.append({"target": node.module, "lineno": lineno})
                        
            return {"imports": imports, "is_topos": is_topos}
        except Exception:
            return {"imports": [], "is_topos": False}
