# xphi.kernel.contract.proposer
import ast
import re
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Set
from collections import defaultdict

from anchor.bind.around import discover_repos

from arch.contract.registry.unified import contract
from arch.contract.registry.path import path_registry, path_contract
from phase.bind.resolver import find_current_self, resolve_path
from watcher.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
log = get_emitter('contract.proposer')

VALID_CONTRACT_ATTRS: Set[str] = set(vars(contract).keys())
VALID_CONTRACT_FUNCS: Set[str] = {
    func.__name__ for func in vars(contract).values() if callable(func)
}
VALID_CONTRACT_FUNCS.add("bootstrap_contract")

class UnifiedToposVisitor(ast.NodeVisitor):
    """Extracts explicit decorators and implicit @flow docstrings."""
    def __init__(self, module_fqn: str):
        self.module_fqn = module_fqn
        self.found_nodes: List[Dict[str, Any]] = []

    def _is_contract_decorator(self, name: str) -> bool:
        if not name: return False
        if name.startswith('contract.'):
            return name.split('.')[1] in VALID_CONTRACT_ATTRS
        return name in VALID_CONTRACT_FUNCS

    def _extract_args(self, call_node: ast.Call) -> Dict[str, Any]:
        extracted = {"_positional": [], "_kwargs": {}}
        
        ## Improved parsing for positional arguments (List support)
        for arg in call_node.args:
            if isinstance(arg, ast.Constant):
                extracted["_positional"].append(arg.value)
            elif isinstance(arg, ast.List):
                extracted["_positional"].append(
                    [elt.value for elt in arg.elts if isinstance(elt, ast.Constant)]
                )
                
        ## Parse keyword arguments
        for kw in call_node.keywords:
            if kw.arg:
                if isinstance(kw.value, ast.Constant):
                    extracted["_kwargs"][kw.arg] = kw.value.value
                elif isinstance(kw.value, ast.List):
                    extracted["_kwargs"][kw.arg] = [
                        elt.value for elt in kw.value.elts if isinstance(elt, ast.Constant)
                    ]
        return extracted

    def _get_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name): return node.id
        elif isinstance(node, ast.Attribute):
            val = self._get_name(node.value)
            return f"{val}.{node.attr}" if val else ""
        return ""

    def _extract_function_hints(self, node: ast.FunctionDef) -> Dict[str, Any]:
        args = [arg.arg for arg in node.args.args if arg.arg not in ('self', 'cls')]
        return {"args": args}

    def _parse_flow_docstring(self, docstring: str) -> Dict[str, Any]:
        """@flow: Searches for @flow patterns in docstrings -> Auto-infers Requires/Emits."""
        if not docstring: return {}
        match = re.search(r'@flow\s+(.+)', docstring)
        if not match: return {}
        
        evidence = match.group(1).strip()
        if '->' not in evidence and '→' not in evidence:
            return {}
            
        parts = re.split(r'\s*(?:->|→)\s*', evidence)
        
        ## Split by comma (,) to convert multiple elements into a proper list
        if len(parts) >= 2:
            return {
                "requires": [p.strip() for p in parts[0].split(',') if p.strip()],
                "emits": [p.strip() for p in parts[-1].split(',') if p.strip()],
                "evidence": evidence
            }
        return {}

    def _analyze_node(self, node: ast.AST, node_type: str, hints: Dict[str, Any]):
        """Constructs a node by comprehensively evaluating decorators and docstrings."""
        docstring = ast.get_docstring(node)
        implicit_contract = self._parse_flow_docstring(docstring)
        
        explicit_contracts = []
        decorator_types = []
        positional_args = []
        
        for decorator in node.decorator_list:
            dec_name = ""
            args = {"_positional": [], "_kwargs": {}}
            
            if isinstance(decorator, ast.Call):
                dec_name = self._get_name(decorator.func)
                args = self._extract_args(decorator)
            elif isinstance(decorator, (ast.Name, ast.Attribute)):
                dec_name = self._get_name(decorator)
            
            if self._is_contract_decorator(dec_name):
                kwargs = args["_kwargs"].copy()
                
                ## @bind: Requires/Emits passed as positionals to explicit kwargs
                if "node" in dec_name:
                    if len(args["_positional"]) >= 2 and not kwargs.get("requires"):
                        kwargs["requires"] = args["_positional"][1]
                    if len(args["_positional"]) >= 3 and not kwargs.get("emits"):
                        kwargs["emits"] = args["_positional"][2]
                
                explicit_contracts.append(kwargs)
                decorator_types.append(dec_name)
                positional_args.extend(args["_positional"])

        ## @identify: valid nodes and assign roles
        if explicit_contracts or implicit_contract:
            if explicit_contracts and implicit_contract:
                role = "Hybrid Node"
            elif explicit_contracts:
                role = "Explicit Node"
            else:
                role = "Implicit Node"

            dec_type = decorator_types[0] if decorator_types else "implicit.flow"
            explicit_base = explicit_contracts[0] if explicit_contracts else {}
            self.found_nodes.append({
                "fqn": f"{self.module_fqn}.{node.name}",
                "decorator_type": dec_type,
                "target_type": node_type,
                "role": role,
                "contract": {
                    "explicit": explicit_base,
                    "implicit": implicit_contract,
                    "inferred": {}
                },
                "shape_hints": hints or {},
                "positional_args": positional_args
            })

    def visit_FunctionDef(self, node: ast.FunctionDef):
        hints = {"signature": self._extract_function_hints(node)}
        self._analyze_node(node, "function", hints)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):
        methods_hints = {}
        for n in node.body:
            if isinstance(n, ast.FunctionDef) and not n.name.startswith("__"):
                methods_hints[n.name] = self._extract_function_hints(n)
                
        hints = {"methods": list(methods_hints.keys()), "signatures": methods_hints}
        self._analyze_node(node, "class", hints)
        self.generic_visit(node)

def generate_task_proposals(repos: List[Path]) -> Dict[str, List[Dict[str, Any]]]:
    proposals = defaultdict(list)
    start_time = time.time()
    scanned_files = 0
    
    for repo in repos:
        if not repo.exists() or not repo.is_dir():
            continue

        for py_file in repo.rglob("*.py"):
            if py_file.name.startswith("_") and py_file.name != "__init__.py":
                continue

            try:
                relative = py_file.relative_to(repo)
                module_fqn = ".".join(relative.with_suffix("").parts)
                
                source = py_file.read_text(encoding="utf-8")
                tree = ast.parse(source, filename=str(py_file))
                
                visitor = UnifiedToposVisitor(module_fqn)
                visitor.visit(tree)
                
                for node_data in visitor.found_nodes:
                    dec_type = node_data.pop("decorator_type")
                    role = node_data.get("role")
                    
                    if 'bootstrap' in dec_type:
                        group = node_data["positional_args"][0] if node_data["positional_args"] else "ungrouped"
                    elif role == "Implicit Node":
                        parts = node_data['fqn'].split('.')
                        group = parts[1] if len(parts) > 1 else "implicit"
                    elif "cli" in dec_type:
                        group = "cli.tasks"
                        node_data["decorator_type"] = dec_type
                    else:
                        group = "phase.nodes"
                        
                    proposals[group].append(node_data)
                    
                scanned_files += 1
            except Exception:
                pass 

    elapsed = time.time() - start_time
    log.info(f"[AST Generator] Scanned {scanned_files} files across {len(repos)} repos in {elapsed:.3f}s")
    return dict(proposals)

def print_console_summary(proposals: Dict[str, List[Dict[str, Any]]]):
    log.info("\n## Topos Proposal (Unified State)")
    total_nodes = 0
    cli_nodes = []
    
    for group, nodes in proposals.items():
        if group == "cli.tasks":
            cli_nodes.extend(nodes)
            total_nodes += len(nodes)
            continue
            
        log.info(f"\n> Group: {group} (Count: {len(nodes)})")
        sorted_nodes = sorted(nodes, key=lambda x: (x.get('target_type', ''), x['fqn']))
        
        for i, node in enumerate(sorted_nodes):
            is_last = (i == len(sorted_nodes) - 1)
            prefix = "  └──" if is_last else "  ├──"
            
            fqn = node['fqn']
            target_type = node.get('target_type', 'unknown')
            role = node.get('role', 'Unknown')
            
            contract_data = node.get('contract', {})
            explicit = contract_data.get('explicit', {})
            implicit = contract_data.get('implicit', {})
            
            req_list = explicit.get('requires') or implicit.get('requires') or []
            emi_list = explicit.get('emits') or implicit.get('emits') or []
            
            req = ",".join(req_list) if isinstance(req_list, list) else req_list
            emi = ",".join(emi_list) if isinstance(emi_list, list) else emi_list
            
            if role == "Implicit Node": tag = "[Flow]"
            elif role == "Hybrid Node": tag = "[Hybr]"
            elif not req and not emi:   tag = "[Fluid]"
            else:                       tag = "[Cryst]"
                
            req_str = req or '~'
            emi_str = emi or '~'
            log.info(f"{prefix} [{target_type[:4].upper()}] {fqn} ({req_str} ➔ {emi_str}) {tag}")
            
        total_nodes += len(nodes)

    if cli_nodes:
        log.info("\n" + "="*50)
        log.info("🚀 EXECUTABLE CLI TASKS (LLM INSTRUCTIONS)")
        log.info("The following commands are available for the LLM or System to execute.")
        log.info("="*50)
        
        for task in sorted(cli_nodes, key=lambda x: x['fqn']):
            fqn = task['fqn']
            explicit = task.get('contract', {}).get('explicit', {})
            cmd_name = explicit.get('name') or (task['positional_args'][0] if task.get('positional_args') else "unknown")
            args = explicit.get('args', [])
            
            exec_module = fqn.rsplit('.', 1)[0] 
            
            log.info(f"  ▶ Command: {cmd_name}")
            log.info(f"    - Module  : {fqn}")
            if args:
                log.info(f"    - Options : {', '.join(args)}")
            log.info(f"    - Execute : python -m {exec_module}")
            log.info("-" * 50)

    log.info(f"\n## Total Unified Nodes: {total_nodes}\n")


@path_contract("contract/flow.json")
def execute_proposer():
    """
    @flow: Source Code AST -> contract/flow.json
    @contract.spec:
    - Requires: Python source files from locally discovered repositories.
    - Emits: A unified JSON map representing the static architecture.
    """
    target_repos = discover_repos(SELF_ROOT)
    if not target_repos:
        log.warning("No repositories discovered under SELF_ROOT.")
        return
        
    tasks = generate_task_proposals(target_repos)
    print_console_summary(tasks)
    
    contract_dir = path_registry.resolve("contract")
    proposal_path = contract_dir / "flow.json"
    proposal_path.parent.mkdir(parents=True, exist_ok=True)
    with open(proposal_path, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, ensure_ascii=False)
        
    log.info(f"Successfully generated unified proposal map at: {proposal_path}")

if __name__ == "__main__":
    execute_proposer()