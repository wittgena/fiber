# phi.ops.dphi.scanner
## @lineage: topos.dphi.scanner
"""
@desc: AST-based scanner to identify Python code that duplicates existing Rust/WASM logic.
       Generates a prompt to refactor Python compute logic into `WasmBroker` RPC calls.
"""
import os
import sys
import ast
from pathlib import Path
from typing import Dict, List, Any, Set

from phase.bind.resolver import resolve_path, load_bound, find_current_self
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.scanner")

SANDBOX_ROOT = resolve_path("sandbox")
OUTPUT_FILE = SANDBOX_ROOT / "wasm_refactor_targets.txt"

EXCLUDED_DIRS = {"__pycache__", "sandbox", "target", ".venv", ".git", "node_modules"}

# WASM 샌드박스에 이미 구현된 API 목록
WASM_APIS = {
    "compute_root_fingerprint", "generate_phase_id", "generate_topos_id",
    "verify_parity", "inscribe_actor", "seal_epoch"
}

class IntegrationNodeVisitor(ast.NodeVisitor):
    def __init__(self):
        self.results = {
            "Hash & Fingerprint (Target: compute_root_fingerprint)": [],
            "Crypto & Signature (Target: inscribe_actor, seal_epoch)": [],
            "Bitwise & Parity ID (Target: generate_phase_id, verify_parity)": [],
            "Validation & Guardrails (Target: verify_packet)": []
        }
        self.current_function = None

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.current_function = node
        
        # 함수 내부를 순회하기 위한 로컬 상태
        self.uses_hashlib = False
        self.uses_crypto = False
        self.uses_bitwise = False
        
        self.generic_visit(node)
        
        # 분석 결과에 따라 카테고리 매핑
        category = None
        if self.uses_crypto:
            category = "Crypto & Signature (Target: inscribe_actor, seal_epoch)"
        elif self.uses_hashlib:
            category = "Hash & Fingerprint (Target: compute_root_fingerprint)"
        elif self.uses_bitwise:
            category = "Bitwise & Parity ID (Target: generate_phase_id, verify_parity)"
        elif node.name.startswith(("validate_", "verify_", "check_")):
            category = "Validation & Guardrails (Target: verify_packet)"

        if category:
            args = [a.arg for a in node.args.args if a.arg != 'self']
            sig = f"def {node.name}({', '.join(args)})"
            self.results[category].append((node.lineno, sig))

        self.current_function = None

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            if "hashlib" in alias.name:
                self.uses_hashlib = True
            if "cryptography" in alias.name or "ed25519" in alias.name:
                self.uses_crypto = True
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        if node.module:
            if "hashlib" in node.module:
                self.uses_hashlib = True
            if "cryptography" in node.module or "ed25519" in node.module:
                self.uses_crypto = True
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp):
        # 파이썬에서 XOR(^), Left Shift(<<), Right Shift(>>) 연산을 사용하는 경우 추적
        if isinstance(node.op, (ast.BitXor, ast.LShift, ast.RShift)):
            if self.current_function:
                self.uses_bitwise = True
        self.generic_visit(node)


def scan_for_refactor_targets() -> Dict[str, List[str]]:
    self_root = find_current_self()
    bound = load_bound(self_root)
    around_repos = bound.get("around", {})
    
    if not around_repos:
        log.error("[ERROR] No 'around' repositories found in bound.json")
        sys.exit(1)

    aggregated_results = {k: [] for k in IntegrationNodeVisitor().results.keys()}
    scanned_files = 0
    matched_items = 0

    for repo_name, repo_meta in around_repos.items():
        repo_path = Path(repo_meta.get("path", ""))
        
        if not repo_path.exists() or not repo_meta.get("is_core", False):
            continue
            
        log.info(f"[SCAN] Inspecting Core Repository for Refactoring: {repo_name}")

        for file_path in repo_path.rglob("*.py"):
            if any(excluded in file_path.parts for excluded in EXCLUDED_DIRS):
                continue
                
            scanned_files += 1
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    source_code = f.read()
                
                tree = ast.parse(source_code, filename=str(file_path))
                visitor = IntegrationNodeVisitor()
                visitor.visit(tree)
                
                for cat, items in visitor.results.items():
                    for lineno, snippet in items:
                        rel_path = file_path.relative_to(repo_path)
                        display_path = f"[{repo_name}] {rel_path}"
                        aggregated_results[cat].append(f"{display_path}:{lineno} -> {snippet}")
                        matched_items += 1
                        
            except SyntaxError:
                pass
            except Exception as e:
                pass

    log.info(f"[SYSTEM] Refactor scan complete. Processed {scanned_files} files, found {matched_items} overlaps.")
    return aggregated_results

def generate_llm_prompt(results: Dict[str, List[str]]):
    """Formats the AST scan results into an optimized refactoring prompt."""
    os.makedirs(SANDBOX_ROOT, exist_ok=True)
    
    prompt = [
        "We have successfully deployed a high-performance Rust(WASM) Execution Engine inside our Python distributed system.",
        "The WASM engine already implements the following core logic: hashing, Ed25519 cryptography, ID generation (Bitwise/XOR parity), and validation.",
        "Below is a list of Python functions in our codebase that likely **duplicate** logic already running in the WASM engine.",
        "Our goal is to **DELETE the heavy Python compute logic** and replace it with lightweight asynchronous RPC calls to the `WasmBroker`.\n"
    ]
    
    for category, matches in results.items():
        prompt.append(f"### {category}")
        if not matches:
            prompt.append("  *(No overlapping logic found)*\n")
        else:
            for match in matches:
                prompt.append(f"- `{match}`")
            prompt.append("\n")
            
    prompt.append(
        "**Task:**\n"
        "1. Select the Top 3 Python functions from the list above that should be refactored immediately to reduce Python CPU overhead.\n"
        "2. For each selected function, write the REFACTORED Python code.\n"
        "3. The refactored code should remove `hashlib` or `cryptography` imports and instead use:\n"
        "   `result = await self.broker.invoke('wasm_method_name', payload_dict)`\n"
        "   Make sure to handle the `ExecutionResult` (check `result.success` and parse `result.output`)."
    )
    
    final_output = "\n".join(prompt)
    
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        f.write(final_output)
    log.info(f"[SUCCESS] Refactoring Prompt saved to: {OUTPUT_FILE}")

if __name__ == '__main__':
    log.info("\n=== [INIT] WASM Overlap & Refactor Scanner ===")
    scan_results = scan_for_refactor_targets()
    generate_llm_prompt(scan_results)
    log.info("=== [DONE] Scanner execution finished ===")