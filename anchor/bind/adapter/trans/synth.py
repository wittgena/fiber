# anchor.bind.adapter.trans.synth
## @lineage: anchor.cli.adapter.trans.synth
import os
import sys
import ast
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Set, List, Tuple
from phase.bind.resolver import find_current_self, resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("rule.synth", phase="SYSTEM")

SELF_ROOT = find_current_self()
WORKSPACE_ROOT = resolve_path("workspace")
PATH = "bridge.llama"

class RuleSynthesizer:
    """
    단순 Diff 추출을 넘어, 원본과 변이된 코드 사이의 '심볼(Symbol)'을 추적하여
    1:1 및 1:N 패키지 분할 상황을 모두 방어할 수 있는 Rule Schema를 합성합니다.
    """
    
    def __init__(self, original_base_path: str, mutated_base_path: str):
        self.original_base = Path(original_base_path)
        self.mutated_base = Path(mutated_base_path)
        # PATH 환경변수를 OS 경로에 맞게 분해 (예: bridge.llama -> bridge/llama)
        self.bridge_path_parts = PATH.split(".")

    def _extract_symbols_via_ast(self, filepath: Path) -> Dict[str, str]:
        """
        AST를 파싱하여 {심볼명: 모듈경로} 형태의 매핑을 반환합니다.
        예: from llama_index.core.llms import BaseLLM -> {"BaseLLM": "llama_index.core.llms"}
        """
        if not filepath.exists():
            return {}
            
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(filepath))
        except SyntaxError:
            log.warning(f"[warning] Syntax error in file, skipping AST parse: {filepath}")
            return {} 
            
        symbol_map = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # alias.name은 원본 심볼명 (as 구문을 써도 원본 이름은 name에 담김)
                    symbol_map[alias.name] = alias.name
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    symbol_map[alias.name] = module
                    
        return symbol_map

    def synthesize_target(self, category: str, name: str) -> Dict[str, Set[str]]:
        """
        특정 Integration을 분석하여 Old Module 경로가 어떤 New Module 경로(들)로 
        치환되었는지 1:N 매핑 사전을 추출합니다.
        """
        category_pkg = category.replace("-", "_")
        name_pkg = name.replace("-", "_")
        
        orig_dir = (self.original_base / f"llama-index-integrations/{category_pkg}" / 
                   f"llama-index-{category.replace('_', '-')}-{name.replace('_', '-')}" /
                   f"llama_index/{category_pkg}/{name_pkg}")
                   
        # PATH 상수를 활용한 동적 경로 할당
        mut_dir = self.mutated_base / Path(*self.bridge_path_parts) / category_pkg / name_pkg
        
        if not mut_dir.exists():
            return {}

        rule_mapping = defaultdict(set)

        for mut_file in mut_dir.rglob("*.py"):
            relative_path = mut_file.relative_to(mut_dir)
            orig_file = orig_dir / relative_path
            
            if not orig_file.exists():
                continue

            orig_symbols = self._extract_symbols_via_ast(orig_file)
            mut_symbols = self._extract_symbols_via_ast(mut_file)
            
            for symbol, old_module in orig_symbols.items():
                if not (old_module.startswith("llama_index") or old_module.startswith("llama-index")):
                    continue

                if symbol in mut_symbols:
                    new_module = mut_symbols[symbol]
                    if old_module != new_module:
                        rule_mapping[old_module].add(new_module)

        return rule_mapping

    def generate_schema(self, targets: list[Tuple[str, str]]) -> str:
        """추출된 원시 매핑 데이터를 엔진이 읽을 수 있는 공식 Rule Schema JSON으로 컴파일"""
        global_mapping = defaultdict(set)
        for category, name in targets:
            log.info(f"[*] Analyzing symbols for: {category}/{name}...")
            local_mapping = self.synthesize_target(category, name)
            
            for old_mod, new_mods in local_mapping.items():
                global_mapping[old_mod].update(new_mods)

        schema = {
            "_meta": {
                "version": "1.0",
                "description": "Auto-synthesized mutation rules protecting against 1:N upstream splits."
            },
            "exact_matches": {},
            "wildcard_matches": [] 
        }

        for old_mod, new_mods in sorted(global_mapping.items()):
            schema["exact_matches"][old_mod] = sorted(list(new_mods))

        return json.dumps(schema, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    base_target = WORKSPACE_ROOT if 'WORKSPACE_ROOT' in globals() else Path("./")
    synthesizer = RuleSynthesizer(
        original_base_path=str(base_target / "temp_llama_clone"), 
        mutated_base_path=str(SELF_ROOT / "brane")
    )
    
    targets = [
        ("llms", "google-genai"),
        ("question_gen", "guidance"),
        ("readers", "s3")
    ]
    
    rule_schema_json = synthesizer.generate_schema(targets)
    log.info("\n## @artifact: Synthesized Mutation Rules (schema.json)")
    log.info(rule_schema_json)