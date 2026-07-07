# anchor.cli.adapter.scan.llm
## @lineage: xphi.adapter.scan.llm
## @lineage: xphi.trans.llm.scanner
## @lineage: xphi.flow.llm.scanner
## @lineage: xphi.flow.scanner.llm
## @lineage: xphi.manager.scanner.llm
import os
import sys
import ast
import importlib
import inspect
import json
import urllib.request
import argparse
from pathlib import Path
from typing import Dict, Any, Set

import anchor.inter.llms as base_path
from anchor.inter.bound.base.llms.base import BaseLLM 
from arch.contract.registry.unified import contract
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

EXT_REPO = "ext-phase"
log = get_emitter("llm.scanner", phase="SYSTEM")

class LLMScanner:
    """Dual-mode LLM scanner with Deep Introspection for Local Modules"""
    
    KNOWN_LLM_BASES = {
        "BaseLLM", "LLM", "CustomLLM", 
        "FunctionCallingLLM", "OpenAILike", "MultiModalLLM"
    }

    GITHUB_LLMS_API = f"https://api.github.com/repos/{EXT_REPO}/llama_index/contents/llama-index-integrations/llms"

    def __init__(self, base_path: str):
        self.base_path = Path(base_path)

    def _get_module_path(self, file_path: Path) -> str:
        try:
            parts = file_path.parts
            if "bound" in parts:
                idx = parts.index("bound")
                module_parts = parts[idx:]
                return ".".join(module_parts).replace(".py", "")
            
            rel_path = file_path.relative_to(Path.cwd())
            return ".".join(rel_path.parts).replace(".py", "")
        except Exception as e:
            log.error(f"[ERROR] Failed to parse module path: {file_path} - {e}")
            return ""

    def _scan_remote_catalog(self) -> Dict[str, Dict[str, Any]]:
        log.info("[*] Fetching remote LLM catalog from GitHub...")
        registry = {}
        req = urllib.request.Request(self.GITHUB_LLMS_API, headers={'User-Agent': 'Theoria-Mutation-Agent'})
        
        try:
            with urllib.request.urlopen(req) as response:
                items = json.loads(response.read().decode())
                for item in items:
                    if item.get("type") == "dir" and item.get("name", "").startswith("llama-index-llms-"):
                        llm_name = item["name"].replace("llama-index-llms-", "")
                        registry[llm_name] = {
                            "status": "available_for_mutation",
                            "source_repo": item.get("html_url"),
                            "type": "remote_catalog",
                            "tags": [llm_name]
                        }
            log.info(f"[+] Acquired {len(registry)} remote module catalogs.")
            return registry
        except Exception as e:
            log.error(f"[-] Remote scan failed: {e}")
            return {}

    def _extract_rich_metadata(self, obj: Any) -> Dict[str, Any]:
        mro = inspect.getmro(obj)
        lineage = [cls.__name__ for cls in mro if cls.__name__ not in ("object", "BaseModel", "Generic")]
        
        accepted_kwargs: Set[str] = set()
        if hasattr(obj, "model_fields"): 
            accepted_kwargs.update(obj.model_fields.keys())
        elif hasattr(obj, "__fields__"): 
            accepted_kwargs.update(obj.__fields__.keys())
            
        accepted_kwargs.update(["additional_kwargs", "callback_manager", "system_prompt"])

        capabilities = {
            "is_function_calling": "FunctionCallingLLM" in lineage,
            "is_openai_like": "OpenAILike" in lineage,
            "is_multimodal": "MultiModalLLM" in lineage,
            "supports_structured_outputs": hasattr(obj, "astructured_predict")
        }

        return {
            "lineage": lineage,
            "accepted_kwargs": list(accepted_kwargs),
            "capabilities": capabilities
        }

    def _scan_local_installed(self) -> Dict[str, Dict[str, Any]]:
        log.info(f"[*] Scanning locally installed LLM modules at: {self.base_path}")
        registry = {}
        
        if not self.base_path.exists():
            log.warning(f"[-] Base path not found: {self.base_path}")
            return registry

        for file_path in self.base_path.rglob("base.py"):
            module_path = self._get_module_path(file_path)
            if not module_path:
                continue

            provider_key = file_path.parent.name
            found_class_info = None

            try:
                module = importlib.import_module(module_path)
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseLLM) and obj is not BaseLLM:
                        rich_meta = self._extract_rich_metadata(obj)
                        
                        found_class_info = {
                            "status": "installed",
                            "module": module_path,
                            "class": name,
                            "type": "local_dynamic_scanned",
                            "tags": [provider_key],
                            **rich_meta  
                        }
                        break
            except ImportError as e:
                log.debug(f"[Import Warning] Skipped deep scan due to missing dependencies ({module_path}): {e}")
            except Exception as e:
                log.error(f"[Inspect Error] Fatal error during dynamic scan ({module_path}): {e}")

            if not found_class_info:
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        tree = ast.parse(f.read())
                    
                    for node in ast.walk(tree):
                        if isinstance(node, ast.ClassDef):
                            base_names = [b.id for b in node.bases if isinstance(b, ast.Name)]
                            if any(name in self.KNOWN_LLM_BASES for name in base_names):
                                found_class_info = {
                                    "status": "installed",
                                    "module": module_path,
                                    "class": node.name,
                                    "type": "local_ast_scanned",
                                    "tags": [provider_key],
                                    "lineage": base_names, 
                                    "accepted_kwargs": [], 
                                    "capabilities": {
                                        "is_function_calling": "FunctionCallingLLM" in base_names,
                                        "is_openai_like": "OpenAILike" in base_names
                                    }
                                }
                                break
                except Exception as e:
                    log.debug(f"[AST Warning] Parse failure: {e}")

            if found_class_info:
                registry[provider_key] = found_class_info

        return registry

    def scan(self, target: str = "local") -> Dict[str, Dict[str, Any]]:
        if target == "remote":
            return self._scan_remote_catalog()
        elif target == "local":
            return self._scan_local_installed()
        else:
            raise ValueError(f"[ERROR] Unsupported target: {target}")


def entry_task(args):
    parser = argparse.ArgumentParser(description="Brane LlamaIndex LLM Scanner")
    parser.add_argument("--target", type=str, choices=["local", "remote"], default="local", help="Scan target: 'local' or 'remote'")
    parser.add_argument("--repo", type=str, default="brane", help="Target repository context (e.g., brane)") # <-- --repo 인자 추가
    parser.add_argument("--out", type=str, default=None, help="Output JSON path (optional)")
    parsed_args = parser.parse_args(args)

    def _execute_scan():
        current_dir = str(Path.cwd())
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)

        # [핵심 로직] 기존 import 객체를 활용하여 실제 OS 경로(Absolute Path) 추출
        if hasattr(base_path, '__path__'):
            actual_base_path = base_path.__path__[0]
        else:
            actual_base_path = os.path.dirname(base_path.__file__)

        log.info(f"[*] Initializing scanner for repo [{parsed_args.repo}]")
        
        # __name__ 대신 도출된 실제 경로 문자열 전달
        scanner = LLMScanner(base_path=actual_base_path)
        try:
            result = scanner.scan(target=parsed_args.target)
            json_output = json.dumps(result, indent=4, ensure_ascii=False)
            
            if parsed_args.out:
                out_path = Path(parsed_args.out)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json_output, encoding="utf-8")
                log.signal(f"[SUCCESS] Scanned features written to: {out_path.resolve()}")
            else:
                log.info(f"\n[{parsed_args.target.upper()} SCAN RESULT]\n{json_output}")
                
        except Exception as e:
            log.error(f"[ERROR] Scanner execution failed: {e}")
            sys.exit(1)

    return CliTaskAdapter(_execute_scan)


# args 목록에 --repo 추가
@contract.cli(
    name="llm.scanner", 
    args=["--target", "--repo", "--out"],
    tags=["llama", "scanner", "llm"],
    entry="entry_task" 
)
def main(args=None):
    if args is not None:
        return entry_task(args)
    
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("llm.scanner", entry_task, __file__)

if __name__ == "__main__":
    main()