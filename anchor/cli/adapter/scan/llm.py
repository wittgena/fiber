# anchor.cli.adapter.scan.llm
import sys
import ast
import importlib
import inspect
import json
import urllib.request
import argparse
from pathlib import Path
from typing import Dict, Any, Set, List, Optional
from dataclasses import dataclass, field, asdict

from anchor.inter.bound.base.llms.base import BaseLLM 
from anchor.registry.resolver.ext import ExtResolver
from bound.adapter.mapper.repo.inter import ProjectLayout

from arch.contract.registry.unified import contract
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

log = get_emitter("scan.llm", phase="SYSTEM")

TARGET_REPO = "brane"

@dataclass
class LLMCapabilities:
    is_function_calling: bool = False
    is_openai_like: bool = False
    is_multimodal: bool = False
    supports_structured_outputs: bool = False

@dataclass
class LLMInfo:
    status: str
    type: str
    layout: Optional[ProjectLayout] = None
    tags: List[str] = field(default_factory=list)
    module: Optional[str] = None
    class_name: Optional[str] = None 
    lineage: List[str] = field(default_factory=list)
    accepted_kwargs: List[str] = field(default_factory=list)
    capabilities: Optional[LLMCapabilities] = None
    source_repo: Optional[str] = None


class LLMScanner:
    KNOWN_LLM_BASES = {
        "BaseLLM", "LLM", "CustomLLM", 
        "FunctionCallingLLM", "OpenAILike", "MultiModalLLM"
    }

    def __init__(self, target: Optional[str] = None):
        self.base_path = ExtResolver.get("local")
        self.target = target
        self.prefix = str(ExtResolver.get("prefix"))
        self.core_namespace = ExtResolver.RULES["constants"]["core_namespace"]

    def _get_module_path(self, file_path: Path) -> str:
        try:
            parts = file_path.parts
            if self.core_namespace in parts:
                idx = parts.index(self.core_namespace)
                return ".".join(parts[idx:]).replace(".py", "")
            return ".".join(file_path.relative_to(Path.cwd()).parts).replace(".py", "")
        except Exception as e:
            log.error(f"[ERROR] Failed to parse module path: {file_path} - {e}")
            return ""

    def _extract_rich_metadata(self, obj: Any) -> Dict[str, Any]:
        mro = inspect.getmro(obj)
        lineage = [cls.__name__ for cls in mro if cls.__name__ not in ("object", "BaseModel", "Generic")]
        
        accepted_kwargs: Set[str] = set()
        if hasattr(obj, "model_fields"): 
            accepted_kwargs.update(obj.model_fields.keys())
        elif hasattr(obj, "__fields__"): 
            accepted_kwargs.update(obj.__fields__.keys())
            
        accepted_kwargs.update(["additional_kwargs", "callback_manager", "system_prompt"])

        capabilities = LLMCapabilities(
            is_function_calling="FunctionCallingLLM" in lineage,
            is_openai_like="OpenAILike" in lineage,
            is_multimodal="MultiModalLLM" in lineage,
            supports_structured_outputs=hasattr(obj, "astructured_predict")
        )
        return {"lineage": lineage, "accepted_kwargs": list(accepted_kwargs), "capabilities": capabilities}

    def _scan_local(self) -> Dict[str, LLMInfo]:
        log.info(f"[*] Scanning locally cloned modules at: {self.base_path}")
        registry: Dict[str, LLMInfo] = {}

        if self.target:
            target_dir = self.base_path / f"{self.prefix}{self.target}"
            if target_dir.exists() and target_dir.is_dir():
                dirs_to_scan = [target_dir]
            else:
                log.warning(f"[-] Target directory not found locally: {target_dir}")
                return registry
        else:
            dirs_to_scan = [d for d in self.base_path.iterdir() if d.is_dir() and d.name.startswith(self.prefix)]

        for repo_dir in dirs_to_scan:
            provider_key = repo_dir.name.replace(self.prefix, "")
            layout_meta = ProjectLayout.resolve(repo_dir)
            if not layout_meta.base_py_locations:
                log.debug(f"[-] No base.py found in {repo_dir.name}. Saving layout meta only.")
                registry[provider_key] = LLMInfo(
                    status="layout_only", type="local_scanned",
                    layout=layout_meta, tags=[provider_key],
                    source_repo=str(repo_dir.resolve())
                )
                continue

            target_base_py = Path(layout_meta.base_py_locations[0])
            module_path = self._get_module_path(target_base_py)
            if not module_path: continue

            found_info = None
            try:
                module = importlib.import_module(module_path)
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseLLM) and obj is not BaseLLM:
                        rich_meta = self._extract_rich_metadata(obj)
                        found_info = LLMInfo(
                            status="installed", type="local_dynamic_scanned",
                            module=module_path, class_name=name, layout=layout_meta,
                            tags=[provider_key], lineage=rich_meta["lineage"],
                            accepted_kwargs=rich_meta["accepted_kwargs"],
                            capabilities=rich_meta["capabilities"], source_repo=str(repo_dir.resolve())
                        )
                        break
            except Exception as e:
                log.debug(f"[Import Warning] Skipped dynamic scan ({module_path}): {e}")

            if not found_info:
                try:
                    with open(target_base_py, "r", encoding="utf-8") as f:
                        for node in ast.walk(ast.parse(f.read())):
                            if isinstance(node, ast.ClassDef):
                                base_names = [b.id for b in node.bases if isinstance(b, ast.Name)]
                                if any(name in self.KNOWN_LLM_BASES for name in base_names):
                                    found_info = LLMInfo(
                                        status="installed", type="local_ast_scanned",
                                        module=module_path, class_name=node.name, layout=layout_meta,
                                        tags=[provider_key], lineage=base_names,
                                        capabilities=LLMCapabilities(
                                            is_function_calling="FunctionCallingLLM" in base_names,
                                            is_openai_like="OpenAILike" in base_names
                                        ), source_repo=str(repo_dir.resolve())
                                    )
                                    break
                except Exception as e:
                    log.debug(f"[AST Warning] Parse failure: {e}")

            registry[provider_key] = found_info if found_info else LLMInfo(
                status="no_llm_class_found", type="local_scanned", layout=layout_meta,
                tags=[provider_key], source_repo=str(repo_dir.resolve())
            )
        return registry

    def _scan_remote(self) -> Dict[str, LLMInfo]:
        log.info("[*] Fetching remote catalog from GitHub (Fallback mode)...")
        registry = {}

        api_url = str(ExtResolver.get("api"))
        req = urllib.request.Request(api_url, headers={'User-Agent': 'Theoria-Mutation-Agent'})
        
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                for item in json.loads(response.read().decode()):
                    if item.get("type") == "dir" and item.get("name", "").startswith(self.prefix):
                        llm_name = item["name"].replace(self.prefix, "")
                        if self.target and llm_name != self.target:
                            continue

                        registry[llm_name] = LLMInfo(
                            status="available_for_mutation", type="remote_catalog",
                            source_repo=item.get("html_url"), tags=[llm_name]
                        )
            log.info(f"[+] Acquired {len(registry)} remote module catalogs.")
            return registry
        except Exception as e:
            log.error(f"[-] Remote scan failed: {e}")
            return {}

    def scan(self) -> Dict[str, Any]:
        raw_result = self._scan_local() if self.base_path and self.base_path.exists() else self._scan_remote()
        return {k: asdict(v) for k, v in raw_result.items()}

def entry_task(args):
    parser = argparse.ArgumentParser(description="Brane LlamaIndex Component Scanner (Unified Edition)")
    parser.add_argument("--out", type=str, default=None, help="Output JSON path (optional)")
    parser.add_argument("--target", type=str, default=None, help="Specific target to scan (e.g., 'openai')")
    parsed_args = parser.parse_args(args)

    def _execute_scan():
        if (cwd := str(Path.cwd())) not in sys.path: sys.path.insert(0, cwd)
        target_msg = f" (Target: {parsed_args.target})" if parsed_args.target else " (Bulk Scan)"
        log.info(f"[*] Initializing scanner for repo [{TARGET_REPO}]{target_msg}")
        
        try:
            result = LLMScanner(target=parsed_args.target).scan()
            json_output = json.dumps(result, indent=4, ensure_ascii=False)
            
            if parsed_args.out:
                out_path = Path(parsed_args.out)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json_output, encoding="utf-8")
                log.signal(f"[SUCCESS] Scanned features written to: {out_path.resolve()}")
            else:
                log.info(f"\n[SCAN RESULT]\n{json_output}")
                
        except Exception as e:
            log.error(f"[ERROR] Scanner execution failed: {e}")
            sys.exit(1)

    return CliTaskAdapter(_execute_scan)

@contract.cli(
    name="scan.llm", 
    args=["--out", "--target"], 
    tags=["llama", "scanner", "llm"], 
    entry="entry_task"
)
def main(args=None):
    if args is not None: return entry_task(args)
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local: entry_task(remain).run()
    else: dispatch_cli("scan.llm", entry_task, __file__)


if __name__ == "__main__":
    main()