# anchor.cli.adapter.trans.analyze.depend
## @lineage: xphi.adapter.trans.analyze.depend
## @lineage: xphi.trans.analyze.depend
## @lineage: xphi.flow.analyze.depend
import os
import sys
import ast
import re
import json
import argparse
import urllib.request
from urllib.error import HTTPError
from pathlib import Path
from typing import Dict, List, Set

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        print("[CRITICAL] Python 3.11 미만이거나 'tomli' 패키지가 설치되지 않았습니다. pip install tomli 를 실행하세요.")
        sys.exit(1)

import anchor.inter.bound as adapter_path
from phase.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter

log = get_emitter("analyze.depend", phase="SYSTEM")

SELF_ROOT = find_current_self()
EXT_REPO = "ext-phase"
TARGET_REPO = "brane"
DEFAULT_TAG = "v0.14.22"
ADAPTER_PATH = adapter_path.__name__

class WebDependencyFetcher:
    """GitHub에서 pyproject.toml 또는 requirements.txt를 읽어 외/내부 의존성을 분리"""
    def __init__(self, tag: str):
        self.tag = tag

    def fetch_dependencies(self, category: str, integration_name: str) -> dict:
        category_dir = category.replace("_", "-")
        name_dir = integration_name.replace("_", "-")
        base_subpath = f"llama-index-integrations/{category}/llama-index-{category_dir}-{name_dir}"
        raw_base_url = f"https://raw.githubusercontent.com/{EXT_REPO}/llama_index/{self.tag}/{base_subpath}"
        
        try:
            req = urllib.request.Request(f"{raw_base_url}/pyproject.toml")
            with urllib.request.urlopen(req) as resp:
                content = resp.read().decode('utf-8')
                return self._parse_toml_deps(content)
        except HTTPError as e:
            if e.code != 404:
                log.warning(f"Failed to fetch TOML for {integration_name}: {e}")
        
        try:
            req = urllib.request.Request(f"{raw_base_url}/requirements.txt")
            with urllib.request.urlopen(req) as resp:
                content = resp.read().decode('utf-8')
                return self._parse_requirements_deps(content)
        except Exception as e:
            log.warning(f"No web dependency manifest found for {integration_name}: {e}")
            
        return {"external": {}, "internal_raw": []}

    def _parse_toml_deps(self, content: str) -> dict:
        external = {}
        internal_raw = []
        try:
            parsed = tomllib.loads(content)
            
            if "project" in parsed and "dependencies" in parsed["project"]:
                deps_list = parsed["project"]["dependencies"]
                for dep in deps_list:
                    match = re.match(r"^([a-zA-Z0-9\-_]+(?:\[.*?\])?)(.*)$", dep.strip())
                    if match:
                        name = match.group(1).strip()
                        ver = match.group(2).strip() or "*"
                        
                        if name.startswith("llama-index"):
                            internal_raw.append(name)
                        else:
                            external[name] = ver
                            
            elif "tool" in parsed and "poetry" in parsed["tool"] and "dependencies" in parsed["tool"]["poetry"]:
                deps_dict = parsed["tool"]["poetry"]["dependencies"]
                for name, ver in deps_dict.items():
                    if name == "python":
                        continue
                    if name.startswith("llama-index"):
                        internal_raw.append(name)
                    else:
                        if isinstance(ver, dict):
                            extras = ver.get("extras", [])
                            if extras:
                                name = f"{name}[{','.join(extras)}]"
                            ver = ver.get("version", "*")
                        external[name] = ver
                        
        except Exception as e:
            pass
            
        return {"external": external, "internal_raw": internal_raw}

    def _parse_requirements_deps(self, content: str) -> dict:
        external = {}
        internal_raw = []
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            
            parts = line.split(">=")[0].split("==")[0].split("<=")[0].split(">")[0].split("<")[0].strip()
            version = line[len(parts):].strip()
            
            if parts.startswith("llama-index"):
                internal_raw.append(parts)
            else:
                external[parts] = version if version else "*"
                
        return {"external": external, "internal_raw": internal_raw}


class LocalAstScanner:
    """로컬 소스코드를 AST로 분석하여 의존성을 추출하고 네임스페이스 기준으로 기록"""
    
    @staticmethod
    def scan_core_usage(target_dir: Path) -> Dict[str, List[str]]:
        """
        결과 포맷: { "bound.adapter.llama.하위모듈": ["utils", "llms.base", ...] }
        """
        core_dependencies = {}
        
        if not target_dir.exists():
            return core_dependencies

        for py_file in target_dir.rglob("*.py"):
            try:
                code = py_file.read_text(encoding="utf-8")
                tree = ast.parse(code, filename=str(py_file))
            except Exception as e:
                log.warning(f"AST Parsing failed for {py_file.name}: {e}")
                continue

            # 파일 경로를 파이썬 네임스페이스 형식으로 치환 (예: "utils.py" -> "utils")
            rel_path = py_file.relative_to(target_dir)
            namespace = ".".join(rel_path.with_suffix('').parts)
            
            for node in ast.walk(tree):
                modules_found = []
                
                if isinstance(node, ast.ImportFrom) and node.module:
                    if node.module.startswith(ADAPTER_PATH):
                        modules_found.append(node.module)
                        
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith(ADAPTER_PATH):
                            modules_found.append(alias.name)
                
                for adapter_mod in modules_found:
                    if adapter_mod not in core_dependencies:
                        core_dependencies[adapter_mod] = []
                    if namespace not in core_dependencies[adapter_mod]:
                        core_dependencies[adapter_mod].append(namespace)
                        
        return core_dependencies


class LlamaInterScanner:
    """전체 패키지를 스캔하고 역방향 의존성 지도를 구성하는 메인 파이프라인"""
    def __init__(self, tag: str = DEFAULT_TAG):
        self.tag = tag
        self.fetcher = WebDependencyFetcher(tag=tag)
        
        self.inter_llms_root = SELF_ROOT / TARGET_REPO / "bound" / "inter" / "llms"
        if not self.inter_llms_root.is_dir():
            self.inter_llms_root = SELF_ROOT / "bound" / "inter" / "llms"

    def run(self, output_file: str):
        log.signal(f"[START] Scanning integration dependencies inside: {self.inter_llms_root}")
        if not self.inter_llms_root.exists():
            log.error(f"Target directory not found: {self.inter_llms_root}")
            sys.exit(1)

        manifest_by_integration = {}
        manifest_by_dependency = {}

        integration_dirs = [d for d in self.inter_llms_root.iterdir() if d.is_dir() and not d.name.startswith("_")]

        for int_dir in integration_dirs:
            integration_name = int_dir.name
            log.info(f"Analyzing LLM Integration Module: '{integration_name}'")
            
            web_deps = self.fetcher.fetch_dependencies(category="llms", integration_name=integration_name)
            local_core_usages = LocalAstScanner.scan_core_usage(int_dir)
            
            # 1. Integration (LLM) 중심 맵핑 저장
            manifest_by_integration[integration_name] = {
                "meta": {
                    "scanned_tag": self.tag,
                    "local_path": str(int_dir.relative_to(SELF_ROOT.parent if SELF_ROOT.name == TARGET_REPO else SELF_ROOT))
                },
                "external_dependencies": web_deps["external"],
                "upstream_internal_packages": web_deps["internal_raw"],
                "bound_adapter_llama_lineage": local_core_usages
            }

            # 2. Dependency (의존성 코어) 중심의 역방향 맵핑 병합
            for adapter_mod, using_namespaces in local_core_usages.items():
                if adapter_mod not in manifest_by_dependency:
                    manifest_by_dependency[adapter_mod] = {}
                manifest_by_dependency[adapter_mod][integration_name] = using_namespaces

        # 최종 Dual-View 구조 도출
        final_manifest = {
            "by_dependency": manifest_by_dependency,
            "by_integration": manifest_by_integration
        }

        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(final_manifest, f, indent=4, ensure_ascii=False)
            
        log.signal(f"[SUCCESS] Dependency Analysis Complete. Manifest written to: {out_path.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Brane LlamaIndex Integration Dependency Analyzer")
    parser.add_argument("--tag", default=DEFAULT_TAG, help=f"Target upstream tag for web inspection (default: {DEFAULT_TAG})")
    parser.add_argument("--output", default="integration_dependencies.json", help="Output JSON path")
    
    args = parser.parse_args()
    
    analyzer = LlamaInterScanner(tag=args.tag)
    analyzer.run(output_file=args.output)

if __name__ == "__main__":
    main()