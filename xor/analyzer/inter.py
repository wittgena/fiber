# xor.analyzer.inter
## @lineage: xor.opt.analyzer.parser.inter
## @lineage: xphi.analyzer.parser.inter
import ast
import re
import urllib.request
from urllib.error import HTTPError
from pathlib import Path
from typing import List, Dict, Any

try:
    import tomllib
except ImportError:
    import toml as tomllib

from adapter.mapper.inter.project import (
    ProjectMeta, 
    WorkflowCommand, 
    UsageSnippet, 
    ModuleMeta, 
    DependencySpec
)

class PyProjectParser:
    """@desc: 로컬 pyproject.toml 분석 및 환경/의존성 메타데이터 추출기"""
    
    def parse(self, path: Path) -> ProjectMeta:
        if not path.exists():
            return ProjectMeta(name="unknown", version="0.0.0", description="", python_requires="")
            
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        project = data.get("project", {})
        
        deps = []
        for dep in project.get("dependencies", []):
            match = re.match(r"^([a-zA-Z0-9_\-]+)(.*)$", dep)
            if match:
                deps.append(DependencySpec(package_name=match.group(1), version_range=match.group(2).strip()))
                
        dev_deps = data.get("dependency-groups", {}).get("dev", [])
        
        return ProjectMeta(
            name=project.get("name", ""),
            version=project.get("version", ""),
            description=project.get("description", ""),
            python_requires=project.get("requires-python", ""),
            dependencies=deps,
            dev_dependencies=dev_deps
        )

class MakefileParser:
    """@desc: Makefile 내 실행 가능한 주요 타겟 및 주석 추출기"""
    
    def __init__(self) -> None:
        # '타겟: ## 설명' 구조를 추출하는 패턴
        self.pattern = re.compile(r"^([a-zA-Z0-9_\-]+):\s*##\s*(.*)$", re.MULTILINE)

    def parse(self, path: Path) -> List[WorkflowCommand]:
        if not path.exists():
            return []
            
        content = path.read_text(encoding="utf-8")
        commands = []
        for match in self.pattern.finditer(content):
            commands.append(WorkflowCommand(target=match.group(1), description=match.group(2).strip()))
        return commands

class ReadmeParser:
    """@desc: README.md 내 마크다운 구조에서 사용 예제 코드(Snippet)만 추출"""
    
    def __init__(self) -> None:
        self.block_pattern = re.compile(r"###\s+(.*?)\n.*?```([a-z]+)\n(.*?)```", re.DOTALL)

    def parse(self, path: Path) -> List[UsageSnippet]:
        if not path.exists():
            return []
            
        content = path.read_text(encoding="utf-8")
        snippets = []
        for match in self.block_pattern.finditer(content):
            snippets.append(UsageSnippet(
                title=match.group(1).strip(),
                language=match.group(2).strip(),
                code=match.group(3).strip()
            ))
        return snippets

class SourceCodeParser:
    """@desc: AST를 사용하여 구현 코드 구조, 타입 시그니처 및 Import 네임스페이스 추출"""

    def parse(self, path: Path) -> ModuleMeta:
        if not path.exists():
            return ModuleMeta()
            
        content = path.read_text(encoding="utf-8")
        tree = ast.parse(content)
        
        module_meta = ModuleMeta(docstring=ast.get_docstring(tree))
        
        for node in ast.walk(tree):
            # [추가됨] Import 및 ImportFrom 노드 추출 로직 병합
            if isinstance(node, ast.ImportFrom) and node.module:
                module_meta.imports.append(node.module)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    module_meta.imports.append(alias.name)

            # 기존 클래스 추출 로직
            elif isinstance(node, ast.ClassDef):
                if node.name.startswith("_"):
                    continue
                    
                base_classes = [b.name for b in node.bases if isinstance(b, ast.Name)]
                class_spec = ClassSpec(
                    name=node.name,
                    base_classes=base_classes,
                    docstring=ast.get_docstring(node)
                )
                
                for item in node.body:
                    if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                        class_spec.properties.append(CodeParameter(
                            name=item.target.id,
                            type_hint=ast.unparse(item.annotation).strip()
                        ))
                    
                    elif isinstance(item, ast.FunctionDef):
                        if item.name.startswith("_") and item.name != "__init__":
                            continue
                            
                        methods_args = []
                        for arg in item.args.args:
                            if arg.arg == "self":
                                continue
                            annotation = ast.unparse(arg.annotation) if arg.annotation else "Any"
                            methods_args.append(CodeParameter(name=arg.arg, type_hint=annotation))
                            
                        return_type = ast.unparse(item.returns) if item.returns else "None"
                        
                        class_spec.methods.append(InterfaceMethod(
                            name=item.name,
                            docstring=ast.get_docstring(item),
                            parameters=methods_args,
                            return_type=return_type
                        ))
                        
                module_meta.classes[node.name] = class_spec
                
        # 중복된 import 구문 제거 (순서 보존)
        module_meta.imports = list(dict.fromkeys(module_meta.imports))
        return module_meta

class WebDependencyFetcher:
    """@desc: 원격 레포지토리에서 pyproject.toml 또는 requirements.txt를 읽어 외/내부 의존성 분류"""
    
    def __init__(self, tag: str = "v0.14.22", ext_repo: str = "ext-phase"):
        self.tag = tag
        self.ext_repo = ext_repo

    def fetch(self, category: str, integration_name: str) -> Dict[str, Any]:
        """외부 패키지 목록(dict)과 내부 업스트림 패키지 목록(list)을 반환합니다."""
        category_dir = category.replace("_", "-")
        name_dir = integration_name.replace("_", "-")
        base_subpath = f"llama-index-integrations/{category}/llama-index-{category_dir}-{name_dir}"
        raw_base_url = f"https://raw.githubusercontent.com/{self.ext_repo}/llama_index/{self.tag}/{base_subpath}"
        
        try:
            req = urllib.request.Request(f"{raw_base_url}/pyproject.toml")
            with urllib.request.urlopen(req) as resp:
                content = resp.read().decode('utf-8')
                return self._parse_toml_deps(content)
        except HTTPError as e:
            if e.code != 404:
                print(f"[Warning] Failed to fetch TOML for {integration_name}: {e}")
        
        try:
            req = urllib.request.Request(f"{raw_base_url}/requirements.txt")
            with urllib.request.urlopen(req) as resp:
                content = resp.read().decode('utf-8')
                return self._parse_requirements_deps(content)
        except Exception as e:
            print(f"[Warning] No web dependency manifest found for {integration_name}: {e}")
            
        return {"external": {}, "internal_raw": []}

    def _parse_toml_deps(self, content: str) -> Dict[str, Any]:
        external = {}
        internal_raw = []
        try:
            parsed = tomllib.loads(content)
            
            if "project" in parsed and "dependencies" in parsed["project"]:
                for dep in parsed["project"]["dependencies"]:
                    match = re.match(r"^([a-zA-Z0-9\-_]+(?:\[.*?\])?)(.*)$", dep.strip())
                    if match:
                        name, ver = match.group(1).strip(), match.group(2).strip() or "*"
                        (internal_raw if name.startswith("llama-index") else external)[name] = ver
                            
            elif "tool" in parsed and "poetry" in parsed["tool"] and "dependencies" in parsed["tool"]["poetry"]:
                for name, ver in parsed["tool"]["poetry"]["dependencies"].items():
                    if name == "python": continue
                    if name.startswith("llama-index"):
                        internal_raw.append(name)
                    else:
                        if isinstance(ver, dict):
                            extras = ver.get("extras", [])
                            name = f"{name}[{','.join(extras)}]" if extras else name
                            ver = ver.get("version", "*")
                        external[name] = ver
                        
        except Exception:
            pass
            
        return {"external": external, "internal_raw": internal_raw}

    def _parse_requirements_deps(self, content: str) -> Dict[str, Any]:
        external = {}
        internal_raw = []
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"): continue
            
            parts = line.split(">=")[0].split("==")[0].split("<=")[0].split(">")[0].split("<")[0].strip()
            version = line[len(parts):].strip() or "*"
            
            if parts.startswith("llama-index"):
                internal_raw.append(parts)
            else:
                external[parts] = version
                
        return {"external": external, "internal_raw": internal_raw}