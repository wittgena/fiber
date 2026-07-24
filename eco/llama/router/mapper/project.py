# eco.llama.router.mapper.project
## @lineage: bound.mapper.inter.project
## @lineage: bound.gateway.adapter.mapper.inter.project
## @lineage: gateway.adapter.mapper.inter.project
## @lineage: eco.mapper.inter.project
## @lineage: adapter.mapper.inter.project
## @lineage: bound.adapter.mapper.inter.project
## @lineage: bound.adapter.mapper.repo.inter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any

@dataclass
class DependencySpec:
    """@desc: pyproject.toml에서 추출한 패키지 의존성 명세"""
    package_name: str
    version_range: str

@dataclass
class ProjectMeta:
    """@desc: 패키지 자체의 명세 및 환경 정보"""
    name: str
    version: str
    description: str
    python_requires: str
    dependencies: List[DependencySpec] = field(default_factory=list)
    dev_dependencies: List[str] = field(default_factory=list)

@dataclass
class WorkflowCommand:
    """@desc: Makefile에서 추출한 개발/실행 명령어 명세"""
    target: str
    description: str

@dataclass
class UsageSnippet:
    """@desc: README.md에서 추출한 LLM용 예제 코드 블록"""
    title: str
    language: str
    code: str

@dataclass
class CodeParameter:
    """@desc: base.py 클래스 속성/생성자 또는 CLI 파라미터 공용 정의"""
    name: str
    type_hint: str
    default_value: Optional[str] = None
    description: Optional[str] = None

@dataclass
class InterfaceMethod:
    """@desc: 퍼블릭 메서드(chat, complete 등)의 시그니처와 명세"""
    name: str
    docstring: Optional[str]
    parameters: List[CodeParameter] = field(default_factory=list)
    return_type: Optional[str] = None

@dataclass
class ClassSpec:
    """@desc: base.py 내 핵심 클래스 인터페이스 정의"""
    name: str
    base_classes: List[str]
    docstring: Optional[str]
    properties: List[CodeParameter] = field(default_factory=list)
    methods: List[InterfaceMethod] = field(default_factory=list)

@dataclass
class ModuleMeta:
    """@desc: 개별 모듈 파일 단위의 통합 메타데이터"""
    docstring: Optional[str] = None
    classes: Dict[str, ClassSpec] = field(default_factory=dict)
    snippets: List[UsageSnippet] = field(default_factory=list)
    imports: List[str] = field(default_factory=list)

@dataclass
class IntegrationManifest:
    """@desc: 패키지 전체 구조와 실행 컨텍스트를 총망라하는 루트 컨테이너"""
    repo_name: str
    project_meta: Optional[ProjectMeta] = None
    workflow: List[WorkflowCommand] = field(default_factory=list)
    modules: Dict[str, ModuleMeta] = field(default_factory=dict)
    external_dependencies: Dict[str, str] = field(default_factory=dict)
    upstream_internal_packages: List[str] = field(default_factory=list)

@dataclass
class ProjectLayout:
    root_dir: str
    pyproject_path: Optional[str] = None
    makefile_path: Optional[str] = None
    readme_path: Optional[str] = None
    base_py_locations: List[str] = field(default_factory=list)

    @classmethod
    def resolve(cls, root_dir: Path) -> "ProjectLayout":
        pyproject = root_dir / "pyproject.toml"
        if not pyproject.exists(): 
            pyproject = next(root_dir.rglob("pyproject.toml"), None)
        
        makefile = root_dir / "Makefile"
        if not makefile.exists(): 
            makefile = next(root_dir.rglob("Makefile"), None)
        
        readme = root_dir / "README.md"
        if not readme.exists(): 
            readme = next(root_dir.rglob("README.md"), None)

        return cls(
            root_dir=str(root_dir.resolve()),
            pyproject_path=str(pyproject.resolve()) if pyproject else None,
            makefile_path=str(makefile.resolve()) if makefile else None,
            readme_path=str(readme.resolve()) if readme else None,
            base_py_locations=[str(p.resolve()) for p in root_dir.rglob("base.py")]
        )