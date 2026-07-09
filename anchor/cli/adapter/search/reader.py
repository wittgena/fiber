# anchor.cli.adapter.search.reader
import os
import ast
from pathlib import Path
from typing import Dict, Protocol, runtime_checkable
import anchor.inter as inter_path
from phase.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter

log = get_emitter("search.reader")
SELF_ROOT = find_current_self()
TARGET_PATH = SELF_ROOT / inter_path.__name__ / "readers"

@runtime_checkable
class searchable(Protocol):
    def search(self, *args, **kwargs) -> Dict:
        ...

class ReaderSearch:
    """
    AST를 이용해 import 없이 'load_data'가 있는 클래스를 정적으로 식별하는 스캐너.
    어떤 추상 클래스도 상속받지 않으며, 스스로의 목적에 맞는 search() 행위만 가집니다.
    """
    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)

    def _get_module_path(self, file_path: Path) -> str:
        """파일 경로를 모듈 FQN으로 변환하는 내부 유틸리티"""
        rel_path = file_path.relative_to(Path.cwd())
        return str(rel_path.with_suffix("")).replace(os.sep, ".")

    def search(self) -> Dict[str, Dict[str, str]]:
        registry = {}
        for file_path in self.base_path.rglob("*.py"):
            if file_path.name == "__init__.py":
                continue
                
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    file_content = f.read()
                    
                # 코드를 실행하지 않고 AST(구문 트리)로 파싱
                tree = ast.parse(file_content)
                
                for node in ast.walk(tree):
                    # 클래스 정의 노드 찾기
                    if isinstance(node, ast.ClassDef):
                        # 클래스 내부에 load_data 메서드가 있는지 확인
                        has_load_data = any(
                            isinstance(n, ast.FunctionDef) and n.name == "load_data"
                            for n in node.body
                        )
                        
                        if has_load_data:
                            # 디렉토리 이름을 포맷(식별자)으로 추정 (예: pymu_pdf, html)
                            format_key = file_path.parent.name 
                            
                            registry[format_key] = {
                                "module": self._get_module_path(file_path),
                                "class": node.name
                            }
            except Exception as e:
                log.info(f"[Warning] Failed to parse {file_path}: {e}")
                
        return registry

if __name__ == "__main__":
    searchner = ReaderSearch(TARGET_PATH)
    if isinstance(searchner, searchable):
        log.info("[System] ReaderSearch가 유효한 searchnable 객체로 인식되었습니다.")
        
    result = searchner.search()
    log.info(f"searchned {len(result)} components.")