# anchor.registry.router.reader
import os
import importlib
import inspect
from pathlib import Path
from typing import Dict, Any, List, Optional

from bound.adapter.cli.search.reader import ReaderSearcher
from watcher.plane.emitter import get_emitter

log = get_emitter("router.reader")

DEFAULT_REGISTRY = {
    "pdf": {
        "module": "anchor.inter.readers.file.pymu_pdf.base",
        "type": "file",
        "tags": ["pdf", "document", "text extract"]
    },
    "markdown": {
        "module": "anchor.inter.readers.file.markdown.base",
        "type": "file",
        "tags": ["md", "markdown"]
    },
    "html_bs": {
        "module": "anchor.inter.readers.web.beautiful_soup_web.base",
        "type": "web",
        "tags": ["html", "web", "url", "scraping"]
    },
    "sitemap": {
        "module": "anchor.inter.readers.web.sitemap.base",
        "type": "web",
        "tags": ["sitemap", "xml", "crawler"]
    },
    "rss": {
        "module": "anchor.inter.readers.web.rss.base",
        "type": "web",
        "tags": ["rss", "news", "feed"]
    }
}

class DataRouter:
    def __init__(self, readers_base_path: str = "anchor/inter/readers"):
        self.base_path = Path(readers_base_path)
        self.registry = {k: v.copy() for k, v in DEFAULT_REGISTRY.items()}
        self._is_searched = False  # 무의미한 중복 I/O 스캔 방지용 플래그

    def _fallback_scan(self, target_tag: str) -> Optional[str]:
        """정적 맵에 없을 때만 ReaderScanner를 호출하여 레지스트리를 병합"""
        if self._is_searched:
            return None # 이미 전체 스캔을 마쳤음에도 없다면 지원하지 않는 포맷

        log.info(f"[*] '{target_tag}'에 대한 정적 맵이 없습니다. ReaderScanner를 호출합니다...")
        searcher = ReaderSearcher(self.base_path)
        seached_data = searcher.search()
        for format_key, meta in seached_data.items():
            if format_key not in self.registry:
                self.registry[format_key] = {
                    "module": meta["module"],
                    "class": meta["class"], # Scanner가 확정한 클래스명 보존
                    "type": "auto_scanned",
                    "tags": [format_key]
                }
        
        self._is_searched = True # 스캔 완료 상태 기록

        # 병합된 레지스트리에서 target_tag 다시 탐색 (정확도 우선 -> 부분 일치)
        if target_tag in self.registry:
            return self.registry[target_tag]["module"]

        for key, meta in self.registry.items():
            if target_tag in key or any(target_tag in tag for tag in meta["tags"]):
                return meta["module"]

        return None

    def get_llm_tool_schema(self) -> Dict[str, Any]:
        """LLM Function Calling에 주입할 Tool Schema 반환"""
        return {
            "name": "data_loader_router",
            "description": "다양한 형태의 파일이나 웹 데이터를 로드합니다. 입력 데이터 특성에 맞는 reader_type을 선택하세요.",
            "available_readers": {
                key: val["tags"] for key, val in self.registry.items()
            }
        }

    def route_and_load(self, reader_type: str, **kwargs) -> List[Any]:
        """요청된 타입에 맞춰 모듈을 지연 로드(Lazy Load)하고 데이터를 추출합니다."""
        
        # 1. 맵 확인 및 Fallback
        meta = self.registry.get(reader_type)
        if not meta:
            module_path = self._fallback_scan(reader_type)
            if not module_path:
                raise ValueError(f"[Error] 지원하지 않거나 식별할 수 없는 reader type 입니다: {reader_type}")
            # 스캔 후 메타데이터 다시 가져오기
            meta = self.registry.get(reader_type, {})
        else:
            module_path = meta["module"]
            
        # 2. 최소 의존성 지연 로드
        module = importlib.import_module(module_path)
        
        # 3. 클래스 인스턴스화
        ReaderClass = None
        
        # Scanner가 식별한 정확한 클래스명이 있다면 즉시 가져옴 (속도 최적화)
        if "class" in meta:
            ReaderClass = getattr(module, meta["class"])
        else:
            # 정적 맵(DEFAULT_REGISTRY)의 경우 클래스명을 모르므로 inspect로 동적 식별
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if hasattr(obj, 'load_data'):
                    ReaderClass = obj
                    break
                
        if not ReaderClass:
            raise RuntimeError(f"'{module_path}' 내에 load_data 메서드를 가진 클래스가 없습니다.")
            
        # 4. 실행 및 반환
        instance = ReaderClass()
        return instance.load_data(**kwargs)

if __name__ == "__main__":
    router = DataRouter()
    log.info("LLM Tools Schema:", router.get_llm_tool_schema())
    try:
        router.route_and_load("xml", file_path="sample.xml")
    except Exception as e:
        # 런타임 에러 처리 (sample.xml이 없거나, xml 관련 의존성이 없을 경우)
        log.info(f"Result: {e}")