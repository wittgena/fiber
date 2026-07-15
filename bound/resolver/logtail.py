# bound.resolver.logtail
"""
@desc: Resolves structured configuration rulesets into actionable Logtail routing rules.
       Aligned with the unified 'resolve()' interface pattern.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Callable, Optional

from bound.adapter.mapper.log.logtail import run_pipeline as topos_mapper
from phase.bind.resolver import resolve_path

logger = logging.getLogger("resolver.logtail")
WORKSPACE_ROOT = resolve_path("workspace")

class LogtailResolver:
    """@desc: Translates static rulesets into a dynamic routing and tailing topology"""
    DEFAULT_RULESET = {
        "global_config": {
            "root_path": str(WORKSPACE_ROOT)
        },
        "targets": [
            {"stream_key": "chain_state", "path": "state/contract_registry.jsonl", "handler": "topos_mapper"},
            {"stream_key": "issue_events", "path": "issue/registry.jsonl", "handler": "topos_mapper"} 
        ]
    }

    def __init__(self, ruleset: Optional[Dict[str, Any]] = None):
        self.ruleset = ruleset if ruleset is not None else self.DEFAULT_RULESET
        
        # 문자열을 실제 파이썬 함수 객체로 바인딩하기 위한 레지스트리
        self._handler_registry = {
            "topos_mapper": topos_mapper
        }

    def resolve(self) -> List[Tuple[Path, str, Callable]]:
        """
        @desc: 리졸버 공통 인터페이스. 
        @return: [(추적할 파일 Path, Stream 식별자, 처리할 콜백 함수), ...]
        """
        resolved_routes = []
        root_dir = Path(self.ruleset.get("global_config", {}).get("root_path", str(WORKSPACE_ROOT)))

        for target in self.ruleset.get("targets", []):
            try:
                stream_key = target.get("stream_key")
                file_path = root_dir / target.get("path")
                handler_name = target.get("handler")
                
                # 핸들러 검증
                handler_func = self._handler_registry.get(handler_name)
                if not handler_func:
                    logger.warning(f"⚠️ Handler '{handler_name}' not found. Skipping route: {stream_key}")
                    continue

                resolved_routes.append((file_path, stream_key, handler_func))
            except Exception as e:
                logger.error(f"🚨 Failed to resolve target {target}: {e}")
                
        logger.info(f"✅ Successfully resolved {len(resolved_routes)} tailing routes.")
        return resolved_routes