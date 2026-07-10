# anchor.registry.model.config.resolver
## @lineage: bound.surface.legacy.config.resolver
import os
import logging
from typing import Any
from watcher.plane.emitter import get_emitter

log = get_emitter("config.resolver")

class ConfigResolver:
    def __init__(self):
        self._local_overrides = {}

    def get(self, name: str, default: Any = None) -> Any:
        try:
            value = getattr(self, name)
            return default if value is None else value
        except AttributeError:
            return default

    def set_override(self, key: str, value: Any):
        self._local_overrides[key] = value

    def __getattr__(self, name: str):
        """
        객체에 없는 속성(key)을 호출할 때 자동으로 실행되는 매직 메서드
        탐색 순위: 1. Gate 오버라이드 -> 2. 환경변수 -> 3. litellm (과도기용 폴백)
        """
        ## Gate 인메모리 오버라이드 확인
        if name in self._local_overrides:
            return self._local_overrides[name]

        ## 시스템 환경 변수 확인 (예: api_key -> API_KEY)
        env_key = name.upper()
        if env_key in os.environ:
            return os.environ[env_key]

        ## [점진적 마이그레이션 영역] litellm 폴백 (향후 이 블록만 삭제하면 됨)
        try:
            import litellm
            if hasattr(litellm, name):
                ## log.debug(f"[Migration] '{name}'을(를) litellm에서 가져왔습니다.")
                return getattr(litellm, name)
        except ImportError:
            pass

        raise AttributeError(f"'{type(self).__name__}' object (brane config) has no attribute '{name}'")
        ## 값을 찾지 못한 경우 안전하게 None 반환
        # return None
    
    def __setattr__(self, name: str, value: Any):
        """
        config.telemetry = False 와 같이 할당할 때 실행되는 매직 메서드.
        Gate 시스템에 저장함과 동시에, 과도기적으로 litellm에도 값을 동기화합니다.
        """
        ## 내부 변수 초기화 허용
        if name == "_local_overrides":
            super().__setattr__(name, value)
            return

        ## Gate 인메모리 오버라이드 갱신
        self._local_overrides[name] = value
        try:
            import litellm
            setattr(litellm, name, value)
            ## log.debug(f"[Migration] litellm.{name} = {value} 동기화 완료")
        except ImportError:
            pass

config = ConfigResolver()