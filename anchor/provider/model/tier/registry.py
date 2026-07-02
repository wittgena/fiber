# anchor.provider.model.tier.registry
## @lineage: anchor.surface.model.tier.registry
import json
import time
from pathlib import Path
from collections import deque
from typing import Dict, List, Optional, Any

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("tier.registry", phase="SYSTEM")

MANIFEST_PATH = resolve_path("res") / "config" / "model_tier_registry.json"

DEFAULT_FALLBACK_CONFIG = {
    "strategy": "sliding_window_rotation",
    "window_seconds": 60,
    "default_penalty_seconds": 45,
    "fallback_pool": [
        {
            "model": "gemini-3.1-flash-lite",
            "provider": "gemini",
            "priority": 1,
            "rpm_limit": 15,
            "rpd_limit": 500,
            "supports_tools": True
        },
        {
            "model": "gemini-2.5-flash-lite",
            "provider": "gemini",
            "priority": 2,
            "rpm_limit": 10,
            "rpd_limit": 20,
            "supports_tools": True
        }
    ]
}

class ModelTierRegistry:
    """
    @manifold: Proactive Rate Limit & Rotation Registry
    @desc: 슬라이딩 윈도우 알고리즘을 사용하여 무료 티어 모델의 RPM을 추적하고, 
           한도 초과가 예상되거나 실제 429 발생 시 모델을 페널티 박스에 격리
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ModelTierRegistry, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.config: Dict[str, Any] = {}
        self.models: List[Dict[str, Any]] = []
        self.window_seconds = 60
        self.penalty_seconds = 45
        
        self.call_history: Dict[str, deque] = {}
        self.penalty_box: Dict[str, float] = {}
        
        self.reload_manifest()

    def reload_manifest(self) -> None:
        """JSON 매니페스트를 로드하고 우선순위대로 정렬합니다. 실패 시 내장 Fallback을 사용합니다."""
        loaded_config = None

        if MANIFEST_PATH.exists():
            try:
                with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                    loaded_config = json.load(f)
            except Exception as e:
                log.error(f"Failed to parse free tier manifest: {e}. Falling back to default.")
        else:
            log.warning(f"Manifest not found at {MANIFEST_PATH}. Using embedded fallback pool.")

        self.config = loaded_config if loaded_config else DEFAULT_FALLBACK_CONFIG
        pool = self.config.get("fallback_pool", [])

        self.models = sorted(pool, key=lambda x: x.get("priority", 99))
        self.window_seconds = self.config.get("window_seconds", 60)
        self.penalty_seconds = self.config.get("default_penalty_seconds", 45)
        
        for m in self.models:
            model_name = m["model"]
            if model_name not in self.call_history:
                self.call_history[model_name] = deque()
                
        log.info(f"Loaded {len(self.models)} models into ModelTierRegistry.")

    def _evict_old_records(self, model_name: str) -> None:
        history = self.call_history.get(model_name)
        if not history:
            return
            
        current_time = time.time()
        while history and current_time - history[0] > self.window_seconds:
            history.popleft()

    def get_optimal_model(self, requires_tools: bool = False) -> Optional[str]:
        current_time = time.time()
        
        for m in self.models:
            model_name = m["model"]
            
            if requires_tools and not m.get("supports_tools", True):
                continue
                
            penalty_until = self.penalty_box.get(model_name, 0)
            if current_time < penalty_until:
                continue 
                
            self._evict_old_records(model_name)
            current_rpm_usage = len(self.call_history[model_name])
            rpm_limit = m.get("rpm_limit", 5)
            
            if current_rpm_usage < (rpm_limit - 1):
                return model_name
                
        log.warning("🚨 All fallback models are currently exhausted or in penalty box.")
        return None

    def record_usage(self, model_name: str) -> None:
        if model_name in self.call_history:
            self.call_history[model_name].append(time.time())

    def record_failure(self, model_name: str, delay_seconds: Optional[float] = None) -> None:
        penalty_duration = delay_seconds if delay_seconds else self.penalty_seconds
        self.penalty_box[model_name] = time.time() + penalty_duration
        log.warning(f"🚫 Model {model_name} hit 429. Placed in penalty box for {penalty_duration}s.")

model_tier_registry = ModelTierRegistry()