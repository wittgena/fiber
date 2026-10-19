# fiber.dev.ex.model.fallback
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("model.fallback", phase="SYSTEM")

MANIFEST_PATH = resolve_path("registry") / "llms" / "model_fallback_registry.json"

DEFAULT_FALLBACK_POOL = [
    {"model": "gemma-3-1b-it-Q4_K_M.gguf", "provider": "llama_server", "priority": 1, "is_local": True},
    {"model": "gemma_2b", "provider": "ollama", "priority": 2, "is_local": True},
    {"model": "gemini-3.1-flash-lite", "provider": "gemini", "priority": 4, "is_local": False},
    {"model": "qwen-3.5-9b", "provider": "lm_studio", "priority": 3, "is_local": True},
]

class FallbackModelRegistry:
    """@manifold: Local-First Model Routing & Fallback Registry"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FallbackModelRegistry, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.models: List[Dict[str, Any]] = []
        self.penalty_box: Dict[str, float] = {}
        self.default_penalty_seconds = 30
        self._manifest_loaded = False

    def _ensure_loaded(self) -> None:
        if not self._manifest_loaded:
            self.reload_manifest()
            self._manifest_loaded = True

    def reload_manifest(self) -> None:
        loaded_pool = None
        if MANIFEST_PATH.exists():
            try:
                with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                    loaded_pool = json.load(f).get("fallback_pool")
            except Exception as e:
                log.error(f"Manifest parse failed: {e}. Using default.")
        
        pool = loaded_pool if loaded_pool else DEFAULT_FALLBACK_POOL
        self.models = sorted(pool, key=lambda x: x.get("priority", 99))
        log.info(f"Loaded {len(self.models)} fallback models (Local-First).")

    def get_fallback_model(self, is_online: bool = True, test_mode: bool = False) -> Optional[str]:
        self._ensure_loaded()
        now = time.time()
        diagnostics = ["[Fallback Registry Diagnostics]"]
        selected_model = None
        
        for m in self.models:
            provider = m["provider"]
            model_name = m["model"]
            is_local = m.get("is_local", False)
            priority = m.get("priority", 99)
            full_path = f"{provider}/{model_name}"
            
            # 오프라인 상태일 경우 클라우드 모델 스킵
            if not is_online and not is_local:
                if test_mode: diagnostics.append(f" [P{priority}] {full_path}: SKIP (System is Offline)")
                continue
                
            # Penalty Box 검증 (최근 장애가 있었는지)
            penalty_until = self.penalty_box.get(full_path, 0)
            if now < penalty_until:
                if test_mode: diagnostics.append(f" [P{priority}] {full_path}: SKIP (In Penalty Box for {int(penalty_until - now)}s)")
                continue 
                
            # 조건 만족 시 즉시 선택 (가장 높은 우선순위)
            if test_mode: diagnostics.append(f" [P{priority}] {full_path}: SELECTED")
            selected_model = full_path
            break
            
        if test_mode: 
            log.info("\n".join(diagnostics))
            
        if not selected_model: 
            log.warning("All fallback models exhausted or in penalty box.")
            
        return selected_model

    def record_failure(self, provider_model_path: str, delay_seconds: Optional[float] = None) -> None:
        """모델 호출 실패(Connection Error 등) 시 패널티 부여"""
        self._ensure_loaded()
        penalty = delay_seconds if delay_seconds else self.default_penalty_seconds
        self.penalty_box[provider_model_path] = time.time() + penalty
        log.warning(f"Model {provider_model_path} failed. Penalized for {penalty}s.")

fallback_model_registry = FallbackModelRegistry()