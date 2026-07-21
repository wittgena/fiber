# bound.resolver.model.tier
## @lineage: bound.registry.model.tier
## @lineage: anchor.registry.model.tier
import json
import time
from pathlib import Path
from collections import deque
from typing import Dict, List, Optional, Any

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("tier.registry", phase="SYSTEM")

MANIFEST_PATH = resolve_path("registry") / "llms" / "model_tier_registry.json"

DEFAULT_FALLBACK_CONFIG = {
    "strategy": "sliding_window_rotation",
    "window_seconds": 60,
    "day_seconds": 86400,
    "default_penalty_seconds": 45,
    "fallback_pool": [
        {"model": "gemini-3.1-flash-lite", "provider": "gemini", "priority": 1, "rpm_limit": 15, "rpd_limit": 500, "supports_tools": True},
        {"model": "gemma-4-31b", "provider": "gemini", "priority": 2, "rpm_limit": 15, "rpd_limit": 1500, "supports_tools": True},
        {"model": "antigravity", "provider": "gemini", "priority": 3, "rpm_limit": 60, "rpd_limit": 100, "supports_tools": True},
        {"model": "gemini-2.5-flash-lite", "provider": "gemini", "priority": 4, "rpm_limit": 10, "rpd_limit": 20, "supports_tools": True},
        {"model": "gemini-3.5-flash", "provider": "gemini", "priority": 5, "rpm_limit": 5, "rpd_limit": 20, "supports_tools": True},
        {"model": "gemini-3-flash", "provider": "gemini", "priority": 6, "rpm_limit": 5, "rpd_limit": 20, "supports_tools": True}
    ]
}

class ModelTierRegistry:
    """
    @manifold: Proactive Rate Limit & Rotation Registry
    @desc: Tracks RPM/RPD via sliding window. Routes to optimal fallback upon limits.
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
        self.day_seconds = 86400
        self.penalty_seconds = 45
        
        self.call_history_rpm: Dict[str, deque] = {}
        self.call_history_rpd: Dict[str, deque] = {}
        self.penalty_box: Dict[str, float] = {}
        
        self.reload_manifest()

    def reload_manifest(self) -> None:
        loaded_config = None
        if MANIFEST_PATH.exists():
            try:
                with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
                    loaded_config = json.load(f)
            except Exception as e:
                log.error(f"Manifest parse failed: {e}. Using default.")
        else:
            log.warning(f"Manifest missing at {MANIFEST_PATH}. Using fallback pool.")

        self.config = loaded_config if loaded_config else DEFAULT_FALLBACK_CONFIG
        
        # Sort models by priority ascending
        self.models = sorted(self.config.get("fallback_pool", []), key=lambda x: x.get("priority", 99))
        self.window_seconds = self.config.get("window_seconds", 60)
        self.day_seconds = self.config.get("day_seconds", 86400)
        self.penalty_seconds = self.config.get("default_penalty_seconds", 45)
        
        # Initialize queues
        for m in self.models:
            model_name = m["model"]
            if model_name not in self.call_history_rpm:
                self.call_history_rpm[model_name] = deque()
            if model_name not in self.call_history_rpd:
                self.call_history_rpd[model_name] = deque()
                
        log.info(f"Loaded {len(self.models)} models into Registry.")

    def _evict_old_records(self, model_name: str) -> None:
        now = time.time()
        
        # Evict stale RPM records
        r_rpm = self.call_history_rpm.get(model_name)
        if r_rpm:
            while r_rpm and now - r_rpm[0] > self.window_seconds: r_rpm.popleft()
                
        # Evict stale RPD records
        r_rpd = self.call_history_rpd.get(model_name)
        if r_rpd:
            while r_rpd and now - r_rpd[0] > self.day_seconds: r_rpd.popleft()

    def get_optimal_model(self, requires_tools: bool = False, test_mode: bool = False) -> Optional[str]:
        """Returns the optimal model considering priority, RPM/RPD limits, and penalty box."""
        now = time.time()
        diagnostics = ["[Tier Registry Routing Diagnostics]"]
        selected_model = None
        
        for m in self.models:
            model_name = m["model"]
            priority = m.get("priority", 99)
            rpm_limit, rpd_limit = m.get("rpm_limit", 5), m.get("rpd_limit", 20)
            
            if requires_tools and not m.get("supports_tools", True):
                if test_mode: diagnostics.append(f" ❌ [P{priority}] {model_name}: SKIP (No Tools)")
                continue
                
            penalty_until = self.penalty_box.get(model_name, 0)
            if now < penalty_until:
                if test_mode: diagnostics.append(f" 🚫 [P{priority}] {model_name}: SKIP (Penalty {int(penalty_until - now)}s)")
                continue 
                
            self._evict_old_records(model_name)
            c_rpm, c_rpd = len(self.call_history_rpm[model_name]), len(self.call_history_rpd[model_name])
            is_rpm_ok, is_rpd_ok = c_rpm < (rpm_limit - 1), c_rpd < (rpd_limit - 1)
            
            if is_rpm_ok and is_rpd_ok:
                if test_mode: diagnostics.append(f" ✅ [P{priority}] {model_name}: SELECTED (RPM: {c_rpm}/{rpm_limit}, RPD: {c_rpd}/{rpd_limit})")
                selected_model = model_name
                break # Cascade routing logic
            else:
                if test_mode:
                    reason = "RPM EXHAUSTED" if not is_rpm_ok else "RPD EXHAUSTED"
                    diagnostics.append(f" ⚠️ [P{priority}] {model_name}: FALLBACK ({reason} - RPM: {c_rpm}/{rpm_limit}, RPD: {c_rpd}/{rpd_limit})")

        if test_mode: log.info("\n".join(diagnostics))
        if not selected_model: log.warning("🚨 All fallback models exhausted or penalized.")
            
        return selected_model

    def record_usage(self, model_name: str) -> None:
        now = time.time()
        if model_name in self.call_history_rpm: self.call_history_rpm[model_name].append(now)
        if model_name in self.call_history_rpd: self.call_history_rpd[model_name].append(now)

    def record_failure(self, model_name: str, delay_seconds: Optional[float] = None) -> None:
        penalty = delay_seconds if delay_seconds else self.penalty_seconds
        self.penalty_box[model_name] = time.time() + penalty
        log.warning(f"🚫 Model {model_name} hit 429. Penalized for {penalty}s.")

    def simulate_load(self, model_name: str, rpm_hits: int = 0, rpd_hits: int = 0) -> None:
        """Injects artificial traffic for routing tests."""
        now = time.time()
        for _ in range(rpm_hits):
            self.call_history_rpm[model_name].append(now - 10)
            self.call_history_rpd[model_name].append(now - 10)
            
        for _ in range(rpd_hits):
            self.call_history_rpd[model_name].append(now - 3600)

model_tier_registry = ModelTierRegistry()