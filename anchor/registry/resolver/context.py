# anchor.registry.resolver.context
import asyncio
import socket
from typing import List, Optional, Tuple, Dict, Any, Type, Protocol

from anchor.registry.model.tier import model_tier_registry
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.context")

class ContextResolver:
    """@desc: Diagnoses infrastructure state to determine optimal model architecture and Surface Scope parameters."""
    ULTIMATE_LOCAL_MODEL = "local-gemma-3"

    @classmethod
    def check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        """Validates network (internet) connectivity."""
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve(cls, requested_model: Optional[str], requested_proxy: bool) -> Tuple[Dict[str, Any], str, bool]:
        """Returns the optimal (Scope Params, Resolved Model, Use Proxy) tuple based on network and quota states."""
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls.check_network_connectivity()

        ## Fallback Matrix Logic
        if not is_online:
            log.warning("🚨 [System Offline] Network connectivity lost.")
            log.warning(f"🔄 Forcing fallback to Local Engine: {cls.ULTIMATE_LOCAL_MODEL}")
            resolved_model = cls.ULTIMATE_LOCAL_MODEL
            if use_proxy:
                log.warning("⚠️ Remote proxy cannot be used offline. Disabling proxy surface.")
                use_proxy = False
        elif not resolved_model:
            optimal_model = model_tier_registry.get_optimal_model(requires_tools=True)
            if optimal_model:
                resolved_model = f"gemini/{optimal_model}"
                log.info(f"✅ Registry resolved optimal model: {resolved_model}")
            else:
                log.warning(f"⚠️ Registry exhausted. Falling back to ultimate local model: {cls.ULTIMATE_LOCAL_MODEL}")
                resolved_model = cls.ULTIMATE_LOCAL_MODEL
                if use_proxy:
                    log.info("ℹ️ Disabling remote proxy to align with local model execution.")
                    use_proxy = False

        scope_kwargs = {
            "use_proxy": use_proxy,
            "show_logs": True,
            "model": resolved_model  
        }
        return scope_kwargs, resolved_model, use_proxy