# fiber.llm.pipeline.interceptor.registry
import os
import json
import importlib
from typing import List, Any
import fiber.llm.pipeline.interceptor as trace_llm_pkg
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("interceptor.registry")

DEFAULT_CONFIG = {
    "pipeline_interceptors": [
        # for example
        # {
        #     "class": "cache.EdgeRedisCache"
        # },
        # {
        #     "class": "router.OnnxSemanticRouter",
        #     "kwargs": {
        #         "fallback_model": "cerebras/qwen-3.8-27b", 
        #         "tool_model": "gemini/gemini-3.1-flash-lite",
        #         "routing_rules": {
        #             "Transform": {"target": "llama_server/gemma-3-1b-it-Q4_K_M.gguf", "threshold": 0.8},
        #             "Knowledge_Retrieval": {"target": "cohere/command-r-08-2024", "threshold": 0.6},
        #             "Complex_Reasoning": {"target": "cohere/command-r-08-2024", "threshold": 0.5}
        #         }
        #     }
        # },
        # {
        #     "class": "guardrail.ActiveBlockingGuardrail"
        # }
    ]
}

class InterceptorConfigLoader:
    
    @classmethod
    def load_from_config(cls) -> List[Any]:
        """Loads and instantiates the global system interceptors from the environment config."""
        config_path = os.getenv("FIBER_INTERCEPTOR_CONFIG_PATH")
        config_data = DEFAULT_CONFIG
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    config_data = json.load(f)
            except Exception as e:
                log.error(f"Failed to parse config at {config_path}, falling back to default. Error: {e}")
        else:
            log.info("No external interceptor config found. Using default pipeline configuration.")
            
        instances = []
        for item in config_data.get("pipeline_interceptors", []):
            full_path = item.get("class")
            kwargs = item.get("kwargs", {})
            
            if not full_path:
                continue
                
            try:
                # Require module.ClassName format for explicit Lazy Loading
                if "." not in full_path:
                    log.warning(f"Invalid format '{full_path}'. Expected 'module.ClassName'. Ignored.")
                    continue
                    
                module_name, class_name = full_path.rsplit(".", 1)
                absolute_module_name = f"{trace_llm_pkg.__name__}.{module_name}"
                
                # Dynamically load only the target module
                module = importlib.import_module(absolute_module_name)
                target_cls = getattr(module, class_name)
                
                # Instantiate the interceptor with the correct kwargs
                instances.append(target_cls(**kwargs))
                
            except ImportError as e:
                log.error(f"Failed to import module for '{full_path}': {e}")
            except AttributeError as e:
                log.error(f"Class not found in module for '{full_path}': {e}")
            except TypeError as e:
                log.error(f"Parameter mismatch for '{full_path}'. Check kwargs in config: {e}")
            except Exception as e:
                log.error(f"Failed to instantiate '{full_path}' with {kwargs}: {e}")
                
        return instances

    @classmethod
    def build_from_payload(cls, items: List[Any]) -> List[Any]:
        """Safely instantiates interceptors provided dynamically via the client payload."""
        instances = []
        if not items:
            return instances
            
        for item in items:
            # Handle both Pydantic models (if passed parsed) and raw Dicts
            full_path = getattr(item, "class_name", None) or (item.get("class") if isinstance(item, dict) else None)
            kwargs = getattr(item, "kwargs", None) or (item.get("kwargs", {}) if isinstance(item, dict) else {})
            
            if not full_path or "." not in full_path:
                log.warning(f"Invalid client payload interceptor format '{full_path}'. Expected 'module.ClassName'. Ignored.")
                continue
                
            try:
                module_name, class_name = full_path.rsplit(".", 1)
                absolute_module_name = f"{trace_llm_pkg.__name__}.{module_name}"
                
                module = importlib.import_module(absolute_module_name)
                target_cls = getattr(module, class_name)
                
                instances.append(target_cls(**kwargs))
                
            except ImportError as e:
                log.error(f"Failed to import module for client payload '{full_path}': {e}")
            except AttributeError as e:
                log.error(f"Class not found in module for client payload '{full_path}': {e}")
            except TypeError as e:
                log.error(f"Parameter mismatch for client payload '{full_path}'. Check kwargs: {e}")
            except Exception as e:
                log.error(f"Failed to instantiate client interceptor '{full_path}' with {kwargs}: {e}")
                
        return instances