# xor.scope.surface.registry
## @lineage: xphi.scope.surface.registry
import importlib
from typing import Type
from xor.scope.surface.config import BaseSurface

SURFACE_REGISTRY = {
    "local": "xor.scope.surface.local.LocalSurface",
    "sandbox": "xor.scope.surface.sandbox.SandboxSurface",
    "proxy": "xor.scope.surface.proxy.ProxySurface"
}

def get_surface_class(surface_type: str) -> Type[BaseSurface]:
    module_path = SURFACE_REGISTRY.get(surface_type)
    if not module_path:
        raise ValueError(f"Unknown surface type: {surface_type}")
    
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)