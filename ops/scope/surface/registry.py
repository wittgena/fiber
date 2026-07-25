# ops.scope.surface.registry
## @lineage: meta.scope.surface.registry
## @lineage: topos.scope.surface.registry
import importlib
from typing import Type
from ops.scope.surface.config import BaseSurface
import ops.scope.surface as surface_pkg

SURFACE = surface_pkg.__name__

SURFACE_REGISTRY = {
    "local": f"{SURFACE}.local.LocalSurface",
    "sandbox": f"{SURFACE}.sandbox.SandboxSurface",
    "proxy": f"{SURFACE}.proxy.ProxySurface"
}

def get_surface_class(surface_type: str) -> Type[BaseSurface]:
    module_path = SURFACE_REGISTRY.get(surface_type)
    if not module_path:
        raise ValueError(f"Unknown surface type: {surface_type}")
    
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, class_name)