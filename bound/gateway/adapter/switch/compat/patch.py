# bound.gateway.adapter.switch.compat.patch
## @lineage: gateway.adapter.switch.compat.patch
## @lineage: eco.switch.compat.patch
## @lineage: adapter.switch.compat.patch
## @lineage: bound.adapter.switch.compat.patch
"""
@role: Compatibility & Resonance Layer for Third-Party Libraries
@desc: Forcibly consolidates and remediates breaking changes or namespace fragmentation 
       in third-party libraries during the bootstrap sequence.
"""
import sys
import importlib
from phase.bind.redirector import PhaseAirlock
from watcher.plane.emitter import get_emitter

log = get_emitter("compat.patch")

def _patch_mcp_20a():
    """Restores the missing 'mcp.types' import resolution issue found in MCP v2.0.a."""
    try:
        import mcp
        
        # Canonical internal path or wrapper module
        CANONICAL_TYPES_PATH = "mcp_types"  
        log.info(f"[Patch] Injecting mcp 2.0.a compatibility layer...")
        
        ## Bind resonance across sys.meta_path and sys.modules
        PhaseAirlock.establish_resonance(
            legacy_path="mcp.types",
            canonical_path=CANONICAL_TYPES_PATH
        )
        
        ## Inject attribute onto the top-level mcp module to support 'from mcp import types' syntax
        canonical_module = importlib.import_module(CANONICAL_TYPES_PATH)
        setattr(mcp, "types", canonical_module)
        sys.modules["mcp"].types = canonical_module
        
    except ImportError:
        log.debug("[Patch] mcp package is not installed. Skipping patch.")
    except Exception as e:
        log.error(f"[Patch] Failed to apply mcp patch: {e}")

def _patch_future_library_example():
    """Placeholder for future compatibility patches targeting other libraries."""
    pass

def apply_patches():
    """Executes all system compatibility patches sequentially."""
    _patch_mcp_20a()
    _patch_future_library_example()

apply_patches()