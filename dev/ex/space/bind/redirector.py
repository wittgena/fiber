# fiber.dev.ex.space.bind.redirector
import sys
import types
import importlib.util
from pathlib import Path
from typing import Optional, Union
from xphi.kernel.space.bind.resolver import find_current_self
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bind.redirector")

SELF_ROOT = find_current_self()

class BindRedirector:
    def __init__(self, target_package: str, local_dir: Union[str, Path], clear_cache: bool = True):
        self.target_package = target_package
        self.local_dir = Path(local_dir).resolve()
        self.clear_cache = clear_cache
        self._is_installed = False

    def find_spec(self, fullname, path, target=None):
        ## Check if it matches the target package or its sub-packages
        if fullname == self.target_package or fullname.startswith(f"{self.target_package}."):
            rel_path = fullname[len(self.target_package):].lstrip(".").replace(".", "/")
            target_path = self.local_dir / rel_path

            ## Check for package structure (presence of __init__.py)
            if target_path.is_dir():
                init_file = target_path / "__init__.py"
                if init_file.exists():
                    return importlib.util.spec_from_file_location(
                        fullname,
                        str(init_file),
                        submodule_search_locations=[str(target_path)]
                    )
            
            ## Check for single file structure (presence of .py)
            py_file = target_path.with_suffix(".py")
            if py_file.exists():
                return importlib.util.spec_from_file_location(fullname, str(py_file))

        return None

    def install(self):
        """Register the custom finder at the highest priority in sys.meta_path."""
        if self._is_installed:
            return

        if self.clear_cache:
            self._clear_sys_modules()

        sys.meta_path.insert(0, self)
        self._is_installed = True
        log.info(f"[Redirector] '{self.target_package}' -> '{self.local_dir}' mapping installed.")

    def uninstall(self):
        """Remove the registered custom finder."""
        if self in sys.meta_path:
            sys.meta_path.remove(self)
        
        if self.clear_cache:
            self._clear_sys_modules()
            
        self._is_installed = False
        log.info(f"[Redirector] '{self.target_package}' mapping uninstalled.")

    def _clear_sys_modules(self):
        """Delete previously loaded cached modules to force a reload."""
        keys_to_del = [
            key for key in sys.modules.keys() 
            if key == self.target_package or key.startswith(f"{self.target_package}.")
        ]
        for key in keys_to_del:
            del sys.modules[key]

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.uninstall()

class PhaseAirlock:
    @classmethod
    def alias(cls, mapping: dict[str, str]):
        for legacy_path, canonical_path in mapping.items():
            try:
                target_module = importlib.import_module(canonical_path)
                sys.modules[legacy_path] = target_module
                log.info(f"[*] Alias: {legacy_path} ➔ {canonical_path}")
            except ImportError as e:
                log.error(f"[!] Failed to load canonical module '{canonical_path}' for alias '{legacy_path}'.")
                raise e