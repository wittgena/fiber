# phi.tenant.router.mapper.scan
## @lineage: bound.router.scan.installed
## @lineage: eco.llama.router.scan.installed
import importlib
import inspect
import pkgutil
from typing import Dict, Any
from dataclasses import asdict
from tenant.llama.bound.base.llms.base import BaseLLM
from phi.tenant.router.mapper.llm import LLMCapabilities, LLMInfo
from topos.resolver.ext import ExtResolver

from watcher.plane.emitter import get_emitter

log = get_emitter("scan.installed", phase="SYSTEM")

class LLMInstalledScanner:
    """
    @manifold: Runtime Introspection Scanner
    @desc: Inspects loaded Python packages to discover LLM implementations instead of scanning files/GitHub.
    """
    def __init__(self, base_pkg: str = "anchor.inter.llms"):
        self.base_pkg = base_pkg
        self.known_bases = set(ExtResolver.RULES.get("base_class", []))

    def _extract_meta(self, obj: Any) -> Dict[str, Any]:
        """Extracts capabilities and accepted kwargs from the class object."""
        mro = inspect.getmro(obj)
        lineage = [c.__name__ for c in mro if c.__name__ not in ("object", "BaseModel", "Generic")]
        
        ## Pydantic v1, v2 호환 필드 추출
        fields = getattr(obj, "model_fields", getattr(obj, "__fields__", {}))
        kwargs = set(fields.keys()) | {"additional_kwargs", "callback_manager", "system_prompt"}

        caps = LLMCapabilities(
            is_function_calling="FunctionCallingLLM" in lineage,
            is_openai_like="OpenAILike" in lineage,
            is_multimodal="MultiModalLLM" in lineage,
            supports_structured_outputs=hasattr(obj, "astructured_predict")
        )
        return {"lineage": lineage, "accepted_kwargs": list(kwargs), "capabilities": caps}

    def _inspect_pkg(self, pkg: Any, registry: Dict[str, LLMInfo], pkg_name: str):
        """Inspects a single package to find and register an LLM class."""
        for name, obj in inspect.getmembers(pkg, inspect.isclass):
            ## Ensure the class is defined in the current package (prevents external imports)
            if obj.__module__ != pkg.__name__:
                continue
                
            base_names = {b.__name__ for b in inspect.getmro(obj)}
            is_target = (issubclass(obj, BaseLLM) and obj is not BaseLLM) or \
                        (bool(base_names & self.known_bases) and name not in self.known_bases)

            if is_target:
                meta = self._extract_meta(obj)
                
                ## Identify the provider (e.g., 'anchor.inter.llms.openai.base' -> 'openai')
                provider = pkg_name.split(".")[-2] if "." in pkg_name else pkg_name
                
                registry[provider] = LLMInfo(
                    status="installed", 
                    type="runtime_introspection",
                    module=pkg_name, 
                    class_name=name, 
                    tags=[provider, name.lower()], 
                    source_repo="sys.path",
                    **meta
                )
                log.debug(f"[+] Found installed LLM: {name} in {pkg_name}")
                break

    def scan(self) -> Dict[str, Any]:
        """Scans the base package and its sub-packages for LLM implementations."""
        log.info(f"[*] Starting runtime introspection on package: {self.base_pkg}")
        registry: Dict[str, LLMInfo] = {}

        try:
            base_obj = importlib.import_module(self.base_pkg)
        except ImportError as e:
            log.warning(f"[-] Base package '{self.base_pkg}' failed to import: {e}")
            return {}

        ## @case: single package
        if not hasattr(base_obj, "__path__"):
            self._inspect_pkg(base_obj, registry, self.base_pkg)
            return {k: asdict(v) for k, v in registry.items()}

        ## @case: Multi-package case (iterate through all sub-packages)
        for _, sub_pkg_name, _ in pkgutil.walk_packages(base_obj.__path__, base_obj.__name__ + "."):
            try:
                sub_pkg = importlib.import_module(sub_pkg_name)
                self._inspect_pkg(sub_pkg, registry, sub_pkg_name)
            except Exception as e:
                log.debug(f"[Import Warning] Failed to inspect sub-package {sub_pkg_name}: {e}")
                
        return {k: asdict(v) for k, v in registry.items()}