# fiber.dev.trace.vcr.injector
## @lineage: fiber.dev.trace.llm.vcr.injector
from typing import Optional
from contextlib import contextmanager

from fiber.llm.model.router.adapter import AdapterRegistry
from fiber.dev.trace.vcr.manager import VCRPlaybackConfig, VCRManager
from fiber.dev.trace.vcr.proxy import VCRAdapterProxy
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("llm.vcr.injector")

class VCRInjector:
    @staticmethod
    def apply(config: VCRPlaybackConfig, fixture_dir: str) -> Optional[VCRManager]:
        if config.mode == "live":
            log.debug("VCRInjector: Mode is 'live', skipping proxy injection.")
            return None
            
        manager = VCRManager(config=config, fixture_dir=fixture_dir)
        if not AdapterRegistry._is_initialized:
            AdapterRegistry.setup_defaults()
            
        log.info(f"VCRInjector: Applying VCRAdapterProxy to AdapterRegistry ({config.mode} mode)")
        patched_count = 0
        
        for task_type, providers in list(AdapterRegistry._adapters.items()):
            for provider_name, original_adapter in list(providers.items()):
                if not getattr(original_adapter, "_is_vcr_patched", False):
                    proxy = VCRAdapterProxy(original_adapter, manager)
                    proxy._is_vcr_patched = True
                    AdapterRegistry._adapters[task_type][provider_name] = proxy
                    patched_count += 1
                    
        for task_type, original_fallback in list(AdapterRegistry._fallback_adapters.items()):
            if not getattr(original_fallback, "_is_vcr_patched", False):
                proxy = VCRAdapterProxy(original_fallback, manager)
                proxy._is_vcr_patched = True
                AdapterRegistry._fallback_adapters[task_type] = proxy
                patched_count += 1
                
        log.debug(f"VCRInjector: Successfully patched {patched_count} adapters.")
        return manager
    
    @staticmethod
    def remove():
        if not AdapterRegistry._is_initialized:
            return

        restored_count = 0
        for task_type, providers in list(AdapterRegistry._adapters.items()):
            for provider_name, adapter in list(providers.items()):
                if getattr(adapter, "_is_vcr_patched", False) and hasattr(adapter, "original_adapter"):
                    AdapterRegistry._adapters[task_type][provider_name] = adapter.original_adapter
                    restored_count += 1
                    
        for task_type, adapter in list(AdapterRegistry._fallback_adapters.items()):
            if getattr(adapter, "_is_vcr_patched", False) and hasattr(adapter, "original_adapter"):
                AdapterRegistry._fallback_adapters[task_type] = adapter.original_adapter
                restored_count += 1

        if restored_count > 0:
            log.info(f"VCRInjector: Teardown complete. Restored {restored_count} original adapters.")

    @classmethod
    @contextmanager
    def use_cassette(cls, config: VCRPlaybackConfig, fixture_dir: str):
        """ex: with VCRInjector.use_cassette(config, "./fixtures"):"""
        manager = cls.apply(config, fixture_dir)
        try:
            yield manager
        finally:
            cls.remove()