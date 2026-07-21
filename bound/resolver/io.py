# bound.resolver.io
## @lineage: bound.registry.io
## @lineage: anchor.registry.io
"""@desc: Core registry I/O manager handling remote fetching, local TTL caching, and conditional network operations"""
from __future__ import annotations
import json
import os
import time
import httpx
from email.utils import formatdate
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Callable, Union

from phase.bind.resolver import resolve_path 
from watcher.plane.emitter import get_emitter 

log = get_emitter("registry.io")
REGISTRY_ROOT = resolve_path("registry") / "llms"

class RegistryIO:
    """@desc: Namespace for registry file operations and network I/O."""
    
    @classmethod
    def save_backup(cls, data: dict, filename: str) -> None:
        """@desc: Persists the successfully fetched remote registry payload to local disk."""
        try:
            REGISTRY_ROOT.mkdir(parents=True, exist_ok=True)
            backup_path = REGISTRY_ROOT / filename
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            log.debug(f"Saved local backup successfully: {backup_path}")
        except Exception as e:
            log.warning(f"Failed to save local backup for {filename}: {e}")

    @classmethod
    def load_backup(cls, filename: str) -> dict:
        """@desc: Retrieves the local backup payload from disk."""
        try:
            backup_path = REGISTRY_ROOT / filename
            if backup_path.exists():
                with open(backup_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            log.warning(f"Local backup not found at {backup_path}")
        except Exception as e:
            log.error(f"Failed to load local backup for {filename}: {e}")
        return {}

    @classmethod
    def _optimize_memory(cls, raw_spec: dict) -> dict:
        """@desc: Drops sparse data (False, 0, null, empty strings) to minimize memory footprint."""
        optimized = {}
        for k, v in raw_spec.items():
            if isinstance(v, dict):
                optimized_sub = cls._optimize_memory(v)
                if optimized_sub:
                    optimized[k] = optimized_sub
            elif v not in (False, 0.0, 0, "", None, [], {}):
                optimized[k] = v
        return optimized

    @classmethod
    def _expand_aliases(cls, cost_map: dict) -> dict:
        """@desc: Expands alias lists into top-level key references within the map."""
        aliases_to_add: Dict[str, dict] = {}
        keys_with_aliases = []

        for model_name, model_info in cost_map.items():
            if not isinstance(model_info, dict):
                continue
            aliases = model_info.get("aliases")
            if isinstance(aliases, list):
                keys_with_aliases.append(model_name)
                for alias in aliases:
                    if alias not in cost_map and alias not in aliases_to_add:
                        aliases_to_add[alias] = model_info 

        for key in keys_with_aliases:
            cost_map[key].pop("aliases", None)

        cost_map.update(aliases_to_add)
        return cost_map

    @classmethod
    def parse_cost_map(cls, raw_data: dict) -> dict:
        """@desc: Unified parser hook specifically for Model Cost Maps."""
        expanded = cls._expand_aliases(raw_data)
        for model_name, spec in expanded.items():
            if isinstance(spec, dict):
                expanded[model_name] = cls._optimize_memory(spec)
        return expanded

    @classmethod
    def fetch_and_validate(
        cls,
        url: str,
        filename: str,
        force_local_env_key: str,
        registry_name: str = "Registry",
        min_count: int = 0,
        parser_hook: Optional[Callable[[dict], dict]] = None,
        cache_ttl_seconds: int = 86400,
    ) -> Tuple[dict, dict]:
        """@desc: Core network fetcher implementing TTL-based Zero-Network Delay and Conditional GET"""
        source_info = {"source": "local", "url": url, "is_env_forced": False, "fallback_reason": None}
        if os.getenv(force_local_env_key, "").lower() == "true":
            source_info["is_env_forced"] = True
            local_data = cls.load_backup(filename)
            return (parser_hook(local_data) if parser_hook else local_data), source_info

        backup_path = REGISTRY_ROOT / filename
        headers = {}
        if backup_path.exists():
            mtime = backup_path.stat().st_mtime
            file_age = time.time() - mtime
            if file_age < cache_ttl_seconds:
                log.debug(f"[{registry_name}] Using valid local cache (TTL < {cache_ttl_seconds}s)")
                local_data = cls.load_backup(filename)
                if local_data and len(local_data) >= min_count:
                    source_info["source"] = "local_cache_ttl"
                    return (parser_hook(local_data) if parser_hook else local_data), source_info

            http_date = formatdate(timeval=mtime, localtime=False, usegmt=True)
            headers["If-Modified-Since"] = http_date

        try:
            response = httpx.get(url, headers=headers, timeout=5)
            if response.status_code == 304:
                log.debug(f"[{registry_name}] Remote map not modified (304). Updating local cache timestamp.")
                os.utime(backup_path, None) 
                local_data = cls.load_backup(filename)
                source_info["source"] = "local_cache_304"
                return (parser_hook(local_data) if parser_hook else local_data), source_info

            response.raise_for_status()
            content = response.json()
            if not isinstance(content, dict) or len(content) < min_count:
                raise ValueError(f"Invalid map format or item count less than min_count({min_count})")

            cls.save_backup(content, filename)
            source_info["source"] = "remote"
            return (parser_hook(content) if parser_hook else content), source_info

        except Exception as e:
            log.warning(f"[{registry_name}] Failed to fetch remote map. Falling back to stale local cache: {e}")
            source_info["fallback_reason"] = str(e)
            source_info["source"] = "stale_local_fallback"
            local_data = cls.load_backup(filename)
            return (parser_hook(local_data) if parser_hook else local_data), source_info

    @classmethod
    def get_model_cost_map(cls, url: str) -> Tuple[dict, dict]:
        """@desc: Concrete factory invoking fetch_and_validate for model cost data."""
        return cls.fetch_and_validate(
            url=url,
            filename="model_prices_and_context_window_backup.json",
            force_local_env_key="LITELLM_LOCAL_MODEL_COST_MAP",
            registry_name="ModelCostMap",
            min_count=50,
            parser_hook=cls.parse_cost_map
        )