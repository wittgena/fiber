# xphi.arch.contract.resolver.ext
## @lineage: arch.contract.resolver.ext
## @lineage: arch.topos.resolver.ext
## @lineage: topos.resolver.ext
## @lineage: bound.resolver.ext.inter
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("resolver.ext")

DEFAULT_RULESET = {
    "constants": {
        "owner": "ext-phase",
        "tag": "v0.14.22",
        "repo_name": "inter-llama",
        "core_namespace": "llama_index",
        "api_base": "https://api.github.com/repos",
        "raw_base": "https://raw.githubusercontent.com",
        "local_path": "anchor/ext/inter-llama"
    },
    "templates": {
        "local": "{local_path}/llama-index-integrations/{category}",
        "repo": "https://github.com/{owner}/{repo_name}.git",
        "source": "llama-index-integrations/{category_pkg}/llama-index-{category_dir}-{name_dir}/{core_namespace}/{category_pkg}/{name_pkg}",
        "api": "{api_base}/{owner}/{repo_name}/contents/llama-index-integrations/{category}",
        "api_content": "{api_base}/{owner}/{repo_name}/contents/llama-index-integrations/{category_pkg}/llama-index-{category_dir}-{name_dir}/{core_namespace}/{category_pkg}/{name_pkg}",
        "raw": "{raw_base}/{owner}/llama_index/{tag}/llama-index-integrations/{category}/llama-index-{category_dir}-{name_dir}",
        "prefix": "llama-index-{category_dir}-"
    },
    "routes": {
        "local": "local",
        "repo": "repo",
        "source": "source",
        "api": "api",
        "api_content": "api_content",
        "raw": "raw",
        "prefix": "prefix"
    },
    "base_class": { "BaseLLM", "LLM", "CustomLLM",  "FunctionCallingLLM", "OpenAILike", "MultiModalLLM"}
}

class ExtResolver:
    _RULESET_PATH = Path(__file__).parent / "ruleset.json"
    RULES = DEFAULT_RULESET.copy()
    
    try:
        if _RULESET_PATH.exists():
            with open(_RULESET_PATH, "r", encoding="utf-8") as f:
                RULES.update(json.load(f))
                log.debug(f"[init] Successfully loaded custom ruleset from {_RULESET_PATH}")
    except (json.JSONDecodeError, IOError) as e:
        log.warning(f"Failed to load {_RULESET_PATH}. Using DEFAULT_RULESET. Error: {e}")

    @classmethod
    def _ctx(cls, **kwargs) -> Dict[str, Any]:
        ctx = {**cls.RULES["constants"], **kwargs}
        category = ctx.setdefault("category", "llms")

        ctx["category_pkg"] = category.replace("-", "_")
        ctx["category_dir"] = category.replace("_", "-")
        if "name" in ctx:
            name = ctx["name"]
            ctx["name_pkg"] = name.replace("-", "_")
            ctx["name_dir"] = name.replace("_", "-")
            
        log.debug(f"[_ctx] Generated context keys: {list(ctx.keys())}")
        return ctx

    @classmethod
    def _fmt(cls, template_key: str, **kwargs) -> str:
        template = cls.RULES["templates"].get(template_key)
        if not template:
            log.warning(f"[_fmt] Missing template for key: '{template_key}'")
            return ""
            
        formatted_result = template.format(**cls._ctx(**kwargs))
        log.debug(f"[_fmt] Template [{template_key}] resolved to -> {formatted_result}")
        return formatted_result

    @classmethod
    def get(cls, route: str, override: Optional[str] = None, **kwargs) -> Union[str, Path, None]:
        log.debug(f"[get] Requested route: '{route}', override: {override}, kwargs: {kwargs}")
        
        template_key = cls.RULES.get("routes", {}).get(route)
        if not template_key:
            log.error(f"[get] Unrecognized route requested: '{route}'")
            return None

        formatted_str = cls._fmt(template_key, **kwargs)
        
        if route == "local":
            final_path = Path.cwd() / (override or formatted_str)
            log.debug(f"[get] Returning Path object: {final_path}")
            return final_path
        
        return formatted_str

    @classmethod
    def inspect_route(cls, route: str, override: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        template_key = cls.RULES.get("routes", {}).get(route)
        debug_data = {
            "requested_route": route,
            "mapped_template_key": template_key,
            "raw_template": cls.RULES.get("templates", {}).get(template_key) if template_key else None,
            "injected_kwargs": kwargs,
            "resolved_context": cls._ctx(**kwargs),
            "override_path": override,
            "final_output": str(cls.get(route, override=override, **kwargs))
        }
        
        log.debug(f"[inspect_route] Route Dump:\n{json.dumps(debug_data, indent=2, ensure_ascii=False)}")
        return debug_data

    @classmethod
    def context(cls, category: str = "llms", tag: Optional[str] = None) -> Dict[str, str]:
        return {
            "local_path": str(cls.get("local", category=category)),
            "api_url": str(cls.get("api", category=category)),
            "tag": tag or cls.RULES["constants"]["tag"],
            "ext_repo": cls.RULES["constants"]["owner"]
        }