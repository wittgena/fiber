# anchor.registry.resolver.ext
from pathlib import Path
from typing import Optional, Dict

class ExtResolver:
    """@desc: Resolves external dependency paths and URLs for integration scanners/analyzers across various categories."""
    
    ## Basic metadata
    DEFAULT_EXT_REPO = "ext-phase"
    DEFAULT_TAG = "v0.14.22"
    GITHUB_REPO_NAME = "inter-llama"
    
    ## Relative path templates
    BASE_INTEGRATION_SUBPATH = "llama-index-integrations/{category}"
    DEFAULT_LOCAL_PATH = "anchor/ext/inter-llama/llama-index-integrations/{category}"

    @classmethod
    def resolve_local_path(cls, category: str = "llms", override_path: Optional[str] = None) -> Path:
        """@desc: Returns the absolute path to the locally cloned integration module for the given category."""
        if override_path:
            return Path.cwd() / override_path
            
        subpath = cls.DEFAULT_LOCAL_PATH.format(category=category)
        return Path.cwd() / subpath

    @classmethod
    def resolve_github_repo_url(cls, repo_owner: str = DEFAULT_EXT_REPO) -> str:
        """@desc: Returns the direct Git clone URL."""
        return f"https://github.com/{repo_owner}/{cls.GITHUB_REPO_NAME}.git"

    @classmethod
    def resolve_source_subpath(cls, category: str, integration_name: str) -> str:
        """@desc: Resolves the exact source code directory path within the LlamaIndex structure."""
        category_dir = category.replace("_", "-")
        name_dir = integration_name.replace("_", "-")
        category_pkg = category.replace("-", "_")
        name_pkg = integration_name.replace("-", "_")
        
        return (
            f"llama-index-integrations/{category_pkg}/"
            f"llama-index-{category_dir}-{name_dir}/"
            f"llama_index/{category_pkg}/{name_pkg}"
        )

    @classmethod
    def resolve_github_api(cls, category: str = "llms", repo_owner: str = DEFAULT_EXT_REPO) -> str:
        """@desc: Returns the GitHub API URL used to query the top-level directory contents of a specific integration category."""
        subpath = cls.BASE_INTEGRATION_SUBPATH.format(category=category)
        return f"https://api.github.com/repos/{repo_owner}/{cls.GITHUB_REPO_NAME}/contents/{subpath}"

    @classmethod
    def resolve_github_api_contents(
        cls, 
        category: str, 
        integration_name: str, 
        repo_owner: str = DEFAULT_EXT_REPO
    ) -> str:
        """@desc: Returns the GitHub API URL for extracting a specific integration's deep source code directory."""
        subpath = cls.resolve_source_subpath(category, integration_name)
        return f"https://api.github.com/repos/{repo_owner}/{cls.GITHUB_REPO_NAME}/contents/{subpath}"

    @classmethod
    def resolve_github_raw(
        cls, 
        integration_name: str, 
        category: str = "llms", 
        tag: str = DEFAULT_TAG, 
        repo_owner: str = DEFAULT_EXT_REPO
    ) -> str:
        """@desc: Returns the base GitHub raw URL for fetching standard files (like pyproject.toml) of a specific integration target."""
        category_dir = category.replace("_", "-")
        name_dir = integration_name.replace("_", "-")
        
        target_dir = f"llama-index-{category_dir}-{name_dir}"
        subpath = cls.BASE_INTEGRATION_SUBPATH.format(category=category)
        
        return f"https://raw.githubusercontent.com/{repo_owner}/llama_index/{tag}/{subpath}/{target_dir}"

    @classmethod
    def resolve_context(
        cls, 
        category: str = "llms", 
        override_local: Optional[str] = None,
        tag: str = DEFAULT_TAG
    ) -> Dict[str, str]:
        """@desc: Returns a comprehensive dictionary containing all necessary paths and URLs for the specified category."""
        return {
            "local_path": str(cls.resolve_local_path(category, override_local)),
            "api_url": cls.resolve_github_api(category),
            "tag": tag,
            "ext_repo": cls.DEFAULT_EXT_REPO
        }