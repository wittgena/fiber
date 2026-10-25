# fiber.infra.mcp.worker.price.model
"""
[MCP Worker Authoring Rules (ToolBinderMixin)]
1. Method Naming & Signature:
   - Define exposed MCP tools with the `tool_<name>` prefix (e.g., `async def tool_lookup_model_cost(self, model_name: str)`).
   - Type hints (`str`, `int`, `float`, `bool`, `list`, `dict`, `Literal`, `Optional`) and default values automatically synthesize the JSON `inputSchema` and `required` list.
2. Docstring Convention (`\"\"\"`):
   - First line: Extracted as the tool-level `description`.
   - Subsequent lines prefixed with `- <param_name>: <desc>` (or `:param <param_name>: <desc>`): Extracted as individual parameter `description` fields.
3. Return & Error Handling:
   - Return native Python `dict`, `list`, or `str` directly; the base protocol wraps and serializes them into MCP `content` envelopes (`isError: False`).
   - Raise `ValueError` / `TypeError` / `KeyError` for invalid arguments (mapped to JSON-RPC `-32602`).
   - Raise `ToolError` for domain-level negative results (mapped to MCP `content` envelope with `isError: True`).
"""
import sys
import time
import asyncio
from typing import Dict, Any

from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol
from xphi.arch.contract.protocol.binder import ToolError
from fiber.llm.provider.cost.registry import RegistryIO
from fiber.llm.provider.cost.filter import extract_aligned_registry, CORE_KEYWORDS


class PriceModelServer(AsyncWorkerProtocol):
    """
    - Provides LLM pricing and context specifications to MCP clients.
    - Automatically bootstraps the filter pipeline if the registry file is missing.
    - Periodically synchronizes the latest API costs in the background.
    """
    def __init__(self):
        # Cache and state management
        self._registry_cache: Dict[str, Any] = {}
        self._last_loaded: float = 0.0
        self._last_remote_sync: float = 0.0

        # TTL Configurations
        self._cache_ttl = 300.0                 # 5 minutes: Local disk hot-reload TTL
        self._sync_ttl = 86400.0                # 24 hours: Remote auto-sync TTL

        # Concurrency lock to prevent overlapping downloads
        self._update_lock = asyncio.Lock()

        super().__init__(agent_name="price.model")
        self.log.info("Initializing Autonomous Model Price Receptor...")

    async def _fetch_and_build_registry(self) -> None:
        """Executes the filter pipeline to fetch remote data and update the local JSON registry."""
        try:
            self.log.info("Triggering cost registry filter pipeline (Remote Fetch)...")
            self._last_remote_sync = time.time()

            # Offload blocking network I/O to a separate thread to protect the event loop
            await asyncio.to_thread(extract_aligned_registry, target_keywords=CORE_KEYWORDS)

            self.log.info("Remote sync completed successfully. Registry file updated.")
        except Exception as e:
            self.log.error(f"Failed to build registry from remote: {e}", exc_info=True)

    async def _get_active_registry(self) -> Dict[str, Any]:
        """Checks TTL and file existence, proactively generating or updating the registry if needed."""
        now = time.time()

        # In-memory cache hit
        if self._registry_cache and (now - self._last_loaded) <= self._cache_ttl:
            return self._registry_cache

        async with self._update_lock:
            # Attempt local disk load
            self._registry_cache = RegistryIO.get_model_cost_map()

            # On-demand bootstrap if the local file is missing or empty
            if not self._registry_cache:
                self.log.warning("Registry file not found or empty. Bootstrapping registry...")
                await self._fetch_and_build_registry()
                self._registry_cache = RegistryIO.get_model_cost_map()

            # Background auto-sync for stale data
            elif (now - self._last_remote_sync) > self._sync_ttl:
                self.log.info("Registry data is stale (> 24h). Spawning background remote sync...")
                asyncio.create_task(self._fetch_and_build_registry())

            self._last_loaded = time.time()
            self.log.debug(f"Oracle Registry active with {len(self._registry_cache)} models.")

        return self._registry_cache

    async def tool_lookup_model_cost(self, model_name: str) -> Dict[str, Any]:
        """Fetch pricing (input/output cost per token) and max context window specifications for a specific LLM.
        - model_name: The exact model identifier (e.g., 'gpt-4o').
        """
        target = model_name.strip().lower()
        if not target:
            raise ValueError("'model_name' is required.")

        registry = await self._get_active_registry()

        matched_info = registry.get(target)
        if not matched_info:
            matched_info = next((v for k, v in registry.items() if target in k.lower()), None)
            if not matched_info:
                raise ToolError(f"Model '{target}' not found in the registry.")

        return matched_info

    async def tool_search_models_by_provider(self, provider: str) -> Dict[str, Any]:
        """List available models and their capabilities for a specific provider (e.g., openai, anthropic, google, openrouter).
        - provider: Provider name.
        """
        target_provider = provider.strip().lower()
        if not target_provider:
            raise ValueError("'provider' is required.")

        registry = await self._get_active_registry()

        matched_models = [
            {
                "id": model_id,
                "max_input_tokens": info.get("max_input_tokens"),
                "supports_vision": info.get("supports_vision", False),
            }
            for model_id, info in registry.items()
            if str(info.get("provider_name", "")).lower() == target_provider
        ]

        if not matched_models:
            raise ToolError(f"No models found for provider '{target_provider}'.")

        return {"count": len(matched_models), "models": matched_models}


def main():
    server = PriceModelServer()
    try:
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        server.log.info("Server shutting down by interrupt.")
        sys.exit(0)


if __name__ == "__main__":
    main()