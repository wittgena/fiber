# fiber.infra.mcp.worker.price.model
import sys
import json
import time
import asyncio
from typing import Dict, Any

from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol
from fiber.llm.provider.cost.registry import RegistryIO
from fiber.llm.provider.cost.filter import extract_aligned_registry, CORE_KEYWORDS

class PriceModelServer(AsyncWorkerProtocol):
    """
    - Provides LLM pricing and context specifications to MCP clients.
    - Automatically bootstraps the filter pipeline if the registry file is missing.
    - Periodically synchronizes the latest API costs in the background.
    """
    def __init__(self):
        super().__init__(agent_name="price.model")
        self.log.info("Initializing Autonomous Model Price Receptor...")
        
        # Cache and state management
        self._registry_cache: Dict[str, Any] = {}
        self._last_loaded: float = 0.0          
        self._last_remote_sync: float = 0.0     
        
        # TTL Configurations
        self._cache_ttl = 300.0                 # 5 minutes: Local disk hot-reload TTL
        self._sync_ttl = 86400.0                # 24 hours: Remote auto-sync TTL
        
        # Concurrency lock to prevent overlapping downloads
        self._update_lock = asyncio.Lock()

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

    async def handle_tools_list(self, req_id: Any):
        tools = [
            {
                "name": "lookup_model_cost",
                "description": "Fetch pricing (input/output cost per token) and max context window specifications for a specific LLM.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "model_name": {"type": "string", "description": "The exact model identifier (e.g., 'gpt-4o')."}
                    },
                    "required": ["model_name"]
                }
            },
            {
                "name": "search_models_by_provider",
                "description": "List available models and their capabilities for a specific provider (e.g., openai, anthropic, google, openrouter).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "provider": {"type": "string", "description": "Provider name."}
                    },
                    "required": ["provider"]
                }
            }
        ]
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "lookup_model_cost":
            await self._execute_lookup_cost(req_id, arguments)
        elif tool_name == "search_models_by_provider":
            await self._execute_search_by_provider(req_id, arguments)
        else:
            await self.send_error(req_id, code=-32601, message=f"Method not found: Unknown tool '{tool_name}'")

    async def _execute_lookup_cost(self, req_id: Any, arguments: Dict[str, Any]):
        model_name = arguments.get("model_name", "").strip().lower()
        if not model_name:
            await self.send_error(req_id, code=-32602, message="Invalid params: 'model_name' is required.")
            return

        registry = await self._get_active_registry()
        
        matched_info = registry.get(model_name)
        if not matched_info:
            fallback_match = next((v for k, v in registry.items() if model_name in k.lower()), None)
            if fallback_match:
                matched_info = fallback_match
            else:
                await self.send_response(req_id, {
                    "content": [{"type": "text", "text": f"Model '{model_name}' not found in the registry."}],
                    "isError": True
                })
                return

        await self.send_response(req_id, {
            "content": [{"type": "text", "text": json.dumps(matched_info, indent=2)}],
            "isError": False
        })

    async def _execute_search_by_provider(self, req_id: Any, arguments: Dict[str, Any]):
        provider = arguments.get("provider", "").strip().lower()
        if not provider:
            await self.send_error(req_id, code=-32602, message="Invalid params: 'provider' is required.")
            return

        registry = await self._get_active_registry()
        
        matched_models = []
        for model_id, info in registry.items():
            if str(info.get("provider_name", "")).lower() == provider:
                matched_models.append({
                    "id": model_id,
                    "max_input_tokens": info.get("max_input_tokens"),
                    "supports_vision": info.get("supports_vision", False)
                })

        if not matched_models:
            await self.send_response(req_id, {
                "content": [{"type": "text", "text": f"No models found for provider '{provider}'."}],
                "isError": True
            })
            return

        await self.send_response(req_id, {
            "content": [{"type": "text", "text": json.dumps({"count": len(matched_models), "models": matched_models}, indent=2)}],
            "isError": False
        })

def main():
    server = PriceModelServer()
    try:
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        server.log.info("Server shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()