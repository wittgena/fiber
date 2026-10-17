# fiber.llm.provider.cost.filter
import argparse
import requests
from typing import Tuple, Union, Dict
from pathlib import Path

from fiber.llm.provider.cost.registry import RegistryIO, DEFAULT_REGISTRY_FILENAME
from fiber.llm.provider.cost.adapter import RegistryAdapters

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

WORKSPACE_ROOT = resolve_path(".")
log = get_emitter("provider.cost.filter")

CORE_KEYWORDS = (
    "openai", "anthropic", "gemini", "google", "vertex_ai", 
    "ollama", "llama_server", "cohere", "cerebras", "groq", 
    "lm_studio", "mistral", "huggingface"
)

URL_SOURCES = {
    "litellm": "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
    "openrouter": "https://openrouter.ai/api/v1/models",
}

def fetch_json(source_name: str, url: str) -> dict:
    try:
        log.info(f"[{source_name}] Fetching remote registry data from: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        log.debug(f"[{source_name}] Successfully fetched {len(str(data)) // 1024} KB of data.")
        return data
    except Exception as e:
        log.error(f"[{source_name}] Request Failed | {e}")
        return {}

def extract_aligned_registry(
    target_keywords: Tuple[str, ...],
    output_filename: Union[str, Path] = DEFAULT_REGISTRY_FILENAME
) -> None:
    log.info(f"Initiating multi-source registry alignment. Core keywords count: {len(target_keywords)}")

    # 1. Fetch
    raw_litellm = fetch_json("LiteLLM", URL_SOURCES["litellm"])
    raw_openrouter = fetch_json("OpenRouter", URL_SOURCES["openrouter"])

    # 2. Normalize
    log.info("--- Starting Normalization via Adapters ---")
    data_openrouter = RegistryAdapters.parse_openrouter(raw_openrouter)
    data_litellm = RegistryAdapters.parse_litellm(raw_litellm)

    # 3. Merge (Priority: LiteLLM > OpenRouter)
    log.info("--- Starting Priority Merge ---")
    unified_registry: Dict[str, Dict] = {}
    
    # 2순위 (OpenRouter) 먼저 적재
    unified_registry.update(data_openrouter)
    log.info(f"[Merge] OpenRouter: {len(data_openrouter)} models loaded.")
    
    # 1순위 (LiteLLM) 덮어쓰기
    overwrite_ll = sum(1 for k in data_litellm if k in unified_registry)
    unified_registry.update(data_litellm)
    log.info(f"[Merge] LiteLLM: {len(data_litellm)} models loaded (Overwrote {overwrite_ll} existing).")
    
    log.info(f"Merged total unique models: {len(unified_registry)}")

    # 4. Filter
    log.info("--- Starting Core Keywords Filtering ---")
    aligned_data = {}
    
    for model_name, model_info in unified_registry.items():
        provider = str(model_info.get("provider_name", "")).strip().lower()
        model_name_lower = model_name.lower()
        
        if model_name_lower.startswith(target_keywords) or provider.startswith(target_keywords):
            aligned_data[model_name] = model_info

    dropped_count = len(unified_registry) - len(aligned_data)
    
    # 5. Save
    RegistryIO.save_registry(aligned_data, str(output_filename))
    log.info(f"Alignment complete: {len(aligned_data)} models saved. (Filtered out {dropped_count} non-core models)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch, normalize, and align multi-source registry data.")
    args = parser.parse_args()

    extract_aligned_registry(target_keywords=CORE_KEYWORDS)