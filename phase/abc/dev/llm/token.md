# fiber.phase.abc.dev.llm.token

## 1. Overview

The `fiber.llm.model` package provides essential utilities for managing LLM agent context windows, encompassing model specification queries, token counting, text splitting, and message trimming.

By maintaining signature parity with widely adopted LLM API conventions (such as `get_supported_openai_params`, `token_counter`, `trim_messages`, and `get_modified_max_tokens`), these modules are designed to integrate seamlessly into existing codebases with minimal modifications.

Concurrently, the module incorporates Pydantic-based integrity defenses and resilient token-decoding logic tailored to the security and architectural requirements of the DPHI ecosystem.

---

## 2. Model Info & Capabilities (`llm.model.info`)

This module provides helper functions to query supported features across diverse LLM models (e.g., Vision, Function Calling, Prompt Caching) and retrieve applicable standard parameters.

### Standard Compatible Interfaces

* `get_supported_openai_params(model, custom_llm_provider=None)`
* `get_supported_regions(model)`: Queries whether a model is supported in a specific region.
* `supports_function_calling(model)` / `supports_parallel_function_calling(model)`
* `supports_vision(model)` / `supports_audio_input(model)`
* `supports_system_messages(model)` / `supports_prompt_caching(model)`
* `supports_reasoning(model)`: Identifies models designed for advanced reasoning (e.g., o1, o3, deepseek-r1).
* `supports_native_structured_output(model)`: Queries whether the provider enforces native JSON schema constraints.

### DPHI Extension Specifications (`ModelPromptSpec` & `ModelFeatures`)

Provides extended capabilities to identify a model's lineage (Family) and reasoning properties.

* **`get_model_prompt_spec(model_name)`**: Normalizes and extracts the model family (e.g., `openai_gpt`, `anthropic_claude`, `meta_llama`). Highly useful for branching prompt formatting templates.
* **`get_features(model)`**: Returns a detailed flag object defining specific model traits, such as whether it operates as a reasoning model (ignoring stop words) or supports prompt caching (e.g., `claude-3-5`).

---

## 3. Token Counter (`llm.model.token.counter`)

Calculates precise token consumption across texts, messages, function schemas, and image metadata.

### Core Usage

```python
from fiber.llm.model.token.counter import token_counter, get_modified_max_tokens

# 1. Standard conversation token counting (including function definitions)
count = token_counter(
    model="gpt-4o", 
    messages=[{"role": "user", "content": "How are you?"}],
    tools=[{"type": "function", "function": {"name": "get_weather", "description": "..."}}]
)

# 2. Dynamic Max Tokens Adjustment (OOM Defense)
# Calculates input tokens and a buffer margin to return a safe max_tokens limit.
safe_max_tokens = get_modified_max_tokens(
    model="gpt-4o",
    base_model="gpt-4o",
    messages=my_messages,
    user_max_tokens=4000,
    buffer_perc=0.1,  # Optional (Default: 0.1)
    buffer_num=10.0   # Optional (Default: 10.0)
)

```

> **Note:** The `buffer_perc` and `buffer_num` parameters are optional. If omitted, the framework automatically applies smart defaults (a 10% buffer) to ensure stability.

### Architectural Improvements

* **Universal Encoder Architecture**: Automatically routes between `tiktoken` and `HuggingFace Tokenizer` based on the model identifier (e.g., `llama-3`), ensuring high precision when evaluating tokens for open-source models.
* **Decoupled `TokenEvaluator**`: Evaluates tokens via Dependency Injection, eliminating reliance on global state configurations.
* **O(1) Vision Computation**: When encountering a `type: image_url` object, the counter applies standard vision-tile calculation formulas (`calculate_tiles_needed`) using solely the image's metadata (width/height pixels). It computes token costs at O(1) speed without initiating network downloads.
* **Heterogeneous Provider Format Support**: Safely parses non-standard JSON blocks internally, such as Anthropic's specific schemas (`tool_use`, `tool_result`) or RAG outputs (`search_results`), to compute accurate token usage.

---

## 4. Token Window & Trimming (`llm.model.token.window`)

Safely truncates conversational histories (`messages`) to adhere to specified token thresholds. It maintains signature parity with conventional `trim_messages` utilities.

### Core Usage

```python
from fiber.llm.model.token.window import trim_messages

trimmed_msgs = trim_messages(
    messages=long_history_messages,
    model="gpt-4o",
    trim_ratio=0.8, # Sets the threshold to 80% of the model's maximum context
)

```

### Safe Eviction Policy

1. **System Prompt Preservation**: System instructions are prioritized for retention. If the threshold is still exceeded, the string is safely truncated at the token level (`ContextWindow.truncate_to_limit`).
2. **Tool Message Preservation**: The most recent `role: tool` execution results are retained to preserve the LLM's immediate execution context.
3. **Rolling Eviction**: Historical dialogue sequences (`user` and `assistant`) are evicted sequentially, starting from the oldest entries.

---

## 5. Token Splitter (`llm.model.token.splitter`)

Designed as a robust, token-safe alternative to conventional `SentenceSplitter` implementations found in widely used frameworks.

### Key Features

* **Token-ID Based Slicing**: Instead of relying on naive string lengths, texts are converted into ID arrays via the model's designated Tokenizer (HuggingFace or tiktoken) before partitioning. This prevents string corruption across multi-byte characters (e.g., Korean, Japanese) at chunk boundaries.
* **Control Token Sanitization**: For open-source models, the splitter safely identifies and strips disruptive control characters (`_strip_huggingface_special_token_ids`) during tokenizer parsing to extract pure text tokens.
* **Overlapping Chunks**: Generates sequential segments with a designated `chunk_overlap`, preventing context loss when constructing Retrieval-Augmented Generation (RAG) pipelines.

```python
from fiber.llm.model.token.splitter import TokenSplitter

splitter = TokenSplitter(
    chunk_size=500,
    chunk_overlap=50,
    model="gpt-3.5-turbo"
)
chunks = splitter.split_text("Very long document text...")

```