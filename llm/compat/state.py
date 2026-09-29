# fiber.llm.compat.state
STATE_EXTRACTION_RULES = {
    "gemini": {
        "fallback_tool_name": "content.parts.0.function_call.name",
        "fallback_tool_args": "content.parts.0.function_call.args",
        "sync_content_paths": ["candidates.0.content.parts.0.text", "choices.0.message.content"],
        "sync_usage_paths": ["usageMetadata", "usage"]
    },
    "ollama": {
        "sync_content_paths": ["message.content", "response", "choices.0.message.content"],
        "sync_usage_paths": ["prompt_eval_count", "usage"]
    },
    "defaults": {
        "role": "assistant",
        "finish_stop": "stop",
        "finish_tool": "tool_calls",
        "stream_content_paths": [
            "delta",                     # Default
            "choices.0.delta.content",   # OpenAI/LiteLLM
            "content.parts.0.text"       # Gemini Native JSON
        ],
        "sync_content_paths": [
            "choices.0.message.content", # Default
            "message.content",           # Generic
            "output"                     # Replicate, etc
        ],
        "sync_usage_paths": [
            "usage",                     # Default
            "meta.usage"
        ]
    }
}

ENDPOINT_ROUTING_RULES = {
    "ollama": {
        "native_suffix": "/api/chat",
        "openai_suffix": "/chat/completions",
        "v1_indicator": "/v1"
    },
    "defaults": {
        "openai_suffix": "/chat/completions"
    }
}