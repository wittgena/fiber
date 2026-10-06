# fiber.llm.compat.state
STATE_EXTRACTION_RULES = {
    "gemini": {
        "fallback_tool_name": "content.parts.0.function_call.name",
        "fallback_tool_args": "content.parts.0.function_call.args",
        "sync_content_paths": ["candidates.0.content.parts.0.text", "choices.0.message.content"],
        "sync_usage_paths": [
            {
                "prompt_tokens": [
                    "raw.usage_metadata.prompt_token_count", 
                    "usage_metadata.prompt_token_count",
                    "usage.prompt_token_count"
                ],
                "completion_tokens": [
                    "raw.usage_metadata.candidates_token_count", 
                    "usage_metadata.candidates_token_count",
                    "usage.candidates_token_count"
                ],
                "total_tokens": [
                    "raw.usage_metadata.total_token_count", 
                    "usage_metadata.total_token_count",
                    "usage.total_token_count"
                ]
            },
            "usage", 
            "usage_metadata",
            "usageMetadata"
        ]
    },
    "anthropic": {
        "sync_content_paths": ["raw.content.0.text", "message.content"],
        "sync_usage_paths": [
            {
                "prompt_tokens": [
                    "raw.usage.input_tokens", 
                    "additional_kwargs.usage.input_tokens", 
                    "usage.input_tokens"
                ],
                "completion_tokens": [
                    "raw.usage.output_tokens", 
                    "additional_kwargs.usage.output_tokens", 
                    "usage.output_tokens"
                ]
            },
            "raw.usage",
            "usage"
        ]
    },
    "ollama": {
        "sync_content_paths": ["message.content", "response", "choices.0.message.content"],
        "sync_usage_paths": ["prompt_eval_count", "usage"]
    },
    "cohere": {
        "sync_content_paths": ["text", "raw.text"],
        "sync_usage_paths": [
            {
                "prompt_tokens": ["raw.meta.billed_units.input_tokens", "raw.meta.tokens.input_tokens"],
                "completion_tokens": ["raw.meta.billed_units.output_tokens", "raw.meta.tokens.output_tokens"],
                "total_tokens": ["raw.meta.tokens.input_tokens"]
            },
            "raw.meta.billed_units"
        ]
    },
    "defaults": {
        "role": "assistant",
        "finish_stop": "stop",
        "finish_tool": "tool_calls",
        "stream_content_paths": [
            "delta",                     # Default
            "choices.0.delta.content",   # OpenAI/LiteLLM
            "content.parts.0.text",      # Gemini Native JSON
            "text",
            "raw.text"
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
    "cohere": {
        "native_suffix": "/v1/chat",
        "openai_suffix": "/v1/chat/completions"
    },
    "defaults": {
        "openai_suffix": "/chat/completions"
    }
}