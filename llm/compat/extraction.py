# fiber.llm.compat.extraction
EXTRACTION_RULES = {
    "openai": {
        "text": ["choices.0.delta.content", "choices.0.message.content"],
        "finish_reason": ["choices.0.finish_reason"],
        "logprobs": ["choices.0.logprobs"],
        "usage": ["usage"],
        "tool_calls": ["choices.0.delta.tool_calls", "choices.0.message.tool_calls"]
    },
    "gemini": {
        "fallback_tool_name": "content.parts.0.function_call.name",
        "fallback_tool_args": "content.parts.0.function_call.args",
        "text": [
            "choices.0.delta.content",
            "content.parts.0.text",
            "candidates.0.content.parts.0.text",
            "choices.0.message.content",
            "text"
        ],
        "finish_reason": [
            "finish_reason",
            "choices.0.finish_reason",
            "candidates.0.finish_reason"
        ],
        "usage": [
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
        ],
        "logprobs": ["logprobs", "choices.0.logprobs"]
    },
    "anthropic": {
        "text": [
            "choices.0.delta.content",
            "delta.text",
            "raw.content.0.text",
            "message.content",
            "text"
        ],
        "finish_reason": [
            "finish_reason",
            "delta.stop_reason",
            "choices.0.finish_reason"
        ],
        "usage": [
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
        "text": [
            "choices.0.delta.content",
            "message.content", 
            "response", 
            "choices.0.message.content"
        ],
        "finish_reason": ["done_reason", "choices.0.finish_reason"],
        "is_finished_cond": {"path": "done", "value": True},
        "usage": [
            "usage",
            {
                "prompt_tokens": "prompt_eval_count",
                "completion_tokens": "eval_count"
            }
        ]
    },
    "cohere": {
        "text": ["choices.0.delta.content", "text", "raw.text"],
        "finish_reason": ["finish_reason", "choices.0.finish_reason"],
        "is_finished_cond": {"path": "is_finished", "value": True},
        "usage": [
            {
                "prompt_tokens": [
                    "meta.billed_units.input_tokens",
                    "raw.meta.billed_units.input_tokens", 
                    "raw.meta.tokens.input_tokens"
                ],
                "completion_tokens": [
                    "meta.billed_units.output_tokens",
                    "raw.meta.billed_units.output_tokens", 
                    "raw.meta.tokens.output_tokens"
                ]
            },
            "raw.meta.billed_units",
            "usage"
        ]
    },
    "text-completion-openai": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
        "usage": "usage"
    },
    "text-completion-codestral": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
        "usage": "usage"
    },
    "azure": {
        "text": ["choices.0.delta.content", "choices.0.message.content"],
        "finish_reason": "choices.0.finish_reason",
        "usage": "usage"
    },
    "azure_text": {
        "text": "choices.0.text",
        "finish_reason": "choices.0.finish_reason",
    },
    "replicate": {
        "text": "output",
        "error": "error",
        "is_finished_cond": {"path": "status", "value": "succeeded"},
        "finish_reason_static": "stop"
    },
    "predibase": {
        "text": "token.text",
        "finish_reason": ["details.finish_reason", "generated_text"]
    },
    "baseten": {
        "text": ["token.text", "model_output.data.0", "model_output", "completion"]
    },
    "ai21": {
        "text": "completions.0.data.text",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "maritalk": {
        "text": "answer",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "aleph_alpha": {
        "text": "completions.0.completion",
        "is_finished_static": True,
        "finish_reason_static": "stop"
    },
    "triton": {
        "text": "text_output",
        "finish_reason": "stop_reason",
        "is_finished_cond": {"path": "is_finished", "value": True},
        "usage": {
            "prompt_tokens": "input_token_count",
            "completion_tokens": "generated_token_count"
        }
    },
    # [InterLLMAdapter 전용 Fallback] - 미등록 프로바이더("inter" 등) 처리용
    "inter_llm_fallback": {
        "text": [
            "choices.0.delta.content",
            "content.parts.0.text",
            "candidates.0.content.parts.0.text",
            "delta.text",
            "choices.0.message.content",
            "message.content",
            "text",
            "raw.text",
            "delta"
        ],
        "finish_reason": [
            "finish_reason", 
            "choices.0.finish_reason", 
            "candidates.0.finish_reason",
            "delta.stop_reason"
        ],
        "usage": ["usage", "usage_metadata"],
        "logprobs": ["logprobs", "choices.0.logprobs"]
    },
    "defaults": {
        "role": "assistant",
        "finish_stop": "stop",
        "finish_tool": "tool_calls",
        "text": [
            "choices.0.delta.content",
            "choices.0.message.content",
            "message.content",
            "output"
        ],
        "usage": [
            "usage",
            "meta.usage"
        ]
    }
}

PROVIDER_RULE_ALIAS = {
    "azure": "openai",
    "azure_ai": "openai",
    "custom_openai": "openai",
    "sagemaker_chat": "openai",
    "nlp_cloud": "openai",
    "llama_server": "openai",
    "generic": "openai",
    "cohere": "cohere",
    # gemini, anthropic은 자체 키가 있으므로 직접 매칭되도록 두고, inter와 vertex_ai만 연결
    "inter": "inter_llm_fallback",
    "vertex_ai": "gemini"
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