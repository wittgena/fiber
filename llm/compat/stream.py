# fiber.llm.compat.stream
STREAM_EXTRACTION_RULES = {
    "openai": {
        "text": "choices.0.delta.content",
        "finish_reason": "choices.0.finish_reason",
        "logprobs": "choices.0.logprobs",
        "usage": "usage",
        "tool_calls": "choices.0.delta.tool_calls"
    },
    "ollama": {
        "text": ["message.content", "choices.0.delta.content"],
        "finish_reason": ["done_reason", "choices.0.finish_reason"],
        "is_finished_cond": {"path": "done", "value": True},
        "usage": [
            "usage",  # 1st: openai
            {         # 2nd: native custom
                "prompt_tokens": "prompt_eval_count",
                "completion_tokens": "eval_count"
            }
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
        "text": "choices.0.delta.content",
        "finish_reason": "choices.0.finish_reason",
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
    }
}

PROVIDER_RULE_ALIAS = {
    "azure": "openai",
    "azure_ai": "openai",
    "custom_openai": "openai",
    "sagemaker_chat": "openai",
    "nlp_cloud": "openai",
    "gemini": "vertex_ai",
    "llama_server": "openai",   # llama.cpp -> openai sse 
    "generic": "openai",        # default
}