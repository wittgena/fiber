# fiber.llm.compat.param
FRAMEWORK_KWARGS = {
    "metadata", "session_id", "trace_id", "call_id", "completion_call_id", "preset_cache_key", "model_info", 
    "model_alias_map", "proxy_server_request", "input_cost_per_token", "output_cost_per_token", 
    "input_cost_per_second", "output_cost_per_second", "cost_per_query", "prompt_id", "prompt_variables",
    "timeout", "request_timeout", "client", "shared_session", "acompletion", "aembedding", "headers", 
    "extra_headers", "custom_llm_provider", "api_key", "api_base", "base_url", "deployment_id", "azure", 
    "aws_region_name", "supports_system_message", "litellm_system_prompt", "base_model",
    "drop_params", "allowed_openai_params", "additional_drop_params", "context_management"
}
AUTH_PREFIXES = ("aws_", "azure_", "vertex_", "tenant_id", "client_id", "client_secret", "bucket_name")
FLAG_KEYS = {
    "no_log", "no-log", "custom_prompt_dict", "async_call", "ssl_verify", "merge_reasoning_content_in_choices", 
    "use_litellm_proxy", "logger_fn", "verbose", "disable_add_transform_inline_image_block", "log_delegator"
}

PROVIDER_ALIAS = {
    "vertex_ai_beta": "vertex_ai", "text-completion-openai": "openai", "azure_ai": "azure", "ollama_chat": "ollama",
    "claude": "anthropic", "anthropic_chat": "anthropic"
}
OPENAI_REGIONAL_HOSTS = {"eu.api.openai.com": "eu", "us.api.openai.com": "us"}

PROVIDER_PARAM_RULES = {
    "defaults": {
        "supported": ["temperature", "top_p", "n", "stream", "stop", "max_tokens", "presence_penalty", "frequency_penalty", "user", "tools", "tool_choice", "logprobs", "top_logprobs", "response_format", "seed"],
        "mapping": {},
        "wrap_in": {},
        "tool_format": "standard",
        "role_mapping": {"developer": "system"}, 
        "system_param": None,
        "message_schema": "openai_array"  # 기본값: [{"role": "...", "content": "..."}]
    },
    "openai": {
        "supported": ["temperature", "top_p", "n", "stream", "stop", "max_tokens", "max_completion_tokens", "presence_penalty", "frequency_penalty", "user", "tools", "tool_choice", "logprobs", "top_logprobs", "response_format", "seed"],
        "mapping": {},
        "wrap_in": {},
        "tool_format": "standard",
        "role_mapping": {}, 
        "system_param": None
    },
    "azure": {
        "supported": ["temperature", "top_p", "n", "stream", "stop", "max_tokens", "max_completion_tokens", "presence_penalty", "frequency_penalty", "user", "tools", "tool_choice", "logprobs", "top_logprobs", "response_format", "seed"],
        "mapping": {},
        "wrap_in": {},
        "tool_format": "standard",
        "role_mapping": {},
        "system_param": None
    },
    "gemini": {
        "supported": ["temperature", "top_p", "top_k", "max_tokens", "max_completion_tokens", "stream", "tools", "tool_choice", "response_format", "n", "stop", "presence_penalty", "frequency_penalty"],
        "mapping": {"max_tokens": "max_output_tokens", "max_completion_tokens": "max_output_tokens", "stop": "stop_sequences"},
        "wrap_in": {},
        "tool_format": "gemini_strict",
        "role_mapping": {"developer": "system"},
        "system_param": None,
        "supports_tools": True 
    },
    "anthropic": {
        "supported": ["temperature", "top_p", "top_k", "stop", "stream", "max_tokens", "max_completion_tokens", "tools", "tool_choice", "thinking", "metadata", "user"],
        "mapping": {"max_completion_tokens": "max_tokens", "stop": "stop_sequences"},
        "wrap_in": {},
        "tool_format": "anthropic",
        "role_mapping": {"developer": "system"},
        "system_param": None 
    },
    "ollama": {
        "supported": ["temperature", "top_p", "top_k", "stream", "tools", "format", "options", "num_ctx", "seed"],
        "mapping": {},
        "wrap_in": {"options": ["temperature", "top_p", "top_k", "num_ctx", "seed"]},
        "tool_format": "standard",
        "role_mapping": {"developer": "system"},
        "system_param": None
    },
    "cohere": {
        "supported": ["temperature", "p", "k", "max_tokens", "stream", "stop_sequences", "seed", "tools"],
        "mapping": {"top_p": "p", "top_k": "k", "stop": "stop_sequences"},
        "wrap_in": {},
        "tool_format": "standard",
        "role_mapping": {},
        "system_param": None,
        "message_schema": "string_last"
    }

}

OPENAI_EMBEDDING_PARAMS = ["dimensions", "encoding_format", "user", "extra_headers", "extra_body"]
DEFAULT_EMBEDDING_PARAM_VALUES = {
    **{k: None for k in OPENAI_EMBEDDING_PARAMS},
    "model": None,
    "custom_llm_provider": "",
    "input": None,
}

DEFAULT_CHAT_COMPLETION_PARAM_VALUES = {
    "functions": None,
    "function_call": None,
    "temperature": None,
    "top_p": None,
    "top_k": None,
    "n": None,
    "stream": None,
    "stream_options": None,
    "stop": None,
    "max_tokens": None,
    "max_completion_tokens": None,
    "modalities": None,
    "prediction": None,
    "audio": None,
    "presence_penalty": None,
    "frequency_penalty": None,
    "logit_bias": None,
    "user": None,
    "model": None,
    "custom_llm_provider": "",
    "response_format": None,
    "seed": None,
    "tools": None,
    "tool_choice": None,
    "max_retries": None,
    "logprobs": None,
    "top_logprobs": None,
    "extra_headers": None,
    "extra_body": None,
    "api_version": None,
    "parallel_tool_calls": None,
    "messages": None,
    "reasoning_effort": None,
    "verbosity": None,
    "thinking": None,
    "web_search_options": None,
    "include_server_side_tool_invocations": None,
    "service_tier": None,
    "safety_identifier": None,
    "prompt_cache_key": None,
    "prompt_cache_retention": None,
    "store": None,
}
