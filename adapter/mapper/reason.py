# adapter.mapper.reason
## @lineage: bound.adapter.mapper.reason
## @lineage: bound.bridge.channel.mapper.reason
## @lineage: bound.channel.bridge.mapper.reason
## @lineage: bound.channel.bridge.map.reason
from adapter.legacy.openai.types import OpenAIChatCompletionFinishReason
from watcher.plane.emitter import get_emitter

log = get_emitter("map.reason")

FINISH_REASON_MAP: dict[str, OpenAIChatCompletionFinishReason] = {
    ## Anthropic
    "stop_sequence": "stop",
    "end_turn": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
    "compaction": "length",
    ## Cohere
    "COMPLETE": "stop",
    "ERROR_TOXIC": "content_filter",
    "ERROR": "stop",
    ## HuggingFace / Together AI
    "eos_token": "stop",
    "eos": "stop",
    ## Gemini / Vertex AI
    "STOP": "stop",
    "MAX_TOKENS": "length",
    "SAFETY": "content_filter",
    "RECITATION": "content_filter",
    "FINISH_REASON_UNSPECIFIED": "stop",
    "MALFORMED_FUNCTION_CALL": "stop",
    "LANGUAGE": "content_filter",
    "OTHER": "content_filter",
    "BLOCKLIST": "content_filter",
    "PROHIBITED_CONTENT": "content_filter",
    "SPII": "content_filter",
    "IMAGE_SAFETY": "content_filter",
    "IMAGE_PROHIBITED_CONTENT": "content_filter",
    "TOO_MANY_TOOL_CALLS": "stop",
    "MALFORMED_RESPONSE": "stop",
    ## Zhipu GLM
    "network_error": "stop",
    "sensitive": "content_filter",
    ## Bedrock
    "guardrail_intervened": "content_filter",
    ## OpenAI passthrough
    "stop": "stop",
    "length": "length",
    "tool_calls": "tool_calls",
    "function_call": "function_call",
    "content_filter": "content_filter",
    ## Anthropic Sonnet 4
    "content_filtered": "content_filter",
}

def map_finish_reason(finish_reason: str) -> OpenAIChatCompletionFinishReason:
    mapped = FINISH_REASON_MAP.get(finish_reason)
    if mapped is None:
        log.warning("Unmapped finish_reason '%s', defaulting to 'stop'", finish_reason)
        return "stop"
    return mapped