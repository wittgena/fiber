# adapter.switch.params
## @lineage: bound.adapter.switch.params
## @lineage: anchor.bind.switch.params
"""
@desc: Acts as the primary switch to dynamically decouple Brane from external LiteLLM dependencies.
@flow: System Ignition -> brane Resolution (LITELLM_CONVERT_SWITCH) -> Unified Adapter Binding
@tag: strangler-fig, graceful-decoupling, boundary-switch, zero-dependency
"""
import os
from pydantic import BaseModel, ConfigDict
from typing import Any, Dict, Iterable, List, Optional, Union
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from watcher.plane.emitter import get_emitter

_RAW_SWITCH = os.getenv("LITELLM_CONVERT_SWITCH", "False").lower()
LITELLM_CONVERT_SWITCH = _RAW_SWITCH in ("true", "1", "yes", "t")
CRISIS_STRICT_MODE = os.getenv("BRANE_CRISIS_STRICT_MODE", "False").lower() in ("true", "1")
log = get_emitter("switch.params")

if LITELLM_CONVERT_SWITCH:
    log.info("Providing zero-migration entry point for external downstream runtime.")
    try:
        from litellm.types.llms.openai import ResponseAPIUsage, ResponsesAPIResponse
        from litellm.types.llms.openai import ResponsesAPIStreamingResponse
        from litellm.types.llms.openai import ToolParam
        from litellm.types.llms.openai import ChatCompletionToolParam
        from litellm.types.llms.openai import OutputFunctionToolCall
        from litellm.types.llms.openai import ChatCompletionToolParamFunctionChunk
        from litellm.types.llms.openai import ResponseInputParam
        from litellm.types.llms.openai import ResponsesAPIResponse
        from litellm.types.llms.openai import ResponsesAPIStreamEvents
        from litellm.types.llms.openai import OutputTextDeltaEvent, RefusalDeltaEvent, ReasoningSummaryTextDeltaEvent, ResponseCompletedEvent
        ## --- 
        from litellm.types.responses.main import GenericResponseOutputItem
        from litellm.types.rerank import RerankResponse
        from litellm.types.completion import (
            ChatCompletionMessageParam,
            ChatCompletionSystemMessageParam,
            ChatCompletionUserMessageParam,
            ChatCompletionAssistantMessageParam,
            ChatCompletionToolMessageParam,
            ChatCompletionFunctionMessageParam,
            ChatCompletionMessageToolCallParam,
            ChatCompletionContentPartParam,
            ChatCompletionMessageToolCall
        )
        from litellm.types.utils import (
            ChatCompletionDeltaToolCall,
            ChatCompletionRedactedThinkingBlock,
            CompletionTokensDetailsWrapper,
            EmbeddingResponse,
            Function,
            HiddenParams,
            ImageResponse,
            PromptTokensDetailsWrapper,
            TranscriptionUsageDurationObject,
            TranscriptionUsageTokensObject,
        )
        from litellm.types.utils import Usage
        ## ---
        from litellm.types.utils import TextChoices, TextCompletionResponse, TranscriptionResponse
        from litellm.types.utils import ChatCompletionMessageToolCall
        from litellm.types.utils import ModelResponse, ModelResponseStream, Delta, StreamingChoices, Choices, Message

        LITELLM_CONVERT_SWITCH = True
    except ImportError:
        LITELLM_CONVERT_SWITCH = False

if not LITELLM_CONVERT_SWITCH:
    try:
        from adapter.legacy.openai.types import ResponseAPIUsage, ResponsesAPIResponse
        from adapter.legacy.openai.types import ResponsesAPIStreamingResponse
        from adapter.legacy.openai.types import ToolParam
        from adapter.legacy.openai.types import ChatCompletionToolParam
        from adapter.legacy.openai.types import OutputFunctionToolCall
        from adapter.legacy.openai.types import ResponseInputParam
        from adapter.legacy.openai.types import ResponsesAPIResponse
        from adapter.legacy.openai.types import ChatCompletionToolParamFunctionChunk
        from adapter.legacy.openai.types import ResponsesAPIStreamEvents
        from adapter.legacy.openai.types import OutputTextDeltaEvent, RefusalDeltaEvent, ReasoningSummaryTextDeltaEvent, ResponseCompletedEvent
        ## ---
        from adapter.legacy.param.response import GenericResponseOutputItem
        from adapter.legacy.param.rerank import RerankResponse
        from adapter.legacy.param.completion import (
            ChatCompletionMessageParam,
            ChatCompletionSystemMessageParam,
            ChatCompletionUserMessageParam,
            ChatCompletionAssistantMessageParam,
            ChatCompletionToolMessageParam,
            ChatCompletionFunctionMessageParam,
            ChatCompletionMessageToolCallParam,
            ChatCompletionContentPartParam
        )
        from adapter.legacy.types import (
            ChatCompletionDeltaToolCall,
            ChatCompletionRedactedThinkingBlock,
            CompletionTokensDetailsWrapper,
            EmbeddingResponse,
            Function,
            HiddenParams,
            ImageResponse,
            PromptTokensDetailsWrapper,
            TranscriptionUsageDurationObject,
            TranscriptionUsageTokensObject,
        )
        from adapter.legacy.types import Usage
        from adapter.legacy.types import TextChoices, TextCompletionResponse, TranscriptionResponse
        from adapter.legacy.types import ModelResponse, ModelResponseStream, Delta, StreamingChoices, Choices, Message
        from adapter.legacy.types import ChatCompletionMessageToolCall
    except ImportError as e:
        raise ImportError(f"Failed to load fallback types from internal modules. Error: {e}")