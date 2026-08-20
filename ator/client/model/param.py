# ator.client.model.param
## @lineage: eco.client.model.param
## @lineage: engine.client.param.model
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from bound.xor.model.types.core import (
    ChatCompletionToolParam,
    ChatCompletionToolParamFunctionChunk,
    ChatCompletionUserMessageParam,
    EmbeddingResponse,
    Function,
    FunctionCall,
    Usage,
    ModelResponse,
    Delta,
    StreamingChoices,
    Choices,
    Message,
    ChatCompletionMessageToolCall,
    OutputFunctionToolCall
)
from bound.xor.model.types.param.response import GenericResponseOutputItem, DeleteResponseResult, DecodedResponseId
from bound.xor.model.types.stream import ModelResponseStream