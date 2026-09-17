# fiber.llm.param
## @lineage: llm.param
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from fiber.llm.types.provider.core import (
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
from fiber.llm.types.param.response import GenericResponseOutputItem, DeleteResponseResult, DecodedResponseId
from fiber.llm.types.provider.stream import ModelResponseStream