# llm.param
## @lineage: agent.llm.param
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from fiber.llm.model.types.core import (
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
from fiber.llm.model.types.param.response import GenericResponseOutputItem, DeleteResponseResult, DecodedResponseId
from fiber.llm.model.types.stream import ModelResponseStream