# agent.anchor.llm.param
## @lineage: ator.client.model.param
## @lineage: eco.client.model.param
## @lineage: engine.client.param.model
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from fiber.agent.anchor.model.types.core import (
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
from fiber.agent.anchor.model.types.param.response import GenericResponseOutputItem, DeleteResponseResult, DecodedResponseId
from fiber.agent.anchor.model.types.stream import ModelResponseStream