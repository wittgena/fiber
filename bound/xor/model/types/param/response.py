# bound.xor.model.types.param.response
## @lineage: eco.model.types.param.response
## @lineage: engine.model.types.param.response
## @lineage: bound.model.types.param.response
## @lineage: llm.types.param.response
## @lineage: eco.mesh.model.types.param.response
## @lineage: runtime.mesh.model.types.param.response
## @lineage: mesh.model.types.param.response
## @lineage: mesh.mapper.param.response
## @lineage: bound.mapper.param.response
from typing import List, Literal, Optional, Union
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from pydantic import PrivateAttr
from typing_extensions import Any, List, Optional, TypedDict
from arch.xor.surge.model import DynamicSurgeModel

Phase = Optional[Literal["commentary", "final_answer"]]

class GenericResponseOutputItemContentAnnotation(DynamicSurgeModel):
    """Annotation for content in a message"""
    type: Optional[str]
    start_index: Optional[int]
    end_index: Optional[int]
    url: Optional[str]
    title: Optional[str]
    pass

class OutputText(DynamicSurgeModel):
    """Text output content from an assistant message"""
    type: Optional[str]  # "output_text"
    text: Optional[str]
    annotations: Optional[List[GenericResponseOutputItemContentAnnotation]]

class OutputFunctionToolCall(DynamicSurgeModel):
    """A tool call to run a function"""
    arguments: Optional[str]
    call_id: Optional[str]
    name: Optional[str]
    type: Optional[str]  # "function_call"
    id: Optional[str]
    status: Literal["in_progress", "completed", "incomplete"]
    phase: Phase = None


class OutputImageGenerationCall(DynamicSurgeModel):
    """An image generation call output"""

    type: Literal["image_generation_call"]
    id: str
    status: Literal["in_progress", "completed", "incomplete", "failed"]
    result: Optional[str]  # Base64 encoded image data (without data:image prefix)


class OutputCodeInterpreterCallLog(DynamicSurgeModel):
    """Log output from a code interpreter call"""

    type: Literal["logs"]
    logs: str


class OutputCodeInterpreterCall(DynamicSurgeModel):
    """A code interpreter / code execution call output"""
    type: Literal["code_interpreter_call"]
    id: str
    code: Optional[str]
    container_id: Optional[str]
    status: Literal["in_progress", "completed", "incomplete", "failed"]
    outputs: Optional[List[OutputCodeInterpreterCallLog]]


def build_code_interpreter_log_outputs(
    content: Any,
) -> Optional[List[OutputCodeInterpreterCallLog]]:
    if not isinstance(content, dict):
        return None
    parts = []
    if content.get("stdout"):
        parts.append(content["stdout"])
    if content.get("stderr"):
        parts.append(f"STDERR: {content['stderr']}")
    logs = "".join(parts)
    return [OutputCodeInterpreterCallLog(type="logs", logs=logs)] if logs else None

class GenericResponseOutputItem(DynamicSurgeModel):
    type: str  # "message"
    id: str
    status: str  # "completed", "in_progress", etc.
    role: str  # "assistant", "user", etc.
    content: List[OutputText]
    phase: Phase = None

class DeleteResponseResult(DynamicSurgeModel):
    id: Optional[str]
    object: Optional[str]
    deleted: Optional[bool]

    # Define private attributes using PrivateAttr
    _hidden_params: dict = PrivateAttr(default_factory=dict)


class DecodedResponseId(TypedDict, total=False):
    """Structure representing a decoded response ID"""

    custom_llm_provider: Optional[str]
    model_id: Optional[str]
    response_id: str
