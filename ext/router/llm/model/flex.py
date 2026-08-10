# router.llm.model.flex
## @lineage: engine.router.llm.model.flex
## @lineage: engine.eco.llm.model.flex
## @lineage: runtime.engine.eco.llm.model.flex
import contextlib
import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, List, Optional, Sequence, Type, TypeVar, Union

with contextlib.suppress(ImportError):
    import yaml

from ext.router.llm.model.funcall import FunctionCallingLLM
from ext.router.llm.model.llm import LLM, ToolSelection
from ext.router.llm.handle.template import BasePromptTemplate
from ext.router.llm.model.types.block import ChatResponse, CompletionResponse
from ext.router.adapter.pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    create_model,
)
from ext.router.adapter.types.base import BasePydanticProgram, Model, PydanticProgramMode

_logger = logging.getLogger(__name__)


# ==========================================
# 1. Output Parser Base & Exceptions
# ==========================================

TModel = TypeVar("TModel", bound=BaseModel)

class OutputParserException(Exception):
    """Exception raised when parsing output from LLM fails."""
    pass

class BaseOutputParser(ABC, Generic[TModel]):
    """Base class for all output parsers."""
    
    @abstractmethod
    def parse(self, text: str) -> Any:
        pass
    
    @abstractmethod
    def format(self, query: str) -> str:
        pass

@dataclass
class StructuredOutput:
    raw_output: str
    parsed_output: Optional[Any] = None


# ==========================================
# 2. JSON & Markdown Extraction
# ==========================================

def _marshal_llm_to_json(output: str) -> str:
    """Extract a substring containing valid JSON or array from a string."""
    output = output.strip()

    left_square = output.find("[")
    left_brace = output.find("{")
    if (left_square < left_brace and left_square != -1) or left_brace == -1:
        left = left_square
        right = output.rfind("]")
    else:
        left = left_brace
        right = output.rfind("}")

    if left == -1 or right == -1:
        raise OutputParserException("Could not find any JSON object or array in the output.")

    return output[left : right + 1]


def parse_json_markdown(text: str) -> Any:
    """Parse JSON string from markdown formatted text with YAML fallback."""
    if "```json" in text:
        text = text.split("```json")[1].strip().strip("```").strip()

    json_string = _marshal_llm_to_json(text)

    try:
        json_obj = json.loads(json_string)
    except json.JSONDecodeError as e_json:
        try:
            # PYYAML은 후행 쉼표(Trailing commas) 등을 허용하므로 폴백으로 사용
            json_obj = yaml.safe_load(json_string)
        except yaml.YAMLError as e_yaml:
            raise OutputParserException(
                f"Got invalid JSON object. Error: {e_json} {e_yaml}. "
                f"Got JSON string: {json_string}"
            )
        except NameError as exc:
            raise ImportError("Please pip install PyYAML to handle lenient JSON parsing.") from exc

    return json_obj


# ==========================================
# 3. Pydantic Output Parser
# ==========================================

PYDANTIC_FORMAT_TMPL = """
Here's a JSON schema to follow:
{schema}

Output a valid JSON object but do not repeat the schema.
"""

class PydanticOutputParser(BaseOutputParser[TModel]):
    def __init__(
        self,
        output_cls: Type[TModel],
        excluded_schema_keys_from_format: Optional[List[str]] = None,
        pydantic_format_tmpl: str = PYDANTIC_FORMAT_TMPL,
    ) -> None:
        self._output_cls = output_cls
        self._excluded_schema_keys_from_format = excluded_schema_keys_from_format or []
        self._pydantic_format_tmpl = pydantic_format_tmpl

    @property
    def output_cls(self) -> Type[TModel]:
        return self._output_cls

    @property
    def format_string(self) -> str:
        """Format string."""
        return self.get_format_string(escape_json=True)

    def get_format_string(self, escape_json: bool = True) -> str:
        """Format string including the JSON schema."""
        schema_dict = self._output_cls.model_json_schema().copy()
        
        for key in self._excluded_schema_keys_from_format:
            schema_dict.pop(key, None)

        schema_str = json.dumps(schema_dict, ensure_ascii=False)
        output_str = self._pydantic_format_tmpl.format(schema=schema_str)
        
        if escape_json:
            return output_str.replace("{", "{{").replace("}", "}}")
        return output_str

    def parse(self, text: str) -> Any:
        parsed_dict = parse_json_markdown(text)
        return self._output_cls.model_validate(parsed_dict)

    def format(self, query: str) -> str:
        return query + "\n\n" + self.get_format_string(escape_json=True)


# ==========================================
# 4. Flexible Models & Incremental Parsing
# ==========================================

class FlexibleModel(BaseModel):
    model_config = ConfigDict(extra="allow")

def create_flexible_model(model: Type[BaseModel]) -> Type[FlexibleModel]:
    return create_model(
        f"Flexible{model.__name__}",
        __base__=FlexibleModel,
        **dict.fromkeys(model.model_fields, (Optional[Any], None)),
    )  # type: ignore


def create_list_model(base_cls: Type[BaseModel]) -> Type[BaseModel]:
    name = f"{base_cls.__name__}List"
    list_items = (
        List[base_cls],  # type: ignore
        Field(
            default_factory=list,  # type: ignore
            repr=False,
            description=f"List of {base_cls.__name__} items",
        ),
    )

    new_cls = create_model(name, items=list_items)
    new_cls.__doc__ = f"A list of {base_cls.__name__} objects. "

    return new_cls

def _repair_incomplete_json(json_str: str) -> str:
    if not json_str.strip():
        return "{}"

    quote_count = json_str.count('"')
    if quote_count % 2 == 1:
        json_str += '"'

    brace_count = json_str.count("{") - json_str.count("}")
    if brace_count > 0:
        json_str += "}" * brace_count

    return json_str


def _parse_partial_list_items(
    list_content: str, field_name: str, output_cls: Type[Model]
) -> list:
    try:
        items = []
        object_pattern = r"\{[^{}]*\}"
        object_matches = re.findall(object_pattern, list_content)

        for obj_str in object_matches:
            try:
                obj_data = json.loads(obj_str)
                items.append(obj_data)
            except (json.JSONDecodeError, SyntaxError):
                try:
                    repaired = _repair_incomplete_json(obj_str)
                    obj_data = json.loads(repaired)
                    items.append(obj_data)
                except (json.JSONDecodeError, SyntaxError):
                    continue

        return items
    except Exception:
        return []


def _extract_partial_list_progress(
    content: str,
    output_cls: Type[Model],
    cur_object: Optional[Union[Model, FlexibleModel]],
    partial_output_cls: Type[FlexibleModel],
) -> Optional[FlexibleModel]:
    if not isinstance(content, str) or cur_object is None:
        return None

    try:
        list_pattern = r'"(\w+)":\s*\[([^\]]*)'
        matches = re.findall(list_pattern, content)

        if not matches:
            return None

        current_data = (
            cur_object.model_dump() if hasattr(cur_object, "model_dump") else {}
        )

        for field_name, list_content in matches:
            if (
                hasattr(output_cls, "model_fields")
                and field_name in output_cls.model_fields
            ):
                items = _parse_partial_list_items(list_content, field_name, output_cls)
                if items:
                    current_data[field_name] = items

        return partial_output_cls.model_validate(current_data)
    except Exception:
        return None


def num_valid_fields(
    obj: Union[BaseModel, Sequence[BaseModel], Dict[str, BaseModel]],
) -> int:
    if isinstance(obj, BaseModel):
        count = 0
        for value in obj.__dict__.values():
            if isinstance(value, (list, tuple)):
                count += sum(num_valid_fields(item) for item in value)
            elif isinstance(value, dict):
                count += sum(num_valid_fields(item) for item in value.values())
            elif isinstance(value, BaseModel):
                count += num_valid_fields(value)
            elif value is not None:
                count += 1
        return count
    elif isinstance(obj, (list, tuple)):
        return sum(num_valid_fields(item) for item in obj)
    elif isinstance(obj, dict):
        return sum(num_valid_fields(item) for item in obj.values())
    else:
        return 1 if obj is not None else 0


# ==========================================
# 5. Streaming Handlers
# ==========================================

def process_streaming_content_incremental(
    chat_response: ChatResponse,
    output_cls: Type[Model],
    cur_object: Optional[Union[Model, FlexibleModel]] = None,
) -> Union[Model, FlexibleModel]:
    """Process streaming response incrementally for basic chat response content."""
    partial_output_cls = create_flexible_model(output_cls)
    content = chat_response.message.content
    
    if not content:
        return cur_object if cur_object is not None else partial_output_cls()
        
    try:
        parsed_obj = partial_output_cls.model_validate_json(content)
    except (ValidationError, ValueError):
        try:
            repaired_json = _repair_incomplete_json(content)
            parsed_obj = partial_output_cls.model_validate_json(repaired_json)
        except (ValidationError, ValueError):
            extracted_obj = _extract_partial_list_progress(
                content, output_cls, cur_object, partial_output_cls
            )
            parsed_obj = (
                extracted_obj if extracted_obj is not None else partial_output_cls()
            )

    if parsed_obj is None:
        if cur_object is not None:
            return cur_object
        else:
            return partial_output_cls()

    try:
        return output_cls.model_validate(parsed_obj.model_dump(exclude_unset=True))
    except ValidationError:
        return parsed_obj


def process_streaming_objects(
    chat_response: ChatResponse | CompletionResponse,
    output_cls: Type[Model],
    cur_objects: Optional[Sequence[Model]] = None,
    allow_parallel_tool_calls: bool = False,
    flexible_mode: bool = True,
    llm: Optional[FunctionCallingLLM | LLM] = None,
) -> Union[Model, List[Model], FlexibleModel, List[FlexibleModel]]:
    if flexible_mode:
        partial_output_cls = create_flexible_model(output_cls)
    else:
        partial_output_cls = output_cls  # type: ignore

    if isinstance(chat_response, CompletionResponse):
        output_cls_args = [chat_response.text]
    elif not chat_response.message.additional_kwargs.get("tool_calls"):
        output_cls_args = [chat_response.message.content or ""]
    else:
        tool_calls: List[ToolSelection] = []
        if not llm:
            raise ValueError("LLM is required to get tool calls")

        if isinstance(chat_response.message.additional_kwargs.get("tool_calls"), list):
            assert isinstance(llm, FunctionCallingLLM)
            tool_calls = llm.get_tool_calls_from_response(
                chat_response, error_on_no_tool_call=False
            )

        if len(tool_calls) == 0:
            return partial_output_cls()

        output_cls_args = [call.tool_kwargs for call in tool_calls]  # type: ignore

    objects = []
    for output_cls_arg in output_cls_args:
        try:
            obj = partial_output_cls.model_validate(output_cls_arg)
            objects.append(obj)
        except (ValidationError, ValueError):
            try:
                if isinstance(output_cls_arg, str):
                    repaired_json = _repair_incomplete_json(output_cls_arg)
                    obj = partial_output_cls.model_validate_json(repaired_json)
                    objects.append(obj)
                else:
                    raise
            except (ValidationError, ValueError) as e2:
                _logger.debug(f"Validation error during streaming: {e2}")
                if cur_objects:
                    objects = cur_objects  # type: ignore
                else:
                    return partial_output_cls()

    if cur_objects is None or num_valid_fields(objects) >= num_valid_fields(
        cur_objects
    ):
        cur_objects = objects  # type: ignore

    new_cur_objects = []
    cur_objects = cur_objects or []
    for o in cur_objects:
        try:
            new_obj = output_cls.model_validate(o.model_dump(exclude_unset=True))
        except ValidationError:
            new_obj = o
        new_cur_objects.append(new_obj)

    if allow_parallel_tool_calls:
        return new_cur_objects
    else:
        if len(new_cur_objects) > 1:
            _logger.warning(
                "Multiple outputs found, returning first one. "
                "If you want to return all outputs, set allow_parallel_tool_calls=True."
            )
        return new_cur_objects[0]


# ==========================================
# 6. Program Getter
# ==========================================

def get_program_for_llm(
    output_cls: Type[Model],
    prompt: BasePromptTemplate,
    llm: LLM,
    pydantic_program_mode: PydanticProgramMode = PydanticProgramMode.DEFAULT,
    **kwargs: Any,
) -> BasePydanticProgram[Model]:
    if pydantic_program_mode == PydanticProgramMode.DEFAULT:
        if llm.metadata.is_function_calling_model:
            from ext.router.parser.prog.function_program import FunctionCallingProgram

            return FunctionCallingProgram.from_defaults(
                output_cls=output_cls,
                llm=llm,
                prompt=prompt,
                **kwargs,
            )
        else:
            from ext.router.parser.prog.llm_program import LLMTextCompletionProgram

            return LLMTextCompletionProgram.from_defaults(
                output_parser=PydanticOutputParser(output_cls=output_cls),
                llm=llm,
                prompt=prompt,
                **kwargs,
            )
            
    elif pydantic_program_mode == PydanticProgramMode.OPENAI:
        from llama_index.program.openai import OpenAIPydanticProgram

        return OpenAIPydanticProgram.from_defaults(
            output_cls=output_cls,
            llm=llm,
            prompt=prompt,  # type: ignore
            **kwargs,
        )
        
    elif pydantic_program_mode == PydanticProgramMode.FUNCTION:
        from ext.router.parser.prog.function_program import FunctionCallingProgram

        return FunctionCallingProgram.from_defaults(
            output_cls=output_cls,
            llm=llm,
            prompt=prompt,
            **kwargs,
        )

    elif pydantic_program_mode == PydanticProgramMode.LLM:
        from ext.router.parser.prog.llm_program import LLMTextCompletionProgram

        return LLMTextCompletionProgram.from_defaults(
            output_parser=PydanticOutputParser(output_cls=output_cls),
            llm=llm,
            prompt=prompt,
            **kwargs,
        )
        
    elif pydantic_program_mode == PydanticProgramMode.LM_FORMAT_ENFORCER:
        try:
            from llama_index.program.lmformatenforcer import LMFormatEnforcerPydanticProgram
        except ImportError:
            raise ImportError(
                "This mode requires the `llama-index-program-lmformatenforcer package. Please"
                " install it by running `pip install llama-index-program-lmformatenforcer`."
            )

        return LMFormatEnforcerPydanticProgram.from_defaults(
            output_cls=output_cls,
            llm=llm,
            prompt=prompt,
            **kwargs,
        )
    else:
        raise ValueError(f"Unsupported pydantic program mode: {pydantic_program_mode}")