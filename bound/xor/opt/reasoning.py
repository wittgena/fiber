# bound.xor.opt.reasoning
## @lineage: bound.xor.opt.manifold.model.reasoning
from typing import TYPE_CHECKING, Any, Optional
import pydantic
import json
import re
from typing import TYPE_CHECKING, Any, Optional, get_args, get_origin, ClassVar, get_type_hints, Callable
import json_repair
import asyncio
import inspect
from jsonschema import ValidationError, validate
from pydantic import BaseModel, TypeAdapter, create_model

from bound.gateway.switch.params import ModelResponseStream
from bound.xor.opt.context import runtime
from bound.xor.opt.callback.base import with_callbacks
from bound.xor.opt.lm import BaseLM

if TYPE_CHECKING:
    from arch.xor.sign.signature import Signature

CUSTOM_TYPE_START_IDENTIFIER = "<<CUSTOM-TYPE-START-IDENTIFIER>>"
CUSTOM_TYPE_END_IDENTIFIER = "<<CUSTOM-TYPE-END-IDENTIFIER>>"

class Type(pydantic.BaseModel):
    def format(self) -> list[dict[str, Any]] | str:
        raise NotImplementedError

    @classmethod
    def description(cls) -> str:
        return ""

    @classmethod
    def extract_custom_type_from_annotation(cls, annotation):
        try:
            if isinstance(annotation, type) and issubclass(annotation, cls):
                return [annotation]
        except TypeError:
            pass

        origin = get_origin(annotation)
        if origin is None:
            return []

        result = []
        # Recurse into all type args
        for arg in get_args(annotation):
            result.extend(cls.extract_custom_type_from_annotation(arg))

        return result

    @pydantic.model_serializer()
    def serialize_model(self):
        formatted = self.format()
        if isinstance(formatted, list):
            return (
                f"{CUSTOM_TYPE_START_IDENTIFIER}{json.dumps(formatted, ensure_ascii=False)}{CUSTOM_TYPE_END_IDENTIFIER}"
            )
        return formatted

    @classmethod
    def adapt_to_native_lm_feature(
        cls,
        signature: type["Signature"],
        field_name: str,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
    ) -> type["Signature"]:
        return signature

    @classmethod
    def is_streamable(cls) -> bool:
        """Whether the custom type is streamable."""
        return False

    @classmethod
    def parse_stream_chunk(cls, chunk: "ModelResponseStream") -> Optional["Type"]:
        return None

    @classmethod
    def parse_lm_response(cls, response: str | dict[str, Any]) -> Optional["Type"]:
        return None


def split_message_content_for_custom_types(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for message in messages:
        if message["role"] != "user":
            # Custom type messages are only in user messages
            continue

        pattern = rf"{CUSTOM_TYPE_START_IDENTIFIER}(.*?){CUSTOM_TYPE_END_IDENTIFIER}"
        result = []
        last_end = 0
        content: str = message["content"]

        for match in re.finditer(pattern, content, re.DOTALL):
            start, end = match.span()

            # Add text before the current block
            if start > last_end:
                result.append({"type": "text", "text": content[last_end:start]})

            # Parse the JSON inside the block
            custom_type_content = match.group(1).strip()
            parsed = None

            for parse_fn in [json.loads, _parse_doubly_quoted_json, json_repair.loads]:
                try:
                    parsed = parse_fn(custom_type_content)
                    break
                except json.JSONDecodeError:
                    continue

            if parsed:
                for custom_type_content in parsed:
                    result.append(custom_type_content)
            else:
                # fallback to raw string if it's not valid JSON
                result.append({"type": "text", "text": custom_type_content})

            last_end = end

        if last_end == 0:
            # No custom type found, return the original message
            continue

        # Add any remaining text after the last match
        if last_end < len(content):
            result.append({"type": "text", "text": content[last_end:]})

        message["content"] = result

    return messages


def _parse_doubly_quoted_json(json_str: str) -> Any:
    return json.loads(json.loads(f'"{json_str}"'))

class Reasoning(Type):
    content: str

    def format(self):
        return f"{self.content}"

    @pydantic.model_validator(mode="before")
    @classmethod
    def validate_input(cls, data: Any):
        if isinstance(data, cls):
            return data

        if isinstance(data, str):
            return {"content": data}

        if isinstance(data, dict):
            if "content" not in data:
                raise ValueError("`content` field is required for `Reasoning`")
            if not isinstance(data["content"], str):
                raise ValueError(f"`content` field must be a string, but received type: {type(data['content'])}")
            return {"content": data["content"]}

        raise ValueError(f"Received invalid value for `Reasoning`: {data}")

    @classmethod
    def adapt_to_native_lm_feature(
        cls,
        signature: type["Signature"],
        field_name: str,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
    ) -> type["Signature"]:
        if "reasoning_effort" in lm_kwargs:
            reasoning_effort = lm_kwargs["reasoning_effort"]
        elif "reasoning_effort" in lm.kwargs:
            reasoning_effort = lm.kwargs["reasoning_effort"]
        else:
            reasoning_effort = "low"

        if reasoning_effort is None or not lm.supports_reasoning:
            return signature

        if "gpt-5" in lm.model and lm.model_type == "chat":
            return signature

        lm_kwargs["reasoning_effort"] = reasoning_effort
        return signature.delete(field_name)

    @classmethod
    def parse_lm_response(cls, response: str | dict[str, Any]) -> Optional["Reasoning"]:
        """Parse the LM response into a Reasoning object."""
        if "reasoning_content" in response:
            return Reasoning(content=response["reasoning_content"])
        return None

    @classmethod
    def parse_stream_chunk(cls, chunk) -> str | None:
        """
        Parse a stream chunk into reasoning content if available.

        Args:
            chunk: A stream chunk from the LM.

        Returns:
            The reasoning content (str) if available, None otherwise.
        """
        try:
            if choices := getattr(chunk, "choices", None):
                return getattr(choices[0].delta, "reasoning_content", None)
        except Exception:
            return None

    @classmethod
    def is_streamable(cls) -> bool:
        return True

    def __repr__(self) -> str:
        return f"{self.content!r}"

    def __str__(self) -> str:
        return self.content

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Reasoning):
            return self.content == other.content
        if isinstance(other, str):
            return self.content == other
        return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __len__(self) -> int:
        return len(self.content)

    def __getitem__(self, key):
        return self.content[key]

    def __contains__(self, item) -> bool:
        return item in self.content

    def __iter__(self):
        return iter(self.content)

    def __add__(self, other):
        if isinstance(other, Reasoning):
            return Reasoning(content=self.content + other.content)
        if isinstance(other, str):
            return self.content + other
        return NotImplemented

    def __radd__(self, other):
        if isinstance(other, str):
            return other + self.content
        if isinstance(other, Reasoning):
            return Reasoning(content=other.content + self.content)
        return NotImplemented

    def __getattr__(self, name):
        if hasattr(str, name):
            return getattr(self.content, name)

        raise AttributeError(
            f"`{type(self).__name__}` object has no attribute '{name}'. "
            f"If you are using `ChainOfThought`, note that the 'reasoning' field in ChainOfThought is now a "
            "`Reasoning` object (not a plain string). "
            f"You can convert it to a string with str(reasoning) or access the content with reasoning.content."
        )

class Code(Type):
    code: str
    language: ClassVar[str] = "python"

    def format(self):
        return f"{self.code}"

    @pydantic.model_serializer()
    def serialize_model(self):
        """Override to bypass the <<CUSTOM-TYPE-START-IDENTIFIER>> and <<CUSTOM-TYPE-END-IDENTIFIER>> tags."""
        return self.format()

    @classmethod
    def description(cls) -> str:
        return (
            "Code represented in a string, specified in the `code` field. If this is an output field, the code "
            f"field should follow the markdown code block format, e.g. \n```{cls.language.lower()}\n{{code}}\n```"
            f"\nProgramming language: {cls.language}"
        )

    @pydantic.model_validator(mode="before")
    @classmethod
    def validate_input(cls, data: Any):
        if isinstance(data, cls):
            return data

        if isinstance(data, str):
            return {"code": _filter_code(data)}

        if isinstance(data, dict):
            if "code" not in data:
                raise ValueError("`code` field is required for `Code`")
            if not isinstance(data["code"], str):
                raise ValueError(f"`code` field must be a string, but received type: {type(data['code'])}")
            return {"code": _filter_code(data["code"])}

        raise ValueError(f"Received invalid value for `Code`: {data}")


def _filter_code(code: str) -> str:
    regex_pattern = r"```(?:[^\n]*)\n(.*?)```"
    match = re.search(regex_pattern, code, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Case 2: ```<code>``` (no language, single-line)
    regex_pattern_simple = r"```(.*?)```"
    match = re.search(regex_pattern_simple, code, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Fallback case
    return code

def _code_class_getitem(cls, language):
    code_with_language_cls = create_model(f"{cls.__name__}_{language}", __base__=cls)
    code_with_language_cls.language = language
    return code_with_language_cls

Code.__class_getitem__ = classmethod(_code_class_getitem)

_TYPE_MAPPING = {"string": str, "integer": int, "number": float, "boolean": bool, "array": list, "object": dict}

class Tool(Type):
    func: Callable
    name: str | None = None
    desc: str | None = None
    args: dict[str, Any] | None = None
    arg_types: dict[str, Any] | None = None
    arg_desc: dict[str, str] | None = None
    has_kwargs: bool = False

    def __init__(
        self,
        func: Callable,
        name: str | None = None,
        desc: str | None = None,
        args: dict[str, Any] | None = None,
        arg_types: dict[str, Any] | None = None,
        arg_desc: dict[str, str] | None = None,
    ):
        super().__init__(func=func, name=name, desc=desc, args=args, arg_types=arg_types, arg_desc=arg_desc)
        self._parse_function(func, arg_desc)

    def _parse_function(self, func: Callable, arg_desc: dict[str, str] | None = None):
        annotations_func = func if inspect.isfunction(func) or inspect.ismethod(func) else func.__call__
        name = getattr(func, "__name__", type(func).__name__)
        desc = getattr(func, "__doc__", None) or getattr(annotations_func, "__doc__", "")
        args = {}
        arg_types = {}

        # Use inspect.signature to get all arg names
        sig = inspect.signature(annotations_func)
        # Get available type hints
        available_hints = get_type_hints(annotations_func)
        # Build a dictionary of arg name -> type (defaulting to Any when missing)
        hints = {param_name: available_hints.get(param_name, Any) for param_name in sig.parameters.keys()}
        default_values = {param_name: sig.parameters[param_name].default for param_name in sig.parameters.keys()}

        # Process each argument's type to generate its JSON schema.
        for k, v in hints.items():
            arg_types[k] = v
            if k == "return":
                continue
            # Check if the type (or its origin) is a subclass of Pydantic's BaseModel
            origin = get_origin(v) or v
            if isinstance(origin, type) and issubclass(origin, BaseModel):
                # Get json schema, and replace $ref with the actual schema
                v_json_schema = _resolve_json_schema_reference(v.model_json_schema())
                args[k] = v_json_schema
            else:
                args[k] = _resolve_json_schema_reference(TypeAdapter(v).json_schema())
            if default_values[k] is not inspect.Parameter.empty:
                args[k]["default"] = default_values[k]
            if arg_desc and k in arg_desc:
                args[k]["description"] = arg_desc[k]

        self.name = self.name or name
        self.desc = self.desc or desc
        self.args = self.args if self.args is not None else args
        self.arg_types = self.arg_types if self.arg_types is not None else arg_types
        self.has_kwargs = any(param.kind == param.VAR_KEYWORD for param in sig.parameters.values())

    def _validate_and_parse_args(self, **kwargs):
        # Validate the args value comply to the json schema.
        for k, v in kwargs.items():
            if k not in self.args:
                if self.has_kwargs:
                    continue
                else:
                    raise ValueError(f"Arg {k} is not in the tool's args.")
            try:
                instance = v.model_dump() if hasattr(v, "model_dump") else v
                type_str = self.args[k].get("type")
                if type_str is not None and type_str != "Any":
                    validate(instance=instance, schema=self.args[k])
            except ValidationError as e:
                raise ValueError(f"Arg {k} is invalid: {e.message}")

        # Parse the args to the correct type.
        parsed_kwargs = {}
        for k, v in kwargs.items():
            if k in self.arg_types and self.arg_types[k] != Any:
                # Create a pydantic model wrapper with a dummy field `value` to parse the arg to the correct type.
                # This is specifically useful for handling nested Pydantic models like `list[list[MyPydanticModel]]`
                pydantic_wrapper = create_model("Wrapper", value=(self.arg_types[k], ...))
                parsed = pydantic_wrapper.model_validate({"value": v})
                parsed_kwargs[k] = parsed.value
            else:
                parsed_kwargs[k] = v
        return parsed_kwargs

    def format(self):
        return str(self)

    def format_as_litellm_function_call(self):
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.desc,
                "parameters": {
                    "type": "object",
                    "properties": self.args,
                    "required": list(self.args.keys()),
                },
            },
        }

    def _run_async_in_sync(self, coroutine):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Run the coroutine outside of "except" block to avoid propagation
            loop = None

        if loop is None:
            return asyncio.run(coroutine)
        return loop.run_until_complete(coroutine)

    @with_callbacks
    def __call__(self, **kwargs):
        parsed_kwargs = self._validate_and_parse_args(**kwargs)
        result = self.func(**parsed_kwargs)
        if asyncio.iscoroutine(result):
            if runtime.allow_tool_async_sync_conversion:
                return self._run_async_in_sync(result)
            else:
                raise ValueError(
                    "You are calling `__call__` on an async tool, please use `acall` instead or enable "
                    "async-to-sync conversion with `runtime.bind(allow_tool_async_sync_conversion=True)` "
                    "or `with runtime.bind(allow_tool_async_sync_conversion=True):`."
                )
        return result

    @with_callbacks
    async def acall(self, **kwargs):
        parsed_kwargs = self._validate_and_parse_args(**kwargs)
        result = self.func(**parsed_kwargs)
        if asyncio.iscoroutine(result):
            return await result
        else:
            # We should allow calling a sync tool in the async path.
            return result

    def __repr__(self):
        return f"Tool(name={self.name}, desc={self.desc}, args={self.args})"

    def __str__(self):
        desc = f", whose description is <desc>{self.desc}</desc>.".replace("\n", "  ") if self.desc else "."
        arg_desc = f"It takes arguments {self.args}."
        return f"{self.name}{desc} {arg_desc}"


class ToolCalls(Type):
    class ToolCall(Type):
        name: str
        args: dict[str, Any]

        def format(self):
            return {
                "type": "function",
                "function": {
                    "name": self.name,
                    "arguments": self.args,
                },
            }

        def execute(self, functions: dict[str, Any] | list[Tool] | None = None) -> Any:
            func = None
            if functions is None:
                # Automatic lookup in caller's globals and locals
                frame = inspect.currentframe().f_back
                try:
                    caller_globals = frame.f_globals
                    caller_locals = frame.f_locals
                    func = caller_locals.get(self.name) or caller_globals.get(self.name)
                finally:
                    del frame

            elif isinstance(functions, dict):
                func = functions.get(self.name)
            elif isinstance(functions, list):
                for tool in functions:
                    if tool.name == self.name:
                        func = tool.func
                        break

            if func is None:
                raise ValueError(f"Tool function '{self.name}' not found. Please pass the tool functions to the `execute` method.")

            try:
                args = self.args or {}
                return func(**args)
            except Exception as e:
                raise RuntimeError(f"Error executing tool '{self.name}': {e}") from e

    tool_calls: list[ToolCall]

    @classmethod
    def from_dict_list(cls, tool_calls_dicts: list[dict[str, Any]]) -> "ToolCalls":
        tool_calls = [cls.ToolCall(**item) for item in tool_calls_dicts]
        return cls(tool_calls=tool_calls)

    @classmethod
    def description(cls) -> str:
        return (
            "Tool calls information, including the name of the tools and the arguments to be passed to it. Arguments must be provided in JSON format."
        )

    def format(self) -> list[dict[str, Any]]:
        return {
            "tool_calls": [tool_call.format() for tool_call in self.tool_calls],
        }

    @pydantic.model_validator(mode="before")
    @classmethod
    def validate_input(cls, data: Any):
        if isinstance(data, cls):
            return data

        # Handle case where data is a list of dicts with "name" and "args" keys
        if isinstance(data, list) and all(
            isinstance(item, dict) and "name" in item and "args" in item for item in data
        ):
            return {"tool_calls": [cls.ToolCall(**item) for item in data]}
        # Handle case where data is a dict
        elif isinstance(data, dict):
            if "tool_calls" in data:
                # Handle case where data is a dict with "tool_calls" key
                tool_calls_data = data["tool_calls"]
                if isinstance(tool_calls_data, list):
                    return {
                        "tool_calls": [
                            cls.ToolCall(**item) if isinstance(item, dict) else item for item in tool_calls_data
                        ]
                    }
            elif "name" in data and "args" in data:
                # Handle case where data is a dict with "name" and "args" keys
                return {"tool_calls": [cls.ToolCall(**data)]}

        raise ValueError(f"Received invalid value for `ToolCalls`: {data}")


def _resolve_json_schema_reference(schema: dict) -> dict:
    """Recursively resolve json model schema, expanding all references."""

    # If there are no definitions to resolve, return the main schema
    if "$defs" not in schema and "definitions" not in schema:
        return schema

    def resolve_refs(obj: Any) -> Any:
        if not isinstance(obj, (dict, list)):
            return obj
        if isinstance(obj, dict):
            if "$ref" in obj:
                ref_path = obj["$ref"].split("/")[-1]
                return resolve_refs(schema["$defs"][ref_path])
            return {k: resolve_refs(v) for k, v in obj.items()}

        # Must be a list
        return [resolve_refs(item) for item in obj]

    # Resolve all references in the main schema
    resolved_schema = resolve_refs(schema)
    # Remove the $defs key as it's no longer needed
    resolved_schema.pop("$defs", None)
    return resolved_schema


def convert_input_schema_to_tool_args(
    schema: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Type], dict[str, str]]:
    args, arg_types, arg_desc = {}, {}, {}
    properties = schema.get("properties", None)
    if properties is None:
        return args, arg_types, arg_desc

    required = schema.get("required", [])

    defs = schema.get("$defs", {})

    for name, prop in properties.items():
        if len(defs) > 0:
            prop = _resolve_json_schema_reference({"$defs": defs, **prop})
        args[name] = prop
        arg_types[name] = _TYPE_MAPPING.get(prop.get("type"), Any)
        arg_desc[name] = prop.get("description", "No description provided.")
        if name in required:
            arg_desc[name] += " (Required)"

    return args, arg_types, arg_desc
