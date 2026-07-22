# bound.xor.opt.formatter
## @lineage: xor.opt.formatter
## @lineage: xphi.xor.opt.formatter
import ast
import enum
import inspect
import json
import types
from collections.abc import Mapping
from typing import Any, Literal, Union, get_args, get_origin
import json_repair
import pydantic
from pydantic import TypeAdapter
from pydantic.fields import FieldInfo

from bound.xor.opt.manifold.model.reasoning import Reasoning
from bound.xor.opt.manifold.model.basetype import Type as SpiType
from bound.xor.opt.manifold.code import Code

from arch.xor.sign.utils import get_spi_field_type

def _annotation_is_subclass(annotation: Any, expected_base: type) -> bool:
    try:
        return inspect.isclass(annotation) and issubclass(annotation, expected_base)
    except TypeError:
        return False


def serialize_for_json(value: Any) -> Any:
    try:
        return TypeAdapter(type(value)).dump_python(value, mode="json")
    except Exception:
        return str(value)


def format_field_value(field_info: FieldInfo, value: Any, assume_text=True) -> str | dict:
    string_value = None
    if isinstance(value, list) and field_info.annotation is str:
        string_value = _format_input_list_field_value(value)
    else:
        jsonable_value = serialize_for_json(value)
        if isinstance(jsonable_value, dict) or isinstance(jsonable_value, list):
            string_value = json.dumps(jsonable_value, ensure_ascii=False)
        else:
            string_value = str(jsonable_value)
    
    if assume_text:
        return string_value
    else:
        return {"type": "text", "text": string_value}


def _get_json_schema(field_type):
    def move_type_to_front(d):
        if isinstance(d, Mapping):
            return {
                k: move_type_to_front(v) for k, v in sorted(d.items(), key=lambda item: (item[0] != "type", item[0]))
            }
        elif isinstance(d, list):
            return [move_type_to_front(item) for item in d]
        return d

    schema = pydantic.TypeAdapter(field_type).json_schema()
    schema = move_type_to_front(schema)
    return schema


def translate_field_type(field_name, field_info):
    field_type = field_info.annotation

    if get_spi_field_type(field_info) == "input" or field_type is str or field_type is Reasoning:
        desc = ""
    elif field_type is bool:
        desc = "must be True or False"
    elif field_type in (int, float):
        desc = f"must be a single {field_type.__name__} value"
    elif _annotation_is_subclass(field_type, enum.Enum):
        enum_vals = "; ".join(str(member.value) for member in field_type)
        desc = f"must be one of: {enum_vals}"
    elif hasattr(field_type, "__origin__") and field_type.__origin__ is Literal:
        desc = (
            # Strongly encourage the LM to avoid choosing values that don't appear in the
            # literal or returning a value of the form 'Literal[<selected_value>]'
            f"must exactly match (no extra characters) one of: {'; '.join([str(x) for x in field_type.__args__])}"
        )
    elif _annotation_is_subclass(field_type, Code) and field_type.description():
        # Code has a rich type description already; avoid duplicating its large schema block.
        desc = ""
    else:
        desc = f"must adhere to the JSON schema: {json.dumps(_get_json_schema(field_type), ensure_ascii=False)}"

    desc = (" " * 8) + f"# note: the value you produce {desc}" if desc else ""
    return f"{{{field_name}}}{desc}"


def find_enum_member(enum, identifier):
    for member in enum:
        if member.value == identifier:
            return member

    if identifier in enum.__members__:
        return enum[identifier]

    raise ValueError(f"{identifier} is not a valid name or value for the enum {enum.__name__}")


def parse_value(value, annotation):
    if annotation is str:
        return str(value)

    if isinstance(annotation, enum.EnumMeta):
        return find_enum_member(annotation, value)

    origin = get_origin(annotation)

    if origin is Literal:
        allowed = get_args(annotation)
        if value in allowed:
            return value

        if isinstance(value, str):
            v = value.strip()
            if v.startswith(("Literal[", "str[")) and v.endswith("]"):
                v = v[v.find("[") + 1 : -1]
            if len(v) > 1 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]

            if v in allowed:
                return v

        raise ValueError(f"{value!r} is not one of {allowed!r}")

    if not isinstance(value, str):
        return TypeAdapter(annotation).validate_python(value)

    if origin in (Union, types.UnionType) and type(None) in get_args(annotation) and str in get_args(annotation):
        # Handle union annotations, e.g., `str | None`, `Optional[str]`, `Union[str, int, None]`, etc.
        return TypeAdapter(annotation).validate_python(value)

    candidate = json_repair.loads(value)  # json_repair.loads returns "" on failure.
    if candidate == "" and value != "":
        try:
            candidate = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            candidate = value

    try:
        return TypeAdapter(annotation).validate_python(candidate)
    except pydantic.ValidationError as e:
        if _annotation_is_subclass(annotation, SpiType):
            try:
                return TypeAdapter(annotation).validate_python(value)
            except Exception:
                raise e
        raise


def get_annotation_name(annotation):
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is None:
        if annotation is Reasoning:
            return "str"
        if hasattr(annotation, "__name__"):
            return annotation.__name__
        else:
            return str(annotation)

    if origin is Literal:
        args_str = ", ".join(
            _quoted_string_for_literal_type_annotation(a) if isinstance(a, str) else get_annotation_name(a)
            for a in args
        )
        return f"{get_annotation_name(origin)}[{args_str}]"
    else:
        args_str = ", ".join(get_annotation_name(a) for a in args)
        return f"{get_annotation_name(origin)}[{args_str}]"


def get_field_description_string(fields: dict) -> str:
    field_descriptions = []
    for idx, (k, v) in enumerate(fields.items()):
        field_message = f"{idx + 1}. `{k}`"
        field_message += f" ({get_annotation_name(v.annotation)})"
        desc = v.json_schema_extra["desc"] if v.json_schema_extra["desc"] != f"${{{k}}}" else ""

        custom_types = SpiType.extract_custom_type_from_annotation(v.annotation)
        for custom_type in custom_types:
            if len(custom_type.description()) > 0:
                desc += f"\n    Type description of {get_annotation_name(custom_type)}: {custom_type.description()}"

        field_message += f": {desc}"
        field_message += (
            f"\nConstraints: {v.json_schema_extra['constraints']}" if v.json_schema_extra.get("constraints") else ""
        )
        field_descriptions.append(field_message)
    return "\n".join(field_descriptions).strip()


def _format_input_list_field_value(value: list[Any]) -> str:
    if len(value) == 0:
        return "N/A"
    if len(value) == 1:
        return _format_blob(value[0])

    return "\n".join([f"[{idx + 1}] {_format_blob(txt)}" for idx, txt in enumerate(value)])


def _format_blob(blob: str) -> str:
    if "\n" not in blob and "«" not in blob and "»" not in blob:
        return f"«{blob}»"

    modified_blob = blob.replace("\n", "\n    ")
    return f"«««\n    {modified_blob}\n»»»"


def _quoted_string_for_literal_type_annotation(s: str) -> str:
    has_single = "'" in s
    has_double = '"' in s

    if has_single and not has_double:
        return f'"{s}"'
    elif has_double and not has_single:
        return f"'{s}'"
    elif has_single and has_double:
        escaped = s.replace("'", "\\'")
        return f"'{escaped}'"
    else:
        return f"'{s}'"
