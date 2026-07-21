# bound.adapter.call.disc.action
from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar
from pydantic import ConfigDict, Field, create_model
from rich.text import Text

from bound.adapter.call.action.message import ImageContent, TextContent
from bound.adapter.call.action.message import content_to_str

from arch.topos.surge.disc import DiscMixin
from watcher.plane.emitter import get_logger

if TYPE_CHECKING:
    from typing import Self

logger = get_logger(__name__)
S = TypeVar("S", bound="Schema")

def display_dict(data) -> Text:
    content = Text()
    if isinstance(data, dict):
        for field_name, field_value in data.items():
            if field_value is None:
                continue
            content.append(f"\n  {field_name}: ", style="bold")
            if isinstance(field_value, str):
                if "\n" in field_value:
                    content.append("\n")
                    for line in field_value.split("\n"):
                        content.append(f"    {line}\n")
                else:
                    content.append(f'"{field_value}"')
            elif isinstance(field_value, (list, dict)):
                content.append(str(field_value))
            else:
                content.append(str(field_value))
    elif isinstance(data, list):
        content.append(f"[List with {len(data)} items]\n")
        for i, item in enumerate(data):
            content.append(f"  [{i}]: ", style="bold")
            if isinstance(item, str):
                content.append(f'"{item}"\n')
            else:
                content.append(f"{item}\n")
    elif isinstance(data, str):
        if "\n" in data:
            content.append("String:\n")
            for line in data.split("\n"):
                content.append(f"  {line}\n")
        else:
            content.append(f'"{data}"')
    elif data is None:
        content.append("null")
    else:
        content.append(str(data))
    return content

def py_type(spec: dict[str, Any]) -> Any:
    """Map JSON schema types to Python types."""
    t = spec.get("type")
    if isinstance(t, (list, tuple, set)):
        types = list(t)
        non_null = [tp for tp in types if tp != "null"]
        if len(non_null) == 1:
            t = non_null[0]
        else:
            return Any
    if t == "array":
        items = spec.get("items", {})
        inner = py_type(items) if isinstance(items, dict) else Any
        return list[inner]  # type: ignore[index]
    if t == "object":
        return dict[str, Any]
    _map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
    }
    if t in _map:
        return _map[t]
    return Any


def _shallow_expand_circular_ref(ref_def: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"type": "object"}
    if "description" in ref_def:
        result["description"] = ref_def["description"]
    return result


def _process_schema_node(
    node: dict[str, Any],
    defs: dict[str, Any],
    _visiting: frozenset[str] | None = None,
) -> dict[str, Any]:
    if _visiting is None:
        _visiting = frozenset()

    # Handle $ref references
    if "$ref" in node:
        ref_path = node["$ref"]
        if ref_path.startswith("#/$defs/"):
            ref_name = ref_path.split("/")[-1]
            if ref_name in defs:
                # Check for circular reference - if we're already visiting this
                # ref in the current path, don't recurse (would cause infinite loop)
                if ref_name in _visiting:
                    logger.debug(
                        "Circular reference detected for '%s', using shallow expansion",
                        ref_name,
                    )
                    # Return generic object to prevent infinite recursion
                    return _shallow_expand_circular_ref(defs[ref_name])

                # Add this ref to the visiting set for this recursion path
                new_visiting = _visiting | {ref_name}
                # Process the referenced definition
                return _process_schema_node(defs[ref_name], defs, new_visiting)

    # Start with a new schema object
    result: dict[str, Any] = {}

    # Copy the basic properties
    if "type" in node:
        result["type"] = node["type"]

    # Handle anyOf (often used for optional fields with None)
    if "anyOf" in node:
        non_null_types = [t for t in node["anyOf"] if t.get("type") != "null"]
        if non_null_types:
            # Process the first non-null type
            processed = _process_schema_node(non_null_types[0], defs, _visiting)
            result.update(processed)

    # Handle description
    if "description" in node:
        result["description"] = node["description"]

    # Handle object properties recursively
    if node.get("type") == "object" and "properties" in node:
        result["type"] = "object"
        result["properties"] = {}

        # Process each property
        for prop_name, prop_schema in node["properties"].items():
            result["properties"][prop_name] = _process_schema_node(
                prop_schema, defs, _visiting
            )

        # Add required fields if present
        if "required" in node:
            result["required"] = node["required"]

    # Handle arrays
    if node.get("type") == "array" and "items" in node:
        result["type"] = "array"
        result["items"] = _process_schema_node(node["items"], defs, _visiting)

    # Handle enum
    if "enum" in node:
        result["enum"] = node["enum"]

    return result


class Schema(DiscMixin):
    """Base schema for input action / output observation."""
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", frozen=True)

    @classmethod
    def to_mcp_schema(cls) -> dict[str, Any]:
        """Convert to JSON schema format compatible with MCP."""
        full_schema = cls.model_json_schema()
        result = _process_schema_node(full_schema, full_schema.get("$defs", {}))
        exclude_fields = set(DiscMixin.model_fields.keys()) | set(
            DiscMixin.model_computed_fields.keys()
        )
        for f in exclude_fields:
            if "properties" in result and f in result["properties"]:
                result["properties"].pop(f)
                # Also remove from required if present
                if "required" in result and f in result["required"]:
                    result["required"].remove(f)

        return result

    @classmethod
    def from_mcp_schema(
        cls: type[S], model_name: str, schema: dict[str, Any]
    ) -> type["S"]:
        assert isinstance(schema, dict), "Schema must be a dict"
        assert schema.get("type") == "object", "Only object schemas are supported"

        props: dict[str, Any] = schema.get("properties", {}) or {}
        required = set(schema.get("required", []) or [])

        fields: dict[str, tuple] = {}
        for fname, spec in props.items():
            spec = spec if isinstance(spec, dict) else {}
            tp = py_type(spec)

            # Add description if present
            desc: str | None = spec.get("description")

            # Required → bare type, ellipsis sentinel
            # Optional → make nullable via `| None`, default None
            if fname in required:
                anno = tp
                default = ...
            else:
                anno = tp | None  # allow explicit null in addition to omission
                default = None

            fields[fname] = (
                anno,
                Field(default=default, description=desc)
                if desc
                else Field(default=default),
            )

        return create_model(model_name, __base__=cls, **fields)  # type: ignore[return-value]


class Action(Schema, ABC):
    """Base schema for input action."""

    @property
    def visualize(self) -> Text:
        content = Text()

        # Display action name
        action_name = self.__class__.__name__
        content.append("Action: ", style="bold")
        content.append(action_name)
        content.append("\n\n")

        # Display all action fields systematically
        content.append("Arguments:", style="bold")
        action_fields = self.model_dump()
        content.append(display_dict(action_fields))

        return content


class Observation(Schema, ABC):
    """Base schema for output observation."""

    ERROR_MESSAGE_HEADER: ClassVar[str] = "[An error occurred during execution.]\n"

    content: list[TextContent | ImageContent] = Field(
        default_factory=list,
        description=(
            "Content returned from the tool as a list of "
            "TextContent/ImageContent objects. "
            "When there is an error, it should be written in this field."
        ),
    )
    is_error: bool = Field(
        default=False, description="Whether the observation indicates an error"
    )

    @classmethod
    def from_text(
        cls,
        text: str,
        is_error: bool = False,
        **kwargs: Any,
    ) -> "Self":
        return cls(content=[TextContent(text=text)], is_error=is_error, **kwargs)

    @property
    def text(self) -> str:
        return "".join(item.text for item in self.content if isinstance(item, TextContent))

    @property
    def to_llm_content(self) -> Sequence[TextContent | ImageContent]:
        llm_content: list[TextContent | ImageContent] = []

        if self.is_error:
            llm_content.append(TextContent(text=self.ERROR_MESSAGE_HEADER))

        llm_content.extend(self.content)
        return llm_content

    @property
    def visualize(self) -> Text:
        text = Text()
        if self.is_error:
            text.append("❌ ", style="red bold")
            text.append(self.ERROR_MESSAGE_HEADER, style="bold red")

        text_parts = content_to_str(self.to_llm_content)
        if text_parts:
            full_content = "".join(text_parts)
            text.append(full_content)
        else:
            text.append("[no text content]")
        return text
