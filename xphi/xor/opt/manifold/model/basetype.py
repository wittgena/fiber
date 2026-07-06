# xphi.xor.opt.manifold.model.basetype
## @lineage: bound.adapter.opt.basetype
## @lineage: xphi.xor.opt.basetype
import json
import re
from typing import TYPE_CHECKING, Any, Optional, get_args, get_origin
import json_repair
import pydantic
from anchor.provider.dsp.base import BaseLM

if TYPE_CHECKING:
    from anchor.surface.switch.params import ModelResponseStream
    from arch.xor.manifold.sign.signature import Signature

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
