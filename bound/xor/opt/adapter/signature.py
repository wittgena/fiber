# bound.xor.opt.adapter.signature
import textwrap
from typing import Any, NamedTuple
from pydantic.fields import FieldInfo
import re
from typing import Any, Protocol
import json_repair
import regex

from bound.xor.opt.adapter.base import Adapter
from bound.xor.opt.callback.base import BaseCallback
from bound.xor.opt.formatter import (
    parse_value,
    format_field_value,
    get_annotation_name,
    get_field_description_string,
    translate_field_type,
)

from arch.xor.sign.signature import Signature, AdapterParseError
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class OutputParser(Protocol):
    """Adapter의 텍스트 파싱을 전담하는 프로토콜(인터페이스)"""
    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        ...

class MarkdownRegexParser:
    """기존 ChatAdapter의 마크다운 형식([[ ## field ## ]]) 응답을 파싱하는 클래스입"""
    def __init__(self):
        self.field_header_pattern = re.compile(r"\[\[ ## (\w+) ## \]\]")

    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        sections = [(None, [])]

        for line in completion.splitlines():
            match = self.field_header_pattern.match(line.strip())
            if match:
                header = match.group(1)
                remaining_content = line[match.end() :].strip()
                sections.append((header, [remaining_content] if remaining_content else []))
            else:
                sections[-1][1].append(line)

        sections = [(k, "\n".join(v).strip()) for k, v in sections]

        fields = {}
        for k, v in sections:
            if (k not in fields) and (k in signature.output_fields):
                try:
                    fields[k] = parse_value(v, signature.output_fields[k].annotation)
                except Exception as e:
                    raise AdapterParseError(
                        adapter_name=type(self).__name__,
                        signature=signature,
                        lm_response=completion,
                        message=f"Failed to parse field {k} with value {v} from the LM response. Error message: {e}",
                    )

        if fields.keys() != signature.output_fields.keys():
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                parsed_result=fields,
                message="Parsed fields do not match the expected output fields in the signature."
            )

        return fields


class JSONRepairParser:
    """부분적으로 깨지거나 마크다운 블록(```json)에 감싸진 데이터도 복구하여 추출"""
    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        if not completion or not completion.strip():
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                message="LM returned an empty response.",
            )

        fields = None
        
        ## 1차 시도: 전체 문자열 직접 파싱
        try:
            fields = json_repair.loads(completion)
        except Exception:
            pass

        ## 2차 시도: 정규식을 통한 JSON 블록 추출 파싱
        if not isinstance(fields, dict):
            pattern = r"\{(?:[^{}]|(?R))*\}"
            match = regex.search(pattern, completion, regex.DOTALL)
            if match:
                try:
                    fields = json_repair.loads(match.group(0))
                except Exception:
                    pass

        ## 파싱 완전 실패 시
        if not isinstance(fields, dict):
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                message="LM response cannot be parsed as a JSON object.",
            )

        filtered_fields = {}
        for k, expected_field in signature.output_fields.items():
            if k in fields:
                try:
                    filtered_fields[k] = parse_value(fields[k], expected_field.annotation)
                except Exception as e:
                    log.debug(f"JSONRepairParser failed to cast field {k}: {e}")
                    # 캐스팅 실패 시 데이터 손실을 막기 위해 원본 할당
                    filtered_fields[k] = fields[k]
            else:
                log.warning(f"JSONRepairParser: Missing field '{k}' in LM response. Filled with None.")
                filtered_fields[k] = None

        return filtered_fields


class FieldInfoWithName(NamedTuple):
    name: str
    info: FieldInfo

class SignatureAdapter(Adapter):
    def __init__(
        self,
        callbacks: list[BaseCallback] | None = None,
        use_native_function_calling: bool = False,
        native_response_types: list[type[type]] | None = None,
        use_json_adapter_fallback: bool = True,
    ):
        super().__init__(
            callbacks=callbacks,
            use_native_function_calling=use_native_function_calling,
            native_response_types=native_response_types,
        )
        self.use_json_adapter_fallback = use_json_adapter_fallback
        self.primary_parser = MarkdownRegexParser()
        self.fallback_parser = JSONRepairParser() if use_json_adapter_fallback else None

    def format_field_description(self, signature: type[Signature]) -> str:
        return (
            f"Your input fields are:\n{get_field_description_string(signature.input_fields)}\n"
            f"Your output fields are:\n{get_field_description_string(signature.output_fields)}"
        )

    def format_field_structure(self, signature: type[Signature]) -> str:
        parts = []
        parts.append("All interactions will be structured in the following way, with the appropriate values filled in.")

        def format_signature_fields_for_instructions(fields: dict[str, FieldInfo]):
            return self.format_field_with_value(
                fields_with_values={
                    FieldInfoWithName(name=field_name, info=field_info): translate_field_type(field_name, field_info)
                    for field_name, field_info in fields.items()
                },
            )

        parts.append(format_signature_fields_for_instructions(signature.input_fields))
        parts.append(format_signature_fields_for_instructions(signature.output_fields))
        parts.append("[[ ## completed ## ]]\n")
        return "\n\n".join(parts).strip()

    def format_task_description(self, signature: type[Signature]) -> str:
        instructions = textwrap.dedent(signature.instructions)
        objective = ("\n" + " " * 8).join([""] + instructions.splitlines())
        return f"In adhering to this structure, your objective is: {objective}"

    def format_user_message_content(
        self,
        signature: type[Signature],
        inputs: dict[str, Any],
        prefix: str = "",
        suffix: str = "",
        main_request: bool = False,
    ) -> str:
        messages = [prefix]
        for k, v in signature.input_fields.items():
            if k in inputs:
                value = inputs.get(k)
                formatted_field_value = format_field_value(field_info=v, value=value)
                messages.append(f"[[ ## {k} ## ]]\n{formatted_field_value}")

        if main_request:
            output_requirements = self.user_message_output_requirements(signature)
            if output_requirements is not None:
                messages.append(output_requirements)

        messages.append(suffix)
        return "\n\n".join(messages).strip()

    def user_message_output_requirements(self, signature: type[Signature]) -> str:
        def type_info(v):
            if v.annotation is not str:
                return f" (must be formatted as a valid Python {get_annotation_name(v.annotation)})"
            else:
                return ""

        message = "Respond with the corresponding output fields, starting with the field "
        message += ", then ".join(f"`[[ ## {f} ## ]]`{type_info(v)}" for f, v in signature.output_fields.items())
        message += ", and then ending with the marker for `[[ ## completed ## ]]`."
        return message

    def format_assistant_message_content(
        self,
        signature: type[Signature],
        outputs: dict[str, Any],
        missing_field_message=None,
    ) -> str:
        assistant_message_content = self.format_field_with_value(
            {
                FieldInfoWithName(name=k, info=v): outputs.get(k, missing_field_message)
                for k, v in signature.output_fields.items()
            },
        )
        assistant_message_content += "\n\n[[ ## completed ## ]]\n"
        return assistant_message_content

    def format_field_with_value(self, fields_with_values: dict[FieldInfoWithName, Any]) -> str:
        output = []
        for field, field_value in fields_with_values.items():
            formatted_field_value = format_field_value(field_info=field.info, value=field_value)
            output.append(f"[[ ## {field.name} ## ]]\n{formatted_field_value}")

        return "\n\n".join(output).strip()

    def format_finetune_data(
        self,
        signature: type[Signature],
        demos: list[dict[str, Any]],
        inputs: dict[str, Any],
        outputs: dict[str, Any],
    ) -> dict[str, list[Any]]:
        system_user_messages = self.format(signature=signature, demos=demos, inputs=inputs)
        assistant_message_content = self.format_assistant_message_content(signature=signature, outputs=outputs)
        assistant_message = {"role": "assistant", "content": assistant_message_content}
        messages = system_user_messages + [assistant_message]
        return {"messages": messages}

    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        try:
            return self.primary_parser.parse(signature, completion)
        except AdapterParseError as e:
            if self.fallback_parser:
                log.warning(f"Markdown parsing failed. Attempting JSON parsing fallback. Reason: {str(e)}")
                return self.fallback_parser.parse(signature, completion)
            raise e