# xphi.xor.opt.json
import json
from typing import Any, get_origin, Dict
import json_repair
import pydantic
import regex
from pydantic.fields import FieldInfo
from bound.adapter.opt.chat import ChatAdapter, FieldInfoWithName
from xphi.xor.manifold.tool import ToolCalls
from xphi.xor.opt.utils import (
    format_field_value,
    get_annotation_name,
    parse_value,
    serialize_for_json,
    translate_field_type,
)
from anchor.provider.dsp.base import BaseLM
from arch.xor.manifold.sign.signature import Signature, SignatureMeta
from xphi.xor.opt.callback.base import BaseCallback
from bound.adapter.opt.exception import AdapterParseError
from watcher.plane.emitter import get_emitter

log = get_emitter("opt.json")

def _has_open_ended_mapping(signature: SignatureMeta) -> bool:
    for field in signature.output_fields.values():
        annotation = field.annotation
        if get_origin(annotation) is dict:
            return True
    return False

class JSONAdapter(ChatAdapter):
    def __init__(self, callbacks: list[BaseCallback] | None = None, use_native_function_calling: bool = True):
        super().__init__(callbacks=callbacks, use_native_function_calling=use_native_function_calling)

    def _json_adapter_call_common(self, lm, lm_kwargs, signature, demos, inputs, call_fn):
        if "response_format" not in lm.supported_params:
            return call_fn(lm, lm_kwargs, signature, demos, inputs)

        has_tool_calls = any(field.annotation == ToolCalls for field in signature.output_fields.values())

        if _has_open_ended_mapping(signature) or (not self.use_native_function_calling and has_tool_calls) or not lm.supports_response_schema:
            lm_kwargs["response_format"] = {"type": "json_object"}
            return call_fn(lm, lm_kwargs, signature, demos, inputs)

    def __call__(
        self,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        signature: type[Signature],
        demos: list[dict[str, Any]],
        inputs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        result = self._json_adapter_call_common(lm, lm_kwargs, signature, demos, inputs, super().__call__)
        if result:
            return result

        try:
            structured_output_model = _get_structured_outputs_response_format(
                signature, self.use_native_function_calling
            )
            lm_kwargs["response_format"] = structured_output_model
            return super().__call__(lm, lm_kwargs, signature, demos, inputs)
        except Exception:
            log.warning("Failed to use structured output format, falling back to JSON mode.")
            lm_kwargs["response_format"] = {"type": "json_object"}
            return super().__call__(lm, lm_kwargs, signature, demos, inputs)

    async def acall(
        self,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        signature: type[Signature],
        demos: list[dict[str, Any]],
        inputs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        result = self._json_adapter_call_common(lm, lm_kwargs, signature, demos, inputs, super().acall)
        if result:
            return await result

        try:
            structured_output_model = _get_structured_outputs_response_format(
                signature, self.use_native_function_calling
            )
            lm_kwargs["response_format"] = structured_output_model
            return await super().acall(lm, lm_kwargs, signature, demos, inputs)
        except Exception:
            log.warning("Failed to use structured output format, falling back to JSON mode.")
            lm_kwargs["response_format"] = {"type": "json_object"}
            return await super().acall(lm, lm_kwargs, signature, demos, inputs)

    def format_field_structure(self, signature: type[Signature]) -> str:
        parts = []
        parts.append("All interactions will be structured in the following way, with the appropriate values filled in.")

        def format_signature_fields_for_instructions(fields: dict[str, FieldInfo], role: str):
            return self.format_field_with_value(
                fields_with_values={
                    FieldInfoWithName(name=field_name, info=field_info): translate_field_type(field_name, field_info)
                    for field_name, field_info in fields.items()
                },
                role=role,
            )

        parts.append("Inputs will have the following structure:")
        parts.append(format_signature_fields_for_instructions(signature.input_fields, role="user"))
        parts.append("Outputs will be a JSON object with the following fields.")
        parts.append(format_signature_fields_for_instructions(signature.output_fields, role="assistant"))
        return "\n\n".join(parts).strip()

    def user_message_output_requirements(self, signature: type[Signature]) -> str:
        def type_info(v):
            return (
                f" (must be formatted as a valid Python {get_annotation_name(v.annotation)})"
                if v.annotation is not str
                else ""
            )

        message = "Respond with a JSON object in the following order of fields: "
        message += ", then ".join(f"`{f}`{type_info(v)}" for f, v in signature.output_fields.items())
        message += "."
        return message

    def format_assistant_message_content(
        self,
        signature: type[Signature],
        outputs: dict[str, Any],
        missing_field_message=None,
    ) -> str:
        fields_with_values = {
            FieldInfoWithName(name=k, info=v): outputs.get(k, missing_field_message)
            for k, v in signature.output_fields.items()
        }
        return self.format_field_with_value(fields_with_values, role="assistant")

    def _extract_text_from_completion(self, completion: Any) -> str:
        log.error(f"[JSONAdapter] Extracting text from completion object of type: {type(completion)}")
        log.error(f"[JSONAdapter] Completion raw value: {completion}")

        if completion is None:
            log.error("[JSONAdapter] Error: completion object is strictly None")
            return ""
        if isinstance(completion, str):
            return completion
        if isinstance(completion, dict):
            return completion.get("text", completion.get("content", str(completion)))
        if hasattr(completion, "choices") and len(completion.choices) > 0:
            choice = completion.choices[0]
            if hasattr(choice, "message"):
                val = getattr(choice.message, "content", "") or ""
                log.error(f"[JSONAdapter] Extracted from message.content: {val}")
                return val
            if hasattr(choice, "text"):
                return choice.text or ""
        if hasattr(completion, "text"):
            return completion.text or ""
        
        fallback = str(completion)
        log.error(f"[JSONAdapter] Fallback cast to string: {fallback}")
        return str(completion)

    def parse(self, signature: type[Signature], completion: Any) -> dict[str, Any]:
        """
        @fix: completion 파라미터 타입을 Any로 확장하고, 
        JSON 직렬화 전에 텍스트를 안전하게 추출(Unwrapping)합니다.
        """
        # 1. 텍스트 안전 추출
        raw_text = self._extract_text_from_completion(completion)
        if not raw_text.strip():
            raise AdapterParseError(
                adapter_name="JSONAdapter",
                signature=signature,
                lm_response=str(completion),
                message="LM returned an empty or unparsable response.",
            )

        fields = None
        
        # 2. 1차 시도: 전체 문자열 JSON 파싱
        try:
            fields = json_repair.loads(raw_text)
        except Exception:
            pass

        # 3. 2차 시도: 정규식으로 JSON 블록 추출 후 파싱
        if not isinstance(fields, dict):
            pattern = r"\{(?:[^{}]|(?R))*\}"
            match = regex.search(pattern, raw_text, regex.DOTALL)
            if match:
                extracted_json = match.group(0)
                try:
                    fields = json_repair.loads(extracted_json)
                except Exception:
                    pass

        # 4. 파싱 실패 시 예외 발생
        if not isinstance(fields, dict):
            raise AdapterParseError(
                adapter_name="JSONAdapter",
                signature=signature,
                lm_response=raw_text,
                message="LM response cannot be serialized to a JSON object.",
            )

        # 5. 서명에 선언된 필드만 필터링
        filtered_fields = {k: v for k, v in fields.items() if k in signature.output_fields}

        # 6. 각 필드의 타입을 시그니처에 맞게 캐스팅
        for k, v in filtered_fields.items():
            if k in signature.output_fields:
                try:
                    filtered_fields[k] = parse_value(v, signature.output_fields[k].annotation)
                except Exception as e:
                    log.debug(f"JSONAdapter failed to cast field {k}: {e}")
                    # 캐스팅 실패 시 일단 원본 값을 유지하여 후속 파이프라인에서 처리할 여지를 둠

        # 7. 필수 필드 누락 검사
        if filtered_fields.keys() != signature.output_fields.keys():
            missing = set(signature.output_fields.keys()) - set(filtered_fields.keys())
            # 누락된 필드가 있다면 None으로 채워넣어 완전 붕괴를 막음 (유연성 확보)
            for m_key in missing:
                filtered_fields[m_key] = None
                
            log.warning(f"JSONAdapter: Missing fields in LM response: {missing}. Filled with None.")

        return filtered_fields

    def format_field_with_value(self, fields_with_values: dict[FieldInfoWithName, Any], role: str = "user") -> str:
        if role == "user":
            output = []
            for field, field_value in fields_with_values.items():
                formatted_field_value = format_field_value(field_info=field.info, value=field_value)
                output.append(f"[[ ## {field.name} ## ]]\n{formatted_field_value}")
            return "\n\n".join(output).strip()
        else:
            d = fields_with_values.items()
            d = {k.name: v for k, v in d}
            return json.dumps(serialize_for_json(d), indent=2, ensure_ascii=False)

    def format_finetune_data(
        self, signature: type[Signature], demos: list[dict[str, Any]], inputs: dict[str, Any], outputs: dict[str, Any]
    ) -> dict[str, list[Any]]:
        raise NotImplementedError


# (이하 _get_structured_outputs_response_format 함수는 원본과 동일하게 유지)
def _get_structured_outputs_response_format(
    signature: SignatureMeta,
    use_native_function_calling: bool = True,
) -> type[pydantic.BaseModel]:
    for name, field in signature.output_fields.items():
        annotation = field.annotation
        if get_origin(annotation) is dict:
            raise ValueError(
                f"Field '{name}' has an open-ended mapping type which is not supported by Structured Outputs."
            )

    fields = {}
    for name, field in signature.output_fields.items():
        annotation = field.annotation
        if use_native_function_calling and annotation == ToolCalls:
            continue
        default = field.default if hasattr(field, "default") else ...
        fields[name] = (annotation, default)

    pydantic_model = pydantic.create_model(
        "SpiProgramOutputs",
        __config__=pydantic.ConfigDict(extra="forbid"),
        **fields,
    )

    schema = pydantic_model.model_json_schema()
    for prop in schema.get("properties", {}).values():
        prop.pop("json_schema_extra", None)

    def enforce_required(schema_part: dict):
        if schema_part.get("type") == "object":
            props = schema_part.get("properties")
            if props is not None:
                schema_part["required"] = list(props.keys())
                schema_part["additionalProperties"] = False
                for sub_schema in props.values():
                    if isinstance(sub_schema, dict):
                        enforce_required(sub_schema)
            else:
                schema_part["properties"] = {}
                schema_part["required"] = []
                schema_part["additionalProperties"] = False
        if schema_part.get("type") == "array" and isinstance(schema_part.get("items"), dict):
            enforce_required(schema_part["items"])
        for key in ("$defs", "definitions"):
            if key in schema_part:
                for def_schema in schema_part[key].values():
                    enforce_required(def_schema)

    enforce_required(schema)
    pydantic_model.model_json_schema = lambda *args, **kwargs: schema
    return pydantic_model