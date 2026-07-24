# bound.xor.opt.adapter.base
from typing import Any, get_origin
import json_repair

from bound.xor.opt.lm import BaseLM
from bound.xor.opt.reasoning import Type, split_message_content_for_custom_types, Reasoning, Tool, ToolCalls
from bound.xor.opt.callback.base import BaseCallback, with_callbacks

from arch.xor.sign.signature import Signature, AdapterParseError
from arch.gov.gate import uuid4
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

_DEFAULT_NATIVE_RESPONSE_TYPES = [Reasoning]

class Adapter:
    def __init__(
        self,
        callbacks: list[BaseCallback] | None = None,
        use_native_function_calling: bool = False,
        native_response_types: list[type[Type]] | None = None,
    ):
        self.callbacks = callbacks or []
        self.use_native_function_calling = use_native_function_calling
        self.native_response_types = native_response_types or _DEFAULT_NATIVE_RESPONSE_TYPES

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.format = with_callbacks(cls.format)
        cls.parse = with_callbacks(cls.parse)

    def _call_preprocess(
        self,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        signature: type[Signature],
        inputs: dict[str, Any],
    ) -> type[Signature]:
        if self.use_native_function_calling:
            tool_call_input_field_name = self._get_tool_call_input_field_name(signature)
            tool_call_output_field_name = self._get_tool_call_output_field_name(signature)

            if tool_call_output_field_name and tool_call_input_field_name is None:
                raise ValueError(
                    f"You provided an output field {tool_call_output_field_name} to receive the tool calls information, "
                    "but did not provide any tools as the input. Please provide a list of tools as the input by adding an "
                    "input field with type `list[Tool]`."
                )

            if tool_call_output_field_name and lm.supports_function_calling:
                tools = inputs[tool_call_input_field_name]
                tools = tools if isinstance(tools, list) else [tools]
                lm_tools = [tool.format_as_litellm_function_call() for tool in tools]
                lm_kwargs["tools"] = lm_tools

                signature_for_native_function_calling = signature.delete(tool_call_output_field_name)
                signature_for_native_function_calling = signature_for_native_function_calling.delete(
                    tool_call_input_field_name
                )
                return signature_for_native_function_calling

        for name, field in signature.output_fields.items():
            if (
                isinstance(field.annotation, type)
                and field.annotation in self.native_response_types
                and issubclass(field.annotation, Type)
            ):
                signature = field.annotation.adapt_to_native_lm_feature(signature, name, lm, lm_kwargs)

        return signature

    def _call_postprocess(
        self,
        processed_signature: type[Signature],
        original_signature: type[Signature],
        outputs: list[dict[str, Any] | str | Any],
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        req_id: str = "UNKNOWN"
    ) -> list[dict[str, Any]]:
        log.debug(f"[Adapter-{req_id} DEBUG: RAW OUTPUT TYPE]")
        log.debug(f"[Adapter-{req_id}] Type of outputs: {type(outputs)}")
        log.debug(f"[Adapter-{req_id}] Length of outputs: {len(outputs) if hasattr(outputs, '__len__') else 'N/A'}")
        if isinstance(outputs, list) and len(outputs) > 0:
            log.debug(f"[Adapter-{req_id}] Type of outputs[0]: {type(outputs[0])}")
            log.debug(f"[Adapter-{req_id}] Dir of outputs[0]: {dir(outputs[0])}")
            log.debug(f"[Adapter-{req_id}] Content of outputs[0]: {outputs[0]}")

        values = []
        tool_call_output_field_name = self._get_tool_call_output_field_name(original_signature)
        for output in outputs:
            output_logprobs = None
            tool_calls = None
            text = None

            if isinstance(output, str):
                text = output
            elif isinstance(output, dict):
                text = output.get("text", output.get("content"))
                output_logprobs = output.get("logprobs")
                tool_calls = output.get("tool_calls")
            elif hasattr(output, "choices") and len(output.choices) > 0:
                choice = output.choices[0]
                if hasattr(choice, "message"):
                    text = getattr(choice.message, "content", None)
                    tool_calls = getattr(choice.message, "tool_calls", None)
                elif hasattr(choice, "text"):
                    text = choice.text
            elif hasattr(output, "text"):
                text = output.text
            else:
                text = str(output)

            if not text and not tool_calls:
                text = None

            if text:
                value = self.parse(processed_signature, text)
                for field_name in original_signature.output_fields.keys():
                    if field_name not in value:
                        value[field_name] = None
            elif tool_calls and tool_call_output_field_name:
                value = {}
                for field_name in original_signature.output_fields.keys():
                    value[field_name] = None
            else:
                log.error(f"[Adapter-{req_id}] 🚨 AdapterParseError: The LM returned an empty or null response.")
                raise AdapterParseError(
                    adapter_name=type(self).__name__,
                    signature=original_signature,
                    lm_response=str(output),
                    message="The LM returned an empty or null response.",
                )

            if tool_calls and tool_call_output_field_name:
                formatted_tool_calls = []
                for v in tool_calls:
                    if isinstance(v, dict):
                        func_name = v.get("function", {}).get("name")
                        args_str = v.get("function", {}).get("arguments", "{}")
                    else:
                        func = getattr(v, "function", None)
                        func_name = getattr(func, "name", "") if func else ""
                        args_str = getattr(func, "arguments", "{}") if func else "{}"
                        
                    formatted_tool_calls.append({
                        "name": func_name,
                        "args": json_repair.loads(args_str),
                    })
                value[tool_call_output_field_name] = ToolCalls.from_dict_list(formatted_tool_calls)

            for name, field in original_signature.output_fields.items():
                if (
                    isinstance(field.annotation, type)
                    and field.annotation in self.native_response_types
                    and issubclass(field.annotation, Type)
                ):
                    parsed_value = field.annotation.parse_lm_response(output)
                    if parsed_value is not None:
                        value[name] = parsed_value

            if output_logprobs:
                value["logprobs"] = output_logprobs

            values.append(value)

        return values

    def __call__(
        self,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        signature: type[Signature],
        demos: list[dict[str, Any]],
        inputs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        req_id = str(uuid4())[:8]
        log.debug(f"[Adapter-{req_id}] 🚀 __call__ START | lm={type(lm).__name__}, signature={signature.__name__}")
        
        processed_signature = self._call_preprocess(lm, lm_kwargs, signature, inputs)
        formatted_inputs = self.format(processed_signature, demos, inputs)
        
        log.debug(f"[Adapter-{req_id}] 📝 Inputs formatted. Invoking synchronous LM call...")
        outputs = lm(messages=formatted_inputs, **lm_kwargs)
        
        log.debug(f"[Adapter-{req_id}] ✅ LM call completed. Initiating postprocessing...")
        results = self._call_postprocess(processed_signature, signature, outputs, lm, lm_kwargs, req_id=req_id)
        
        log.debug(f"[Adapter-{req_id}] 🏁 __call__ END")
        return results

    async def acall(
        self,
        lm: BaseLM,
        lm_kwargs: dict[str, Any],
        signature: type[Signature],
        demos: list[dict[str, Any]],
        inputs: dict[str, Any],
    ) -> list[dict[str, Any]]:
        req_id = str(uuid4())[:8]
        log.debug(f"[Adapter-{req_id}] ⚡ acall START | lm={type(lm).__name__}, signature={signature.__name__}")
        
        processed_signature = self._call_preprocess(lm, lm_kwargs, signature, inputs)
        formatted_inputs = self.format(processed_signature, demos, inputs)
        
        log.debug(f"[Adapter-{req_id}] 📝 Inputs formatted. Awaiting asynchronous LM call...")
        outputs = await lm.acall(messages=formatted_inputs, **lm_kwargs)
        if not isinstance(outputs, list):
            log.debug(f"[Adapter-{req_id}] Wrapping single output in list to unify type.")
            outputs = [outputs]
            
        log.debug(f"[Adapter-{req_id}] ✅ Await completed. Initiating postprocessing...")
        results = self._call_postprocess(processed_signature, signature, outputs, lm, lm_kwargs, req_id=req_id)
        
        log.debug(f"[Adapter-{req_id}] 🏁 acall END")
        return results

    def format(self, signature: type[Signature], demos: list[dict[str, Any]], inputs: dict[str, Any]) -> list[dict[str, Any]]:
        inputs_copy = dict(inputs)
        
        messages = []
        system_message = self.format_system_message(signature)
        messages.append({"role": "system", "content": system_message})
        messages.extend(self.format_demos(signature, demos))
        
        content = self.format_user_message_content(signature, inputs_copy, main_request=True)
        messages.append({"role": "user", "content": content})

        messages = split_message_content_for_custom_types(messages)
        return messages

    def format_system_message(self, signature: type[Signature]) -> str:
        return f"{self.format_field_description(signature)}\n{self.format_field_structure(signature)}\n{self.format_task_description(signature)}"

    def format_field_description(self, signature: type[Signature]) -> str: raise NotImplementedError
    def format_field_structure(self, signature: type[Signature]) -> str: raise NotImplementedError
    def format_task_description(self, signature: type[Signature]) -> str: raise NotImplementedError
    def format_user_message_content(self, signature: type[Signature], inputs: dict[str, Any], prefix: str = "", suffix: str = "", main_request: bool = False) -> str: raise NotImplementedError
    def format_assistant_message_content(self, signature: type[Signature], outputs: dict[str, Any], missing_field_message: str | None = None) -> str: raise NotImplementedError
    
    def format_demos(self, signature: type[Signature], demos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        complete_demos, incomplete_demos = [], []
        for demo in demos:
            is_complete = all(k in demo and demo[k] is not None for k in signature.fields)
            has_input = any(k in demo for k in signature.input_fields)
            has_output = any(k in demo for k in signature.output_fields)
            if is_complete: complete_demos.append(demo)
            elif has_input and has_output: incomplete_demos.append(demo)

        messages = []
        incomplete_demo_prefix = "This is an example of the task, though some input or output fields are not supplied."
        for demo in incomplete_demos:
            messages.append({"role": "user", "content": self.format_user_message_content(signature, demo, prefix=incomplete_demo_prefix)})
            messages.append({"role": "assistant", "content": self.format_assistant_message_content(signature, demo, missing_field_message="Not supplied for this particular example. ")})
        for demo in complete_demos:
            messages.append({"role": "user", "content": self.format_user_message_content(signature, demo)})
            messages.append({"role": "assistant", "content": self.format_assistant_message_content(signature, demo, missing_field_message="Not supplied for this conversation history message. ")})
        return messages

    def _get_tool_call_input_field_name(self, signature: type[Signature]) -> str:
        for name, field in signature.input_fields.items():
            origin = get_origin(field.annotation)
            if origin is list and field.annotation.__args__[0] == Tool: return name
            if field.annotation == Tool: return name
        return None

    def _get_tool_call_output_field_name(self, signature: type[Signature]) -> str:
        for name, field in signature.output_fields.items():
            if field.annotation == ToolCalls: return name
        return None

    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        raise NotImplementedError