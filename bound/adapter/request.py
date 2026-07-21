# bound.adapter.request
## @lineage: bound.transport.adapter.request
from typing import Any, Dict, Mapping, List, Optional, Type, Union, cast, get_type_hints, overload
from pydantic import BaseModel

from adapter.legacy.action.param.format import type_to_response_format_param
from adapter.legacy.openai.types import ResponsesAPIOptionalRequestParams, ResponsesAPIResponse, ResponseText

from bound.bridge.response.stream.identity import IdentityRouter
from bound.resolver.model.config.response import BaseResponsesAPIConfig
from bound.resolver.model.config.resolver import config

from watcher.plane.emitter import get_emitter

log = get_emitter("convert.request")

def _get_val(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def _set_val(obj: Any, key: str, val: Any) -> None:
    if isinstance(obj, dict):
        obj[key] = val
    else:
        try:
            setattr(obj, key, val)
        except AttributeError:
            log.debug(f"Could not set attribute '{key}' on object of type {type(obj)}", exc_info=True)

def add_openai_metadata(metadata: Optional[Mapping[str, Any]]) -> Optional[Dict[str, str]]:
    if metadata is None:
        return None
    visible_metadata = {
        str(k): v for k, v in metadata.items()
        if k != "hidden_params" and isinstance(v, str)
    }
    if len(visible_metadata) > 16:
        return dict(list(visible_metadata.items())[:16])
    return visible_metadata.copy()

def _apply_openai_param_overrides(optional_params: dict, non_default_params: dict, allowed_openai_params: list):
    if not allowed_openai_params:
        return optional_params
    for param in allowed_openai_params:
        if param not in optional_params and param in non_default_params:
            optional_params[param] = non_default_params.pop(param)
    return optional_params

class RequestBuilder:
    """@desc: Handles the validation, mapping, and preprocessing of API request parameters"""
    
    @classmethod
    def check_valid_arg(
        cls, 
        supported_params: Optional[List[str]], 
        non_default_params: Dict, 
        drop_params: Optional[bool], 
        custom_llm_provider: Optional[str], 
        model: str
    ):
        if supported_params is None:
            return
        unsupported_params = {k: v for k, v in non_default_params.items() if k not in supported_params}
        if unsupported_params:
            if config.drop_params or drop_params:
                pass 
            else:
                raise config.UnsupportedParamsError(
                    status_code=500,
                    message=f"{custom_llm_provider} does not support parameters: {unsupported_params}, for model={model}."
                )

    @classmethod
    def build_optional_params(
        cls, 
        model: str, 
        provider_config: BaseResponsesAPIConfig, 
        params: ResponsesAPIOptionalRequestParams, 
        allowed_params: Optional[List[str]] = None
    ) -> Dict:
        supported = provider_config.get_supported_openai_params(model)
        non_default = cast(Dict, params)
        
        cls.check_valid_arg(supported + (allowed_params or []), non_default, config.drop_params, provider_config.custom_llm_provider, model)
        mapped = provider_config.map_openai_params(response_api_optional_params=params, model=model, drop_params=config.drop_params)
        return _apply_openai_param_overrides(mapped, non_default, allowed_params or [])

    @classmethod
    def preprocess_requested_params(cls, params: Dict[str, Any]) -> ResponsesAPIOptionalRequestParams:
        valid_keys = get_type_hints(ResponsesAPIOptionalRequestParams).keys()
        custom_llm_provider = params.pop("custom_llm_provider", None)
        special_params = params.pop("kwargs", {})
        additional_drop_params = params.pop("additional_drop_params", None)

        from adapter.legacy.action.param.optional import PreProcessNonDefaultParams

        non_default = PreProcessNonDefaultParams.base_pre_process_non_default_params(
            passed_params=params, 
            special_params=special_params, 
            custom_llm_provider=custom_llm_provider,
            additional_drop_params=additional_drop_params, 
            default_param_values={k: None for k in valid_keys},
            additional_endpoint_specific_params=["input"],
        )

        if "previous_response_id" in non_default:
            non_default["previous_response_id"] = IdentityRouter.decode_previous_response_id_to_original_previous_response_id(non_default["previous_response_id"])

        if "metadata" in non_default:
            converted = add_openai_metadata(non_default["metadata"])
            if converted is not None:
                non_default["metadata"] = converted
            else:
                non_default.pop("metadata", None)

        return cast(ResponsesAPIOptionalRequestParams, non_default)

    @classmethod
    def convert_text_format(cls, text_format: Optional[Union[Type["BaseModel"], dict]], text: Optional["ResponseText"] = None) -> Optional["ResponseText"]:
        if text_format is not None and text is None:
            fmt = type_to_response_format_param(text_format)
            if fmt is not None:
                return {
                    "format": {
                        "type": fmt["type"], "name": fmt["json_schema"]["name"],
                        "schema": fmt["json_schema"]["schema"], "strict": fmt["json_schema"]["strict"],
                    }
                }
        return text

    @classmethod
    def restore_encrypted_inputs(cls, request_input: Any) -> Any:
        if not isinstance(request_input, list):
            return request_input
        for item in request_input:
            if isinstance(item, dict):
                item_id = item.get("id")
                if item_id and isinstance(item_id, str):
                    decoded = IdentityRouter._decode_encrypted_item_id(item_id)
                    if decoded:
                        item["id"] = decoded["item_id"]
                enc_content = item.get("encrypted_content")
                if enc_content and isinstance(enc_content, str):
                    _, unwrapped = IdentityRouter._unwrap_encrypted_content_with_model_id(enc_content)
                    if unwrapped != enc_content:
                        item["encrypted_content"] = unwrapped
        return request_input

class IdentityMutator:
    """@desc: Handles recursive tree traversal and payload mutation for Response identities"""
    
    @classmethod
    def inject_model_identity(cls, response: Union[ResponsesAPIResponse, Dict[str, Any]], custom_llm_provider: Optional[str], litellm_metadata: Optional[Dict[str, Any]] = None) -> Union[ResponsesAPIResponse, Dict[str, Any]]:
        meta = litellm_metadata or {}
        model_id = meta.get("model_info", {}).get("id")
        resp_id = _get_val(response, "id")
        
        if resp_id is None:
            return response

        updated_id = IdentityRouter._build_responses_api_response_id(model_id=model_id, custom_llm_provider=custom_llm_provider, response_id=resp_id)
        _set_val(response, "id", updated_id)

        if meta.get("encrypted_content_affinity_enabled"):
            cls._update_encrypted_content(response, model_id)

        cls._encode_containers_in_response(response, custom_llm_provider, model_id)
        return response

    @classmethod
    def _update_encrypted_content(cls, response: Union["ResponsesAPIResponse", Dict[str, Any]], model_id: Optional[str]) -> None:
        if not model_id: return
        output = _get_val(response, "output")
        if not isinstance(output, list): return

        for item in output:
            item_id = _get_val(item, "id")
            enc_content = _get_val(item, "encrypted_content")
            if enc_content and isinstance(enc_content, str):
                _set_val(item, "encrypted_content", IdentityRouter._wrap_encrypted_content_with_model_id(enc_content, model_id))
                if item_id and isinstance(item_id, str):
                    _set_val(item, "id", IdentityRouter._build_encrypted_item_id(model_id, item_id))

    @classmethod
    def _encode_containers_in_response(cls, response: Union[ResponsesAPIResponse, Dict[str, Any]], custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        output = _get_val(response, "output", [])
        for item in output:
            cls.encode_item(item, custom_llm_provider, model_id)

    @classmethod
    def encode_item(cls, item: Any, custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        """Recursive encoder for container_id"""
        if item is None: return

        def _maybe_encode(cid: str) -> Optional[str]:
            if IdentityRouter._decode_container_id(cid).get("custom_llm_provider") is not None:
                return None
            return IdentityRouter._build_container_id(custom_llm_provider=custom_llm_provider, model_id=model_id, container_id=cid)

        cid = _get_val(item, "container_id")
        if isinstance(cid, str):
            enc = _maybe_encode(cid)
            if enc: _set_val(item, "container_id", enc)

        nested = _get_val(item, "code_interpreter_call")
        if nested is not None:
            cls.encode_item(nested, custom_llm_provider, model_id)

        if _get_val(item, "type") == "message":
            cls.encode_message(item, custom_llm_provider, model_id)

    @classmethod
    def encode_message(cls, item: Any, custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        content = _get_val(item, "content")
        if isinstance(content, list):
            for part in content:
                annotations = _get_val(part, "annotations")
                if isinstance(annotations, list):
                    for ann in annotations:
                        cls.encode_item(ann, custom_llm_provider, model_id)

    @classmethod
    def collect_all_ids(cls, response: Any) -> list[str]:
        if response is None: return []
        collected: set[str] = set()
        for item in _get_val(response, "output", []):
            cls.collect_item(item, collected)
        return list(collected)

    @classmethod
    def collect_item(cls, item: Any, collected: set[str]) -> None:
        if item is None: return
        cid = _get_val(item, "container_id")
        if isinstance(cid, str) and cid: collected.add(cid)

        nested = _get_val(item, "code_interpreter_call")
        if nested is not None: cls.collect_item(nested, collected)

        if _get_val(item, "type") == "message":
            content = _get_val(item, "content")
            if isinstance(content, list):
                for part in content:
                    annotations = _get_val(part, "annotations")
                    if isinstance(annotations, list):
                        for ann in annotations: 
                            cls.collect_item(ann, collected)