# bound.resolver.model.api.base
from typing import Any, Dict, Mapping, List, Optional, Type, Union, cast, get_type_hints, overload
from pydantic import BaseModel

from bound.mapper.param.legacy import LiteLLM_Params
from eco.llama.router.locator import get_llm_provider
from bound.resolver.openai.types import ResponsesAPIOptionalRequestParams, ResponsesAPIResponse, ResponseText

from bound.resolver.model.config.response import BaseResponsesAPIConfig
from bound.resolver.model.config.resolver import config
from bound.gateway.parser.stream.identity import IdentityRouter
from bound.gateway.parser.request import RequestBuilder, IdentityMutator, _get_val

from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.api")

def get_api_base(model: str, optional_params: Union[dict, LiteLLM_Params]) -> Optional[str]:
    """@desc: Resolves the dynamic API Base URL based on the model, provider, and runtime parameters"""
    if isinstance(optional_params, LiteLLM_Params):
        params = optional_params
    elif isinstance(optional_params, dict):
        try:
            params_dict = optional_params.copy()
            if "model" not in params_dict:
                params_dict["model"] = model
            params = LiteLLM_Params(**params_dict)
        except Exception as e:
            log.debug(f"Failed to parse optional_params into LiteLLM_Params: {e}")
            return None
    else:
        return None

    ## Explicit API Base Override
    if params.api_base is not None:
        return params.api_base

    ## Model Alias Resolution
    resolved_model = config.model_alias_map.get(model, model) if config.model_alias_map else model

    ## Dynamic Provider Inference
    try:
        _, provider, _, dynamic_api_base = get_llm_provider(
            model=resolved_model,
            custom_llm_provider=params.custom_llm_provider,
            api_base=params.api_base,
            api_key=params.api_key,
        )
    except Exception as e:
        log.debug(f"Error inferring LLM provider for api_base resolution: {e}")
        provider, dynamic_api_base = None, None

    if dynamic_api_base is not None:
        return dynamic_api_base

    ## Stream endpoint suffix determination
    is_stream = getattr(optional_params, "stream", False) or getattr(params, "stream", False)
    content_endpoint = "streamGenerateContent" if is_stream else "generateContent"

    ## Vertex AI Routing
    if params.vertex_location and params.vertex_project:
        return (
            f"{params.vertex_location}-aiplatform.googleapis.com/v1/"
            f"projects/{params.vertex_project}/locations/{params.vertex_location}/"
            f"publishers/google/models/{resolved_model}:{content_endpoint}"
        )

    ## Standard Provider Routing
    if not provider:
        return None

    if provider == "gemini":
        return f"https://generativelanguage.googleapis.com/v1beta/models/{resolved_model}:{content_endpoint}"
        
    if provider == "openai":
        return "https://api.openai.com"

    return None

class APIBridge:
    """@desc: Stable public Facade for downstream modules"""

    @staticmethod
    def _decode_container_id(container_id: str) -> Dict[str, Any]:
        return IdentityRouter._decode_container_id(container_id)

    @staticmethod
    def _decode_responses_api_response_id(response_id: str) -> str:
        return IdentityRouter._decode_responses_api_response_id(response_id)

    """Request Parameter Building"""
    @staticmethod
    def _check_valid_arg(supported_params: Optional[List[str]], non_default_params: Dict, drop_params: Optional[bool], custom_llm_provider: Optional[str], model: str):
        return RequestBuilder.check_valid_arg(supported_params, non_default_params, drop_params, custom_llm_provider, model)

    @staticmethod
    def get_optional_params_responses_api(model: str, responses_api_provider_config: BaseResponsesAPIConfig, response_api_optional_params: ResponsesAPIOptionalRequestParams, allowed_openai_params: Optional[List[str]] = None) -> Dict:
        return RequestBuilder.build_optional_params(model, responses_api_provider_config, response_api_optional_params, allowed_openai_params)

    @staticmethod
    def get_requested_response_api_optional_param(params: Dict[str, Any]) -> ResponsesAPIOptionalRequestParams:
        return RequestBuilder.preprocess_requested_params(params)

    @staticmethod
    def convert_text_format_to_text_param(text_format: Optional[Union[Type["BaseModel"], dict]], text: Optional["ResponseText"] = None) -> Optional["ResponseText"]:
        return RequestBuilder.convert_text_format(text_format, text)

    @staticmethod
    def _restore_encrypted_content_item_ids_in_input(request_input: Any) -> Any:
        return RequestBuilder.restore_encrypted_inputs(request_input)

    """Response Identity Mutation"""
    @overload
    @staticmethod
    def _update_responses_api_response_id_with_model_id(responses_api_response: ResponsesAPIResponse, custom_llm_provider: Optional[str], litellm_metadata: Optional[Dict[str, Any]] = None) -> ResponsesAPIResponse: ...

    @overload
    @staticmethod
    def _update_responses_api_response_id_with_model_id(responses_api_response: Dict[str, Any], custom_llm_provider: Optional[str], litellm_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]: ...
    
    @staticmethod
    def _update_responses_api_response_id_with_model_id(responses_api_response: Union[ResponsesAPIResponse, Dict[str, Any]], custom_llm_provider: Optional[str], litellm_metadata: Optional[Dict[str, Any]] = None) -> Union[ResponsesAPIResponse, Dict[str, Any]]:
        return IdentityMutator.inject_model_identity(responses_api_response, custom_llm_provider, litellm_metadata)

    @staticmethod
    def _update_encrypted_content_item_ids_in_response(response: Union["ResponsesAPIResponse", Dict[str, Any]], model_id: Optional[str]) -> Union["ResponsesAPIResponse", Dict[str, Any]]:
        IdentityMutator._update_encrypted_content(response, model_id)
        return response

    @staticmethod
    def _update_container_ids_in_response(responses_api_response: Union[ResponsesAPIResponse, Dict[str, Any]], custom_llm_provider: Optional[str], litellm_metadata: Optional[Dict[str, Any]] = None) -> Union[ResponsesAPIResponse, Dict[str, Any]]:
        meta = litellm_metadata or {}
        model_id = meta.get("model_info", {}).get("id")
        IdentityMutator._encode_containers_in_response(responses_api_response, custom_llm_provider, model_id)
        return responses_api_response

    """Tree Traversal (Encoding & Collecting)"""
    @staticmethod
    def _encode_container_id_on_output_item(item: Any, custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        IdentityMutator.encode_item(item, custom_llm_provider, model_id)

    @staticmethod
    def _encode_container_ids_in_message_content(content: Any, custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        if isinstance(content, list):
            for part in content:
                IdentityMutator.encode_message(part, custom_llm_provider, model_id)

    @staticmethod
    def _encode_container_ids_in_annotations(annotations: Any, custom_llm_provider: Optional[str], model_id: Optional[str]) -> None:
        if isinstance(annotations, list):
            for ann in annotations:
                IdentityMutator.encode_item(ann, custom_llm_provider, model_id)

    @staticmethod
    def collect_container_ids_from_responses_response(response: Any) -> list[str]:
        return IdentityMutator.collect_all_ids(response)

    @staticmethod
    def _collect_container_ids_from_output_item(item: Any, collected: set[str]) -> None:
        IdentityMutator.collect_item(item, collected)

    @staticmethod
    def _collect_container_ids_from_message_content(content: Any, collected: set[str]) -> None:
        if isinstance(content, list):
            for part in content:
                annotations = _get_val(part, "annotations")
                if isinstance(annotations, list):
                    for ann in annotations:
                        IdentityMutator.collect_item(ann, collected)

    @staticmethod
    def _collect_container_ids_from_annotations(annotations: Any, collected: set[str]) -> None:
        if isinstance(annotations, list):
            for ann in annotations:
                IdentityMutator.collect_item(ann, collected)

"""@legacy.compat"""
ResponsesAPIRequestUtils = APIBridge