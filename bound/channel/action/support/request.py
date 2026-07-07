# bound.channel.action.support.request
## @lineage: bound.channel.client.action.support.request
## @lineage: anchor.channel.client.action.support.request
## @lineage: anchor.channel.action.support.request
## @lineage: bound.channel.support.api.request
## @lineage: bound.channel.api.request
## @lineage: bound.bridge.api.request
## @lineage: bound.client.api.request
## @lineage: bound.handler.support.utils
import base64
import re
from typing import (
    Any,
    Dict,
    Mapping,
    Iterable,
    List,
    Optional,
    Type,
    Union,
    cast,
    get_type_hints,
    overload,
)
from pydantic import BaseModel
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.config.resolver import config
from bound.channel.action.param.optional import PreProcessNonDefaultParams
from bound.surface.legacy.openai.types import (
    ResponsesAPIOptionalRequestParams,
    ResponsesAPIResponse,
    ResponseText,
)
from bound.channel.response.identity import ResponseIdentityManager
from anchor.provider.param.response import DecodedResponseId
from watcher.plane.emitter import get_emitter

log = get_emitter("responses.utils")

def get_requester_metadata(metadata: dict):
    if not metadata:
        return None

    requester_metadata = metadata.get("requester_metadata")
    if isinstance(requester_metadata, dict):
        cleaned_metadata = add_openai_metadata(requester_metadata)
        if cleaned_metadata:
            return cleaned_metadata

    cleaned_metadata = add_openai_metadata(metadata)
    if cleaned_metadata:
        return cleaned_metadata

    return None

def add_openai_metadata(
    metadata: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, str]]:
    if metadata is None:
        return None
    visible_metadata: Dict[str, str] = {
        str(k): v
        for k, v in metadata.items()
        if k != "hidden_params" and isinstance(v, str)
    }
    if len(visible_metadata) > 16:
        filtered_metadata = {}
        idx = 0
        for k, v in visible_metadata.items():
            if idx < 16:
                filtered_metadata[k] = v
            idx += 1
        visible_metadata = filtered_metadata
    return visible_metadata.copy()

def _apply_openai_param_overrides(
    optional_params: dict, non_default_params: dict, allowed_openai_params: list
):
    if allowed_openai_params:
        for param in allowed_openai_params:
            if param in optional_params:
                continue
            if param not in non_default_params:
                continue
            optional_params[param] = non_default_params.pop(param)
    return optional_params


class ResponsesAPIRequestUtils:
    """Helper utils for constructing ResponseAPI requests"""

    @staticmethod
    def _check_valid_arg(
        supported_params: Optional[List[str]],
        non_default_params: Dict,
        drop_params: Optional[bool],
        custom_llm_provider: Optional[str],
        model: str,
    ):
        if supported_params is None:
            return
        unsupported_params = {}
        for k in non_default_params.keys():
            if k not in supported_params:
                unsupported_params[k] = non_default_params[k]
        if unsupported_params:
            if config.drop_params is True or (
                drop_params is not None and drop_params is True
            ):
                pass
            else:
                raise config.UnsupportedParamsError(
                    status_code=500,
                    message=f"{custom_llm_provider} does not support parameters: {unsupported_params}, for model={model}. To drop these, set `litellm.drop_params=True` or for proxy:\n\n`litellm_settings:\n drop_params: true`\n",
                )

    @staticmethod
    def get_optional_params_responses_api(
        model: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        response_api_optional_params: ResponsesAPIOptionalRequestParams,
        allowed_openai_params: Optional[List[str]] = None,
    ) -> Dict:
        supported_params = responses_api_provider_config.get_supported_openai_params(model)
        non_default_params = cast(Dict, response_api_optional_params)
        ResponsesAPIRequestUtils._check_valid_arg(
            supported_params=supported_params + (allowed_openai_params or []),
            non_default_params=non_default_params,
            drop_params=config.drop_params,
            custom_llm_provider=responses_api_provider_config.custom_llm_provider,
            model=model,
        )

        # Map parameters to provider-specific format
        mapped_params = responses_api_provider_config.map_openai_params(
            response_api_optional_params=response_api_optional_params,
            model=model,
            drop_params=config.drop_params,
        )

        # add any allowed_openai_params to the mapped_params
        mapped_params = _apply_openai_param_overrides(
            optional_params=mapped_params,
            non_default_params=non_default_params,
            allowed_openai_params=allowed_openai_params or [],
        )

        return mapped_params

    @staticmethod
    def get_requested_response_api_optional_param(
        params: Dict[str, Any],
    ) -> ResponsesAPIOptionalRequestParams:
        """
        Filter parameters to only include those defined in ResponsesAPIOptionalRequestParams.

        Args:
            params: Dictionary of parameters to filter

        Returns:
            ResponsesAPIOptionalRequestParams instance with only the valid parameters
        """
        valid_keys = get_type_hints(ResponsesAPIOptionalRequestParams).keys()
        custom_llm_provider = params.pop("custom_llm_provider", None)
        special_params = params.pop("kwargs", {})

        additional_drop_params = params.pop("additional_drop_params", None)
        non_default_params = (
            PreProcessNonDefaultParams.base_pre_process_non_default_params(
                passed_params=params,
                special_params=special_params,
                custom_llm_provider=custom_llm_provider,
                additional_drop_params=additional_drop_params,
                default_param_values={k: None for k in valid_keys},
                additional_endpoint_specific_params=["input"],
            )
        )

        # decode previous_response_id if it's a litellm encoded id
        if "previous_response_id" in non_default_params:
            decoded_previous_response_id = ResponseIdentityManager.decode_previous_response_id_to_original_previous_response_id(
                non_default_params["previous_response_id"]
            )
            non_default_params["previous_response_id"] = decoded_previous_response_id

        if "metadata" in non_default_params:
            converted_metadata = add_openai_metadata(non_default_params["metadata"])
            if converted_metadata is not None:
                non_default_params["metadata"] = converted_metadata
            else:
                non_default_params.pop("metadata", None)

        return cast(ResponsesAPIOptionalRequestParams, non_default_params)

    # fmt: off
    @overload
    @staticmethod
    def _update_responses_api_response_id_with_model_id(
        responses_api_response: ResponsesAPIResponse,
        custom_llm_provider: Optional[str],
        litellm_metadata: Optional[Dict[str, Any]] = None,
    ) -> ResponsesAPIResponse: 
        ...

    @overload
    @staticmethod
    def _update_responses_api_response_id_with_model_id(
        responses_api_response: Dict[str, Any],
        custom_llm_provider: Optional[str],
        litellm_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]: 
        ...

    # fmt: on

    @staticmethod
    def _update_responses_api_response_id_with_model_id(
        responses_api_response: Union[ResponsesAPIResponse, Dict[str, Any]],
        custom_llm_provider: Optional[str],
        litellm_metadata: Optional[Dict[str, Any]] = None,
    ) -> Union[ResponsesAPIResponse, Dict[str, Any]]:
        """Update the responses_api_response_id with model_id and custom_llm_provider.

        Handles both ``ResponsesAPIResponse`` objects and plain dictionaries returned
        by some streaming providers.
        """
        litellm_metadata = litellm_metadata or {}
        model_info: Dict[str, Any] = litellm_metadata.get("model_info", {}) or {}
        model_id = model_info.get("id")

        # access the response id based on the object type
        if isinstance(responses_api_response, dict):
            response_id = responses_api_response.get("id")
        else:
            response_id = getattr(responses_api_response, "id", None)

        # If no response_id, return the response as-is (likely an error response)
        if response_id is None:
            return responses_api_response

        updated_id = ResponseIdentityManager._build_responses_api_response_id(
            model_id=model_id,
            custom_llm_provider=custom_llm_provider,
            response_id=response_id,
        )

        if isinstance(responses_api_response, dict):
            responses_api_response["id"] = updated_id
        else:
            responses_api_response.id = updated_id

        if litellm_metadata.get("encrypted_content_affinity_enabled"):
            responses_api_response = (
                ResponsesAPIRequestUtils._update_encrypted_content_item_ids_in_response(
                    response=responses_api_response,
                    model_id=model_id,
                )
            )

        # Encode container IDs in the response output
        responses_api_response = (
            ResponsesAPIRequestUtils._update_container_ids_in_response(
                responses_api_response=responses_api_response,
                custom_llm_provider=custom_llm_provider,
                litellm_metadata=litellm_metadata,
            )
        )

        return responses_api_response

    @staticmethod
    def _update_encrypted_content_item_ids_in_response(
        response: Union["ResponsesAPIResponse", Dict[str, Any]],
        model_id: Optional[str],
    ) -> Union["ResponsesAPIResponse", Dict[str, Any]]:
        if not model_id:
            return response

        output: Optional[list] = None
        if isinstance(response, dict):
            output = response.get("output")
        else:
            output = getattr(response, "output", None)

        if not isinstance(output, list):
            return response

        for item in output:
            if isinstance(item, dict):
                item_id = item.get("id")
                encrypted_content = item.get("encrypted_content")

                if encrypted_content and isinstance(encrypted_content, str):
                    # Always wrap encrypted_content with model_id for redundancy
                    item["encrypted_content"] = ResponseIdentityManager._wrap_encrypted_content_with_model_id(encrypted_content, model_id)
                    # Also encode the ID if present
                    if item_id and isinstance(item_id, str):
                        item["id"] = ResponseIdentityManager._build_encrypted_item_id(model_id, item_id)
            else:
                item_id = getattr(item, "id", None)
                encrypted_content = getattr(item, "encrypted_content", None)

                if encrypted_content and isinstance(encrypted_content, str):
                    # Always wrap encrypted_content with model_id for redundancy
                    try:
                        item.encrypted_content = ResponseIdentityManager._wrap_encrypted_content_with_model_id(encrypted_content, model_id)
                    except AttributeError:
                        pass
                    # Also encode the ID if present
                    if item_id and isinstance(item_id, str):
                        try:
                            item.id = ResponseIdentityManager._build_encrypted_item_id(model_id, item_id)
                        except AttributeError:
                            pass

        return response

    @staticmethod
    def _restore_encrypted_content_item_ids_in_input(request_input: Any) -> Any:
        """Decode litellm-encoded item IDs in request input back to original IDs.

        Called before forwarding the request to the upstream provider so the
        provider receives the original item IDs and unwrapped encrypted_content.

        Handles both:
        1. Items with encoded IDs (encitem_...)
        2. Items with wrapped encrypted_content (litellm_enc:...)
        """
        if not isinstance(request_input, list):
            return request_input

        for item in request_input:
            if isinstance(item, dict):
                item_id = item.get("id")
                if item_id and isinstance(item_id, str):
                    decoded = ResponseIdentityManager._decode_encrypted_item_id(item_id)
                    if decoded:
                        item["id"] = decoded["item_id"]

                encrypted_content = item.get("encrypted_content")
                if encrypted_content and isinstance(encrypted_content, str):
                    _, unwrapped = ResponseIdentityManager._unwrap_encrypted_content_with_model_id(encrypted_content)
                    if unwrapped != encrypted_content:
                        item["encrypted_content"] = unwrapped

        return request_input

    @staticmethod
    def _encode_container_ids_in_annotations(
        annotations: Any,
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
    ) -> None:
        """Encode ``container_id`` on each annotation (e.g. ``container_file_citation``)."""
        if not annotations or not isinstance(annotations, list):
            return
        for ann in annotations:
            ResponsesAPIRequestUtils._encode_container_id_on_output_item(
                ann,
                custom_llm_provider,
                model_id,
            )

    @staticmethod
    def _encode_container_ids_in_message_content(
        content: Any,
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
    ) -> None:
        """Walk message ``content`` parts and encode citation ``container_id`` values."""
        if not content:
            return
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    ResponsesAPIRequestUtils._encode_container_ids_in_annotations(
                        part.get("annotations"),
                        custom_llm_provider,
                        model_id,
                    )
                else:
                    ResponsesAPIRequestUtils._encode_container_ids_in_annotations(
                        getattr(part, "annotations", None),
                        custom_llm_provider,
                        model_id,
                    )

    @staticmethod
    def _encode_container_id_on_output_item(
        item: Any,
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
    ) -> None:
        if item is None:
            return

        def _maybe_encode(container_id: str) -> Optional[str]:
            decoded = ResponseIdentityManager._decode_container_id(container_id)
            if decoded.get("custom_llm_provider") is not None:
                return None
            return ResponseIdentityManager._build_container_id(
                custom_llm_provider=custom_llm_provider,
                model_id=model_id,
                container_id=container_id,
            )

        if isinstance(item, dict):
            cid = item.get("container_id")
            if isinstance(cid, str):
                enc = _maybe_encode(cid)
                if enc is not None:
                    item["container_id"] = enc
            nested = item.get("code_interpreter_call")
            if isinstance(nested, dict):
                nc = nested.get("container_id")
                if isinstance(nc, str):
                    enc = _maybe_encode(nc)
                    if enc is not None:
                        nested["container_id"] = enc
            if item.get("type") == "message":
                ResponsesAPIRequestUtils._encode_container_ids_in_message_content(
                    item.get("content"),
                    custom_llm_provider,
                    model_id,
                )
            return

        cid_attr = getattr(item, "container_id", None)
        if isinstance(cid_attr, str):
            enc = _maybe_encode(cid_attr)
            if enc is not None:
                try:
                    setattr(item, "container_id", enc)
                except Exception:
                    log.debug(
                        "Could not set container_id on streaming output item",
                        exc_info=True,
                    )

        nested_obj = getattr(item, "code_interpreter_call", None)
        if nested_obj is not None:
            ResponsesAPIRequestUtils._encode_container_id_on_output_item(
                nested_obj,
                custom_llm_provider,
                model_id,
            )

        if getattr(item, "type", None) == "message":
            ResponsesAPIRequestUtils._encode_container_ids_in_message_content(
                getattr(item, "content", None),
                custom_llm_provider,
                model_id,
            )

    @staticmethod
    def _collect_container_ids_from_annotations(
        annotations: Any,
        collected: set[str],
    ) -> None:
        if not annotations or not isinstance(annotations, list):
            return
        for ann in annotations:
            ResponsesAPIRequestUtils._collect_container_ids_from_output_item(
                ann, collected
            )

    @staticmethod
    def _collect_container_ids_from_message_content(
        content: Any,
        collected: set[str],
    ) -> None:
        if not content:
            return
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict):
                    ResponsesAPIRequestUtils._collect_container_ids_from_annotations(
                        part.get("annotations"),
                        collected,
                    )
                else:
                    ResponsesAPIRequestUtils._collect_container_ids_from_annotations(
                        getattr(part, "annotations", None),
                        collected,
                    )

    @staticmethod
    def _collect_container_ids_from_output_item(
        item: Any,
        collected: set[str],
    ) -> None:
        """Collect managed or raw ``container_id`` values from one output item."""
        if item is None:
            return

        if isinstance(item, dict):
            cid = item.get("container_id")
            if isinstance(cid, str) and cid:
                collected.add(cid)
            nested = item.get("code_interpreter_call")
            if isinstance(nested, dict):
                nc = nested.get("container_id")
                if isinstance(nc, str) and nc:
                    collected.add(nc)
            if item.get("type") == "message":
                ResponsesAPIRequestUtils._collect_container_ids_from_message_content(
                    item.get("content"),
                    collected,
                )
            return

        cid_attr = getattr(item, "container_id", None)
        if isinstance(cid_attr, str) and cid_attr:
            collected.add(cid_attr)

        nested_obj = getattr(item, "code_interpreter_call", None)
        if nested_obj is not None:
            ResponsesAPIRequestUtils._collect_container_ids_from_output_item(
                nested_obj, collected
            )

        if getattr(item, "type", None) == "message":
            ResponsesAPIRequestUtils._collect_container_ids_from_message_content(
                getattr(item, "content", None),
                collected,
            )

    @staticmethod
    def collect_container_ids_from_responses_response(response: Any) -> list[str]:
        """Return unique container IDs referenced in a Responses API payload."""
        if response is None:
            return []

        if isinstance(response, dict):
            output = response.get("output", [])
        else:
            output = getattr(response, "output", []) or []

        collected: set[str] = set()
        if output:
            for item in output:
                ResponsesAPIRequestUtils._collect_container_ids_from_output_item(
                    item, collected
                )
        return list(collected)

    @staticmethod
    def _update_container_ids_in_response(
        responses_api_response: Union[ResponsesAPIResponse, Dict[str, Any]],
        custom_llm_provider: Optional[str],
        litellm_metadata: Optional[Dict[str, Any]] = None,
    ) -> Union[ResponsesAPIResponse, Dict[str, Any]]:
        """Encode container IDs in the response output with provider/model info.

        This walks through all output items and encodes any container_id fields
        so that follow-up container API calls can auto-route to the correct provider.
        """
        litellm_metadata = litellm_metadata or {}
        model_info: Dict[str, Any] = litellm_metadata.get("model_info", {}) or {}
        model_id = model_info.get("id")

        # Get the output list
        if isinstance(responses_api_response, dict):
            output = responses_api_response.get("output", [])
        else:
            output = getattr(responses_api_response, "output", [])

        if not output:
            return responses_api_response

        for item in output:
            ResponsesAPIRequestUtils._encode_container_id_on_output_item(
                item=item,
                custom_llm_provider=custom_llm_provider,
                model_id=model_id,
            )

        return responses_api_response

    @staticmethod
    def convert_text_format_to_text_param(
        text_format: Optional[Union[Type["BaseModel"], dict]],
        text: Optional["ResponseText"] = None,
    ) -> Optional["ResponseText"]:
        if text_format is not None and text is None:
            from bound.channel.action.param.format import type_to_response_format_param

            # Convert Pydantic model to response format
            response_format = type_to_response_format_param(text_format)
            if response_format is not None:
                # Create ResponseText object with the format
                # The responses API expects the format to have name at the top level
                text = {
                    "format": {
                        "type": response_format["type"],
                        "name": response_format["json_schema"]["name"],
                        "schema": response_format["json_schema"]["schema"],
                        "strict": response_format["json_schema"]["strict"],
                    }
                }
                return text
        return text