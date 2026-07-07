# bound.transport.channel.response.identity
## @lineage: bound.channel.response.identity
## @lineage: bound.channel.client.response.identity
## @lineage: anchor.channel.client.response.identity
## @lineage: anchor.channel.response.identity
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
from starlette.datastructures import Headers
from bound.bridge.mcp.parser.header import MCPHeaderParser
from anchor.provider.param.response import DecodedResponseId
from bound.surface.legacy.types import SpecialEnums
from watcher.plane.emitter import get_emitter

log = get_emitter("support.identity")

class ResponseIdentityManager:
    @staticmethod
    def _build_container_id(
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
        container_id: str,
    ) -> str:
        """Build a managed container ID with provider and model info encoded.

        Format: cntr_{base64("litellm:custom_llm_provider:{provider};model_id:{model};container_id:{original}")}
        """
        # Avoid serializing Python None as the literal string "None" (breaks router affinity).
        provider_part = "" if custom_llm_provider is None else custom_llm_provider
        model_part = "" if model_id is None else model_id
        assembled_id = f"litellm:custom_llm_provider:{provider_part};model_id:{model_part};container_id:{container_id}"
        base64_encoded_id = base64.b64encode(assembled_id.encode("utf-8")).decode(
            "utf-8"
        )
        return f"cntr_{base64_encoded_id}"

    @staticmethod
    def _build_encrypted_item_id(model_id: str, item_id: str) -> str:
        assembled = f"litellm:model_id:{model_id};item_id:{item_id}"
        encoded = base64.b64encode(assembled.encode("utf-8")).decode("utf-8")
        return f"encitem_{encoded}"
    
    @staticmethod
    def _build_responses_api_response_id(
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
        response_id: str,
    ) -> str:
        """Build the responses_api_response_id"""
        assembled_id: str = str(
            SpecialEnums.LITELLM_MANAGED_RESPONSE_COMPLETE_STR.value
        ).format(custom_llm_provider, model_id, response_id)
        base64_encoded_id: str = base64.b64encode(assembled_id.encode("utf-8")).decode(
            "utf-8"
        )
        return f"resp_{base64_encoded_id}"

    @staticmethod
    def _decode_container_id(container_id: str) -> DecodedResponseId:
        try:
            # If it doesn't start with cntr_, it's not a managed ID
            if not container_id.startswith("cntr_"):
                return DecodedResponseId(
                    custom_llm_provider=None,
                    model_id=None,
                    response_id=container_id,
                )

            # Remove prefix and decode
            cleaned_id = container_id.replace("cntr_", "")
            decoded_id = base64.b64decode(cleaned_id.encode("utf-8")).decode("utf-8")

            # Parse components using regex to handle semicolons in the container_id
            if not decoded_id.startswith("litellm:"):
                return DecodedResponseId(
                    custom_llm_provider=None,
                    model_id=None,
                    response_id=container_id,
                )

            # Use regex to extract the three parts, allowing semicolons in container_id
            # Format: litellm:custom_llm_provider:{provider};model_id:{model};container_id:{container}
            # * for provider/model allows empty segments (missing router model_id).
            pattern = r"^litellm:custom_llm_provider:([^;]*);model_id:([^;]*);container_id:(.+)$"
            match = re.match(pattern, decoded_id)

            if not match:
                return DecodedResponseId(
                    custom_llm_provider=None,
                    model_id=None,
                    response_id=container_id,
                )

            raw_provider = match.group(1)
            raw_model_id = match.group(2)
            custom_llm_provider = None if raw_provider in ("", "None") else raw_provider
            model_id = None if raw_model_id in ("", "None") else raw_model_id
            original_container_id = match.group(3)

            return DecodedResponseId(
                custom_llm_provider=custom_llm_provider,
                model_id=model_id,
                response_id=original_container_id,
            )
        except Exception as e:
            log.debug(f"Error decoding container_id '{container_id}': {e}")
            return DecodedResponseId(
                custom_llm_provider=None,
                model_id=None,
                response_id=container_id,
            )

    
    @staticmethod
    def _decode_encrypted_item_id(encoded_id: str) -> Optional[Dict[str, str]]:
        if not encoded_id.startswith("encitem_"):
            return None
        try:
            cleaned = encoded_id[len("encitem_") :]
            # Restore any padding that may have been stripped in transit
            missing = len(cleaned) % 4
            if missing:
                cleaned += "=" * (4 - missing)
            decoded = base64.b64decode(cleaned.encode("utf-8")).decode("utf-8")
            # Split on first ";" only so that semicolons inside item_id are preserved
            parts = decoded.split(";", 1)
            if len(parts) < 2:
                return None
            model_id = parts[0].replace("litellm:model_id:", "")
            item_id = parts[1].replace("item_id:", "")
            return {"model_id": model_id, "item_id": item_id}
        except Exception:
            return None

    @staticmethod
    def _decode_responses_api_response_id(
        response_id: str,
    ) -> DecodedResponseId:
        try:
            # Remove prefix and decode
            cleaned_id = response_id.replace("resp_", "")
            decoded_id = base64.b64decode(cleaned_id.encode("utf-8")).decode("utf-8")

            # Parse components using known prefixes
            if ";" not in decoded_id:
                return DecodedResponseId(
                    custom_llm_provider=None,
                    model_id=None,
                    response_id=response_id,
                )

            parts = decoded_id.split(";")

            # Format: litellm:custom_llm_provider:{};model_id:{};response_id:{}
            custom_llm_provider = None
            model_id = None

            if (
                len(parts) >= 3
            ):  # Full format with custom_llm_provider, model_id, and response_id
                custom_llm_provider_part = parts[0]
                model_id_part = parts[1]
                response_part = parts[2]

                custom_llm_provider = custom_llm_provider_part.replace(
                    "litellm:custom_llm_provider:", ""
                )
                model_id = model_id_part.replace("model_id:", "")
                decoded_response_id = response_part.replace("response_id:", "")
            else:
                decoded_response_id = response_id

            return DecodedResponseId(
                custom_llm_provider=custom_llm_provider,
                model_id=model_id,
                response_id=decoded_response_id,
            )
        except Exception as e:
            log.debug(f"Error decoding response_id '{response_id}': {e}")
            return DecodedResponseId(
                custom_llm_provider=None,
                model_id=None,
                response_id=response_id,
            )
    
    @staticmethod
    def _unwrap_encrypted_content_with_model_id(
        wrapped_content: str,
    ) -> tuple[Optional[str], str]:
        if not wrapped_content.startswith("litellm_enc:"):
            return None, wrapped_content

        try:
            # Split on first ";" to separate metadata from content
            parts = wrapped_content.split(";", 1)
            if len(parts) < 2:
                return None, wrapped_content

            metadata_b64 = parts[0].replace("litellm_enc:", "")
            original_content = parts[1]

            # Restore padding if needed
            missing = len(metadata_b64) % 4
            if missing:
                metadata_b64 += "=" * (4 - missing)

            decoded_metadata = base64.b64decode(metadata_b64.encode("utf-8")).decode(
                "utf-8"
            )
            model_id = decoded_metadata.replace("model_id:", "")
            return model_id, original_content
        except Exception:
            return None, wrapped_content

    @staticmethod
    def _wrap_encrypted_content_with_model_id(
        encrypted_content: str, model_id: str
    ) -> str:
        """Wrap encrypted_content with model_id metadata for affinity routing.

        When Codex or other clients send items with encrypted_content but no ID,
        we encode the model_id directly into the encrypted_content itself.

        Format: ``litellm_enc:{base64("model_id:{model_id}")};{original_encrypted_content}``
        """
        metadata = f"model_id:{model_id}"
        encoded_metadata = base64.b64encode(metadata.encode("utf-8")).decode("utf-8")
        return f"litellm_enc:{encoded_metadata};{encrypted_content}"


    @staticmethod
    def decode_container_id_to_original(container_id: str) -> str:
        decoded = ResponsesAPIRequestUtils._decode_container_id(container_id)
        return decoded.get("response_id", container_id)

    @staticmethod
    def decode_previous_response_id_to_original_previous_response_id(
        previous_response_id: str,
    ) -> str:
        decoded_response_id = (
            ResponsesAPIRequestUtils._decode_responses_api_response_id(
                previous_response_id
            )
        )
        return decoded_response_id.get("response_id", previous_response_id)

    @staticmethod
    def extract_mcp_headers_from_request(
        secret_fields: Optional[Dict[str, Any]],
        tools: Optional[Iterable[Any]],
    ) -> tuple[
        Optional[str],
        Optional[Dict[str, Dict[str, str]]],
        Optional[Dict[str, str]],
        Optional[Dict[str, str]],
    ]:
        from starlette.datastructures import Headers

        # Extract headers from secret_fields which contains the original request headers
        raw_headers_from_request: Optional[Dict[str, str]] = None
        if secret_fields and isinstance(secret_fields, dict):
            raw_headers_from_request = secret_fields.get("raw_headers")

        mcp_auth_header: Optional[str] = None
        mcp_server_auth_headers: Optional[Dict[str, Dict[str, str]]] = None
        oauth2_headers: Optional[Dict[str, str]] = None

        if raw_headers_from_request:
            headers_obj = Headers(raw_headers_from_request)
            mcp_auth_header = MCPHeaderParser.get_mcp_auth_header(headers_obj)
            mcp_server_auth_headers = MCPHeaderParser.get_mcp_server_auth_headers(headers_obj)
            oauth2_headers = MCPHeaderParser.get_oauth2_headers(headers_obj)

        if tools:
            for tool in tools:
                if isinstance(tool, dict) and tool.get("type") == "mcp":
                    tool_headers = tool.get("headers", {})
                    if tool_headers and isinstance(tool_headers, dict):
                        headers_obj_from_tool = Headers(tool_headers)
                        # 변경점: MCPRequestHandler 대체
                        tool_mcp_server_auth_headers = (
                            MCPHeaderParser.get_mcp_server_auth_headers(headers_obj_from_tool)
                        )
                        
                        if tool_mcp_server_auth_headers:
                            if mcp_server_auth_headers is None:
                                mcp_server_auth_headers = {}
                            for (
                                server_alias,
                                headers_dict,
                            ) in tool_mcp_server_auth_headers.items():
                                if server_alias not in mcp_server_auth_headers:
                                    mcp_server_auth_headers[server_alias] = {}
                                mcp_server_auth_headers[server_alias].update(
                                    headers_dict
                                )
                        
                        if raw_headers_from_request is None:
                            raw_headers_from_request = {}
                        raw_headers_from_request.update(tool_headers)

        return (
            mcp_auth_header,
            mcp_server_auth_headers,
            oauth2_headers,
            raw_headers_from_request,
        )
    
    @staticmethod
    def get_model_id_from_response_id(response_id: Optional[str]) -> Optional[str]:
        """Get the model_id from the response_id"""
        if response_id is None:
            return None
        decoded_response_id = ResponsesAPIRequestUtils._decode_responses_api_response_id(response_id)
        return decoded_response_id.get("model_id") or None