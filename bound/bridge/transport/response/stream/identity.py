# bound.bridge.transport.response.stream.identity
## @lineage: bound.bridge.response.stream.identity
## @lineage: bound.transport.response.stream.identity
## @lineage: bound.transport.stream.api.identity
## @lineage: bound.surface.response.identity
import base64
import re
from typing import Any, Dict, Iterable, Optional, Type
from starlette.datastructures import Headers

from bound.surface.legacy.param.response import DecodedResponseId
from bound.bridge.transport.protocol.mcp.parser.header import MCPHeaderParser
from watcher.plane.emitter import get_emitter

log = get_emitter("response.identity")

SYSTEM_PREFIX = "brane"

## ID 발급 시 사용되는 Prefix 맵
ID_PREFIXES = {
    "CONTAINER": "cntr_",
    "ENCRYPTED_ITEM": "encitem_",
    "RESPONSE": "resp_",
    "ENCRYPTED_META": f"{SYSTEM_PREFIX}_enc:",
}

## 내부 속성 Key 맵
KEYS = {
    "PROVIDER": "custom_llm_provider",
    "MODEL": "model_id",
    "CONTAINER": "container_id",
    "ITEM": "item_id",
    "RESPONSE": "response_id",
}

## 템플릿 문자열 포맷 맵
TEMPLATES = {
    "CONTAINER": f"{SYSTEM_PREFIX}:{KEYS['PROVIDER']}:{{provider}};{KEYS['MODEL']}:{{model}};{KEYS['CONTAINER']}:{{container}}",
    "ENC_ITEM": f"{SYSTEM_PREFIX}:{KEYS['MODEL']}:{{model}};{KEYS['ITEM']}:{{item}}",
    "RESPONSE": f"{SYSTEM_PREFIX}:{KEYS['PROVIDER']}:{{provider}};{KEYS['MODEL']}:{{model}};{KEYS['RESPONSE']}:{{response}}",
}

class IdentityRouter:
    
    @staticmethod
    def _build_container_id(
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
        container_id: str,
    ) -> str:
        provider_part = "" if custom_llm_provider is None else custom_llm_provider
        model_part = "" if model_id is None else model_id
        
        assembled_id = TEMPLATES["CONTAINER"].format(
            provider=provider_part, model=model_part, container=container_id
        )
        base64_encoded_id = base64.b64encode(assembled_id.encode("utf-8")).decode("utf-8")
        return f"{ID_PREFIXES['CONTAINER']}{base64_encoded_id}"

    @staticmethod
    def _build_encrypted_item_id(model_id: str, item_id: str) -> str:
        assembled = TEMPLATES["ENC_ITEM"].format(model=model_id, item=item_id)
        encoded = base64.b64encode(assembled.encode("utf-8")).decode("utf-8")
        return f"{ID_PREFIXES['ENCRYPTED_ITEM']}{encoded}"
    
    @staticmethod
    def _build_responses_api_response_id(
        custom_llm_provider: Optional[str],
        model_id: Optional[str],
        response_id: str,
    ) -> str:
        provider_part = "" if custom_llm_provider is None else custom_llm_provider
        model_part = "" if model_id is None else model_id
        
        assembled_id = TEMPLATES["RESPONSE"].format(
            provider=provider_part, model=model_part, response=response_id
        )
        base64_encoded_id = base64.b64encode(assembled_id.encode("utf-8")).decode("utf-8")
        return f"{ID_PREFIXES['RESPONSE']}{base64_encoded_id}"

    @staticmethod
    def _decode_container_id(container_id: str) -> DecodedResponseId:
        fallback = DecodedResponseId(custom_llm_provider=None, model_id=None, response_id=container_id)
        try:
            if not container_id.startswith(ID_PREFIXES["CONTAINER"]):
                return fallback

            cleaned_id = container_id.replace(ID_PREFIXES["CONTAINER"], "")
            decoded_id = base64.b64decode(cleaned_id.encode("utf-8")).decode("utf-8")

            if not decoded_id.startswith(f"{SYSTEM_PREFIX}:"):
                return fallback

            # Regex 동적 생성
            pattern = rf"^{SYSTEM_PREFIX}:{KEYS['PROVIDER']}:([^;]*);{KEYS['MODEL']}:([^;]*);{KEYS['CONTAINER']}:(.+)$"
            match = re.match(pattern, decoded_id)

            if not match:
                return fallback

            raw_provider, raw_model_id, original_container_id = match.groups()
            return DecodedResponseId(
                custom_llm_provider=None if raw_provider in ("", "None") else raw_provider,
                model_id=None if raw_model_id in ("", "None") else raw_model_id,
                response_id=original_container_id,
            )
        except Exception as e:
            log.debug(f"Error decoding container_id '{container_id}': {e}")
            return fallback
    
    @staticmethod
    def _decode_encrypted_item_id(encoded_id: str) -> Optional[Dict[str, str]]:
        if not encoded_id.startswith(ID_PREFIXES["ENCRYPTED_ITEM"]):
            return None
        try:
            cleaned = encoded_id[len(ID_PREFIXES["ENCRYPTED_ITEM"]) :]
            missing = len(cleaned) % 4
            if missing:
                cleaned += "=" * (4 - missing)
            decoded = base64.b64decode(cleaned.encode("utf-8")).decode("utf-8")
            
            parts = decoded.split(";", 1)
            if len(parts) < 2:
                return None
                
            model_id = parts[0].replace(f"{SYSTEM_PREFIX}:{KEYS['MODEL']}:", "")
            item_id = parts[1].replace(f"{KEYS['ITEM']}:", "")
            return {"model_id": model_id, "item_id": item_id}
        except Exception:
            return None

    @staticmethod
    def _decode_responses_api_response_id(response_id: str) -> DecodedResponseId:
        fallback = DecodedResponseId(custom_llm_provider=None, model_id=None, response_id=response_id)
        try:
            if not response_id.startswith(ID_PREFIXES["RESPONSE"]):
                return fallback
                
            cleaned_id = response_id.replace(ID_PREFIXES["RESPONSE"], "")
            missing = len(cleaned_id) % 4
            if missing:
                cleaned_id += "=" * (4 - missing)
            decoded_id = base64.b64decode(cleaned_id.encode("utf-8")).decode("utf-8")

            if ";" not in decoded_id:
                return fallback

            parts = decoded_id.split(";")
            if len(parts) >= 3:
                custom_llm_provider = parts[0].replace(f"{SYSTEM_PREFIX}:{KEYS['PROVIDER']}:", "")
                model_id = parts[1].replace(f"{KEYS['MODEL']}:", "")
                decoded_response_id = parts[2].replace(f"{KEYS['RESPONSE']}:", "")
            else:
                custom_llm_provider, model_id = None, None
                decoded_response_id = response_id

            return DecodedResponseId(
                custom_llm_provider=custom_llm_provider,
                model_id=model_id,
                response_id=decoded_response_id,
            )
        except Exception as e:
            log.debug(f"Error decoding response_id '{response_id}': {e}")
            return fallback
    
    @staticmethod
    def _unwrap_encrypted_content_with_model_id(wrapped_content: str) -> tuple[Optional[str], str]:
        if not wrapped_content.startswith(ID_PREFIXES["ENCRYPTED_META"]):
            return None, wrapped_content
        try:
            parts = wrapped_content.split(";", 1)
            if len(parts) < 2:
                return None, wrapped_content

            metadata_b64 = parts[0].replace(ID_PREFIXES["ENCRYPTED_META"], "")
            original_content = parts[1]

            missing = len(metadata_b64) % 4
            if missing:
                metadata_b64 += "=" * (4 - missing)

            decoded_metadata = base64.b64decode(metadata_b64.encode("utf-8")).decode("utf-8")
            model_id = decoded_metadata.replace(f"{KEYS['MODEL']}:", "")
            return model_id, original_content
        except Exception:
            return None, wrapped_content

    @staticmethod
    def _wrap_encrypted_content_with_model_id(encrypted_content: str, model_id: str) -> str:
        metadata = f"{KEYS['MODEL']}:{model_id}"
        encoded_metadata = base64.b64encode(metadata.encode("utf-8")).decode("utf-8")
        return f"{ID_PREFIXES['ENCRYPTED_META']}{encoded_metadata};{encrypted_content}"

    @classmethod
    def decode_container_id_to_original(cls, container_id: str) -> str:
        # 버그 수정: ResponsesAPIRequestUtils -> cls 교체
        decoded = cls._decode_container_id(container_id)
        return decoded.response_id if decoded.response_id else container_id

    @classmethod
    def decode_previous_response_id_to_original_previous_response_id(cls, previous_response_id: str) -> str:
        # 버그 수정: ResponsesAPIRequestUtils -> cls 교체
        decoded = cls._decode_responses_api_response_id(previous_response_id)
        return decoded.response_id if decoded.response_id else previous_response_id

    @staticmethod
    def extract_mcp_headers_from_request(
        secret_fields: Optional[Dict[str, Any]], tools: Optional[Iterable[Any]]
    ) -> tuple[Optional[str], Optional[Dict[str, Dict[str, str]]], Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        from starlette.datastructures import Headers

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
                        tool_mcp_server_auth_headers = MCPHeaderParser.get_mcp_server_auth_headers(headers_obj_from_tool)
                        
                        if tool_mcp_server_auth_headers:
                            if mcp_server_auth_headers is None:
                                mcp_server_auth_headers = {}
                            for server_alias, headers_dict in tool_mcp_server_auth_headers.items():
                                if server_alias not in mcp_server_auth_headers:
                                    mcp_server_auth_headers[server_alias] = {}
                                mcp_server_auth_headers[server_alias].update(headers_dict)
                        
                        if raw_headers_from_request is None:
                            raw_headers_from_request = {}
                        raw_headers_from_request.update(tool_headers)

        return mcp_auth_header, mcp_server_auth_headers, oauth2_headers, raw_headers_from_request
    
    @classmethod
    def get_model_id_from_response_id(cls, response_id: Optional[str]) -> Optional[str]:
        if response_id is None:
            return None
        decoded = cls._decode_responses_api_response_id(response_id)
        return decoded.model_id