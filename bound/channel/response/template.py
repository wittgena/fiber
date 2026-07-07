# bound.channel.response.template
## @lineage: bound.channel.client.response.template
## @lineage: anchor.channel.client.response.template
import base64
import json
from typing import Any, Dict, List, Optional, Union
from watcher.plane.emitter import get_emitter

log = get_emitter("response.template")

class TemplateDecoder:
    @staticmethod
    def is_base64_encoded_unified_id(uid: str) -> bool:
        if not isinstance(uid, str) or not uid:
            return False
        try:
            decoded_bytes = base64.b64decode(uid, validate=True)
            decoded_str = decoded_bytes.decode("utf-8")
            return "llm_output_file_id," in decoded_str or "provider_resource_id" in decoded_str
        except Exception:
            return False

    @staticmethod
    def decode_file_id(uid: str) -> str:
        try:
            return base64.b64decode(uid).decode("utf-8")
        except Exception:
            return uid

    @staticmethod
    def parse_vector_store_id(uid: str) -> dict:
        try:
            decoded = base64.b64decode(uid).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            return {}


class ResponseTemplateProcessor:
    """Response Template의 Input 및 Tools 객체를 변환하는 프로세서"""
    def __init__(
        self, 
        model_id: Optional[str] = None, 
        model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None
    ):
        self.model_id = model_id
        self.mapping = model_file_id_mapping or {}

    def process_input(self, input_data: Any) -> Union[str, List[Dict[str, Any]]]:
        if isinstance(input_data, str) or not isinstance(input_data, list):
            return input_data

        updated_input = []
        for item in input_data:
            if not isinstance(item, dict):
                updated_input.append(item)
                continue

            updated_item = item.copy()
            content = item.get("content")

            if isinstance(content, list):
                updated_item["content"] = self._process_input_content(content)

            updated_input.append(updated_item)
        
        return updated_input

    def _process_input_content(self, content: List[Any]) -> List[Any]:
        updated_content = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "input_file":
                file_id = item.get("file_id")
                if file_id:
                    updated_item = item.copy()
                    updated_item["file_id"] = self._resolve_input_file_id(file_id)
                    updated_content.append(updated_item)
                    continue
            
            updated_content.append(item)
        return updated_content

    def _resolve_input_file_id(self, file_id: str) -> str:
        # 1. 맵핑 테이블에 존재하는지 확인
        if self.model_id and file_id in self.mapping:
            return self.mapping[file_id].get(self.model_id) or file_id

        # 2. Base64로 인코딩된 Unified ID인지 확인 및 디코딩
        if TemplateDecoder.is_base64_encoded_unified_id(file_id):
            decoded_str = TemplateDecoder.decode_file_id(file_id)
            if "llm_output_file_id," in decoded_str:
                return decoded_str.split("llm_output_file_id,")[1].split(";")[0]

        # 3. 매칭되지 않으면 원본 반환
        return file_id

    def process_tools(self, tools: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
        if not tools or not isinstance(tools, list):
            return tools

        # Pass 1: decode unified vector_store_ids (조건 없이 항상 실행)
        updated_tools = self._decode_vector_store_ids(tools)

        # Pass 2: mapping이 필요한 code_interpreter 처리
        if not self.mapping or not self.model_id:
            return updated_tools

        final_tools = []
        for tool in updated_tools:
            if not isinstance(tool, dict) or tool.get("type") != "code_interpreter":
                final_tools.append(tool)
                continue

            updated_tool = tool.copy()
            container = tool.get("container")

            if isinstance(container, dict) and isinstance(container.get("file_ids"), list):
                updated_container = container.copy()
                updated_container["file_ids"] = [
                    self._resolve_tool_file_id(fid) for fid in container.get("file_ids")
                ]
                updated_tool["container"] = updated_container
            
            final_tools.append(updated_tool)
            
        return final_tools

    def _resolve_tool_file_id(self, file_id: Any) -> Any:
        if isinstance(file_id, str) and file_id in self.mapping:
            return self.mapping[file_id].get(self.model_id) or file_id
        return file_id

    def _decode_vector_store_ids(self, tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        updated_tools = []
        for tool in tools:
            if not isinstance(tool, dict) or tool.get("type") != "file_search":
                updated_tools.append(tool)
                continue

            vector_store_ids = tool.get("vector_store_ids")
            if not isinstance(vector_store_ids, list):
                updated_tools.append(tool)
                continue

            decoded_ids = []
            for vs_id in vector_store_ids:
                if isinstance(vs_id, str) and TemplateDecoder.is_base64_encoded_unified_id(vs_id):
                    parsed_dict = TemplateDecoder.parse_vector_store_id(vs_id)
                    provider_resource_id = parsed_dict.get("provider_resource_id")

                    if provider_resource_id:
                        decoded_ids.append(provider_resource_id)
                    else:
                        log.warning("file_search tool contains unified vector_store_id '%s' that could not be parsed.", vs_id)
                        decoded_ids.append(vs_id)
                else:
                    decoded_ids.append(vs_id)

            updated_tool = tool.copy()
            updated_tool["vector_store_ids"] = decoded_ids
            updated_tools.append(updated_tool)

        return updated_tools


## @public.api: 기존 시그니처를 유지하여 외부 호환성을 보장
def update_responses_input_with_model_file_ids(
    input: Any,
    model_id: Optional[str] = None,
    model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None,
) -> Union[str, List[Dict[str, Any]]]:
    processor = ResponseTemplateProcessor(model_id, model_file_id_mapping)
    return processor.process_input(input)


def update_responses_tools_with_model_file_ids(
    tools: Optional[List[Dict[str, Any]]],
    model_id: Optional[str] = None,
    model_file_id_mapping: Optional[Dict[str, Dict[str, str]]] = None,
) -> Optional[List[Dict[str, Any]]]:
    processor = ResponseTemplateProcessor(model_id, model_file_id_mapping)
    return processor.process_tools(tools)