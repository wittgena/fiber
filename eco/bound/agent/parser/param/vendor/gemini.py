# engine.parser.param.vendor.gemini
## @lineage: xor.parser.param.vendor.gemini
## @lineage: engine.config.llm.gemini
from typing import Dict, Any, List

class GoogleAIStudioGeminiConfig:
    def get_supported_openai_params(self, model: str) -> List[str]:
        return [
            "temperature", "top_p", "max_tokens", "max_completion_tokens",
            "stream", "tools", "tool_choice", "response_format", "n", "stop",
            "presence_penalty", "frequency_penalty"
        ]

    def map_openai_params(self, non_default_params: dict, optional_params: dict, model: str, drop_params: bool) -> dict:
        mapping = {
            "max_tokens": "max_output_tokens",
            "max_completion_tokens": "max_output_tokens",
            "stop": "stop_sequences"
        }
        
        for oa_key, gemini_key in mapping.items():
            if oa_key in non_default_params:
                optional_params[gemini_key] = non_default_params.pop(oa_key)

        if "tools" in non_default_params:
            tools = non_default_params.pop("tools")
            cleaned_tools = self._clean_and_reshape_tools(tools)
            if cleaned_tools:
                optional_params["tools"] = cleaned_tools

        if "tool_choice" in non_default_params:
            tc = non_default_params.pop("tool_choice")
            if tc != "auto" and isinstance(tc, dict) and tc.get("function", {}).get("name"):
                optional_params["tool_config"] = {
                    "function_calling_config": {
                        "mode": "ANY",
                        "allowed_function_names": [tc["function"]["name"]]
                    }
                }
        
        return optional_params

    def _clean_and_reshape_tools(self, tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """OpenAPI 스키마의 불순물을 제거하고, Gemini가 요구하는 [{"function_declarations": [...]}] 포맷으로 감싸서 반환"""
        function_declarations = []
        for t in tools:
            if t.get("type") == "function" and "function" in t:
                func = dict(t["function"]) 
                func.pop("summary", None)
                params = func.get("parameters", {})
                if params:
                    self._convert_schema_to_gemini_strict(params)
                    func["parameters"] = params
                    
                function_declarations.append(func)
                
        if function_declarations:
            return [{"function_declarations": function_declarations}]
            
        return []

    def _convert_schema_to_gemini_strict(self, schema: Any):
        """OpenAPI 스키마를 Gemini C++ 백엔드가 허용하는 Strict Protobuf 규격으로 치환"""
        if not isinstance(schema, dict):
            return
        
        if "type" in schema and isinstance(schema["type"], str):
            schema["type"] = schema["type"].upper()
        
        schema.pop("summary", None)
        schema.pop("title", None) 
        schema.pop("required", None)
        schema.pop("additionalProperties", None)
        
        for key, value in schema.items():
            if isinstance(value, dict):
                self._convert_schema_to_gemini_strict(value)
            elif isinstance(value, list):
                for item in value:
                    self._convert_schema_to_gemini_strict(item)