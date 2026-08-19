# engine.parser.token.evaluator
## @lineage: eco.model.token.counter
## @lineage: engine.model.token.counter
import json
from typing import (
    Any,
    Callable,
    List,
    Literal,
    Mapping,
    Optional,
    Tuple,
    Union,
    cast,
)

from eco.model.types.anthropic import AnthropicMessagesToolResultParam, AnthropicMessagesToolUseParam
from eco.model.types.openai import (
    ChatCompletionNamedToolChoiceParam,
    AllMessageValues, 
    OpenAIMessageContent
)
from ator.client.model.param import ChatCompletionToolParam
from eco.model.types.general import SelectTokenizerResponse
from arch.model.config import config
from eco.bound.agent.adapter.constants import DEFAULT_IMAGE_TOKEN_COUNT

from ator.client.model.param import Message

from watcher.plane.emitter import get_emitter

log = get_emitter("token.counter")

TokenCounterFunction = Callable[[str], int]

class TokenEvaluator:
    def __init__(
        self,
        encode_fn: TokenCounterFunction,
        tokens_per_message: int = 3,
        tokens_per_name: int = 1,
        use_default_image_token_count: bool = False,
        default_token_count: Optional[int] = None,
    ):
        ## @contract: 명시적으로 주입받은 인코더 사용 (전역 참조 탈피)
        self.count_function = encode_fn
        self.tokens_per_message = tokens_per_message
        self.tokens_per_name = tokens_per_name
        self.use_default_img_count = use_default_image_token_count
        self.default_token_count = default_token_count

    # ==========================================
    # Text & Message Counting Logic (순수 수학 연산)
    # ==========================================

    def count_text(self, text: Union[str, List[str]]) -> int:
        if isinstance(text, list):
            text_to_count = "".join(t for t in text if isinstance(t, str))
        else:
            text_to_count = text
        return self.count_function(text_to_count)

    def count_messages(
        self,
        messages: List[AllMessageValues],
        tools: Optional[List[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionNamedToolChoiceParam] = None,
        count_response_tokens: bool = False,
    ) -> int:
        num_tokens = 0
        if not messages:
            return num_tokens

        for message in messages:
            num_tokens += self.tokens_per_message
            for key, value in message.items():
                if value is None:
                    continue
                
                if key == "tool_calls":
                    if isinstance(value, list):
                        for tool_call in value:
                            if "function" in tool_call:
                                func_args = tool_call["function"].get("arguments", [])
                                num_tokens += self.count_function(str(func_args))
                    else:
                        raise ValueError(f"Unsupported type {type(value)} for key tool_calls in message {message}")
                
                elif isinstance(value, str):
                    num_tokens += self.count_function(value)
                    if key == "name":
                        num_tokens += self.tokens_per_name
                
                elif key == "content" and isinstance(value, list):
                    num_tokens += self._count_content_list(value)
                
                elif key == "search_results" and isinstance(value, list):
                    search_results_text = self._extract_search_results_text(value)
                    if search_results_text:
                        num_tokens += self.count_function(search_results_text)

        if not count_response_tokens:
            includes_system = any(msg.get("role") == "system" for msg in messages)
            num_tokens += self._count_extra_tools(tools, tool_choice, includes_system)

        return num_tokens

    def _count_extra_tools(self, tools, tool_choice, includes_system_message: bool) -> int:
        num_tokens = 3 
        if tools:
            num_tokens += self.count_function(self._format_function_definitions(tools))
            num_tokens += 9
            
        if tools and includes_system_message:
            num_tokens -= 4
            
        if tool_choice == "none":
            num_tokens += 1
        elif isinstance(tool_choice, dict):
            num_tokens += 7
            num_tokens += self.count_function(str(tool_choice["function"]["name"]))

        return num_tokens

    def _count_content_list(self, content_list: OpenAIMessageContent) -> int:
        try:
            num_tokens = 0
            for c in content_list:
                if isinstance(c, str):
                    num_tokens += self.count_function(c)
                elif c["type"] == "text":
                    num_tokens += self.count_function(str(c.get("text", "")))
                elif c["type"] == "image_url":
                    # @safety: I/O를 수행하지 않고 메타데이터만으로 계산
                    num_tokens += self._calculate_img_tokens_from_metadata(c.get("image_url", {}))
                elif c["type"] in ("tool_use", "tool_result"):
                    num_tokens += self._count_anthropic_content(c)
                elif c["type"] == "thinking":
                    thinking_text = str(c.get("thinking", ""))
                    if thinking_text:
                        num_tokens += self.count_function(thinking_text)
            return num_tokens
        except Exception as e:
            if self.default_token_count is not None:
                return self.default_token_count
            raise ValueError(f"Error getting number of tokens from content list: {e}")

    def _calculate_img_tokens_from_metadata(self, image_data: Any) -> int:
        """
        @desc: 네트워크 다운로드 없이, 이미 주입된 메타데이터(가로/세로) 기반으로 수학적 연산만 수행합니다.
        """
        if self.use_default_img_count:
            return DEFAULT_IMAGE_TOKEN_COUNT

        detail = "auto"
        if isinstance(image_data, dict):
            detail = image_data.get("detail", "auto")
            
        if detail == "low":
            return 85
            
        # 전처리 단계에서 ext/vision.py를 통해 가로/세로가 채워졌다고 가정
        # 없을 경우 안전하게 1타일 크기의 기본값으로 처리하여 블로킹 방지
        width = image_data.get("width", 512) if isinstance(image_data, dict) else 512
        height = image_data.get("height", 512) if isinstance(image_data, dict) else 512
        
        from eco.model.token.ext.vision import VisionMetadataExtractor
        tiles_needed = VisionMetadataExtractor.calculate_tiles_needed(width, height)
        return 85 + (170 * tiles_needed)

    def _count_anthropic_content(self, content: Mapping[str, Any]) -> int:
        typeddict_cls = self._validate_anthropic_content(content)
        type_hints = getattr(typeddict_cls, "__annotations__", {})
        tokens = 0
        skip_fields = {"type", "id", "tool_use_id", "cache_control", "is_error"}

        for field_name in type_hints.keys():
            if field_name in skip_fields:
                continue
            field_value = content.get(field_name)
            if field_value is None:
                continue
                
            try:
                if isinstance(field_value, str):
                    tokens += self.count_function(field_value)
                elif isinstance(field_value, list):
                    tokens += self._count_content_list(field_value)
                elif isinstance(field_value, dict):
                    tokens += self.count_function(str(field_value))
            except Exception as e:
                if self.default_token_count is not None:
                    return self.default_token_count
                raise ValueError(f"Error counting field '{field_name}': {e}")
        return tokens

    @staticmethod
    def _validate_anthropic_content(content: Mapping[str, Any]) -> type:
        content_type = content.get("type")
        if not content_type:
            raise ValueError("Anthropic content missing required field: 'type'")
        mapping = {
            "tool_use": AnthropicMessagesToolUseParam,
            "tool_result": AnthropicMessagesToolResultParam,
        }
        expected_cls = mapping.get(content_type)
        if expected_cls is None:
            raise ValueError(f"Unknown Anthropic content type: '{content_type}'")
        return expected_cls

    @staticmethod
    def _extract_search_results_text(search_results: object) -> str:
        if not isinstance(search_results, list):
            return ""
        texts = ""
        for result in search_results:
            if not isinstance(result, dict):
                continue
            for key in ("source", "title"):
                value = result.get(key)
                if isinstance(value, str):
                    texts += value
            content = result.get("content")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        text = block.get("text")
                        if isinstance(text, str):
                            texts += text
            
            citations = result.get("citations")
            if citations is not None:
                texts += json.dumps(citations, separators=(",", ":"))
        return texts

    @classmethod
    def _format_function_definitions(cls, tools) -> str:
        lines = ["namespace functions {", ""]
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            function = tool.get("function")
            if not isinstance(function, dict):
                params = tool.get("input_schema") or tool.get("parameters") or {}
                if not isinstance(params, dict):
                    params = {}
                function = {
                    "name": tool.get("name"),
                    "description": tool.get("description"),
                    "parameters": params,
                }
                
            function_name = function.get("name")
            if not function_name:
                continue
                
            if function_description := function.get("description"):
                lines.append(f"// {function_description}")
                
            parameters = function.get("parameters") or {}
            if not isinstance(parameters, dict):
                parameters = {}
                
            properties = parameters.get("properties")
            if properties and properties.keys():
                lines.append(f"type {function_name} = (_: {{")
                lines.append(cls._format_object_parameters(parameters, 0))
                lines.append("}) => any;")
            else:
                lines.append(f"type {function_name} = () => any;")
            lines.append("")
            
        lines.append("} // namespace functions")
        return "\n".join(lines)

    @classmethod
    def _format_object_parameters(cls, parameters, indent) -> str:
        properties = parameters.get("properties")
        if not properties:
            return ""
        required_params = parameters.get("required", [])
        lines = []
        for key, props in properties.items():
            description = props.get("description")
            if description:
                lines.append(f"// {description}")
            question = "" if required_params and key in required_params else "?"
            lines.append(f"{key}{question}: {cls._format_type(props, indent)},")
        return "\n".join([" " * max(0, indent) + line for line in lines])

    @classmethod
    def _format_type(cls, props, indent) -> str:
        obj_type = props.get("type")
        if obj_type == "string":
            return " | ".join([f'"{item}"' for item in props["enum"]]) if "enum" in props else "string"
        elif obj_type == "array":
            return f"{cls._format_type(props['items'], indent)}[]"
        elif obj_type == "object":
            return f"{{\n{cls._format_object_parameters(props, indent + 2)}\n}}"
        elif obj_type in ["integer", "number"]:
            return " | ".join([f'"{item}"' for item in props["enum"]]) if "enum" in props else "number"
        elif obj_type == "boolean":
            return "boolean"
        elif obj_type == "null":
            return "null"
        return "any"


# ==========================================
# External Export Facades (시그니처 유지)
# ==========================================

def token_counter(
    model="",
    custom_tokenizer: Optional[Union[dict, SelectTokenizerResponse]] = None,
    text: Optional[Union[str, List[str]]] = None,
    messages: Optional[List[Union[AllMessageValues, Message]]] = None,
    count_response_tokens: Optional[bool] = False,
    tools: Optional[List[ChatCompletionToolParam]] = None,
    tool_choice: Optional[ChatCompletionNamedToolChoiceParam] = None,
    use_default_image_token_count: Optional[bool] = False,
    default_token_count: Optional[int] = None,
) -> int:
    """
    @desc: 외부 호출을 위한 파사드입니다. 기존 서명을 완벽하게 유지합니다.
    """
    from eco.model.token.convert import convert_list_message_to_dict
    from eco.model.token.encoder import encode # 정규화된 인코더 사용
    
    if text is not None and messages is not None:
        raise ValueError("text and messages cannot both be set")

    # 1. 의존성 격리: Facade에서 전역 설정을 읽어 Evaluator에 주입
    tokens_per_message = 3
    tokens_per_name = 1
    safe_model = model or "gpt-3.5-turbo"
    if "gpt-3.5-turbo-0301" in safe_model:
        tokens_per_message = 4
        tokens_per_name = -1

    # 2. 순수 엔진 초기화 (명시적 인코더 주입)
    evaluator = TokenEvaluator(
        encode_fn=lambda txt: len(encode(model=safe_model, text=txt, custom_tokenizer=custom_tokenizer)),
        tokens_per_message=tokens_per_message,
        tokens_per_name=tokens_per_name,
        use_default_image_token_count=bool(use_default_image_token_count),
        default_token_count=default_token_count,
    )

    if text is not None:
        if tools or tool_choice:
            raise ValueError("tools or tool_choice cannot be set if using text")
        return evaluator.count_text(text)

    elif messages is not None:
        new_messages = cast(List[AllMessageValues], convert_list_message_to_dict(messages))
        return evaluator.count_messages(
            messages=new_messages,
            tools=tools,
            tool_choice=tool_choice,
            count_response_tokens=bool(count_response_tokens),
        )
    else:
        raise ValueError("Either text or messages must be provided")


def get_modified_max_tokens(
    model: str,
    base_model: str,
    messages: Optional[List[AllMessageValues]],
    user_max_tokens: Optional[int],
    buffer_perc: Optional[float],
    buffer_num: Optional[float],
) -> Optional[int]:
    """
    @desc: 기존 서명을 유지하면서 리팩토링된 순수 token_counter를 활용합니다.
    """
    try:
        if user_max_tokens is None:
            return None

        _model_info = config.get_model_info(model=model)
        max_output_tokens = config.get_max_tokens(model=base_model)

        if max_output_tokens is None:
            return user_max_tokens

        input_tokens = token_counter(model=base_model, messages=messages)

        buffer_perc = buffer_perc if buffer_perc is not None else 0.1
        buffer_num = buffer_num if buffer_num is not None else 10.0
        token_buffer = max(buffer_perc * input_tokens, buffer_num)

        input_tokens += int(token_buffer)

        if _model_info.get("max_input_tokens") == max_output_tokens:
            if input_tokens > max_output_tokens:
                pass
            elif user_max_tokens + input_tokens > max_output_tokens:
                user_max_tokens = int(max_output_tokens - input_tokens)
        elif user_max_tokens > max_output_tokens:
            user_max_tokens = max_output_tokens

        return user_max_tokens

    except Exception as e:
        log.debug(f"[token.counter.py] get_modified_max_tokens() - Error: {e}")
        return user_max_tokens

def calculate_fallback_usage(
    model: str,
    messages: List[Union[AllMessageValues, Message]],
    completion_text: str,
    custom_tokenizer: Optional[Union[dict, SelectTokenizerResponse]] = None,
) -> dict[str, int]:
    try:
        prompt_tokens = token_counter(
            model=model, 
            messages=messages, 
            custom_tokenizer=custom_tokenizer
        )
    except Exception as e:
        log.warning(f"[token.counter] Failed to calculate prompt tokens for fallback: {e}")
        prompt_tokens = 0
        
    try:
        completion_tokens = token_counter(
            model=model, 
            text=completion_text, 
            count_response_tokens=True, 
            custom_tokenizer=custom_tokenizer
        )
    except Exception as e:
        log.warning(f"[token.counter] Failed to calculate completion tokens for fallback: {e}")
        completion_tokens = 0
        
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens
    }