# bound.transport.stream.processor.chunk
## @lineage: bound.surface.stream.processor.chunk
## @lineage: bound.transport.stream.processor
import json
import logging
import time
import traceback
from typing import Any, Dict, List, Optional, Union, cast, Tuple
from pydantic import BaseModel

from anchor.registry.model.api.base import get_api_base
from anchor.registry.model.config.resolver import config

from bound.adapter.surface.legacy.openai.types import OpenAIChatCompletionChunk
from bound.adapter.surface.legacy.info import ProviderTypes
from bound.adapter.surface.legacy.param.legacy import GenericLiteLLMParams
from bound.adapter.surface.legacy.types import Delta, GenericStreamingChunk as GChunk

from bound.adapter.switch.params import ModelResponse, ModelResponseStream, StreamingChoices, Usage
from bound.adapter.mapper.reason import map_finish_reason
from bound.transport.adapter.header import process_response_headers
from bound.transport.stream.support import preserve_upstream_non_openai_attributes
from xphi.analyzer.parser.stream.chunk import StreamChunkParser

from arch.gov.gate import uuid
from bound.watcher.delegator import LogDelegator 
from watcher.plane.emitter import get_emitter

AUDIO_ATTRIBUTE = "audio"
IMAGE_ATTRIBUTE = "images"
TOOL_CALLS_ATTRIBUTE = "tool_calls"
FUNCTION_CALL_ATTRIBUTE = "function_call"

log = get_emitter("stream.processor")

class StreamChunkProcessor:
    """
    원시 스트림 청크(Raw Chunk)를 입력받아 시스템의 표준 규격인 ModelResponseStream 객체로 
    정제, 조립, 상태 관리를 전담하는 프로세서 클래스입니다.
    """

    def __init__(
        self,
        model: str,
        custom_llm_provider: Optional[str],
        logging_obj: LogDelegator,
        completion_stream: Any = None,
        _response_headers: Optional[dict] = None,
        chunks_ref: Optional[List] = None,  # ✅ FIX 1: Wrapper의 chunks 참조 주입
    ):
        self.model = model
        self.custom_llm_provider = custom_llm_provider
        self.logging_obj = logging_obj
        self.completion_stream = completion_stream  # Fake streaming(petals 등)을 위한 참조

        # 상태(State) 변수 초기화
        self.sent_first_chunk = False
        self.sent_last_chunk = False
        self.holding_chunk = ""
        self.complete_response = ""
        self.response_uptil_now = ""
        
        self.received_finish_reason: Optional[str] = None
        self.intermittent_finish_reason: Optional[str] = None
        self.system_fingerprint: Optional[str] = None
        self.response_id: Optional[str] = None
        self.created: Optional[int] = None
        
        self.chunks: List = chunks_ref if chunks_ref is not None else []
        self._repeated_messages_count = 1
        self.is_function_call = self._check_is_function_call(logging_obj)
        self.tool_call = False

        litellm_params: GenericLiteLLMParams = GenericLiteLLMParams(
            **self.logging_obj.model_call_details.get("litellm_params", {})
        )
        self.merge_reasoning_content_in_choices: bool = (
            litellm_params.merge_reasoning_content_in_choices or False
        )
        self.sent_first_thinking_block = False
        self.sent_last_thinking_block = False
        
        self.special_tokens = [
            "<|assistant|>", "<|system|>", "<|user|>", 
            "<s>", "</s>", "<|im_end|>", "<|im_start|>"
        ]

        _model_info: Dict = litellm_params.model_info or {}
        _api_base = get_api_base(
            model=model or "",
            optional_params=self.logging_obj.model_call_details.get("litellm_params", {}),
        )

        self._hidden_params = {
            "model_id": (_model_info.get("id", None)),
            "api_base": _api_base,
        }
        self._hidden_params["additional_headers"] = process_response_headers(_response_headers or {})
        
        _cached_logging_provider = self.logging_obj.model_call_details.get("custom_llm_provider", None)
        self._cached_logging_llm_provider: Optional[str] = _cached_logging_provider
        
        _effective_model = model or ""
        if custom_llm_provider == "openai" and custom_llm_provider != _cached_logging_provider:
            _effective_model = "{}/{}".format(_cached_logging_provider, _effective_model)
            
        self._cached_model_name: str = _effective_model
        self._base_hidden_params: Dict[str, Any] = {**self._hidden_params, "response_cost": None}

    ## Core Processing Method (핵심 조립 메서드)
    def process_raw_chunk(self, chunk: Any) -> Optional[ModelResponseStream]:
        """원시 청크를 받아 파싱, 상태 맵핑, 포맷팅 후 표준 규격으로 반환합니다."""
        if hasattr(chunk, "id"):
            self.response_id = chunk.id
            
        model_response = self.model_response_creator()
        completion_obj: Dict[str, Any] = {"content": ""}
        response_obj: Dict[str, Any] = {}

        try:
            # 1.1 Early Bypass (캐시 응답 또는 커스텀 프로바이더의 기파싱된 청크)
            if isinstance(chunk, ModelResponseStream):
                if self.custom_llm_provider == "cached_response":
                    response_obj = self._format_cached_response(chunk)
                elif self.custom_llm_provider in config._custom_providers:
                    return self._handle_custom_provider_passthrough(chunk)

            # 1.2 예외적 스트리밍 벤더 처리
            elif self.custom_llm_provider in ["petals", "palm"]:
                response_obj, completion_obj["content"] = self._handle_fake_streaming()
                
            elif self.custom_llm_provider == "vertex_ai" and not isinstance(chunk, ModelResponseStream):
                response_obj = self._handle_vertex_ai_chunk(chunk, model_response)
                if response_obj is None: return None

            # 1.3 통합 라우팅 파싱 (StreamChunkParser 활용)
            else:
                if self.custom_llm_provider in [ProviderTypes.AZURE.value, ProviderTypes.AZURE_AI.value]:
                    if isinstance(chunk, BaseModel) and hasattr(chunk, "model"):
                        self.model = getattr(chunk, "model", self.model)
                
                parsed = StreamChunkParser.parse(self.custom_llm_provider, chunk)
                if parsed is None:
                    return None
                response_obj = parsed

            # 1.4 공통 상태 맵핑
            completion_obj["content"] = response_obj.get("text", "")
            
            if response_obj.get("is_finished"):
                if response_obj.get("finish_reason") == "error":
                    raise Exception(f"{self.custom_llm_provider} streaming error. Chunk={response_obj}")
                self.received_finish_reason = response_obj.get("finish_reason", "stop")
            else:
                self.intermittent_finish_reason = response_obj.get("finish_reason")

            # 파싱된 메타데이터(usage, logprobs 등) 적용
            self._apply_parsed_metadata(model_response, response_obj)

            # 1.5 Tool Call / Function Call 포맷팅
            tool_calls = response_obj.get("tool_calls")
            if tool_calls and len(tool_calls) > 0:
                if self.is_function_call:
                    completion_obj["function_call"] = tool_calls[0]["function"]
                    completion_obj["tool_calls"] = None
                else:
                    completion_obj["tool_calls"] = tool_calls
                self.tool_call = True

            # 1.6 최종 반환 로직 위임
            return self.return_processed_chunk_logic(
                completion_obj=completion_obj,
                model_response=model_response, 
                response_obj=response_obj,
            )

        except StopIteration:
            raise StopIteration
        except Exception as e:
            traceback.format_exc()
            log.exception(f"Error processing chunk: {str(e)}")
            raise e

    ## 메타데이터 및 상태 매핑 헬퍼
    def _apply_parsed_metadata(self, model_response: ModelResponseStream, response_obj: Dict[str, Any]):
        if response_obj.get("logprobs") is not None:
            model_response.choices[0].logprobs = response_obj["logprobs"]

        usage = response_obj.get("usage")
        if usage is not None:
            if isinstance(usage, dict):
                model_response.usage = config.Usage(
                    prompt_tokens=usage.get("prompt_tokens"),
                    completion_tokens=usage.get("completion_tokens"),
                    total_tokens=usage.get("total_tokens")
                )
            elif isinstance(usage, Usage):
                model_response.usage = usage
            elif isinstance(usage, BaseModel):
                model_response.usage = config.Usage(**usage.model_dump())

        original_chunk = response_obj.get("original_chunk")
        if original_chunk is not None:
            if hasattr(original_chunk, "id"):
                self.set_model_id(original_chunk.id, model_response)
            if hasattr(original_chunk, "system_fingerprint"):
                model_response.system_fingerprint = original_chunk.system_fingerprint
                self.system_fingerprint = original_chunk.system_fingerprint
            if hasattr(original_chunk, "provider_specific_fields"):
                self.copy_model_response_level_provider_specific_fields(original_chunk, model_response)

    def finish_reason_handler(self) -> ModelResponseStream:
        model_response = self.model_response_creator()
        _finish_reason = self.received_finish_reason or self.intermittent_finish_reason
        
        if _finish_reason is not None:
            model_response.choices[0].finish_reason = _finish_reason
        else:
            model_response.choices[0].finish_reason = "stop"

        if model_response.choices[0].finish_reason == "stop" and self.tool_call:
            model_response.choices[0].finish_reason = "tool_calls"
            
        return model_response

    ## 특이 벤더(Edge cases) 전처리 헬퍼
    def _handle_fake_streaming(self) -> Tuple[Dict[str, Any], str]:
        if self.completion_stream is None or len(self.completion_stream) == 0:
            if self.received_finish_reason is not None:
                raise StopIteration
            self.received_finish_reason = "stop"
            
        chunk_size = 30
        new_chunk = self.completion_stream[:chunk_size]
        self.completion_stream = self.completion_stream[chunk_size:]
        
        return {"text": new_chunk, "is_finished": False, "finish_reason": None}, new_chunk

    def _format_cached_response(self, chunk: ModelResponseStream) -> Dict[str, Any]:
        return {
            "text": chunk.choices[0].delta.content or "",
            "is_finished": True,
            "finish_reason": chunk.choices[0].finish_reason,
            "original_chunk": chunk,
            "tool_calls": getattr(chunk.choices[0].delta, "tool_calls", None),
        }

    def _handle_custom_provider_passthrough(self, chunk: ModelResponseStream) -> Optional[ModelResponseStream]:
        _has_content = bool(
            chunk.choices and chunk.choices[0].delta is not None and 
            (chunk.choices[0].delta.content or chunk.choices[0].delta.tool_calls)
        )
        if self.received_finish_reason is not None and not _has_content:
            raise StopIteration
            
        if chunk.choices and chunk.choices[0].finish_reason:
            self.received_finish_reason = chunk.choices[0].finish_reason
            if not _has_content:
                return None
            chunk.choices[0].finish_reason = None
        return chunk

    def _handle_vertex_ai_chunk(self, chunk: Any, model_response: ModelResponseStream) -> Optional[Dict[str, Any]]:
        import proto # type: ignore
        response_obj = {"text": "", "is_finished": False, "finish_reason": None}
        
        if not hasattr(chunk, "candidates"):
            response_obj["text"] = str(chunk)
            return response_obj

        try:
            response_obj["text"] = chunk.text
        except Exception as e:
            if "Part has no text." in str(e):
                function_call = chunk.candidates[0].content.parts[0].function_call
                args_dict = {}
                for key, val in function_call.args.items():
                    if isinstance(val, proto.marshal.collections.repeated.RepeatedComposite):
                        args_dict[key] = [v for v in val]
                    else:
                        args_dict[key] = val
                        
                args_str = json.dumps(args_dict)
                _delta_obj = config.utils.Delta(
                    content=None,
                    tool_calls=[{
                        "id": f"call_{str(uuid.uuid4())}",
                        "function": {"arguments": args_str, "name": function_call.name},
                        "type": "function",
                    }],
                )
                response_obj["original_chunk"] = ModelResponseStream(
                    choices=[StreamingChoices(delta=_delta_obj)]
                )
            else:
                raise e

        candidate = chunk.candidates[0]
        if hasattr(candidate, "finish_reason") and candidate.finish_reason.name != "FINISH_REASON_UNSPECIFIED":
            if candidate.finish_reason.name == "SAFETY":
                raise Exception(f"Blocked by VertexAI Safety. {str(chunk)}")
            response_obj["is_finished"] = True
            response_obj["finish_reason"] = map_finish_reason(candidate.finish_reason.name)

        return response_obj

    ## 상태 조작 및 속성 확인 유틸리티
    def model_response_creator(self, chunk: Optional[dict] = None, hidden_params: Optional[dict] = None) -> ModelResponseStream:
        _model = self._cached_model_name
        _logging_obj_llm_provider = self._cached_logging_llm_provider

        args: Dict[str, Any] = {"model": _model}
        if chunk is not None:
            chunk.pop("model", None)
            args.update({k: v for k, v in chunk.items() if k != "stream"})

        model_response = ModelResponseStream(**args)
        
        if self.response_id is not None:
            model_response.id = self.response_id
        if self.system_fingerprint is not None:
            model_response.system_fingerprint = self.system_fingerprint

        if self.created is not None:
            model_response.created = self.created
        else:
            self.created = model_response.created

        model_response._hidden_params = {
            **(hidden_params or {}),
            "custom_llm_provider": _logging_obj_llm_provider,
            "created_at": time.time(),
            **self._base_hidden_params,
        }

        if not (len(model_response.choices) > 0 and getattr(model_response.choices[0], "delta") is not None):
            model_response.choices = [StreamingChoices(finish_reason=None)]
            
        return model_response

    def return_processed_chunk_logic(self, completion_obj: Dict[str, Any], model_response: ModelResponseStream, response_obj: Dict[str, Any]) -> Optional[ModelResponseStream]:
        is_chunk_non_empty = self.is_chunk_non_empty(completion_obj, model_response, response_obj)

        if is_chunk_non_empty:
            self.raise_on_model_repetition()
            hold, model_response_str = self.check_special_tokens(
                chunk=completion_obj["content"],
                finish_reason=model_response.choices[0].finish_reason,
            )

            if hold is False:
                original_chunk = response_obj.get("original_chunk", None)
                if original_chunk:
                    if len(original_chunk.choices) > 0:
                        choices = []
                        for choice in original_chunk.choices:
                            try:
                                if isinstance(choice, BaseModel):
                                    choice_json = choice.model_dump()
                                    choice_json.pop("finish_reason", None)
                                    choices.append(StreamingChoices(**choice_json))
                            except Exception:
                                choices.append(StreamingChoices())
                        setattr(model_response, "choices", choices)
                    else:
                        return None
                        
                    model_response.system_fingerprint = original_chunk.system_fingerprint
                    setattr(model_response, "citations", getattr(original_chunk, "citations", None))
                    preserve_upstream_non_openai_attributes(model_response=model_response, original_chunk=original_chunk)
                    model_response = self.strip_role_from_delta(model_response)
                else:
                    completion_obj["content"] = model_response_str
                    if self.sent_first_chunk is False:
                        completion_obj["role"] = "assistant"
                        self.sent_first_chunk = True
                    if response_obj.get("provider_specific_fields") is not None:
                        completion_obj["provider_specific_fields"] = response_obj["provider_specific_fields"]
                        
                    model_response.choices[0].delta = Delta(**completion_obj)
                    _index: Optional[int] = completion_obj.get("index")
                    if _index is not None:
                        model_response.choices[0].index = _index

                self._optional_combine_thinking_block_in_choices(model_response=model_response)
                return model_response
            else:
                return None
                
        elif self.received_finish_reason is not None:
            if self.sent_last_chunk is True:
                if self.custom_llm_provider == "bedrock" and "trace" in model_response:
                    return model_response
                
                if hasattr(model_response, "usage"):
                    self.chunks.append(model_response)
                    return model_response
                return None
                
            if len(self.holding_chunk) > 0:
                if model_response.choices[0].delta.content is None:
                    model_response.choices[0].delta.content = self.holding_chunk
                else:
                    model_response.choices[0].delta.content = self.holding_chunk + model_response.choices[0].delta.content
                self.holding_chunk = ""

            _is_delta_empty = self.is_delta_empty(delta=model_response.choices[0].delta)
            _original_chunk = response_obj.get("original_chunk", None)
            if _original_chunk is not None:
                preserve_upstream_non_openai_attributes(model_response=model_response, original_chunk=_original_chunk)

            if _is_delta_empty:
                model_response.choices[0].delta = Delta(content=None)
                model_response.choices[0].finish_reason = map_finish_reason(finish_reason=self.received_finish_reason)
                self.sent_last_chunk = True

            return model_response
            
        elif self._has_special_delta_content(model_response):
            return self._handle_special_delta_content(model_response)
        else:
            if hasattr(model_response, "usage"):
                self.chunks.append(model_response)
            return None

    def _check_is_function_call(self, logging_obj) -> bool:
        if hasattr(logging_obj, "optional_params") and isinstance(logging_obj.optional_params, dict):
            optional_params = logging_obj.optional_params
            if "functions" in optional_params and optional_params.get("functions"):
                return True
        return False

    def is_chunk_non_empty(self, completion_obj: Dict[str, Any], model_response: ModelResponseStream, response_obj: Dict[str, Any]) -> bool:
        delta = model_response.choices[0].delta
        return (
            ("content" in completion_obj and isinstance(completion_obj["content"], str) and len(completion_obj["content"]) > 0) or
            ("tool_calls" in completion_obj and completion_obj["tool_calls"] is not None and len(completion_obj["tool_calls"]) > 0) or
            ("function_call" in completion_obj and completion_obj["function_call"] is not None) or
            (hasattr(delta, "tool_calls") and delta.tool_calls is not None and len(delta.tool_calls) > 0) or
            (hasattr(delta, "function_call") and delta.function_call is not None) or
            (hasattr(delta, "reasoning_content") and delta.reasoning_content is not None) or
            (hasattr(delta, "provider_specific_fields") and delta.provider_specific_fields is not None) or
            ("provider_specific_fields" in response_obj and response_obj["provider_specific_fields"] is not None) or
            (hasattr(delta, "annotations") and delta.annotations is not None) or
            (not self.sent_first_chunk and hasattr(delta, "role") and delta.role is not None) or
            (getattr(delta, "reasoning_items", None) is not None)
        )

    def is_delta_empty(self, delta: Delta) -> bool:
        if delta.content or delta.tool_calls is not None or delta.function_call is not None:
            return False
        return True

    def set_model_id(self, id: str, model_response: ModelResponseStream) -> ModelResponseStream:
        if self.response_id is None and id and isinstance(id, str) and id.strip():
            self.response_id = id
        if id and isinstance(id, str) and id.strip():
            model_response._hidden_params["received_model_id"] = id
        if self.response_id is not None and isinstance(self.response_id, str):
            model_response.id = self.response_id
        return model_response

    def copy_model_response_level_provider_specific_fields(self, original_chunk: Union[ModelResponseStream, OpenAIChatCompletionChunk], model_response: ModelResponseStream) -> ModelResponseStream:
        provider_specific_fields = getattr(original_chunk, "provider_specific_fields", None)
        if provider_specific_fields is not None:
            model_response.provider_specific_fields = provider_specific_fields
            for k, v in provider_specific_fields.items():
                setattr(model_response, k, v)
        return model_response

    def strip_role_from_delta(self, model_response: ModelResponseStream) -> ModelResponseStream:
        if self.sent_first_chunk is False:
            model_response.choices[0].delta["role"] = "assistant"
            self.sent_first_chunk = True
        elif self.sent_first_chunk is True and hasattr(model_response.choices[0].delta, "role"):
            _initial_delta = model_response.choices[0].delta.model_dump()
            _initial_delta.pop("role", None)
            model_response.choices[0].delta = Delta(**_initial_delta)
        return model_response

    def _optional_combine_thinking_block_in_choices(self, model_response: ModelResponseStream) -> None:
        if self.merge_reasoning_content_in_choices is True:
            reasoning_content = getattr(model_response.choices[0].delta, "reasoning_content", None)
            if reasoning_content:
                if self.sent_first_thinking_block is False:
                    if model_response.choices[0].delta.content is None:
                        model_response.choices[0].delta.content = ""
                    model_response.choices[0].delta.content += "<think>" + reasoning_content
                    self.sent_first_thinking_block = True
                elif self.sent_first_thinking_block is True and getattr(model_response.choices[0].delta, "reasoning_content", None):
                    model_response.choices[0].delta.content = reasoning_content
            elif self.sent_first_thinking_block is True and not self.sent_last_thinking_block and model_response.choices[0].delta.content:
                model_response.choices[0].delta.content = "</think>" + (model_response.choices[0].delta.content or "")
                self.sent_last_thinking_block = True

            if hasattr(model_response.choices[0].delta, "reasoning_content"):
                del model_response.choices[0].delta.reasoning_content

    def check_special_tokens(self, chunk: str, finish_reason: Optional[str]):
        hold = False
        if self.custom_llm_provider != "sagemaker":
            return hold, chunk

        if finish_reason:
            for token in self.special_tokens:
                if token in chunk:
                    chunk = chunk.replace(token, "")
            return hold, chunk

        if self.sent_first_chunk is True:
            return hold, chunk

        curr_chunk = (self.holding_chunk + chunk).strip()
        for token in self.special_tokens:
            if len(curr_chunk) < len(token) and curr_chunk in token:
                hold = True
                self.holding_chunk = curr_chunk
            elif len(curr_chunk) >= len(token):
                if token in curr_chunk:
                    self.holding_chunk = curr_chunk.replace(token, "")
                    hold = True

        if hold is False:
            self.holding_chunk = ""
        return hold, curr_chunk

    def raise_on_model_repetition(self) -> None:
        if len(self.chunks) < 2: return
        last_content = self.chunks[-1].choices[0].delta.content
        if last_content is None or not isinstance(last_content, str) or len(last_content) <= 2:
            self._repeated_messages_count = 1
            return

        second_to_last_content = self.chunks[-2].choices[0].delta.content
        if last_content == second_to_last_content:
            self._repeated_messages_count += 1
        else:
            self._repeated_messages_count = 1

        limit = getattr(config, "REPEATED_STREAMING_CHUNK_LIMIT", 100)
        if self._repeated_messages_count >= limit:
            raise config.InternalServerError(
                message=f"The model is repeating the same chunk = {last_content}.",
                model="",
                llm_provider="",
            )

    def _has_special_delta_content(self, model_response: ModelResponseStream) -> bool:
        if len(model_response.choices) == 0: return False
        delta = model_response.choices[0].delta
        if getattr(delta, TOOL_CALLS_ATTRIBUTE, None) is not None or getattr(delta, FUNCTION_CALL_ATTRIBUTE, None) is not None:
            return True
        if hasattr(delta, AUDIO_ATTRIBUTE) and getattr(delta, AUDIO_ATTRIBUTE, None) is not None:
            return True
        if hasattr(delta, IMAGE_ATTRIBUTE) and getattr(delta, IMAGE_ATTRIBUTE, None) is not None:
            return True
        return False

    def _handle_special_delta_content(self, model_response: ModelResponseStream) -> ModelResponseStream:
        return self.strip_role_from_delta(model_response)

    def _has_special_delta_attribute(self, delta, attribute_name: str) -> bool:
        return delta is not None and getattr(delta, attribute_name, None) is not None

    def _copy_delta_attribute(self, source_delta, target_delta, attribute_name: str) -> None:
        setattr(target_delta, attribute_name, getattr(source_delta, attribute_name))

    def _has_any_special_delta_attributes(self, delta) -> bool:
        for attribute in [AUDIO_ATTRIBUTE, IMAGE_ATTRIBUTE]:
            if self._has_special_delta_attribute(delta, attribute): return True
        return False

    def _handle_special_delta_attributes(self, delta, model_response: "ModelResponseStream") -> None:
        for attribute in [AUDIO_ATTRIBUTE, IMAGE_ATTRIBUTE]:
            if self._has_special_delta_attribute(delta, attribute):
                self._copy_delta_attribute(delta, model_response.choices[0].delta, attribute)