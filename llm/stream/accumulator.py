# fiber.llm.stream.accumulator
## @lineage: llm.stream.accumulator
import time
from typing import Any, Dict, List, Optional

from xphi.arch.event.next import uuid
from fiber.llm.stream.parser.chunk import ParsedChunk
from fiber.llm.model.types.core import (
    Choices,
    Delta,
    Message,
    ModelResponse,
    Usage,
    Function,
    FunctionCall,
    ChatCompletionMessageToolCall,
)
from fiber.llm.model.types.stream import ModelResponseStream, StreamingChoices
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("stream.accumulator")

class StreamAccumulator:
    def __init__(self, model: str, custom_llm_provider: Optional[str] = None):
        self.model = model
        self.custom_llm_provider = custom_llm_provider

        # 상태 제어 변수
        self.response_uptil_now: str = ""
        self.sent_first_chunk: bool = False
        self.sent_last_chunk: bool = False
        self.received_finish_reason: Optional[str] = None
        
        # Tool Call 누적 맵 (인덱스 기준 병합)
        self.tool_calls_map: Dict[int, Dict[str, Any]] = {}
        self.function_call_accum: Dict[str, str] = {"name": "", "arguments": ""}

        # Incremental Build를 위한 최종 응답(Final Response) 객체 초기화
        # DynamicSurgeModel 덕분에 Field(default_factory)가 적용되어 빈 객체가 안전하게 생성됨
        self.final_response = ModelResponse(
            model=model,
            object="chat.completion",
            created=int(time.time()),
            choices=[Choices(index=0, message=Message(role="assistant", content=""), finish_reason=None)],
            usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        )

    def push(self, parsed: ParsedChunk) -> Optional[ModelResponseStream]:
        """
        정제된 표준 파싱 결과를 받아 상태를 누적하고 스트림용 청크(ModelResponseStream)를 반환합니다.
        """
        # 파싱된 데이터가 유효하지 않으면 Skip
        if not parsed:
            return None

        # -------------------------------------------------------------
        # 1. 속성 추출
        # -------------------------------------------------------------
        chunk_id = parsed.get("id")
        text = parsed.get("text", "")
        tool_calls = parsed.get("tool_calls")
        usage = parsed.get("usage")
        logprobs = parsed.get("logprobs")
        sys_fingerprint = parsed.get("system_fingerprint")
        provider_fields = parsed.get("provider_specific_fields")
        finish_reason = parsed.get("finish_reason")
        is_finished = parsed.get("is_finished", False)

        if finish_reason:
            self.received_finish_reason = finish_reason

        if is_finished and not finish_reason:
            self.received_finish_reason = "stop"

        # -------------------------------------------------------------
        # 2. 최종 응답(Final Response) 점진적 조립 (Incremental Build)
        # -------------------------------------------------------------
        # ID & 메타데이터 누적
        if chunk_id and not getattr(self.final_response, "id", None):
            self.final_response.id = chunk_id
        if sys_fingerprint and not getattr(self.final_response, "system_fingerprint", None):
            self.final_response.system_fingerprint = sys_fingerprint

        message = self.final_response.choices[0].message

        # 텍스트 누적
        if text:
            message.content = (message.content or "") + text
            self.response_uptil_now = message.content

        # Tool Calls 누적
        if tool_calls:
            self._accumulate_tool_calls(message, tool_calls)

        # Usage 누적
        if usage:
            self.final_response.usage = Usage(**usage)

        # Finish Reason 누적
        if self.received_finish_reason:
            self.final_response.choices[0].finish_reason = self.received_finish_reason

        # -------------------------------------------------------------
        # 3. 반환용 스트림 청크(Delta) 생성
        # -------------------------------------------------------------
        delta_kwargs: Dict[str, Any] = {}
        
        # 첫 번째 청크에만 Role 포함
        if not self.sent_first_chunk:
            delta_kwargs["role"] = "assistant"
            self.sent_first_chunk = True

        if text:
            delta_kwargs["content"] = text
        if tool_calls:
            delta_kwargs["tool_calls"] = tool_calls
        if provider_fields:
            delta_kwargs["provider_specific_fields"] = provider_fields

        # 청크가 완전히 비어있고 종료 상태라면 마지막 청크로 표기
        is_delta_empty = not (text or tool_calls or provider_fields)
        if is_delta_empty and is_finished:
            self.sent_last_chunk = True
            delta_kwargs["content"] = None # 명시적 빈 값

        stream_chunk = ModelResponseStream(
            id=chunk_id or self.final_response.id,
            model=self.model,
            system_fingerprint=sys_fingerprint,
            provider_specific_fields=provider_fields,
            choices=[
                StreamingChoices(
                    index=0,
                    delta=Delta(**delta_kwargs),
                    finish_reason=self.received_finish_reason if is_finished else None,
                    logprobs=logprobs,
                )
            ]
        )

        return stream_chunk

    def get_complete_response(self) -> ModelResponse:
        """
        스트림 종료 시 호출되어, 완벽하게 조립이 끝난 단일 ModelResponse 객체를 반환합니다.
        (O(1) 시간 복잡도로 즉시 반환 가능)
        """
        # 스트림이 강제 종료되었더라도 최소한의 finish_reason 보장
        if not self.final_response.choices[0].finish_reason:
            self.final_response.choices[0].finish_reason = self.received_finish_reason or "stop"

        # Tool Call이 발생했는데 텍스트가 없는 경우 content를 None으로 처리 (OpenAI 표준)
        if not self.final_response.choices[0].message.content and getattr(self.final_response.choices[0].message, "tool_calls", None):
            self.final_response.choices[0].message.content = None

        return self.final_response

    # =================================================================
    # Private Helpers
    # =================================================================
    def _accumulate_tool_calls(self, message: Message, delta_tool_calls: List[Any]) -> None:
        """Delta의 파편화된 Tool Call 객체를 인덱스 기반으로 병합합니다."""
        for tc in delta_tool_calls:
            idx = tc.get("index", 0) if isinstance(tc, dict) else getattr(tc, "index", 0)
            
            # 초기화
            if idx not in self.tool_calls_map:
                tc_id = tc.get("id") if isinstance(tc, dict) else getattr(tc, "id", None)
                tc_type = tc.get("type", "function") if isinstance(tc, dict) else getattr(tc, "type", "function")
                
                func_obj = tc.get("function", {}) if isinstance(tc, dict) else getattr(tc, "function", None)
                f_name = func_obj.get("name", "") if isinstance(func_obj, dict) else getattr(func_obj, "name", "")
                
                self.tool_calls_map[idx] = {
                    "id": tc_id or f"call_{uuid.uuid4().hex[:8]}",
                    "type": tc_type,
                    "function": {"name": f_name, "arguments": ""}
                }

            # Arguments 문자열 이어 붙이기
            func_obj = tc.get("function", {}) if isinstance(tc, dict) else getattr(tc, "function", None)
            f_args = func_obj.get("arguments", "") if isinstance(func_obj, dict) else getattr(func_obj, "arguments", "")
            if f_args:
                self.tool_calls_map[idx]["function"]["arguments"] += f_args

        # Map을 객체 리스트로 변환하여 Message에 갱신
        formatted_tcs = []
        for i in sorted(self.tool_calls_map.keys()):
            tc_data = self.tool_calls_map[i]
            formatted_tcs.append(
                ChatCompletionMessageToolCall(
                    id=tc_data["id"],
                    type=tc_data["type"],
                    function=Function(
                        name=tc_data["function"]["name"], 
                        arguments=tc_data["function"]["arguments"]
                    )
                )
            )
        message.tool_calls = formatted_tcs