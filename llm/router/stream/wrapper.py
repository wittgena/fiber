# fiber.llm.router.stream.wrapper
import asyncio
import collections.abc
import time
import anyio
import httpx
from typing import Any, AsyncIterator, Callable, Optional, NoReturn, Dict, List

from xphi.arch.bound.client.constants import MAX_STREAMING_DURATION_SECONDS
from xphi.arch.contract.config.resolver import config
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.event.next import uuid

from fiber.llm.exception.mapping import exception_type
from fiber.llm.exception.eco import OpenAIError, APIResponseValidationError, MidStreamFallbackError
from fiber.llm.response import ModelResponseStream
from fiber.llm.router.stream.parser.chunk import StreamChunkParser, ParsedChunk
from fiber.llm.types.provider.core import (
    Choices, Delta, Message, ModelResponse, Usage, Function,
    ChatCompletionMessageToolCall
)
from fiber.llm.types.provider.stream import StreamingChoices

log = get_emitter("stream.wrapper")

class StreamAccumulator:
    def __init__(self, model: str, custom_llm_provider: Optional[str] = None):
        self.model = model
        self.custom_llm_provider = custom_llm_provider

        self.response_uptil_now: str = ""
        self.sent_first_chunk: bool = False
        self.sent_last_chunk: bool = False
        self.received_finish_reason: Optional[str] = None
        
        self.tool_calls_map: Dict[int, Dict[str, Any]] = {}
        self.function_call_accum: Dict[str, str] = {"name": "", "arguments": ""}
        self.final_response = ModelResponse(
            model=model,
            object="chat.completion",
            created=int(time.time()),
            choices=[Choices(index=0, message=Message(role="assistant", content=""), finish_reason=None)],
            usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        )

    def push(self, parsed: ParsedChunk) -> Optional[ModelResponseStream]:
        if not parsed:
            return None

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

        if chunk_id and not getattr(self.final_response, "id", None):
            self.final_response.id = chunk_id
        if sys_fingerprint and not getattr(self.final_response, "system_fingerprint", None):
            self.final_response.system_fingerprint = sys_fingerprint

        message = self.final_response.choices[0].message

        if text:
            message.content = (message.content or "") + text
            self.response_uptil_now = message.content

        if tool_calls:
            self._accumulate_tool_calls(message, tool_calls)

        if usage:
            safe_usage = {k: (v if v is not None else 0) for k, v in usage.items()}
            try:
                self.final_response.usage = Usage(**safe_usage)
            except Exception as e:
                log.warning(f"Failed to cast Usage object from stream. Skipping: {e}")

        if self.received_finish_reason:
            self.final_response.choices[0].finish_reason = self.received_finish_reason

        delta_kwargs: Dict[str, Any] = {}
        if not self.sent_first_chunk:
            delta_kwargs["role"] = "assistant"
            self.sent_first_chunk = True

        if text: delta_kwargs["content"] = text
        if tool_calls: delta_kwargs["tool_calls"] = tool_calls
        if provider_fields: delta_kwargs["provider_specific_fields"] = provider_fields

        is_delta_empty = not (text or tool_calls or provider_fields)
        if is_delta_empty and is_finished:
            self.sent_last_chunk = True
            delta_kwargs["content"] = None

        stream_chunk = ModelResponseStream(
            id=chunk_id or self.final_response.id,
            model=self.model,
            system_fingerprint=sys_fingerprint,
            provider_specific_fields=provider_fields,
            usage=self.final_response.usage if (is_finished or usage) else None,
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
        if not self.final_response.choices[0].finish_reason:
            self.final_response.choices[0].finish_reason = self.received_finish_reason or "stop"
        if not self.final_response.choices[0].message.content and getattr(self.final_response.choices[0].message, "tool_calls", None):
            self.final_response.choices[0].message.content = None
        return self.final_response

    def _accumulate_tool_calls(self, message: Message, delta_tool_calls: List[Any]) -> None:
        for tc in delta_tool_calls:
            idx = tc.get("index", 0) if isinstance(tc, dict) else getattr(tc, "index", 0)
            
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

            func_obj = tc.get("function", {}) if isinstance(tc, dict) else getattr(tc, "function", None)
            f_args = func_obj.get("arguments", "") if isinstance(func_obj, dict) else getattr(func_obj, "arguments", "")
            if f_args:
                self.tool_calls_map[idx]["function"]["arguments"] += f_args

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

class StreamWrapper:
    def __init__(
        self,
        completion_stream: Any,
        model: str,
        system_meta: Optional[Any] = None,
        custom_llm_provider: Optional[str] = None,
        stream_options: Optional[dict] = None,
        make_call: Optional[Callable] = None,
        _response_headers: Optional[dict] = None,
        messages: Optional[List[Any]] = None,
    ):
        self.completion_stream = completion_stream
        self.model = model
        self.system_meta = system_meta
        self.custom_llm_provider = custom_llm_provider
        self.make_call = make_call
        self.messages = messages or []
        
        self._stream_created_time = time.time()
        self.cache_hit = (self.custom_llm_provider == "cached_response")
        self.accumulator = StreamAccumulator(self.model, self.custom_llm_provider)
        
        self.rules = []
        if self.system_meta and hasattr(self.system_meta, "metadata"):
            self.rules = self.system_meta.metadata.get("post_call_rules", [])

    def _check_max_streaming_duration(self) -> None:
        elapsed = time.time() - self._stream_created_time
        if elapsed > MAX_STREAMING_DURATION_SECONDS:
            raise config.Timeout(
                message=f"Stream exceeded max streaming duration of {MAX_STREAMING_DURATION_SECONDS}s",
                model=self.model or "", llm_provider=self.custom_llm_provider or "",
            )

    async def aclose(self):
        if self.completion_stream is not None:
            stream_to_close = self.completion_stream
            self.completion_stream = None
            with anyio.CancelScope(shield=True):
                try:
                    if hasattr(stream_to_close, "aclose"): 
                        await stream_to_close.aclose()
                    elif hasattr(stream_to_close, "close"):
                        result = stream_to_close.close()
                        if result is not None: 
                            await result
                except BaseException as e:
                    log.debug(f"StreamWrapper.aclose error: {e}")

    async def _fetch_async_stream(self):
        if self.completion_stream is None and self.make_call is not None:
            self.completion_stream = await self.make_call(client=config.module_level_aclient)

    async def _check_rules(self):
        if not self.rules:
            return
            
        current_text = self.accumulator.response_uptil_now
        for rule in self.rules:
            decision = await rule(current_text) if asyncio.iscoroutinefunction(rule) else rule(current_text)
                
            if isinstance(decision, bool) and decision is False:
                raise APIResponseValidationError(
                    message="LLM Response failed post-call-rule check", 
                    llm_provider=self.custom_llm_provider or "", 
                    model=self.model or "unknown"
                )
            elif isinstance(decision, dict) and decision.get("decision", True) is False:
                raise APIResponseValidationError(
                    message=decision.get("message", "LLM Response failed post-call-rule check"), 
                    llm_provider=self.custom_llm_provider or "", 
                    model=self.model or "unknown"
                )

    def _process_usage_and_hooks(self, processed_chunk: ModelResponseStream) -> None:
        """스트림 종료 감지, 토큰 폴백 연산(필요시), 그리고 과금 및 로깅 훅 트리거를 전담"""
        # 스트림 종료 조건 식별
        is_stream_end = self.accumulator.sent_last_chunk or (
            processed_chunk.choices and processed_chunk.choices[0].finish_reason is not None
        )

        # 현재 청크 기준 토큰 파악
        total_tokens = 0
        if hasattr(processed_chunk, "usage") and processed_chunk.usage:
            usage_data = processed_chunk.usage.model_dump() if hasattr(processed_chunk.usage, "model_dump") else processed_chunk.usage
            total_tokens = usage_data.get("total_tokens", 0)

        # 종료되었으나 토큰이 0인 경우 오프라인 계산 수행
        if is_stream_end and total_tokens == 0:
            try:
                from fiber.llm.model.token.counter import calculate_fallback_usage
                from fiber.llm.types.provider.core import Usage
                
                log.debug(f"Provider did not yield usage. Calculating fallback tokens for {self.model}...")
                fallback_usage = calculate_fallback_usage(
                    model=self.model,
                    messages=self.messages,
                    completion_text=self.accumulator.response_uptil_now
                )
                total_tokens = fallback_usage.get("total_tokens", 0)
                
                # 계산된 토큰 정보를 스트림 청크 및 누적 응답 객체에 강제 주입
                fallback_usage_obj = Usage(**fallback_usage)
                processed_chunk.usage = fallback_usage_obj
                self.accumulator.final_response.usage = fallback_usage_obj
            except Exception as e:
                log.warning(f"Failed to calculate fallback tokens: {e}")

        # 스트림이 완전히 종료된 시점에 단 1회 훅(Hook) 순회 및 실행
        if is_stream_end and self.system_meta:
            # 과금/단일 콜백 (토큰이 0보다 클 때만)
            on_complete = self.system_meta.framework_flags.get("on_stream_complete")
            if on_complete and callable(on_complete) and total_tokens > 0:
                on_complete(total_tokens)
                self.system_meta.framework_flags["on_stream_complete"] = None
                
            # 로깅/다중 콜백 (토큰이 0이어도 종료 로깅을 위해 반드시 실행)
            hooks = self.system_meta.framework_flags.get("on_stream_complete_hooks", [])
            for hook in list(hooks): 
                if callable(hook):
                    hook(total_tokens=total_tokens)
            self.system_meta.framework_flags["on_stream_complete_hooks"] = []

    def __aiter__(self) -> AsyncIterator["ModelResponseStream"]: 
        return self

    async def __anext__(self) -> "ModelResponseStream":
        self._check_max_streaming_duration()
        try:
            await self._fetch_async_stream()
            if not isinstance(self.completion_stream, collections.abc.AsyncIterable):
                raise TypeError("StreamWrapper now exclusively supports AsyncIterable sources.")
            
            while True:
                raw_chunk = await self.completion_stream.__anext__()
                if raw_chunk is None or raw_chunk == "None" or (self.custom_llm_provider == "gemini" and hasattr(raw_chunk, "parts") and len(raw_chunk.parts) == 0):
                    continue
                
                parsed_dict = StreamChunkParser.parse(self.custom_llm_provider, raw_chunk)
                processed_chunk = self.accumulator.push(parsed_dict)
                
                if not processed_chunk:
                    continue

                await self._check_rules()

                if self.system_meta and "time_to_first_token" not in self.system_meta.framework_flags:
                    self.system_meta.framework_flags["time_to_first_token"] = time.time()

                # ✨ [개선] 분리된 Usage 및 Hook 처리기 호출 (코드가 극도로 깔끔해짐)
                self._process_usage_and_hooks(processed_chunk)

                if not config.get("disable_streaming_logging", False):
                    log.trace("Stream chunk yielded", model=self.model, chunk_id=getattr(processed_chunk, "id", None))
                
                return processed_chunk

        except StopAsyncIteration:
            raise StopAsyncIteration
        except Exception as e:
            self._on_exception_caught(e)

    def _on_exception_caught(self, e: Exception) -> NoReturn:
        is_timeout = isinstance(e, httpx.TimeoutException)
        if is_timeout: 
            e.args = (*e.args, f"\nRequest Timeout - {config.request_timeout}")
            
        log.warning(f"Caught exception during streaming [{type(e).__name__}]: {str(e)}")
        self._fire_fallback_error(e)

    def _fire_fallback_error(self, e: Exception) -> NoReturn:
        if isinstance(e, (OpenAIError, APIResponseValidationError)): 
            mapped_exception = e
        else:
            try: 
                mapped_exception = exception_type(
                    model=self.model, custom_llm_provider=self.custom_llm_provider, 
                    original_exception=e, completion_kwargs={}, extra_kwargs={}
                )
            except Exception as mapping_error: 
                mapped_exception = mapping_error

        raise MidStreamFallbackError(
            message=str(mapped_exception), 
            model=self.model, 
            llm_provider=self.custom_llm_provider or "unknown",
            original_exception=mapped_exception, 
            generated_content=self.accumulator.response_uptil_now,
            is_pre_first_chunk=not self.accumulator.sent_first_chunk,
        )