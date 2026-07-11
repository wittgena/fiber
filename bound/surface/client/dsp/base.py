# bound.surface.client.dsp.base
## @lineage: bound.surface.dsp.base
## @lineage: anchor.provider.dsp.base
## @lineage: anchor.registry.provider.dsp.base
## @lineage: xphi.xor.dsp.llm.base
## @lineage: anchor.model.dsp.llm.base
## @lineage: anchor.model.llm.base
import json
import datetime
import uuid
from typing import Any, TextIO

from xphi.scope.dsp.context import runtime
from xphi.xor.opt.callback.base import with_callbacks
from xphi.watcher.format import pretty_print_history
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
MAX_HISTORY_SIZE = 10_000
GLOBAL_HISTORY = []

class BaseLM:
    def __init__(self, model, model_type="chat", temperature=0.0, max_tokens=1000, cache=True, **kwargs):
        self.model = model
        self.model_type = model_type
        self.cache = cache
        self.kwargs = dict(temperature=temperature, max_tokens=max_tokens, **kwargs)
        self.history = []

    @property
    def supports_function_calling(self) -> bool:
        """Whether the model supports function calling (tool use)."""
        return False

    @property
    def supports_reasoning(self) -> bool:
        """Whether the model supports native reasoning (extended thinking)."""
        return False

    @property
    def supports_response_schema(self) -> bool:
        """Whether the model supports structured output via response schema."""
        return False

    @property
    def supported_params(self) -> set[str]:
        """Set of supported OpenAI-style parameter names for the model."""
        return set()

    def _process_lm_response(self, response, prompt, messages, **kwargs):
        log.debug("[DEBUG: BaseLM._process_lm_response START]")
        log.debug(f"Raw Response Type: {type(response)}")
        merged_kwargs = {**self.kwargs, **kwargs}
        try:
            if self.model_type == "responses":
                log.debug(f"model_type == responses")
                outputs = self._process_response(response)
            else:
                log.debug(f"model_type != responses")
                outputs = self._process_completion(response, merged_kwargs)

            log.debug(f"Processed Outputs Type: {type(outputs)}")
            log.debug(f"Processed Outputs Content: {outputs}")
        except Exception as e:
            log.error(f"🚨 ERROR in _process_lm_response: {e}", exc_info=True)
            raise e

        if runtime.disable_history:
            return outputs

        ## Logging, with removed api key & where `cost` is None on cache hit.
        kwargs = {k: v for k, v in kwargs.items() if not k.startswith("api_")}
        if isinstance(response, str):
            usage_data = {}
            cost_data = None
            response_model_name = self.model
        else:
            usage_data = getattr(response, "usage", {})
            if hasattr(usage_data, "model_dump"):
                usage_data = usage_data.model_dump()
            elif not isinstance(usage_data, dict):
                usage_data = dict(usage_data) if usage_data else {}
                
            cost_data = getattr(response, "_hidden_params", {}).get("response_cost")
            response_model_name = getattr(response, "model", self.model)

        entry = {
            "prompt": prompt,
            "messages": messages,
            "kwargs": kwargs,
            "response": response,
            "outputs": outputs,
            "usage": usage_data,
            "cost": cost_data,
            "timestamp": datetime.datetime.now().isoformat(),
            "uuid": str(uuid.uuid4()),
            "model": self.model,
            "response_model": response_model_name,
            "model_type": self.model_type,
        }
        self.update_history(entry)
        return outputs
    
    @with_callbacks
    def __call__(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs
    ) -> list[dict[str, Any] | str]:
        req_id = str(uuid.uuid4())[:8] # 트래킹용 ID
        log.debug(f"[DEBUG-{req_id}] BaseLM.__call__ START | model={self.model}")
        
        response = self.forward(prompt=prompt, messages=messages, **kwargs)
        log.debug(f"[DEBUG-{req_id}] forward Success. Response Type: {type(response)}")
        
        outputs = self._process_lm_response(response, prompt, messages, **kwargs)
        log.debug(f"[DEBUG-{req_id}] _process_lm_response END. Output length: {len(outputs)}")
        return outputs

    @with_callbacks
    async def acall(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs
    ) -> list[dict[str, Any] | str]:
        req_id = str(uuid.uuid4())[:8]
        log.debug(f"[DEBUG-{req_id}: BaseLM.acall START]")
        try:
            response = await self.aforward(prompt=prompt, messages=messages, **kwargs)
            log.debug(f"[DEBUG-{req_id}: BaseLM.acall] aforward Success. Response Type: {type(response)}")
            
            outputs = self._process_lm_response(response, prompt, messages, **kwargs)
            log.debug(f"[DEBUG-{req_id}: BaseLM.acall] _process_lm_response Success. Final Outputs: {outputs}")
            return outputs
            
        except Exception as e:
            log.error(f"🚨 [DEBUG-{req_id}] ERROR in BaseLM.acall: {e}", exc_info=True)
            raise e

    def forward(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs
    ):
        raise NotImplementedError("Subclasses must implement this method.")

    async def aforward(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs
    ):
        raise NotImplementedError("Subclasses must implement this method.")

    def copy(self, **kwargs):
        import copy

        new_instance = copy.deepcopy(self)
        new_instance.history = []

        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(new_instance, key, value)
            if (key in self.kwargs) or (not hasattr(self, key)):
                if value is None:
                    new_instance.kwargs.pop(key, None)
                else:
                    new_instance.kwargs[key] = value
        if hasattr(new_instance, "_warned_zero_temp_rollout"):
            new_instance._warned_zero_temp_rollout = False

        return new_instance

    def inspect_history(self, n: int = 1, file: "TextIO | None" = None) -> None:
        pretty_print_history(self.history, n, file=file)

    def update_history(self, entry):
        if runtime.disable_history:
            return

        # Global LM history
        if len(GLOBAL_HISTORY) >= MAX_HISTORY_SIZE:
            GLOBAL_HISTORY.pop(0)

        GLOBAL_HISTORY.append(entry)

        if runtime.max_history_size == 0:
            return

        if len(self.history) >= runtime.max_history_size:
            self.history.pop(0)

        self.history.append(entry)

        # Per-module history
        caller_modules = runtime.caller_modules or []
        for module in caller_modules:
            if len(module.history) >= runtime.max_history_size:
                module.history.pop(0)
            module.history.append(entry)

    def _process_completion(self, response, merged_kwargs):
        """Process the response of OpenAI chat completion API and extract outputs."""
        log.debug("========== [CRITICAL DEBUG: MODEL RESPONSE DUMP] ==========")
        
        # [핵심 방어 코드 추가] response가 이미 순수 문자열인 경우, 복잡한 파싱 없이 즉시 반환
        if isinstance(response, str):
            log.debug(f"Response is a raw string. Bypassing extraction logic. Length: {len(response)}")
            return [response]
            
        try:
            # LiteLLM/Pydantic 호환 객체 덤프
            if hasattr(response, "model_dump_json"):
                log.debug(response.model_dump_json(indent=2))
            elif hasattr(response, "model_dump"):
                log.debug(json.dumps(response.model_dump(), indent=2))
            elif isinstance(response, dict):
                log.debug(json.dumps(response, indent=2))
        except Exception as e:
            log.debug(f"Dump failed: {e}")
        log.debug("===========================================================")

        outputs = []
        
        # response가 choices 속성을 가지는지 확인
        choices = getattr(response, "choices", [])
        if not choices and isinstance(response, dict):
            choices = response.get("choices", [])

        for c in choices:
            output = {}
            content = None
            
            ## A. OpenAI 표준 경로 시도
            if hasattr(c, "message"):
                content = getattr(c.message, "content", None)
                if not content and isinstance(c.message, dict):
                    content = c.message.get("content")
            elif hasattr(c, "text"):
                content = c.text
            elif isinstance(c, dict):
                content = c.get("text", c.get("message", {}).get("content"))

            ## B. Gemini 네이티브 포맷 누수 1차 방어 (response 루트 탐색)
            if not content:
                log.warning("OpenAI standard content is empty. Searching Native Gemini paths.")
                try:
                    # response 객체 내부의 "content" 속성을 dict 형태로 파헤침
                    if hasattr(response, "content") and isinstance(response.content, dict):
                        parts = response.content.get("parts", [])
                        if parts and len(parts) > 0 and isinstance(parts[0], dict):
                            content = parts[0].get("text")
                    # response 자체가 dict인 경우
                    elif isinstance(response, dict) and "content" in response and isinstance(response["content"], dict):
                        parts = response["content"].get("parts", [])
                        if parts and len(parts) > 0 and isinstance(parts[0], dict):
                            content = parts[0].get("text")
                except Exception as e:
                    log.error(f"Failed native path 1: {e}")

            ## C. provider_specific_fields 탐색 (LiteLLM 특성 방어)
            if not content and hasattr(c, "message"):
                try:
                    psf = getattr(c.message, "provider_specific_fields", {})
                    if psf and isinstance(psf, dict):
                        ## 특정 프로바이더가 여기에 답을 숨기는 경우가 있음
                        ## 텍스트로 유추될만한 긴 문자열이 있다면 잡아챔
                        for v in psf.values():
                            if isinstance(v, str) and len(v) > 20: 
                                content = v
                                break
                except Exception as e:
                    log.error(f"Failed provider path: {e}")

            ## D. 마지막 보루: choices[0] 자체를 문자열 캐스팅
            if not content:
                 log.warning("All extraction failed. Forcing string casting on choice object.")
                 content = str(c)

            ## 빈 값 정규화
            output["text"] = content if content is not None else ""
            log.debug(f"[Extraction Result] Length: {len(output['text'])}")
            if hasattr(c, "message"):
                reasoning = getattr(c.message, "reasoning_content", None)
                if reasoning:
                    output["reasoning_content"] = reasoning

            if merged_kwargs.get("logprobs"):
                logprobs = getattr(c, "logprobs", None)
                if logprobs is None and isinstance(c, dict):
                    logprobs = c.get("logprobs")
                if logprobs:
                    output["logprobs"] = logprobs

            if hasattr(c, "message"):
                tool_calls = getattr(c.message, "tool_calls", None)
                if tool_calls is None and isinstance(c.message, dict):
                    tool_calls = c.message.get("tool_calls")
                if tool_calls:
                    output["tool_calls"] = tool_calls

            citations = self._extract_citations_from_response(c)
            if citations:
                output["citations"] = citations

            outputs.append(output)

        if all(len(output) == 1 for output in outputs):
            outputs = [output.get("text", "") for output in outputs]
            
        return outputs

    def _extract_citations_from_response(self, choice):
        try:
            # Check for citations in LiteLLM provider_specific_fields
            citations_data = choice.message.provider_specific_fields.get("citations")
            if isinstance(citations_data, list):
                return [citation for citations in citations_data for citation in citations]
        except Exception:
            return None

    def _process_response(self, response):
        text_outputs = []
        tool_calls = []
        reasoning_contents = []

        for output_item in response.output:
            output_item_type = output_item.type
            if output_item_type == "message":
                for content_item in output_item.content:
                    text_outputs.append(content_item.text)
            elif output_item_type == "function_call":
                tool_calls.append(output_item.model_dump())
            elif output_item_type == "reasoning":
                if getattr(output_item, "content", None) and len(output_item.content) > 0:
                    for content_item in output_item.content:
                        reasoning_contents.append(content_item.text)
                elif getattr(output_item, "summary", None) and len(output_item.summary) > 0:
                    for summary_item in output_item.summary:
                        reasoning_contents.append(summary_item.text)

        result = {}
        if len(text_outputs) > 0:
            result["text"] = "".join(text_outputs)
        if len(tool_calls) > 0:
            result["tool_calls"] = tool_calls
        if len(reasoning_contents) > 0:
            result["reasoning_content"] = "".join(reasoning_contents)
        # All `response.output` items map to one answer, so we return a list of size 1.
        return [result]


def inspect_history(n: int = 1, file: "TextIO | None" = None) -> None:
    pretty_print_history(GLOBAL_HISTORY, n, file=file)
