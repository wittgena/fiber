# anchor.dsp.instance
## @lineage: bound.surface.dsp.instance
## @lineage: bound.surface.client.dsp.instance
## @lineage: anchor.provider.dsp.instance
## @lineage: anchor.registry.provider.dsp.instance
import re
import threading
import warnings
from typing import Any, Literal

from anchor.dsp.base import BaseLM
from anchor.registry.model.support import supports_function_calling, supports_reasoning, supports_response_schema, get_supported_openai_params
from anchor.dsp.training.openai import OpenAIProvider
from anchor.dsp.training.base import Provider, ReinforceJob, TrainingJob
from anchor.dsp.delegator import DSPDelegator
from anchor.dsp.cache import request_cache
from anchor.dsp.training.format import TrainDataFormat
from bound.surface.exception import ContextWindowExceededError

from xor.scope.dsp.context import runtime
from xor.opt.callback.base import BaseCallback

from arch.gov.gate import uuid4
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class DSPInstance(BaseLM):
    """A language model supporting chat or responses completion requests for use with modules"""
    def __init__(
        self,
        model: str,
        model_type: Literal["chat", "responses"] = "chat",
        temperature: float | None = None,
        max_tokens: int | None = None,
        cache: bool = True,
        callbacks: list[BaseCallback] | None = None,
        num_retries: int = 1,
        provider: Provider | None = None,
        finetuning_model: str | None = None,
        launch_kwargs: dict[str, Any] | None = None,
        train_kwargs: dict[str, Any] | None = None,
        use_developer_role: bool = False,
        **kwargs,
    ):
        self.model = model
        self.model_type = model_type
        self.cache = cache
        self.provider = provider or self.infer_provider()
        self.callbacks = callbacks or []
        self.history = []
        self.num_retries = num_retries
        self.finetuning_model = finetuning_model
        self.launch_kwargs = launch_kwargs or {}
        self.train_kwargs = train_kwargs or {}
        self.use_developer_role = use_developer_role
        self._warned_zero_temp_rollout = False
        self.delegator = DSPDelegator()

        model_family = model.split("/")[-1].lower() if "/" in model else model.lower()
        model_pattern = re.match(
            r"^(?:o[1345](?:-(?:mini|nano|pro))?(?:-\d{4}-\d{2}-\d{2})?|gpt-5(?!-chat)(?:-.*)?)$",
            model_family,
        )

        if model_pattern:
            if (temperature and temperature != 1.0) or (max_tokens and max_tokens < 16000):
                raise ValueError(
                    "OpenAI's reasoning models require passing temperature=1.0 or None and max_tokens >= 16000 or None to "
                    "`settings.LM(...)`, e.g., settings.LM('openai/gpt-5', temperature=1.0, max_tokens=16000)"
                )
            self.kwargs = dict(temperature=temperature, max_completion_tokens=max_tokens, **kwargs)
            if self.kwargs.get("rollout_id") is None:
                self.kwargs.pop("rollout_id", None)
        else:
            self.kwargs = dict(temperature=temperature, max_tokens=max_tokens, **kwargs)
            if self.kwargs.get("rollout_id") is None:
                self.kwargs.pop("rollout_id", None)

        self._warn_zero_temp_rollout(self.kwargs.get("temperature"), self.kwargs.get("rollout_id"))

    @property
    def _provider_name(self) -> str:
        """Extract the provider name from the model string (e.g., 'openai' from 'openai/gpt-4o')."""
        if "/" in self.model:
            return self.model.split("/", 1)[0]
        return "openai"

    @property
    def supports_function_calling(self) -> bool:
        return supports_function_calling(model=self.model)

    @property
    def supports_reasoning(self) -> bool:
        return supports_reasoning(self.model)

    @property
    def supports_response_schema(self) -> bool:
        return supports_response_schema(model=self.model, custom_llm_provider=self._provider_name)

    @property
    def supported_params(self) -> set[str]:
        params = get_supported_openai_params(model=self.model, custom_llm_provider=self._provider_name)
        return set(params) if params else set()

    def _warn_zero_temp_rollout(self, temperature: float | None, rollout_id):
        if not self._warned_zero_temp_rollout and rollout_id is not None and temperature == 0:
            warnings.warn(
                "rollout_id has no effect when temperature=0; set temperature>0 to bypass the cache.",
                stacklevel=3,
            )
            self._warned_zero_temp_rollout = True

    def _get_cached_completion_fn(self, completion_fn, cache):
        ignored_args_for_cache_key = ["api_key", "api_base", "base_url"]
        if cache:
            completion_fn = request_cache(
                cache_arg_name="request",
                ignored_args_for_cache_key=ignored_args_for_cache_key,
            )(completion_fn)

        litellm_cache_args = {"no-cache": True, "no-store": True}
        return completion_fn, litellm_cache_args

    def forward(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs
    ):
        req_id = str(uuid4())[:8]
        kwargs = dict(kwargs)
        cache = kwargs.pop("cache", self.cache)
        log.debug(f"[DSPInstance-{req_id}] 🚀 forward START | model={self.model}, type={self.model_type}, cache={cache}")

        messages = messages or [{"role": "user", "content": prompt}]
        if self.use_developer_role and self.model_type == "responses":
            messages = [{**m, "role": "developer"} if m.get("role") == "system" else m for m in messages]
        kwargs = {**self.kwargs, **kwargs}
        self._warn_zero_temp_rollout(kwargs.get("temperature"), kwargs.get("rollout_id"))
        if kwargs.get("rollout_id") is None:
            kwargs.pop("rollout_id", None)

        if self.model_type == "chat":
            completion_target = self.delegator.delegate_completion
        elif self.model_type == "responses":
            completion_target = self.delegator.delegate_responses
        else:
            log.error(f"[DSPInstance-{req_id}] 🚨 Unsupported model_type: {self.model_type}")
            raise ValueError(f"Unsupported model_type: {self.model_type}")
            
        completion_fn, litellm_cache_args = self._get_cached_completion_fn(completion_target, cache)

        try:
            log.debug(f"[DSPInstance-{req_id}] ⚙️ Executing completion_fn...")
            results = completion_fn(
                request=dict(model=self.model, messages=messages, **kwargs),
                num_retries=self.num_retries,
                cache=litellm_cache_args,
            )
            log.debug(f"[DSPInstance-{req_id}] ✅ completion_fn SUCCESS")
        except ContextWindowExceededError as e:
            log.error(f"[DSPInstance-{req_id}] 🚨 ContextWindowExceededError: {e}")
            raise ContextWindowExceededError(model=self.model) from e
        except Exception as e:
            log.error(f"[DSPInstance-{req_id}] 🚨 Exception in completion_fn: {e}")
            raise e

        self._check_truncation(results)
        if not getattr(results, "cache_hit", False) and runtime.usage_tracker and hasattr(results, "usage"):
            runtime.usage_tracker.add_usage(self.model, dict(results.usage))
        
        log.debug(f"[DSPInstance-{req_id}] 🏁 forward END")
        return results

    async def aforward(
        self,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        **kwargs,
    ):
        req_id = str(uuid4())[:8]
        kwargs = dict(kwargs)
        cache = kwargs.pop("cache", self.cache)
        log.debug(f"[DSPInstance-{req_id}] ⚡ aforward START | model={self.model}, type={self.model_type}, cache={cache}")

        messages = messages or [{"role": "user", "content": prompt}]
        if self.use_developer_role and self.model_type == "responses":
            messages = [{**m, "role": "developer"} if m.get("role") == "system" else m for m in messages]
        kwargs = {**self.kwargs, **kwargs}
        self._warn_zero_temp_rollout(kwargs.get("temperature"), kwargs.get("rollout_id"))
        if kwargs.get("rollout_id") is None:
            kwargs.pop("rollout_id", None)

        if self.model_type == "chat":
            completion_target = self.delegator.delegate_acompletion
        elif self.model_type == "responses":
            completion_target = self.delegator.delegate_aresponses
        else:
            log.error(f"[DSPInstance-{req_id}] 🚨 Unsupported model_type: {self.model_type}")
            raise ValueError(f"Unsupported model_type: {self.model_type}")
            
        completion_fn, litellm_cache_args = self._get_cached_completion_fn(completion_target, cache)

        try:
            log.debug(f"[DSPInstance-{req_id}] ⚙️ Awaiting completion_fn...")
            results = await completion_fn(
                request=dict(model=self.model, messages=messages, **kwargs),
                num_retries=self.num_retries,
                cache=litellm_cache_args,
            )
            log.debug(f"[DSPInstance-{req_id}] ✅ completion_fn SUCCESS")
        except ContextWindowExceededError as e:
            log.error(f"[DSPInstance-{req_id}] 🚨 ContextWindowExceededError: {e}")
            raise ContextWindowExceededError(model=self.model) from e
        except Exception as e:
            log.error(f"[DSPInstance-{req_id}] 🚨 Exception in completion_fn: {e}")
            raise e

        self._check_truncation(results)
        if not getattr(results, "cache_hit", False) and runtime.usage_tracker and hasattr(results, "usage"):
            runtime.usage_tracker.add_usage(self.model, dict(results.usage))
        
        log.debug(f"[DSPInstance-{req_id}] 🏁 aforward END")
        return results

    def launch(self, launch_kwargs: dict[str, Any] | None = None):
        self.provider.launch(self, launch_kwargs)

    def kill(self, launch_kwargs: dict[str, Any] | None = None):
        self.provider.kill(self, launch_kwargs)

    def finetune(
        self,
        train_data: list[dict[str, Any]],
        train_data_format: TrainDataFormat | None,
        train_kwargs: dict[str, Any] | None = None,
    ) -> TrainingJob:
        if not self.provider.finetunable:
            raise ValueError(
                f"Provider {self.provider} does not support fine-tuning, please specify your provider by explicitly "
                "setting `provider` when creating the `settings.LM` instance."
            )

        def thread_function_wrapper():
            return self._run_finetune_job(job)

        thread = threading.Thread(target=thread_function_wrapper)
        train_kwargs = train_kwargs or self.train_kwargs
        model_to_finetune = self.finetuning_model or self.model
        job = self.provider.TrainingJob(
            thread=thread,
            model=model_to_finetune,
            train_data=train_data,
            train_data_format=train_data_format,
            train_kwargs=train_kwargs,
        )
        thread.start()
        return job

    def reinforce(self, train_kwargs) -> ReinforceJob:
        err = f"Provider {self.provider} does not implement the reinforcement learning interface."
        assert self.provider.reinforceable, err
        job = self.provider.ReinforceJob(lm=self, train_kwargs=train_kwargs)
        job.initialize()
        return job

    def _run_finetune_job(self, job: TrainingJob):
        try:
            model = self.provider.finetune(
                job=job,
                model=job.model,
                train_data=job.train_data,
                train_data_format=job.train_data_format,
                train_kwargs=job.train_kwargs,
            )
            lm = self.copy(model=model)
            job.set_result(lm)
        except Exception as err:
            log.error(err)
            job.set_result(err)

    def infer_provider(self) -> Provider:
        if OpenAIProvider.is_provider_model(self.model):
            return OpenAIProvider()
        return Provider()

    def dump_state(self):
        state_keys = [
            "model",
            "model_type",
            "cache",
            "num_retries",
            "finetuning_model",
            "launch_kwargs",
            "train_kwargs",
        ]
        filtered_kwargs = {k: v for k, v in self.kwargs.items() if k != "api_key"}
        return {key: getattr(self, key) for key in state_keys} | filtered_kwargs

    def _check_truncation(self, results):
        if self.model_type != "responses":
            choices = results.get("choices", []) if isinstance(results, dict) else getattr(results, "choices", [])
            has_length_truncation = any(
                (c.get("finish_reason") if isinstance(c, dict) else getattr(c, "finish_reason", None)) == "length" 
                for c in choices
            )

            if has_length_truncation:
                log.warning(
                    f"LM response was truncated due to exceeding max_tokens={self.kwargs.get('max_tokens')}. "
                    "You can inspect the latest LM interactions with `inspect_history()`. "
                    "To avoid truncation, consider passing a larger max_tokens when setting up settings.LM. "
                    f"You may also consider increasing the temperature (currently {self.kwargs.get('temperature')}) "
                    " if the reason for truncation is repetition."
                )