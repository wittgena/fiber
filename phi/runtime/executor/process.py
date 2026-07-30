# phi.runtime.executor.process
from typing import Any, Dict, List, Optional, Union

from bound.mapper.exception import exception_type
from bound.stream.wrapper import StreamWrapper

from tenant.switch.params import ModelResponse, ModelResponseStream

from tenant.phi.router.llm import ModuleMissingError
from tenant.phi.registry.adapter import AdapterRegistry
from phi.runtime.executor.pre import CompletionPreprocessor

from watcher.plane.emitter import get_emitter

log = get_emitter("executor.process")

async def async_core_completion(
    model: str,
    messages: List = [],
    **kwargs,
) -> Union[ModelResponse, StreamWrapper]:
    if model is None:
        raise ValueError("model param not passed in.")

    preprocessor = CompletionPreprocessor(model=model, messages=messages, kwargs=kwargs)
    ctx = preprocessor.build()
    log.debug(f"[bound.completion] 코어 진입: model={ctx.model}, provider={ctx.custom_llm_provider}")

    try:
        adapter = AdapterRegistry.get_adapter(task_type="llm", provider_name=ctx.custom_llm_provider)
        if hasattr(adapter, "execute") and callable(adapter.execute):
            import inspect
            if inspect.iscoroutinefunction(adapter.execute):
                response = await adapter.execute(ctx)
            else:
                response = adapter.execute(ctx)
        else:
            raise RuntimeError("유효한 어댑터 실행 함수를 찾을 수 없습니다.")

        if ctx.stream is True and isinstance(response, ModelResponseStream):
            return StreamWrapper(
                completion_stream=response, 
                model=ctx.model, 
                custom_llm_provider=ctx.custom_llm_provider, 
                logging_obj=ctx.logging_obj,
            )
        return response
    except ModuleMissingError as te:
        ## @fast_fail: 치명적 토폴로지 누락 오류는 절대 래핑(감싸기)하거나 재시도하지 않고 즉시 상위로 투과
        log.error(f"[bound.completion] 치명적 구조 결함 감지. 실행 즉각 중단: {te}")
        raise te
    except Exception as e:
        log.error(f"[bound.completion] 코어 엔진 예외 발생: {str(e)}")
        if ctx.logging_obj:
            ctx.logging_obj.post_call(
                input=ctx.messages, api_key=ctx.api_key, original_response=str(e), additional_args={"headers": getattr(ctx, 'headers', {})}
            )
        error_completion_kwargs = {"model": model, "messages": messages, **ctx.original_kwargs}
        raise exception_type(
            model=ctx.model, custom_llm_provider=ctx.custom_llm_provider, original_exception=e,
            completion_kwargs=error_completion_kwargs, extra_kwargs=ctx.original_kwargs,
        )