# xphi.xor.opt.callback.base
## @lineage: xphi.xor.dsp.handler.stream.callback
## @lineage: xphi.reflect.dsp.handler.stream.callback
import functools
import inspect
from contextvars import ContextVar
from typing import Any, Callable

from phase.gov.proto.gate import uuid4
from watcher.plane.emitter import get_emitter

ACTIVE_CALL_ID = ContextVar("active_call_id", default=None)
log = get_emitter(__name__)

class BaseCallback:
    def on_module_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_module_end(
        self,
        call_id: str,
        outputs: Any | None,
        exception: Exception | None = None,
    ):
        pass

    def on_lm_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_lm_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        pass

    def on_adapter_format_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_adapter_format_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        pass

    def on_adapter_parse_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_adapter_parse_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        pass

    def on_tool_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_tool_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        pass

    def on_evaluate_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        pass

    def on_evaluate_end(
        self,
        call_id: str,
        outputs: Any | None,
        exception: Exception | None = None,
    ):
        pass


def with_callbacks(fn):
    """Decorator to add callback functionality to instance methods."""

    def _execute_start_callbacks(instance, fn, call_id, callbacks, args, kwargs):
        """Execute all start callbacks for a function call."""
        short_id = call_id[:8]
        log.debug(f"[Callback-{short_id}] 🚀 Executing {len(callbacks)} start callbacks for {fn.__name__}")
        
        inputs = inspect.getcallargs(fn, instance, *args, **kwargs)
        if "self" in inputs:
            inputs.pop("self")
        elif "instance" in inputs:
            inputs.pop("instance")
        for callback in callbacks:
            try:
                handler = _get_on_start_handler(callback, instance, fn)
                log.debug(f"[Callback-{short_id}] Triggering {handler.__name__} on {callback.__class__.__name__}")
                handler(call_id=call_id, instance=instance, inputs=inputs)
            except Exception as e:
                log.warning(f"[Callback-{short_id}] 🚨 Error when calling start callback {callback}: {e}")

    def _execute_end_callbacks(instance, fn, call_id, results, exception, callbacks):
        """Execute all end callbacks for a function call."""
        short_id = call_id[:8]
        log.debug(f"[Callback-{short_id}] 🏁 Executing {len(callbacks)} end callbacks for {fn.__name__}")
        
        for callback in callbacks:
            try:
                handler = _get_on_end_handler(callback, instance, fn)
                log.debug(f"[Callback-{short_id}] Triggering {handler.__name__} on {callback.__class__.__name__}")
                handler(
                    call_id=call_id,
                    outputs=results,
                    exception=exception,
                )
            except Exception as e:
                log.warning(f"[Callback-{short_id}] 🚨 Error when applying callback {callback}'s end handler on function {fn.__name__}: {e}")

    def _get_active_callbacks(instance):
        """Get combined global and instance-level callbacks."""
        from bound.channel.compat.switch.dsp.settings import settings
        return settings.get("callbacks", []) + getattr(instance, "callbacks", [])

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(instance, *args, **kwargs):
            callbacks = _get_active_callbacks(instance)
            if not callbacks:
                return await fn(instance, *args, **kwargs)

            # [수정] phase.gate의 uuid4 활용
            call_id = uuid4().hex
            short_id = call_id[:8]
            log.debug(f"[Callback-{short_id}] ⚡ async_wrapper START | fn={fn.__name__}, instance={instance.__class__.__name__}")

            _execute_start_callbacks(instance, fn, call_id, callbacks, args, kwargs)

            ## Active ID must be set right before the function is called, not before calling the callbacks.
            parent_call_id = ACTIVE_CALL_ID.get()
            ACTIVE_CALL_ID.set(call_id)

            results = None
            exception = None
            try:
                results = await fn(instance, *args, **kwargs)
                log.debug(f"[Callback-{short_id}] ✅ async_wrapper fn execution SUCCESS")
                return results
            except Exception as e:
                exception = e
                log.debug(f"[Callback-{short_id}] 🚨 async_wrapper fn execution EXCEPTION: {e}")
                raise exception
            finally:
                ACTIVE_CALL_ID.set(parent_call_id)
                _execute_end_callbacks(instance, fn, call_id, results, exception, callbacks)
                log.debug(f"[Callback-{short_id}] 🛑 async_wrapper END")

        return async_wrapper
    else:
        @functools.wraps(fn)
        def sync_wrapper(instance, *args, **kwargs):
            callbacks = _get_active_callbacks(instance)
            if not callbacks:
                return fn(instance, *args, **kwargs)

            call_id = uuid4().hex
            short_id = call_id[:8]
            log.debug(f"[Callback-{short_id}] ⚡ sync_wrapper START | fn={fn.__name__}, instance={instance.__class__.__name__}")

            _execute_start_callbacks(instance, fn, call_id, callbacks, args, kwargs)

            ## Active ID must be set right before the function is called, not before calling the callbacks.
            parent_call_id = ACTIVE_CALL_ID.get()
            ACTIVE_CALL_ID.set(call_id)

            results = None
            exception = None
            try:
                results = fn(instance, *args, **kwargs)
                log.debug(f"[Callback-{short_id}] ✅ sync_wrapper fn execution SUCCESS")
                return results
            except Exception as e:
                exception = e
                log.debug(f"[Callback-{short_id}] 🚨 sync_wrapper fn execution EXCEPTION: {e}")
                raise exception
            finally:
                ACTIVE_CALL_ID.set(parent_call_id)
                _execute_end_callbacks(instance, fn, call_id, results, exception, callbacks)
                log.debug(f"[Callback-{short_id}] 🛑 sync_wrapper END")

        return sync_wrapper


def _get_on_start_handler(callback: BaseCallback, instance: Any, fn: Callable) -> Callable:
    """Selects the appropriate on_start handler of the callback based on the instance and function name."""
    mro_names = [base.__name__ for base in instance.__class__.__mro__]

    if "BaseLM" in mro_names:
        return callback.on_lm_start
    elif "Evaluate" in mro_names:
        return callback.on_evaluate_start

    if "Adapter" in mro_names:
        if fn.__name__ == "format":
            return callback.on_adapter_format_start
        elif fn.__name__ == "parse":
            return callback.on_adapter_parse_start
        else:
            raise ValueError(f"Unsupported adapter method for using callback: {fn.__name__}.")

    if "Tool" in mro_names:
        return callback.on_tool_start

    ## We treat everything else as a module.
    return callback.on_module_start


def _get_on_end_handler(callback: BaseCallback, instance: Any, fn: Callable) -> Callable:
    """Selects the appropriate on_end handler of the callback based on the instance and function name."""
    mro_names = [base.__name__ for base in instance.__class__.__mro__]
    if "BaseLM" in mro_names:
        return callback.on_lm_end
    elif "Evaluate" in mro_names:
        return callback.on_evaluate_end

    if "Adapter" in mro_names:
        if fn.__name__ == "format":
            return callback.on_adapter_format_end
        elif fn.__name__ == "parse":
            return callback.on_adapter_parse_end
        else:
            raise ValueError(f"Unsupported adapter method for using callback: {fn.__name__}.")

    if "Tool" in mro_names:
        return callback.on_tool_end

    # We treat everything else as a module.
    return callback.on_module_end