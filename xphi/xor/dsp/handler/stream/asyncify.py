# xphi.xor.dsp.handler.stream.asyncify
## @lineage: xphi.reflect.dsp.handler.stream.asyncify
## @lineage: xphi.opt.dsp.stream.asyncify
## @lineage: bound.xor.dsp.stream.asyncify
## @lineage: xor.dsp.stream.asyncify
## @lineage: bound.channel.bridge.dsp.stream.asyncify
## @lineage: channel.bridge.dsp.stream.asyncify
## @lineage: gov.gateway.io.resource.asyncify
from typing import TYPE_CHECKING, Any, Awaitable, Callable
import asyncer
from anyio import CapacityLimiter
from bound.channel.compat.switch.dsp.settings import settings

if TYPE_CHECKING:
    from xphi.xor.opt.module.meta import Module

_limiter = None


def get_async_max_workers():
    return settings.async_max_workers


def get_limiter():
    async_max_workers = get_async_max_workers()

    global _limiter
    if _limiter is None:
        _limiter = CapacityLimiter(async_max_workers)
    elif _limiter.total_tokens != async_max_workers:
        _limiter.total_tokens = async_max_workers

    return _limiter


def asyncify(program: "Module") -> Callable[[Any, Any], Awaitable[Any]]:
    """
    Wraps a spi program so that it can be called asynchronously. This is useful for running a
    program in parallel with another task (e.g., another spi program).

    This implementation propagates the current thread's configuration context to the worker thread.

    Args:
        program: The spi program to be wrapped for asynchronous execution.

    Returns:
        An async function: An async function that, when awaited, runs the program in a worker thread.
            The current thread's configuration context is inherited for each call.
    """

    async def async_program(*args, **kwargs) -> Any:
        # Capture the current overrides at call-time.
        from bound.channel.compat.switch.dsp.settings import thread_local_overrides

        parent_overrides = thread_local_overrides.get().copy()

        def wrapped_program(*a, **kw):
            from bound.channel.compat.switch.dsp.settings import thread_local_overrides

            original_overrides = thread_local_overrides.get()
            token = thread_local_overrides.set({**original_overrides, **parent_overrides.copy()})
            try:
                return program(*a, **kw)
            finally:
                thread_local_overrides.reset(token)

        # Create a fresh asyncified callable each time, ensuring the latest context is used.
        call_async = asyncer.asyncify(wrapped_program, abandon_on_cancel=True, limiter=get_limiter())
        return await call_async(*args, **kwargs)

    return async_program
