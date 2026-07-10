# bound.surface.bridge.channel.convert.asyncify
## @lineage: bound.channel.convert.asyncify
## @lineage: bound.bridge.channel.convert.asyncify
## @lineage: bound.channel.bridge.convert.asyncify
## @lineage: bound.channel.action.support.asyncify
import asyncio
import functools
from typing import Awaitable, Callable, Optional
import anyio
import anyio.to_thread
from typing_extensions import ParamSpec, TypeVar

T_ParamSpec = ParamSpec("T_ParamSpec")
T_Retval = TypeVar("T_Retval")


def function_has_argument(function: Callable, arg_name: str) -> bool:
    """Helper function to check if a function has a specific argument."""
    import inspect

    signature = inspect.signature(function)
    return arg_name in signature.parameters


def asyncify(
    function: Callable[T_ParamSpec, T_Retval],
    *,
    cancellable: bool = False,
    limiter: Optional[anyio.CapacityLimiter] = None,
) -> Callable[T_ParamSpec, Awaitable[T_Retval]]:
    async def wrapper(
        *args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs
    ) -> T_Retval:
        partial_f = functools.partial(function, *args, **kwargs)
        if function_has_argument(anyio.to_thread.run_sync, "abandon_on_cancel"):
            return await anyio.to_thread.run_sync(
                partial_f,
                abandon_on_cancel=cancellable,
                limiter=limiter,
            )

        return await anyio.to_thread.run_sync(
            partial_f,
            cancellable=cancellable,
            limiter=limiter,
        )
    return wrapper

def run_async_function(async_function, *args, **kwargs):
    from concurrent.futures import ThreadPoolExecutor
    def run_in_new_loop():
        """Run the coroutine in a new event loop within this thread."""
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            return new_loop.run_until_complete(async_function(*args, **kwargs))
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)

    try:
        _ = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_new_loop)
            return future.result()
    except RuntimeError:
        return run_in_new_loop()
