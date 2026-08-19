# engine.xor.bridge.tosync
## @lineage: xor.bridge.tosync
## @lineage: xor.tosync
import asyncio
import functools
from typing import Awaitable, Callable, Optional, AsyncGenerator, Any
import anyio
import anyio.to_thread
from typing_extensions import ParamSpec, TypeVar

class AsyncToSyncBridge:
    @staticmethod
    def run_coroutine(coro: Any) -> Any:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply(loop)
        return loop.run_until_complete(coro)

class SyncStreamAdapter:
    def __init__(self, async_gen: AsyncGenerator):
        self.async_gen = async_gen
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.loop.run_until_complete(self.async_gen.__anext__())
        except StopAsyncIteration:
            raise StopIteration

def run_async_function(async_function, *args, **kwargs):
    from concurrent.futures import ThreadPoolExecutor
    def run_in_new_loop():
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