# bound.surface.bridge.tosync
import asyncio
from typing import AsyncGenerator, Any

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