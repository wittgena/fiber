# bound.bridge.protocol.acp.conn.queue
## @lineage: bound.transport.protocol.acp.conn.queue
## @lineage: bound.adapter.protocol.acp.conn.queue
from __future__ import annotations
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from contextlib import suppress
from typing import Protocol
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any, Protocol
from enum import Enum

from arch.xor.store.message import MessageStateStore
from phase.runtime.task.supervisor import TaskSupervisor

RequestRunner = Callable[[dict[str, Any]], Awaitable[Any]]
NotificationRunner = Callable[[dict[str, Any]], Awaitable[None]]

@dataclass(slots=True)
class RpcTask:
    kind: RpcTaskKind
    message: dict[str, Any]

class MessageQueue(Protocol):
    async def publish(self, task: RpcTask) -> None: ...

    async def close(self) -> None: ...

    def task_done(self) -> None: ...

    async def join(self) -> None: ...

    def __aiter__(self) -> AsyncIterator[RpcTask]: ...


class InMemoryMessageQueue:
    """Simple in-memory broker for RPC task dispatch."""

    def __init__(self, *, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue[RpcTask | None] = asyncio.Queue(maxsize=maxsize)
        self._closed = False

    async def publish(self, task: RpcTask) -> None:
        if self._closed:
            msg = "mssage queue already closed"
            raise RuntimeError(msg)
        await self._queue.put(task)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._queue.put(None)

    async def join(self) -> None:
        await self._queue.join()

    def task_done(self) -> None:
        with suppress(ValueError):
            self._queue.task_done()

    def __aiter__(self) -> AsyncIterator[RpcTask]:
        return _QueueIterator(self)


class _QueueIterator:
    def __init__(self, queue: InMemoryMessageQueue) -> None:
        self._queue = queue

    def __aiter__(self) -> _QueueIterator:
        return self

    async def __anext__(self) -> RpcTask:
        item = await self._queue._queue.get()
        if item is None:
            self._queue.task_done()
            raise StopAsyncIteration
        return item

class RpcTaskKind(Enum):
    REQUEST = "request"
    NOTIFICATION = "notification"

class MessageDispatcher(Protocol):
    def start(self) -> None: ...
    async def stop(self) -> None: ...

class DefaultMessageDispatcher(MessageDispatcher):
    """Background worker that consumes RPC tasks from a broker, coordinating with the store."""

    def __init__(
        self,
        *,
        queue: MessageQueue,
        supervisor: TaskSupervisor,
        store: MessageStateStore,
        request_runner: RequestRunner,
        notification_runner: NotificationRunner,
    ) -> None:
        self._queue = queue
        self._supervisor = supervisor
        self._store = store
        self._request_runner = request_runner
        self._notification_runner = notification_runner
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is not None:
            msg = "dispatcher already started"
            raise RuntimeError(msg)
        self._task = self._supervisor.create(self._run(), name="acp.Dispatcher.loop")

    async def _run(self) -> None:
        try:
            async for task in self._queue:
                try:
                    if task.kind is RpcTaskKind.REQUEST:
                        await self._dispatch_request(task.message)
                    else:
                        await self._dispatch_notification(task.message)
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            return

    async def stop(self) -> None:
        await self._queue.close()
        if self._task is not None:
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _dispatch_request(self, message: dict[str, Any]) -> None:
        record = self._store.begin_incoming(message.get("method", ""), message.get("params"))

        async def runner() -> None:
            try:
                result = await self._request_runner(message)
            except Exception as exc:
                self._store.fail_incoming(record, exc)
                raise
            else:
                self._store.complete_incoming(record, result)

        self._supervisor.create(runner(), name="acp.Dispatcher.request")

    async def _dispatch_notification(self, message: dict[str, Any]) -> None:
        async def runner() -> None:
            await self._notification_runner(message)

        self._supervisor.create(runner(), name="acp.Dispatcher.notification")
