# xphi.xor.dsp.handler.stream.streamify
## @lineage: xphi.reflect.dsp.handler.stream.streamify
## @lineage: xphi.opt.dsp.stream.streamify
import asyncio
import contextvars
import logging
import threading
from asyncio import iscoroutinefunction
from queue import Queue
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Generator
import orjson

from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import MemoryObjectSendStream
from bound.channel.compat.check import is_model_response_stream
from bound.channel.compat.switch.dsp.settings import settings
from xphi.xor.opt.exam.prediction import Prediction

from xphi.xor.dsp.handler.stream.messages import StatusMessage, StatusMessageProvider, StatusStreamingCallback
from xphi.xor.dsp.handler.stream.listener import StreamListener, find_predictor_for_stream_listeners

from xphi.xor.dsp.handler.stream.asyncify import asyncify

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from xphi.xor.opt.module.meta import Module


def streamify(
    program: "Module",
    status_message_provider: StatusMessageProvider | None = None,
    stream_listeners: list[StreamListener] | None = None,
    include_final_prediction_in_output_stream: bool = True,
    is_async_program: bool = False,
    async_streaming: bool = True,
) -> Callable[[Any, Any], Awaitable[Any]]:
    stream_listeners = stream_listeners or []
    if len(stream_listeners) > 0:
        predict_id_to_listener = find_predictor_for_stream_listeners(program, stream_listeners)
    else:
        predict_id_to_listener = {}

    if is_async_program:
        program = program.acall
    elif not iscoroutinefunction(program):
        program = asyncify(program)

    callbacks = list(settings.callbacks)
    status_streaming_callback = StatusStreamingCallback(status_message_provider)
    if not any(isinstance(c, StatusStreamingCallback) for c in callbacks):
        callbacks.append(status_streaming_callback)

    async def generator(args, kwargs, stream: MemoryObjectSendStream):
        with settings.context(send_stream=stream, callbacks=callbacks, stream_listeners=stream_listeners):
            prediction = await program(*args, **kwargs)

        await stream.send(prediction)

    async def async_streamer(*args, **kwargs):
        send_stream, receive_stream = create_memory_object_stream(16)
        async with create_task_group() as tg, send_stream, receive_stream:
            tg.start_soon(generator, args, kwargs, send_stream)

            async for value in receive_stream:
                if is_model_response_stream(value):
                    if len(predict_id_to_listener) == 0:
                        # No listeners are configured, yield the chunk directly for backwards compatibility.
                        yield value
                    else:
                        # We are receiving a chunk from the LM's response stream, delegate it to the listeners to
                        # determine if we should yield a value to the user.
                        for listener in predict_id_to_listener[value.predict_id]:
                            # In some special cases such as Citation API, it is possible that multiple listeners
                            # return values at the same time due to the chunk buffer of the listener.
                            if output := listener.receive(value):
                                yield output
                elif isinstance(value, StatusMessage):
                    yield value
                elif isinstance(value, Prediction):
                    # Flush remaining buffered tokens before yielding the Prediction instance
                    for listener in stream_listeners:
                        if final_chunk := listener.finalize():
                            yield final_chunk

                    if include_final_prediction_in_output_stream:
                        yield value
                    elif (
                        len(stream_listeners) == 0
                        or any(listener.cache_hit for listener in stream_listeners)
                        or not any(listener.stream_start for listener in stream_listeners)
                    ):
                        yield value
                    return
                else:
                    # This wildcard case allows for customized streaming behavior.
                    # It is useful when a users have a custom LM which returns stream chunks in a custom format.
                    # We let those chunks pass through to the user to handle them as needed.
                    yield value

    if async_streaming:
        return async_streamer
    else:

        def sync_streamer(*args, **kwargs):
            output = async_streamer(*args, **kwargs)
            return apply_sync_streaming(output)

        return sync_streamer


def apply_sync_streaming(async_generator: AsyncGenerator) -> Generator:
    """Convert the async streaming generator to a sync generator."""
    queue = Queue()  # Queue to hold items from the async generator
    stop_sentinel = object()  # Sentinel to signal the generator is complete

    # To propagate prediction request ID context to the child thread
    context = contextvars.copy_context()

    def producer():
        """Runs in a background thread to fetch items asynchronously."""

        async def runner():
            try:
                async for item in async_generator:
                    queue.put(item)
            finally:
                # Signal completion
                queue.put(stop_sentinel)

        context.run(asyncio.run, runner())

    # Start the producer in a background thread
    thread = threading.Thread(target=producer, daemon=True)
    thread.start()

    # Consume items from the queue
    while True:
        item = queue.get()  # Block until an item is available
        if item is stop_sentinel:
            break
        yield item


async def streaming_response(streamer: AsyncGenerator) -> AsyncGenerator:
    async for value in streamer:
        if isinstance(value, Prediction):
            data = {"prediction": dict(value.items(include_spi=False))}
            yield f"data: {orjson.dumps(data).decode()}\n\n"
        elif is_model_response_stream(value):
            data = {"chunk": value.json()}
            yield f"data: {orjson.dumps(data).decode()}\n\n"
        elif isinstance(value, str) and value.startswith("data:"):
            # The chunk value is an OpenAI-compatible streaming chunk value,
            # e.g. "data: {"finish_reason": "stop", "index": 0, "is_finished": True, ...}",
            # so yield it directly
            yield value
        else:
            raise ValueError(f"Unknown chunk value type: {value}")
    yield "data: [DONE]\n\n"
