# xor.opt.callback.stream
## @lineage: xphi.xor.opt.callback.stream
import asyncio
import concurrent.futures
from dataclasses import dataclass
from typing import Any
from asyncer import syncify
from xor.scope.dsp.context import runtime
from xor.opt.callback.base import BaseCallback

@dataclass
class StreamResponse:
    predict_name: str
    signature_field_name: str
    chunk: str
    is_last_chunk: bool


@dataclass
class StatusMessage:
    message: str


def sync_send_to_stream(stream, message):
    async def _send():
        await stream.send(message)

    try:
        asyncio.get_running_loop()
        def run_in_new_loop():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                return new_loop.run_until_complete(_send())
            finally:
                new_loop.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_in_new_loop)
            return future.result()
    except RuntimeError:
        return syncify(_send)()


class StatusMessageProvider:
    def tool_start_status_message(self, instance: Any, inputs: dict[str, Any]):
        return f"Calling tool {instance.name}..."

    def tool_end_status_message(self, outputs: Any):
        return "Tool calling finished! Querying the LLM with tool calling results..."

    def module_start_status_message(self, instance: Any, inputs: dict[str, Any]):
        pass

    def module_end_status_message(self, outputs: Any):
        pass

    def lm_start_status_message(self, instance: Any, inputs: dict[str, Any]):
        pass

    def lm_end_status_message(self, outputs: Any):
        pass


class StatusStreamingCallback(BaseCallback):
    def __init__(self, status_message_provider: StatusMessageProvider | None = None):
        self.status_message_provider = status_message_provider or StatusMessageProvider()

    def on_tool_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        stream = runtime.send_stream
        if stream is None or instance.name == "finish":
            return

        status_message = self.status_message_provider.tool_start_status_message(instance, inputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))

    def on_tool_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        stream = runtime.send_stream
        if stream is None or outputs == "Completed.":
            return

        status_message = self.status_message_provider.tool_end_status_message(outputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))

    def on_lm_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        stream = runtime.send_stream
        if stream is None:
            return

        status_message = self.status_message_provider.lm_start_status_message(instance, inputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))

    def on_lm_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        stream = runtime.send_stream
        if stream is None:
            return

        status_message = self.status_message_provider.lm_end_status_message(outputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))

    def on_module_start(
        self,
        call_id: str,
        instance: Any,
        inputs: dict[str, Any],
    ):
        stream = runtime.send_stream
        if stream is None:
            return

        status_message = self.status_message_provider.module_start_status_message(instance, inputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))

    def on_module_end(
        self,
        call_id: str,
        outputs: dict[str, Any] | None,
        exception: Exception | None = None,
    ):
        stream = runtime.send_stream
        if stream is None:
            return

        status_message = self.status_message_provider.module_end_status_message(outputs)
        if status_message:
            sync_send_to_stream(stream, StatusMessage(status_message))
