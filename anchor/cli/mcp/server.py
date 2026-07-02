# anchor.cli.mcp.server
import sys
import warnings
import anyio
import anyio.lowlevel
from contextlib import asynccontextmanager
from io import TextIOWrapper

from mcp_types import jsonrpc_message_adapter
from mcp.shared._context_streams import create_context_streams
from mcp.shared.message import SessionMessage
from mcp.server.lowlevel.server import Server

from watcher.plane.emitter import get_emitter

log = get_emitter("mcp.server")

if not sys.warnoptions:
    warnings.simplefilter("ignore")

@asynccontextmanager
async def stdio_server(stdin: anyio.AsyncFile[str] | None = None, stdout: anyio.AsyncFile[str] | None = None):
    if not stdin:
        stdin = anyio.wrap_file(TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace"))
    if not stdout:
        stdout = anyio.wrap_file(TextIOWrapper(sys.stdout.buffer, encoding="utf-8"))

    read_stream_writer, read_stream = create_context_streams[SessionMessage | Exception](0)
    write_stream, write_stream_reader = create_context_streams[SessionMessage](0)

    async def stdin_reader():
        try:
            async with read_stream_writer:
                async for line in stdin:
                    try:
                        message = jsonrpc_message_adapter.validate_json(line, by_name=False)
                    except Exception as exc:
                        await read_stream_writer.send(exc)
                        continue

                    session_message = SessionMessage(message)
                    await read_stream_writer.send(session_message)
        except anyio.ClosedResourceError:  # pragma: no cover
            await anyio.lowlevel.checkpoint()

    async def stdout_writer():
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    json = session_message.message.model_dump_json(by_alias=True, exclude_unset=True)
                    await stdout.write(json + "\n")
                    await stdout.flush()
        except anyio.ClosedResourceError:  # pragma: no cover
            await anyio.lowlevel.checkpoint()

    async with anyio.create_task_group() as tg:
        tg.start_soon(stdin_reader)
        tg.start_soon(stdout_writer)
        yield read_stream, write_stream

async def main() -> None:
    server: Server[dict[str, object]] = Server("mcp")
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

if __name__ == "__main__":
    anyio.run(main, backend="trio")
