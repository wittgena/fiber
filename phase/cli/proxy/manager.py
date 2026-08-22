# phase.cli.proxy.manager
## @lineage: cli.proxy.manager
## @lineage: meta.cli.proxy.manager
import os
import sys
import time
import subprocess
import warnings
from urllib.parse import urlparse
from functools import partial
from typing import Annotated, Optional

import typer
import anyio
import anyio.lowlevel
from mcp_types import ServerRequest, ClientResult, ServerNotification, Implementation
from mcp.shared.message import SessionMessage
from mcp.shared.session import RequestResponder
from mcp.client._transport import ReadStream, WriteStream
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client

from watcher.plane.emitter import get_emitter

if not sys.warnoptions:
    warnings.simplefilter("ignore")

log = get_emitter("cli.manager")

app = typer.Typer(
    name="brane",
    help="Brane Infrastructure & MCP Gateway Management Tools",
    add_completion=False,
    no_args_is_help=True,
)

SERVICES = {
    "gateway": {
        "type": "uvicorn",
        "module": "xphi.proxy.gateway:app",
        "default_port": 8000,
        "env_port_key": "GATEWAY_PORT",
        "description": "L7 Ingress API Gateway (FastAPI)",
    },
    "pypi": {
        "type": "python",
        "module": "xphi.proxy.pypi.server",
        "default_port": 8083,
        "env_port_key": "PYPI_PORT",
        "description": "PyPI Membrane Proxy & SSE Server (aiohttp)",
    },
    "broker": {
        "type": "uvicorn",
        "module": "xphi.proxy.auth.broker:app",
        "default_port": 9000,
        "env_port_key": "MCP_PORT",
        "description": "Authorization Server (Token Issuer)",
    },
}

def _run_service(name: str, svc: dict, port: int, reload: bool) -> subprocess.Popen:
    """Executes a subprocess based on the service type (uvicorn or python module)."""
    run_env = os.environ.copy()
    run_env[svc["env_port_key"]] = str(port)

    if svc["type"] == "uvicorn":
        cmd = ["uvicorn", svc["module"], "--host", "0.0.0.0", "--port", str(port)]
        if reload:
            cmd.append("--reload")
    elif svc["type"] == "python":
        cmd = [sys.executable, "-m", svc["module"]]
    else:
        raise ValueError(f"Unknown service type: {svc['type']}")

    return subprocess.Popen(cmd, env=run_env)

@app.command()
def run(
    service: Annotated[str, typer.Argument(help="Service to run: 'gateway', 'pypi', 'broker', or 'all'")],
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Override default port")] = None,
    reload: Annotated[bool, typer.Option("--reload", "-r", help="Enable auto-reload for development")] = False,
):
    """Start Brane infrastructure services."""
    if service not in SERVICES and service != "all":
        log.error(f"Unknown service: {service}. Available: {', '.join(SERVICES.keys())}, all")
        raise typer.Exit(1)

    processes: list[subprocess.Popen] = []

    try:
        if service == "all":
            log.info("🚀 Starting entire Brane Infrastructure...")
            
            ## Start the backend background server first (PyPI Proxy)
            pypi_port = 8083
            log.info(f"Starting [PyPI Membrane] on port {pypi_port}")
            processes.append(_run_service("pypi", SERVICES["pypi"], pypi_port, reload))
            
            ## Wait for port binding
            time.sleep(2)
            
            ## Start the Ingress Gateway
            gw_port = port or SERVICES["gateway"]["default_port"]
            log.info(f"Starting [Gateway] on port {gw_port}")
            processes.append(_run_service("gateway", SERVICES["gateway"], gw_port, reload))
            log.info("✅ All services are running. Press Ctrl+C to stop.")
            for p in processes:
                p.wait()
        else:
            svc = SERVICES[service]
            run_port = port or svc["default_port"]
            log.info(f"🚀 Starting {service} ({svc['description']}) on port {run_port}...")
            
            p = _run_service(service, svc, run_port, reload)
            processes.append(p)
            p.wait()
    except KeyboardInterrupt:
        log.info("\n🛑 Shutting down Brane services...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        log.info("Shutdown complete.")
        raise typer.Exit(0)
    except Exception as e:
        log.error(f"Failed to run service: {e}")
        raise typer.Exit(1)

@app.command()
def ls():
    """List available Brane infrastructure services."""
    log.info("\n📦 Available Brane Services:")
    log.info("-" * 65)
    for name, info in SERVICES.items():
        log.info(f" • {name:<10} | Port: {info['default_port']:<5} | {info['description']}")
    log.info("-" * 65)
    log.info("Usage: brane run <service_name> | brane run all\n")

async def _client_message_handler(
    message: RequestResponder[ServerRequest, ClientResult] | ServerNotification | Exception,
) -> None:
    if isinstance(message, Exception):
        log.error(f"Error from server: {message}")
        return
    log.info(f"Received message: {message}")

async def _run_client_session(
    read_stream: ReadStream[SessionMessage | Exception],
    write_stream: WriteStream[SessionMessage],
    client_info: Implementation | None = None,
):
    async with ClientSession(
        read_stream,
        write_stream,
        message_handler=_client_message_handler,
        client_info=client_info,
    ) as session:
        log.info("🔄 Initializing MCP Session...")
        await session.initialize()
        log.info("✅ Session Initialized successfully!")
        await anyio.sleep_forever()

async def async_client_main(command_or_url: str, args: list[str], env_dict: dict[str, str]):
    if urlparse(command_or_url).scheme in ("http", "https"):
        log.info(f"Connecting to SSE endpoint: {command_or_url}")
        async with sse_client(command_or_url) as streams:
            await _run_client_session(*streams)
    else:
        log.info(f"Executing Stdio command: {command_or_url} {' '.join(args)}")
        server_parameters = StdioServerParameters(command=command_or_url, args=args, env=env_dict)
        async with stdio_client(server_parameters) as streams:
            await _run_client_session(*streams)

@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def client(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="HTTP(S) SSE URL or Stdio Command to connect to")],
):
    """Test MCP Server connection via SSE or Stdio."""
    args = ctx.args
    env_dict = dict(os.environ) 
    try:
        anyio.run(partial(async_client_main, target, args, env_dict), backend="trio")
    except KeyboardInterrupt:
        log.info("Disconnecting client...")

if __name__ == "__main__":
    app()