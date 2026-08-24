# phase.flow.cli.mcp
import importlib.metadata
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
import warnings
from pathlib import Path
from urllib.parse import urlparse
from functools import partial
from typing import Annotated, Any, Optional

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
from mcp.server.lowlevel.server import Server as LowLevelServer

from xphi.watcher.plane.emitter import get_emitter

try:
    import dotenv
except ImportError:
    dotenv = None

if not sys.warnoptions:
    warnings.simplefilter("ignore")

# --- Loggers ---
log_mcp = get_emitter("debug.mcp")
log_brane = get_emitter("cli.manager")

# --- Typer Apps ---
app = typer.Typer(
    help="Brane & MCP Combined CLI Tools",
    add_completion=False,
    no_args_is_help=True,
)

mcp_app = typer.Typer(name="mcp", help="Brane MCP Development & Execution Tools", no_args_is_help=True)
brane_app = typer.Typer(name="brane", help="Brane Infrastructure & MCP Gateway Management Tools", no_args_is_help=True)

app.add_typer(mcp_app, name="mcp")
app.add_typer(brane_app, name="brane")

# ==========================================
# 1. MCP Subcommands & Helpers
# ==========================================

def _mcp_requirement(package: str = "mcp") -> str:
    try:
        version = importlib.metadata.version("mcp")
    except importlib.metadata.PackageNotFoundError:
        return package
    if ".dev" in version or "+" in version:
        return package
    return f"{package}=={version}"

def _get_uv_path() -> str:
    uv_path = shutil.which("uv")
    if not uv_path:
        log_mcp.warning("uv executable not found in PATH. Falling back to 'uv'.")
        return "uv"
    return uv_path

def _get_npx_command():
    if sys.platform == "win32":
        for cmd in ["npx.cmd", "npx.exe", "npx"]:
            try:
                subprocess.run([cmd, "--version"], check=True, capture_output=True, shell=True)
                return cmd
            except subprocess.CalledProcessError:
                continue
        return None
    return "npx"

def _parse_env_var(env_var: str) -> tuple[str, str]:
    if "=" not in env_var:
        log_mcp.error(f"Invalid environment variable format: {env_var}. Must be KEY=VALUE")
        sys.exit(1)
    key, value = env_var.split("=", 1)
    return key.strip(), value.strip()

def _build_uv_command(
    file_spec: str,
    with_editable: Path | None = None,
    with_packages: list[str] | None = None,
) -> list[str]:
    cmd = ["uv", "run", "--with", _mcp_requirement()]
    if with_editable:
        cmd.extend(["--with-editable", str(with_editable)])
    if with_packages:
        for pkg in with_packages:
            if pkg:
                cmd.extend(["--with", pkg])
    cmd.extend(["mcp", "run", file_spec])
    return cmd

def _parse_file_path(file_spec: str) -> tuple[Path, str | None]:
    has_windows_drive = len(file_spec) > 1 and file_spec[1] == ":"
    if ":" in (file_spec[2:] if has_windows_drive else file_spec):
        file_str, server_object = file_spec.rsplit(":", 1)
    else:
        file_str, server_object = file_spec, None

    file_path = Path(file_str).expanduser().resolve()
    if not file_path.exists():
        log_mcp.error(f"File not found: {file_path}")
        sys.exit(1)
    if not file_path.is_file():
        log_mcp.error(f"Not a file: {file_path}")
        sys.exit(1)
    return file_path, server_object

def _import_server(file: Path, server_object: str | None = None):
    file_dir = str(file.parent)
    if file_dir not in sys.path:
        sys.path.insert(0, file_dir)

    spec = importlib.util.spec_from_file_location("server_module", file)
    if not spec or not spec.loader:
        log_mcp.error("Could not load module", extra={"file": str(file)})
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def _check_server_object(server_object: Any, object_name: str):
        try:
            from server.mcp.server import MCPServer
            if not isinstance(server_object, MCPServer):
                log_mcp.error(f"The server object {object_name} is not an MCPServer.")
                if isinstance(server_object, LowLevelServer):
                    log_mcp.warning("Note: Low level Server class is not yet fully supported here.")
                return False
            return True
        except ImportError:
            log_mcp.warning("Could not import server.mcp.server.MCPServer for validation.")
            return True

    if not server_object:
        for name in ["mcp", "server", "app"]:
            if hasattr(module, name):
                if not _check_server_object(getattr(module, name), f"{file}:{name}"):
                    continue
                return getattr(module, name)
        log_mcp.error(f"No valid server object found in {file}.")
        sys.exit(1)

    if ":" in server_object:
        module_name, object_name = server_object.split(":", 1)
        try:
            server_module = importlib.import_module(module_name)
            server = getattr(server_module, object_name, None)
        except ImportError:
            log_mcp.error(f"Could not import module '{module_name}'")
            sys.exit(1)
    else:
        server = getattr(module, server_object, None)

    if server is None:
        log_mcp.error(f"Server object '{server_object}' not found")
        sys.exit(1)
    if not _check_server_object(server, server_object):
        sys.exit(1)

    return server

@mcp_app.command("dev")
def mcp_dev(
    file_spec: str = typer.Argument(..., help="Python file to run, optionally with :object suffix"),
    with_editable: Annotated[Path | None, typer.Option("--with-editable", "-e", exists=True, resolve_path=True)] = None,
    with_packages: Annotated[list[str], typer.Option("--with", help="Additional packages to install")] = [],
) -> None:
    """Run an MCP server via the interactive MCP Inspector."""
    file, server_object = _parse_file_path(file_spec)
    log_mcp.info(f"Starting MCP Inspector for {file.name}...")

    try:
        server = _import_server(file, server_object)
        if hasattr(server, "dependencies"):
            with_packages = list(set(with_packages + server.dependencies))

        uv_cmd = _build_uv_command(file_spec, with_editable, with_packages)
        npx_cmd = _get_npx_command()
        
        if not npx_cmd:
            log_mcp.error("npx not found. Please install Node.js.")
            sys.exit(1)

        shell = sys.platform == "win32"
        process = subprocess.run(
            [npx_cmd, "@modelcontextprotocol/inspector"] + uv_cmd,
            check=True,
            shell=shell,
            env=dict(os.environ.items()),
        )
        sys.exit(process.returncode)
    except subprocess.CalledProcessError as e:
        log_mcp.error(f"Dev server failed: {e}")
        sys.exit(e.returncode)

@mcp_app.command("run")
def mcp_run(
    file_spec: str = typer.Argument(..., help="Python file to run"),
    transport: Annotated[str | None, typer.Option("--transport", "-t")] = None,
    port: Annotated[int | None, typer.Option("--port", "-p")] = None,
):
    """Run the MCP server locally with the specified transport."""
    file, server_object = _parse_file_path(file_spec)
    server = _import_server(file, server_object)

    module = sys.modules[server.__module__]
    script_config = getattr(module, "mcp_config", {})
    
    final_transport = transport or script_config.get("transport", "stdio")
    final_port = port or script_config.get("port", 8000)
    
    kwargs = {}
    if "event_store" in script_config:
        kwargs["event_store"] = script_config["event_store"]
    if "retry_interval" in script_config:
        kwargs["retry_interval"] = script_config["retry_interval"]

    try:
        log_mcp.info(f"🚀 Running server with transport: {final_transport}")
        if final_transport in ["sse", "streamable-http"]:
            kwargs["port"] = final_port
            server.run(transport=final_transport, **kwargs)
        else:
            server.run(transport="stdio", **kwargs)
    except Exception:
        log_mcp.exception("Failed to run server")
        sys.exit(1)

@mcp_app.command("config")
def mcp_generate_config(
    file_spec: str = typer.Argument(..., help="Python file to run, optionally with :object suffix"),
    server_name: Annotated[str | None, typer.Option("--name", "-n", help="Custom name for the server")] = None,
    with_editable: Annotated[Path | None, typer.Option("--with-editable", "-e", exists=True, resolve_path=True)] = None,
    with_packages: Annotated[list[str], typer.Option("--with", help="Additional packages to install")] = [],
    env_vars: Annotated[list[str], typer.Option("--env-var", "-v", help="Environment variables (KEY=VALUE)")] = [],
    env_file: Annotated[Path | None, typer.Option("--env-file", "-f", exists=True, resolve_path=True)] = None,
) -> None:
    """Generate universal MCP JSON config (for Cursor, Windsurf, Claude, etc.)."""
    file, server_object = _parse_file_path(file_spec)

    name = server_name
    server = None
    if not name:
        try:
            server = _import_server(file, server_object)
            name = getattr(server, "name", file.stem)
        except Exception:
            name = file.stem

    server_dependencies = getattr(server, "dependencies", []) if server else []
    if server_dependencies:
        with_packages = list(set(with_packages + server_dependencies))

    env_dict = {}
    if env_file:
        if dotenv:
            try:
                env_dict.update({k: v for k, v in dotenv.dotenv_values(env_file).items() if v is not None})
            except Exception as e:
                log_mcp.error(f"Failed to load .env file: {e}")
                sys.exit(1)
        else:
            log_mcp.warning("python-dotenv is not installed. Ignoring .env file.")

    for env_var in env_vars:
        key, value = _parse_env_var(env_var)
        env_dict[key] = value

    args = ["run", "--frozen"]
    packages = {_mcp_requirement("mcp[cli]")}
    if with_packages:
        packages.update(pkg for pkg in with_packages if pkg)

    for pkg in sorted(packages):
        args.extend(["--with", pkg])

    if with_editable:
        args.extend(["--with-editable", str(with_editable)])

    has_windows_drive = len(file_spec) > 1 and file_spec[1] == ":"
    if ":" in (file_spec[2:] if has_windows_drive else file_spec):
        file_path, server_obj = file_spec.rsplit(":", 1)
        file_spec = f"{Path(file_path).resolve()}:{server_obj}"
    else:
        file_spec = str(Path(file_spec).resolve())

    args.extend(["mcp", "run", file_spec])

    server_config: dict[str, Any] = {
        "command": _get_uv_path(),
        "args": args
    }
    if env_dict:
        server_config["env"] = env_dict

    final_payload = {
        "mcpServers": {
            name: server_config
        }
    }

    log_mcp.info("\n" + "="*50)
    log_mcp.info(f"✨ Copy this configuration into your MCP Client")
    log_mcp.info(f"   (Cursor / Windsurf / Claude Desktop)")
    log_mcp.info("="*50 + "\n")
    log_mcp.info(json.dumps(final_payload, indent=2))
    log_mcp.info("\n" + "="*50)


# ==========================================
# 2. Brane Subcommands & Helpers
# ==========================================

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

@brane_app.command("run")
def brane_run(
    service: Annotated[str, typer.Argument(help="Service to run: 'gateway', 'pypi', 'broker', or 'all'")],
    port: Annotated[Optional[int], typer.Option("--port", "-p", help="Override default port")] = None,
    reload: Annotated[bool, typer.Option("--reload", "-r", help="Enable auto-reload for development")] = False,
):
    """Start Brane infrastructure services."""
    if service not in SERVICES and service != "all":
        log_brane.error(f"Unknown service: {service}. Available: {', '.join(SERVICES.keys())}, all")
        raise typer.Exit(1)

    processes: list[subprocess.Popen] = []

    try:
        if service == "all":
            log_brane.info("🚀 Starting entire Brane Infrastructure...")
            
            pypi_port = 8083
            log_brane.info(f"Starting [PyPI Membrane] on port {pypi_port}")
            processes.append(_run_service("pypi", SERVICES["pypi"], pypi_port, reload))
            
            time.sleep(2)
            
            gw_port = port or SERVICES["gateway"]["default_port"]
            log_brane.info(f"Starting [Gateway] on port {gw_port}")
            processes.append(_run_service("gateway", SERVICES["gateway"], gw_port, reload))
            log_brane.info("✅ All services are running. Press Ctrl+C to stop.")
            for p in processes:
                p.wait()
        else:
            svc = SERVICES[service]
            run_port = port or svc["default_port"]
            log_brane.info(f"🚀 Starting {service} ({svc['description']}) on port {run_port}...")
            
            p = _run_service(service, svc, run_port, reload)
            processes.append(p)
            p.wait()
    except KeyboardInterrupt:
        log_brane.info("\n🛑 Shutting down Brane services...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        log_brane.info("Shutdown complete.")
        raise typer.Exit(0)
    except Exception as e:
        log_brane.error(f"Failed to run service: {e}")
        raise typer.Exit(1)

@brane_app.command("ls")
def brane_ls():
    """List available Brane infrastructure services."""
    log_brane.info("\n📦 Available Brane Services:")
    log_brane.info("-" * 65)
    for name, info in SERVICES.items():
        log_brane.info(f" • {name:<10} | Port: {info['default_port']:<5} | {info['description']}")
    log_brane.info("-" * 65)
    log_brane.info("Usage: <cli> brane run <service_name> | <cli> brane run all\n")

async def _client_message_handler(
    message: RequestResponder[ServerRequest, ClientResult] | ServerNotification | Exception,
) -> None:
    if isinstance(message, Exception):
        log_brane.error(f"Error from server: {message}")
        return
    log_brane.info(f"Received message: {message}")

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
        log_brane.info("🔄 Initializing MCP Session...")
        await session.initialize()
        log_brane.info("✅ Session Initialized successfully!")
        await anyio.sleep_forever()

async def async_client_main(command_or_url: str, args: list[str], env_dict: dict[str, str]):
    if urlparse(command_or_url).scheme in ("http", "https"):
        log_brane.info(f"Connecting to SSE endpoint: {command_or_url}")
        async with sse_client(command_or_url) as streams:
            await _run_client_session(*streams)
    else:
        log_brane.info(f"Executing Stdio command: {command_or_url} {' '.join(args)}")
        server_parameters = StdioServerParameters(command=command_or_url, args=args, env=env_dict)
        async with stdio_client(server_parameters) as streams:
            await _run_client_session(*streams)

@brane_app.command("client", context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def brane_client(
    ctx: typer.Context,
    target: Annotated[str, typer.Argument(help="HTTP(S) SSE URL or Stdio Command to connect to")],
):
    """Test MCP Server connection via SSE or Stdio."""
    args = ctx.args
    env_dict = dict(os.environ) 
    try:
        anyio.run(partial(async_client_main, target, args, env_dict), backend="trio")
    except KeyboardInterrupt:
        log_brane.info("Disconnecting client...")

if __name__ == "__main__":
    app()