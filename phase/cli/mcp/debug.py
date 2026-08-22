# phase.cli.mcp.debug
## @lineage: cli.mcp.debug
## @lineage: meta.cli.mcp.debug
import importlib.metadata
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated, Any
import typer

from mcp.server.lowlevel.server import Server as LowLevelServer
from watcher.plane.emitter import get_emitter

try:
    import dotenv
except ImportError:
    dotenv = None

log = get_emitter("debug.mcp")

app = typer.Typer(
    name="mcp",
    help="Brane MCP Development & Execution Tools",
    add_completion=False,
    no_args_is_help=True,
)

def _mcp_requirement(package: str = "mcp") -> str:
    """Get the current MCP package version requirement."""
    try:
        version = importlib.metadata.version("mcp")
    except importlib.metadata.PackageNotFoundError:
        return package
    if ".dev" in version or "+" in version:
        return package
    return f"{package}=={version}"

def _get_uv_path() -> str:
    """Get the full path to the uv executable."""
    uv_path = shutil.which("uv")
    if not uv_path:
        log.warning("uv executable not found in PATH. Falling back to 'uv'.")
        return "uv"
    return uv_path

def _get_npx_command():
    """Get the correct npx command for the current platform."""
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
        log.error(f"Invalid environment variable format: {env_var}. Must be KEY=VALUE")
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
        log.error(f"File not found: {file_path}")
        sys.exit(1)
    if not file_path.is_file():
        log.error(f"Not a file: {file_path}")
        sys.exit(1)

    return file_path, server_object

def _import_server(file: Path, server_object: str | None = None):
    file_dir = str(file.parent)
    if file_dir not in sys.path:
        sys.path.insert(0, file_dir)

    spec = importlib.util.spec_from_file_location("server_module", file)
    if not spec or not spec.loader:
        log.error("Could not load module", extra={"file": str(file)})
        sys.exit(1)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def _check_server_object(server_object: Any, object_name: str):
        # Local project import based on your structure
        from server.mcp.server import MCPServer
        if not isinstance(server_object, MCPServer):
            log.error(f"The server object {object_name} is not an MCPServer.")
            if isinstance(server_object, LowLevelServer):
                log.warning("Note: Low level Server class is not yet fully supported here.")
            return False
        return True

    if not server_object:
        for name in ["mcp", "server", "app"]:
            if hasattr(module, name):
                if not _check_server_object(getattr(module, name), f"{file}:{name}"):
                    continue
                return getattr(module, name)

        log.error(f"No valid server object found in {file}.")
        sys.exit(1)

    if ":" in server_object:
        module_name, object_name = server_object.split(":", 1)
        try:
            server_module = importlib.import_module(module_name)
            server = getattr(server_module, object_name, None)
        except ImportError:
            log.error(f"Could not import module '{module_name}'")
            sys.exit(1)
    else:
        server = getattr(module, server_object, None)

    if server is None:
        log.error(f"Server object '{server_object}' not found")
        sys.exit(1)

    if not _check_server_object(server, server_object):
        sys.exit(1)

    return server

@app.command()
def dev(
    file_spec: str = typer.Argument(..., help="Python file to run, optionally with :object suffix"),
    with_editable: Annotated[Path | None, typer.Option("--with-editable", "-e", exists=True, resolve_path=True)] = None,
    with_packages: Annotated[list[str], typer.Option("--with", help="Additional packages to install")] = [],
) -> None:
    """Run an MCP server via the interactive MCP Inspector."""
    file, server_object = _parse_file_path(file_spec)
    log.info(f"Starting MCP Inspector for {file.name}...")

    try:
        server = _import_server(file, server_object)
        if hasattr(server, "dependencies"):
            with_packages = list(set(with_packages + server.dependencies))

        uv_cmd = _build_uv_command(file_spec, with_editable, with_packages)
        npx_cmd = _get_npx_command()
        
        if not npx_cmd:
            log.error("npx not found. Please install Node.js.")
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
        log.error(f"Dev server failed: {e}")
        sys.exit(e.returncode)


@app.command()
def run(
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
        log.info(f"🚀 Running server with transport: {final_transport}")
        if final_transport in ["sse", "streamable-http"]:
            kwargs["port"] = final_port
            server.run(transport=final_transport, **kwargs)
        else:
            server.run(transport="stdio", **kwargs)
    except Exception:
        log.exception("Failed to run server")
        sys.exit(1)

@app.command(name="config")
def generate_config(
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
                log.error(f"Failed to load .env file: {e}")
                sys.exit(1)
        else:
            log.warning("python-dotenv is not installed. Ignoring .env file.")

    for env_var in env_vars:
        key, value = _parse_env_var(env_var)
        env_dict[key] = value

    # Build uv args
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

    # Construct the final JSON payload
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

    # log.info nicely formatted JSON to stdout for easy copy/paste or piping
    log.info("\n" + "="*50)
    log.info(f"✨ Copy this configuration into your MCP Client")
    log.info(f"   (Cursor / Windsurf / Claude Desktop)")
    log.info("="*50 + "\n")
    log.info(json.dumps(final_payload, indent=2))
    log.info("\n" + "="*50)

if __name__ == "__main__":
    app()