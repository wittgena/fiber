# anchor.cli.secure.auth
## @lineage: anchor.cli.auth
## @lineage: anchor.cli.proxy
import os
import subprocess
import sys
import time
from typing import Annotated
import typer
from watcher.plane.emitter import get_emitter

log = get_emitter("cli.auth")

app = typer.Typer(
    name="brane",
    help="Brane API Gateway & Auth Management Tools",
    add_completion=False,
    no_args_is_help=True,
)

SERVICES = {
    "broker": {
        "module": "anchor.mcp.server.auth.broker:app",
        "default_port": 9000,
        "description": "Authorization Server (Token Issuer)",
    },
    "proxy": {
        "module": "anchor.mcp.proxy:app",
        "default_port": 8080,
        "description": "Brane API Gateway (Reverse Proxy)",
    },
}

def _run_uvicorn(module: str, port: int, reload: bool, env: dict | None = None) -> subprocess.Popen:
    """uvicorn 프로세스를 백그라운드(또는 포그라운드)로 실행합니다."""
    cmd = ["uvicorn", module, "--host", "0.0.0.0", "--port", str(port)]
    if reload:
        cmd.append("--reload")
    
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
        
    return subprocess.Popen(cmd, env=run_env)


@app.command()
def run(
    service: Annotated[
        str, 
        typer.Argument(help="Service to run: 'proxy', 'broker', or 'all'")
    ],
    port: Annotated[int | None, typer.Option("--port", "-p", help="Override default port")] = None,
    reload: Annotated[bool, typer.Option("--reload", "-r", help="Enable auto-reload for development")] = False,
):
    """Run Brane infrastructure services."""
    
    if service not in SERVICES and service != "all":
        log.error(f"Unknown service: {service}. Available: {', '.join(SERVICES.keys())}, all")
        sys.exit(1)

    processes: list[subprocess.Popen] = []

    try:
        if service == "all":
            log.info("🚀 Starting entire Brane Infrastructure (Broker + Proxy)...")
            # 브로커 실행
            broker_port = 9000
            log.info(f"Starting Broker on port {broker_port}")
            processes.append(_run_uvicorn(SERVICES["broker"]["module"], broker_port, reload))
            
            # 프록시가 브로커를 인식할 수 있도록 약간의 딜레이
            time.sleep(1)
            
            # 프록시 실행
            proxy_port = port or 8080
            log.info(f"Starting Proxy Gateway on port {proxy_port}")
            processes.append(_run_uvicorn(SERVICES["proxy"]["module"], proxy_port, reload))
            
            log.info("✅ All services are running. Press Ctrl+C to stop.")
            
            # 메인 스레드 대기
            for p in processes:
                p.wait()
                
        else:
            # 단일 서비스 실행
            svc = SERVICES[service]
            run_port = port or svc["default_port"]
            log.info(f"🚀 Starting {service} ({svc['description']}) on port {run_port}...")
            
            p = _run_uvicorn(svc["module"], run_port, reload)
            processes.append(p)
            p.wait()

    except KeyboardInterrupt:
        log.info("\n🛑 Shutting down Brane services...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        log.info("Shutdown complete.")
        sys.exit(0)
    except Exception as e:
        log.exception(f"Failed to run service: {e}")
        sys.exit(1)


@app.command()
def list():
    """List available Brane infrastructure services."""
    log.info("\n📦 Available Brane Services:")
    log.info("-" * 50)
    for name, info in SERVICES.items():
        log.info(f" • {name:<10} : {info['description']} (Default Port: {info['default_port']})")
    log.info("-" * 50)
    log.info("Run with: brane run <service_name>\n")


if __name__ == "__main__":
    app()