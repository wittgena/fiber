# fiber.dev.ex.space.demo.server
import os
import sys
import asyncio

from fiber.gateway.mcp.connector import MCPServerConnector
import fiber.dev.ex.worker.legacy.oracle as worker_oracle
import fiber.dev.ex.worker.legacy.finlib as worker_finlib
import fiber.dev.ex.worker.search.archive as worker_search

from xphi.kernel.ops.boot import main_async, teardown
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("space.demo.server")

async def run_integrated_server():
    log.info("[TestServer] 🚀 Igniting All-in-One MCP Edge Server...")
    connectors = [
        MCPServerConnector(
            target_id="oracle-01", 
            execution_target=f"{sys.executable} -m {worker_oracle.__name__}", 
            mode="multiplex"
        ),
        MCPServerConnector(
            target_id="finlib-01", 
            execution_target=f"{sys.executable} -m {worker_finlib.__name__}", 
            mode="linear"
        ),
        MCPServerConnector(
            target_id="search-archive-01", 
            execution_target=f"{sys.executable} -m {worker_search.__name__}", 
            mode="multiplex"
        )
    ]

    # 커넥터를 비동기 백그라운드 태스크로 실행
    connector_tasks = [asyncio.create_task(c.run()) for c in connectors]
    
    # Core Daemons (rest_edge, rpc_worker) 백그라운드 실행
    core_task = asyncio.create_task(main_async())

    log.info("[TestServer] ✅ Core Daemons and 3 Workers are running concurrently.")

    try:
        await asyncio.gather(core_task, *connector_tasks)
    except asyncio.CancelledError:
        log.info("\n[TestServer] Shutdown signal received. Cleaning up connectors...")
        for c in connectors:
            c.running = False

def main():
    os.environ["KERNEL_DAEMONS"] = "rest_edge,rpc_worker"
    os.environ["NODE_PROFILE"] = "EDGE"
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"
    try:
        PhaseReactor.ignite(main_coro_func=run_integrated_server, teardown_hook=teardown)
    except KeyboardInterrupt:
        log.info("[TestServer] 👋 Gracefully terminated all-in-one test server.")

if __name__ == "__main__":
    main()