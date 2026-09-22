# fiber.dev.ex.demo.server
import os
import sys
import asyncio

from fiber.gateway.node.worker.connector import WorkerConnector
import fiber.gateway.node.worker.mcp.oracle as agent_oracle
import fiber.gateway.node.worker.mcp.finlib as agent_finlib
import fiber.gateway.node.worker.search.archive as agent_search

from xphi.kernel.ops.boot import main_async, teardown
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ex.demo.server")

async def run_integrated_server():
    log.info("[TestServer] 🚀 Igniting All-in-One MCP Edge Server...")

    # 1. 워커 커넥터 생성 (E2E 설정과 동일한 모드 부여)
    connectors = [
        WorkerConnector(
            target_id="oracle-01", 
            execution_target=f"{sys.executable} -m {agent_oracle.__name__}", 
            mode="multiplex"
        ),
        WorkerConnector(
            target_id="finlib-01", 
            execution_target=f"{sys.executable} -m {agent_finlib.__name__}", 
            mode="linear"
        ),
        WorkerConnector(
            target_id="search-archive-01", 
            execution_target=f"{sys.executable} -m {agent_search.__name__}", 
            mode="multiplex"
        )
    ]

    # 2. 커넥터를 비동기 백그라운드 태스크로 실행
    connector_tasks = [asyncio.create_task(c.run()) for c in connectors]
    
    # 3. Core Daemons (rest_edge, rpc_worker) 백그라운드 실행
    core_task = asyncio.create_task(main_async())

    log.info("[TestServer] ✅ Core Daemons and 3 Workers are running concurrently.")

    try:
        # 무한 대기 (모든 컴포넌트가 동일한 루프 위에서 동작)
        await asyncio.gather(core_task, *connector_tasks)
    except asyncio.CancelledError:
        log.info("\n[TestServer] Shutdown signal received. Cleaning up connectors...")
        for c in connectors:
            c.running = False
        # 코어 데몬 종료는 PhaseReactor의 teardown_hook이 안전하게 처리함

def main():
    # CLI에서 'fiber daemon -s core'를 입력한 것과 동일한 환경 변수 강제 주입
    os.environ["KERNEL_DAEMONS"] = "rest_edge,rpc_worker"
    os.environ["NODE_PROFILE"] = "EDGE"
    os.environ["GATEWAY_TOPOLOGY"] = "EMBEDDED_BYPASS"

    # OS 제어권 위임 (UVLoop 적용 및 시그널 핸들링)
    try:
        PhaseReactor.ignite(main_coro_func=run_integrated_server, teardown_hook=teardown)
    except KeyboardInterrupt:
        log.info("[TestServer] 👋 Gracefully terminated all-in-one test server.")

if __name__ == "__main__":
    main()