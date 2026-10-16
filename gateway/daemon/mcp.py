# fiber.gateway.daemon.mcp
import os
import asyncio
from contextlib import suppress
from typing import Dict, List

from fiber.gateway.mcp.connector import MCPServerConnector
from fiber.phase.cli.connect import (
    ResolvedTarget,
    EXTERNAL_MCP_CATALOG,
    discover_builtin_workers,
    resolve_connector_spec,
)
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.mcp")


@contract.daemon("mcp_worker")
class McpWorkerDaemon(AbstractDaemon):
    """
    커널 부팅 시 fiber.phase.cli.connect의 타겟 해석기(AST 내장 탐색 + 외부 설치 스캔/대행)를 공유하여
    MCP 워커들을 MCPServerConnector로 일괄 마운트하고 생명주기(Self-Healing/Teardown)를 관리하는 데몬.
    """
    def __init__(self, ctx):
        super().__init__("McpWorkerDaemon")
        self.ctx = ctx
        self.base_package = os.getenv("MCP_WORKER_PACKAGE", "fiber.dev.worker")
        self.connectors: Dict[str, MCPServerConnector] = {}
        self._connector_tasks: Dict[str, asyncio.Task] = {}
        self._target_specs: Dict[str, ResolvedTarget] = {}

    def _collect_target_specs(self) -> List[ResolvedTarget]:
        """
        1) 기본값: fiber.dev.worker 하위의 내장 워커(Canonical ID 기준)들을 자동 수집
        2) MCP_WORKER_TARGETS 지정 시: 내장 워커 + EXTERNAL_MCP_CATALOG 대행 워커 + 커스텀 타겟을
           connect.py의 resolve_connector_spec을 통해 일관되게 해석하여 수집
        """
        builtin_map = discover_builtin_workers(self.base_package)

        # 중복 실행 방지를 위해 대표 별칭(Canonical ID, 예: worker.data, worker.docs)만 1차 추출
        canonical_builtins: Dict[str, tuple[str, str]] = {}
        seen_cmds = set()
        for tid, (cmd, default_mode) in builtin_map.items():
            if cmd not in seen_cmds:
                seen_cmds.add(cmd)
                canonical_builtins[tid] = (cmd, default_mode)

        filter_str = os.getenv("MCP_WORKER_TARGETS", "").strip()
        resolved_list: List[ResolvedTarget] = []

        # 케이스 A: 환경변수 미지정 시 내장 워커 전체 자동 마운트
        if not filter_str:
            for tid, (cmd, default_mode) in canonical_builtins.items():
                spec = ResolvedTarget(
                    target_id=tid,
                    exec_cmd=cmd,
                    mode=default_mode,
                    transport="stdio",
                )
                self._target_specs[tid] = spec
                resolved_list.append(spec)
            return resolved_list

        # 케이스 B: "ALL" 지정 시 내장 워커 전체 + 외부 대행 카탈로그 전체 마운트
        if filter_str.upper() == "ALL":
            requested_ids = list(canonical_builtins.keys()) + [
                k for k in EXTERNAL_MCP_CATALOG.keys() if k not in canonical_builtins
            ]
        else:
            # 케이스 C: 콤마로 구분된 특정 타겟 목록만 선택 마운트 (예: "worker.data,worker.repomix")
            requested_ids = [t.strip() for t in filter_str.split(",") if t.strip()]

        for raw_entry in requested_ids:
            # "custom-id=https://..." 형태 지원 또는 프리셋 ID 그대로 사용
            if "=" in raw_entry:
                tid, exec_expr = [p.strip() for p in raw_entry.split("=", 1)]
            else:
                tid, exec_expr = raw_entry, raw_entry

            try:
                # 내장 워커면 AST에서 추출된 기본 모드(multiplex/linear)를 보존
                preferred_mode = canonical_builtins[exec_expr][1] if exec_expr in canonical_builtins else None
                spec = resolve_connector_spec(
                    target=tid,
                    exec_cmd=exec_expr,
                    mode=preferred_mode,
                )
                self._target_specs[tid] = spec
                resolved_list.append(spec)
            except BaseException as e:
                log.error(f"[{self.name}] Failed to resolve worker target '{raw_entry}': {e}")

        return resolved_list

    def _spawn_connector_task(self, spec: ResolvedTarget):
        connector = MCPServerConnector(
            target_id=spec.target_id,
            execution_target=spec.exec_cmd,
            mode=spec.mode,
            transport_type=spec.transport,
        )
        self.connectors[spec.target_id] = connector
        task = asyncio.create_task(
            connector.run(),
            name=f"mcp-connector-{spec.target_id}",
        )
        self._connector_tasks[spec.target_id] = task
        log.info(
            f"[{self.name}] 🔌 Mounted [{spec.target_id}] -> {spec.exec_cmd} "
            f"(Mode: {spec.mode.upper()}, Transport: {spec.transport.upper()})"
        )

    async def run(self):
        log.info(f"[{self.name}] Starting Autonomous MCP Worker Supervisor Daemon...")
        try:
            specs = self._collect_target_specs()
            if not specs:
                log.warning(f"[{self.name}] No MCP workers discovered or configured.")
            else:
                log.info(f"[{self.name}] Igniting {len(specs)} resolved MCP worker connector(s)...")
                for spec in specs:
                    self._spawn_connector_task(spec)

            # 데몬 메인 감시 루프: 비정상 종료된 커넥터 감지 및 자동 재기동(Self-Healing)
            while self.running:
                for target_id, task in list(self._connector_tasks.items()):
                    if task.done():
                        with suppress(asyncio.CancelledError):
                            exc = task.exception()
                            if exc:
                                log.error(
                                    f"[{self.name}] Connector [{target_id}] fractured with error: {exc}. "
                                    f"Respawning in 2s...",
                                    exc_info=exc,
                                )
                            else:
                                log.warning(f"[{self.name}] Connector [{target_id}] exited unexpectedly. Respawning...")

                        if self.running:
                            await asyncio.sleep(2.0)
                            spec = self._target_specs.get(target_id)
                            if spec:
                                self._spawn_connector_task(spec)

                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            log.info(f"[{self.name}] Shutdown signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal error in McpWorkerDaemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing {len(self._connector_tasks)} MCP Worker Connector(s)...")
        shutdown_timeout = float(os.getenv("SHUTDOWN_TIMEOUT", 10.0))

        # 1. 커넥터 내부 종료 플래그 또는 stop 메서드가 있을 경우 우선 호출
        for connector in self.connectors.values():
            if hasattr(connector, "running"):
                connector.running = False
            if hasattr(connector, "stop") and callable(connector.stop):
                with suppress(Exception):
                    res = connector.stop()
                    if asyncio.iscoroutine(res):
                        await res

        # 2. 실행 중인 모든 커넥터 태스크 일괄 취소 및 대기
        pending_tasks = [t for t in self._connector_tasks.values() if not t.done()]
        for task in pending_tasks:
            task.cancel()

        if pending_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*pending_tasks, return_exceptions=True),
                    timeout=shutdown_timeout,
                )
            except asyncio.TimeoutError:
                log.warning(f"[{self.name}] Timed out waiting for connectors to evaporate.")

        self._connector_tasks.clear()
        self.connectors.clear()
        self._target_specs.clear()
        log.info(f"[{self.name}] MCP Worker Daemon resource cleanup complete.")