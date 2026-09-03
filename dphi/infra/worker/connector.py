# fiber.dphi.infra.worker.connector
import os
import sys
import json
import asyncio
import logging
import argparse
from typing import Dict, Any, Optional

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from fiber.dphi.rpc.client import InternalRpcClient
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("worker.connector")

class LegacyStdioTransport:
    """
    기존 문명(3rd Party MCP Server)과의 물리적 단절을 연결하는 브릿지.
    표준 입출력(stdin/stdout)을 통해 순수 JSON-RPC를 주입하고 추출합니다.
    """
    def __init__(self, command: str):
        self.command = command
        self.process: Optional[asyncio.subprocess.Process] = None

    async def start(self):
        log.info(f"[Transport] Booting legacy MCP server: {self.command}")
        # 자식 프로세스로 기존 MCP 서버 실행
        self.process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # stderr(에러 로그)를 백그라운드에서 읽어 커넥터 로그로 출력 (디버깅 지원)
        asyncio.create_task(self._monitor_stderr())
        log.info(f"[Transport] Legacy server running (PID: {self.process.pid})")

    async def _monitor_stderr(self):
        """레거시 서버의 에러 출력을 비동기로 모니터링"""
        while self.process and not self.process.stderr.at_eof():
            line = await self.process.stderr.readline()
            if line:
                log.warning(f"[Legacy STDERR] {line.decode('utf-8').strip()}")

    async def execute_payload(self, json_rpc_payload: Dict[str, Any]) -> Dict[str, Any]:
        """순수 MCP 스펙을 레거시에 주입하고 응답을 대기"""
        if not self.process or self.process.returncode is not None:
            raise RuntimeError("Legacy process is dead.")

        try:
            # 1. Inject: 레거시 프로세스의 stdin으로 JSON-RPC 전송 (Newline delimited)
            raw_msg = json.dumps(json_rpc_payload) + "\n"
            self.process.stdin.write(raw_msg.encode('utf-8'))
            await self.process.stdin.drain()

            # 2. Extract: 레거시 프로세스의 stdout에서 결과 수신
            raw_output = await self.process.stdout.readline()
            if not raw_output:
                raise RuntimeError("EOF reached while reading legacy stdout.")
                
            return json.loads(raw_output.decode('utf-8').strip())

        except Exception as e:
            log.error(f"[Transport] Legacy Execution Failed: {e}")
            raise


class McpConnectorDaemon:
    """
    DPHI Message Bus를 구독하며, 외부와 단절된 망(VPC) 내에서 
    이벤트를 Pull 방식으로 가져와 레거시 서버를 트리거하는 사이드카 데몬.
    """
    def __init__(self, target_id: str, legacy_command: str):
        self.target_id = target_id
        self.transport = LegacyStdioTransport(legacy_command)
        self.tunnel = None
        self.rpc = InternalRpcClient()
        self.listen_channel = f"mcp.intent.queue.{self.target_id}"
        self.running = False

    async def run(self):
        # 1. 물리적 레거시 서버 가동
        await self.transport.start()
        
        # 2. DPHI 내부망 (Message Bus) 연결
        self.tunnel = await TunnelFactory.get_default()
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.listen_channel)
        
        self.running = True
        log.info(f"[Connector:{self.target_id}] 🚀 Listening for Deterministic Intents on DPHI Bus...")
        log.info(f"[Connector:{self.target_id}] Channel: {self.listen_channel}")

        try:
            # 3. 비동기 이벤트 주도(Event-Driven) 구독 루프
            async for msg in pubsub.listen():
                if not self.running:
                    break
                    
                if msg and msg["type"] == "message":
                    intent_data = json.loads(msg["data"])
                    # I/O 블로킹을 막기 위해 개별 트랜잭션을 백그라운드 태스크로 던짐
                    asyncio.create_task(self.process_intent(intent_data))
                    
        except asyncio.CancelledError:
            log.info("[Connector] Shutdown signal received.")
        except Exception as e:
            log.error(f"[Connector] Fatal Bus Error: {e}", exc_info=True)
        finally:
            await pubsub.unsubscribe(self.listen_channel)
            await pubsub.close()
            if self.transport.process:
                self.transport.process.terminate()

    async def process_intent(self, intent_data: Dict[str, Any]):
        """단일 MCP 인텐트의 상태를 관리하고 물리적 실행을 중계"""
        handle_id = intent_data.get("handle_id")
        action = intent_data.get("action")
        payload = intent_data.get("payload", {})

        if not handle_id:
            log.warning("[Connector] Received intent without handle_id. Dropping.")
            return

        try:
            log.debug(f"[Connector] Bridging Intent {action} to Legacy Server (Handle: {handle_id})")
            
            # 1. 물리적 변환 및 주입 (Legacy 실행)
            legacy_result = await self.transport.execute_payload(payload)

            # 2. 실행 결과를 RPC로 보고 (상태 전이 제안)
            # -> 이 요청을 받은 DPHI Core Handler가 LogicStream을 만들어 원장 상태를 "RESOLVED"로 전이시킴
            await self.rpc.call("mcp.bridge.resolve_state", {
                "handle_id": handle_id,
                "status": "RESOLVED",
                "executable_payload": legacy_result
            })
            log.info(f"[Connector] Intent {handle_id} resolved and reported to Core.")

        except Exception as e:
            # 상태 3: FAULTED (레거시 프로세스 크래시 또는 타임아웃)
            log.error(f"[Connector] Legacy execution crashed for handle {handle_id}: {e}")
            try:
                # 크래시 발생 시에도 FAULTED 상태 전이를 제안
                await self.rpc.call("mcp.bridge.resolve_state", {
                    "handle_id": handle_id,
                    "status": "FAULTED",
                    "error_detail": str(e)
                })
            except Exception as rpc_e:
                log.critical(f"[Connector] Failed to report FAULTED state to Core: {rpc_e}")


def main():
    parser = argparse.ArgumentParser(description="Fiber DPHI Egress Sidecar Connector")
    parser.add_argument("--target", required=True, help="Gateway에 등록된 이 서버의 고유 ID (예: db-server-01)")
    parser.add_argument("--exec", required=True, help="기존 MCP 서버를 구동하는 명령어 (예: 'node build/index.js')")
    
    args = parser.parse_args()

    # Logging 설정
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")

    daemon = McpConnectorDaemon(target_id=args.target, legacy_command=args.exec)
    
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        log.info("[Connector] Exiting gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()