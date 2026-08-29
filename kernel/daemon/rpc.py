# fiber.kernel.daemon.rpc
import os
import json
import asyncio
from contextlib import suppress
from typing import Optional, Dict, Callable

from fiber.dphi.rpc.handler import INTERNAL_HANDLERS_REGISTRY, WorkerContext
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.daemon.base import AbstractDaemon
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory

log = get_emitter("daemon.rpc")

@contract.daemon("rpc_worker")
class RpcWorkerDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RpcWorkerDaemon")
        self.app_ctx = ctx  # Global daemon context (or Mock context for testing)
        
        # 운영 환경에서 스케일 아웃을 대비한 식별자 매핑
        self.topic = os.getenv("RPC_QUEUE_TOPIC", "internal.rpc.queue")
        self.group = os.getenv("RPC_QUEUE_GROUP", "internal_workers")
        self.worker_id = os.getenv("RPC_WORKER_ID", f"worker-{os.getpid()}")
        
        self.routes: Dict[str, Callable] = INTERNAL_HANDLERS_REGISTRY
        self.tunnel = None
        self.worker_ctx: Optional[WorkerContext] = None
        self._tasks = set()

    async def _init_context(self):
        """
        Worker 구동에 필요한 무거운 의존성들을 초기화합니다.
        순환 참조 방지를 위해 함수 내부에 Import 배치.
        """
        from fiber.dphi.adapter.anchor import NexusAnchor
        from fiber.kernel.receptor.gov.policy import IngressPolicyEngine, ToposSequencer, FuelAllocator, HealthMonitor

        from xphi.kernel.dphi.broker import DphiBroker
        from xphi.xor.stream.edge import LogStreamStore
        from xphi.arch.eco.adapter.transaction import ExchangeAdapter
        from xphi.kernel.dphi.adapter.utxo import UtxoAdapter
        from xphi.kernel.space.sandbox import BenchProfile
        from xphi.kernel.dphi.adapter.sign import NodeSigner

        log.info(f"[{self.name}] Initializing Headless Worker Dependencies...")
        
        broker = getattr(self.app_ctx, "broker", None) or DphiBroker()
        store = getattr(self.app_ctx, "store", None) or LogStreamStore()
        
        # TODO: 운영(Production) 환경에서는 실제 위원회 키와 노드 키를 주입해야 합니다.
        nexus = NexusAnchor(broker=broker, consensus_threshold=1, allowed_committee=[])
        node_pubkey = NodeSigner.get_instance().pubkey_hex if hasattr(NodeSigner, 'get_instance') else "mock_pubkey"
        exchange_adapter = ExchangeAdapter(clearing_house_pub_key=node_pubkey)
        utxo_adapter = UtxoAdapter(broker=broker)
        
        policy_engine = IngressPolicyEngine(
            sequencer=ToposSequencer(), allocator=FuelAllocator(), monitor=HealthMonitor()
        )
        profile_service = BenchProfile()

        # 전역 의존성 객체 래핑
        self.worker_ctx = WorkerContext(
            broker=broker,
            store=store,
            nexus=nexus,
            exchange_adapter=exchange_adapter,
            utxo_adapter=utxo_adapter,
            policy_engine=policy_engine,
            profile_service=profile_service
        )

    async def run(self):
        log.info(f"[{self.name}] Initiating Autonomous RPC Worker Daemon...")
        try:
            # 1. 의존성 및 터널 연결
            await self._init_context()
            self.tunnel = await TunnelFactory.get_default()
            log.info(f"[{self.name}] 🚀 Worker [{self.worker_id}] listening on topic: {self.topic}")
            
            # 2. 메인 컨슘 루프
            while self.running:
                try:
                    # 블로킹(block=2000ms) 방식으로 스트림 대기
                    messages = await self.tunnel.stream_consume(
                        self.topic, self.group, self.worker_id, count=10, block=2000
                    )
                    
                    # 3. 비동기 메시지 처리 태스크 스케줄링
                    for stream_name, msg_list in messages:
                        for message_id, msg_data in msg_list:
                            task = asyncio.create_task(self.process_message(message_id, msg_data))
                            self._tasks.add(task)
                            # GC 처리를 위해 작업 완료 시 set에서 자동 제거
                            task.add_done_callback(self._tasks.discard)
                            
                except asyncio.TimeoutError:
                    # block=2000 에 의한 정상적인 타임아웃, 루프 재개
                    pass 
                except Exception as e:
                    if self.running:
                        log.error(f"[{self.name}] Stream Consume Error: {e}")
                        await asyncio.sleep(1)

        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal execution error. Evaporating daemon: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def process_message(self, message_id: str, msg_data: dict):
        """단일 RPC 메시지의 디코딩, 실행, 응답, ACK를 처리하는 워커 로직"""
        reply_to = None
        request_id = None
        try:
            payload_str = msg_data.get("payload") or msg_data.get(b"payload")
            if not payload_str: 
                return

            payload = json.loads(payload_str)
            method = payload.get("method")
            params = payload.get("params", {})
            reply_to = payload.get("reply_to") 
            request_id = payload.get("id")
            
            # 1. 라우팅 테이블 조회
            handler = self.routes.get(method)
            if not handler:
                response = {"error": True, "code": 404, "message": f"Method {method} not found"}
                log.warning(f"[{self.name}] Unknown method invoked: {method}")
            else:
                # [IMPROVED] 2. 핸들러 크래시(예: DB 타임아웃, 문법 오류) 방어벽 구축
                try:
                    response = await handler(params, self.worker_ctx)
                except Exception as handler_exc:
                    log.error(f"[{self.name}] Unhandled Exception in {method}: {handler_exc}", exc_info=True)
                    response = {"error": True, "code": 500, "message": f"Internal Worker Execution Error: {str(handler_exc)}"}
                
            # 3. 응답(Reply) 퍼블리시
            if reply_to:
                await self.tunnel.publish(reply_to, json.dumps({
                    "id": request_id,
                    "result": response if not response.get("error") else None,
                    "error": response if response.get("error") else None
                }))
                
            # 4. 안전하게 처리 완료됨을 원장에 알림 (ACK)
            await self.tunnel.stream_ack(self.topic, self.group, message_id)
            
        except Exception as e:
            log.error(f"[{self.name}] Message Processing Failed critically: {e}", exc_info=True)
            # 페이로드 자체가 찢어졌거나 악의적인 메시지일 경우 무한 재시도를 막기 위해 강제 ACK
            with suppress(Exception):
                await self.tunnel.stream_ack(self.topic, self.group, message_id)

    async def _teardown(self):
        """데몬 종료 시 자원 회수 및 정리"""
        log.info(f"[{self.name}] Releasing RPC Worker resources...")
        
        # 실행 중인 모든 메시지 처리 태스크 취소 대기
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
                
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        log.info(f"[{self.name}] Resource cleanup complete.")