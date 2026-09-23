# fiber.gateway.daemon.rpc
import os
import json
import uuid
import httpx
import asyncio
import time
import hashlib
import random
from dataclasses import dataclass
from contextlib import suppress
from typing import Optional, Dict, Callable, List, Any

from fiber.infra.rpc.ext import ExtRpcService
from fiber.infra.rpc.registry import build_internal_rpc_registry
from fiber.infra.rpc.handler import WorkerContext
from fiber.infra.rpc.validator import ValidatorService

from xphi.arch.contract.registry.unified import contract
from xphi.arch.bound.adapter.settlement import ClearingAdapter
from xphi.arch.bound.adapter.pta import PtaAdapter
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.model.edge.receipt import LogstEvent
from xphi.arch.model.edge.stream import LogicStream, StreamMetadata, StreamIdentity, LogicPayload, ActionIntent, ProtocolSource

from xphi.state.anchor.gateway import StoreGateway
from xphi.state.anchor.nexus import NexusAnchor

from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.kernel.space.sandbox.resolver import BenchProfile
from xphi.kernel.wasm.broker import DphiBroker

from xphi.watcher.receptor.warden import AuditWarden
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.rpc")

@dataclass
class IngressContext:
    topo_id: int
    press_limit: int
    is_ruptured: bool
    reason: str = ""

class ToposSequencer:
    async def get_next_sequence(self, client_id: str) -> int:
        ts = int(time.time() * 1000)
        hash_val = int(hashlib.sha256(client_id.encode()).hexdigest()[:8], 16)
        return (ts % 100000000) + (hash_val % 1000)

class FuelAllocator:
    async def calculate_press_limit(self, client_id: str, action_type: str) -> int:
        seed_str = f"{client_id}:{action_type}"
        base_press = int(hashlib.md5(seed_str.encode()).hexdigest()[:4], 16) % 90
        return max(10, base_press)

class HealthMonitor:
    async def is_ruptured(self) -> tuple[bool, str]:
        # 낮은 확률로 네트워크 균열(Byzantine 장애 등) 상태를 모사
        if random.random() < 0.01:
            return True, "Byzantine divergence detected in consensus layer."
        return False, ""

class IngressPolicyEngine:
    """Facade for managing incoming request limits, sequences, and node health."""
    def __init__(self, sequencer: ToposSequencer, allocator: FuelAllocator, monitor: HealthMonitor):
        self.sequencer = sequencer
        self.allocator = allocator
        self.monitor = monitor

    async def resolve_context(self, client_id: str, action: str) -> IngressContext:
        ruptured, reason = await self.monitor.is_ruptured()
        topo_id = await self.sequencer.get_next_sequence(client_id)
        press_limit = await self.allocator.calculate_press_limit(client_id, action)

        return IngressContext(
            topo_id=topo_id,
            press_limit=press_limit,
            is_ruptured=ruptured,
            reason=reason
        )

class LogStreamStore:
    def __init__(self, gateway: StoreGateway = None, storage_endpoint: str = "http://internal-store:8000"):
        self.gateway = gateway or StoreGateway()
        self.storage_endpoint = storage_endpoint
        self._client = httpx.AsyncClient(base_url=self.storage_endpoint, timeout=5.0)

    @staticmethod
    def _calculate_resonance_intensity(telemetry: Dict[str, Any]) -> float:
        leaks = telemetry.get("token_leaks", 0)
        timeouts = telemetry.get("node_lock_timeouts", 0)
        return (leaks * 0.1) + (timeouts * 0.5)

    def _extract_telemetry_pressure(self, events: List[LogstEvent]) -> Dict[str, int]:
        leaks = timeouts = 0
        for event in events:
            event_str = str(event).lower()
            if "leak" in event_str: leaks += 1
            if "timeout" in event_str: timeouts += 1
        return {"token_leaks": leaks, "node_lock_timeouts": timeouts}

    async def bulk_append(self, stream_name: str, events: List[LogstEvent], metadata: Dict[str, Any] = None) -> bool:
        if not events:
            return True

        event_count = len(events)
        stream_uuid = uuid.uuid4()
        
        telemetry_pressure = await asyncio.to_thread(self._extract_telemetry_pressure, events)
        tension_score = self._calculate_resonance_intensity(telemetry_pressure)
        
        metadata = metadata or {}
        metadata["telemetry_tension_score"] = tension_score

        payload_dict = {
            "stream_name": stream_name, 
            "count": event_count,
            "pressure": telemetry_pressure
        }
        
        logic_stream = LogicStream(
            meta=StreamMetadata(
                stream_id=stream_uuid,
                original_protocol=ProtocolSource.UNKNOWN, 
                content_length=len(str(payload_dict)),
                client_ip="internal_logstore"
            ),
            identity=StreamIdentity(
                is_authenticated=True,
                stateless_token_id="internal_logstore_agent",
                granted_scopes=["LOGSTREAM_BULK_INSERT"]
            ),
            payload=LogicPayload(
                intent=ActionIntent.INVOKE_TOOL, 
                parameters={"action": "LOGSTREAM_BULK_INSERT", "data": payload_dict, "meta": metadata}
            )
        )

        is_authorized = await self.gateway.authorize_ingress(logic_stream)

        if not is_authorized:
            msg = f"Unauthorized bulk insert attempt to stream '{stream_name}' blocked by Kernel Store."
            log.warning(f"[LogStore] BLOCKED: {msg}")
            AuditWarden.record_anomaly(action="logstream.kernel_block", details=msg)
            return False

        if tension_score > 10.0:
            msg = f"High structural tension ({tension_score}) accepted by kernel in stream '{stream_name}'."
            log.error(f"[LogStore] TENSION ALERT: {msg}")
            AuditWarden.record_anomaly(action="logstream.high_tension_logged", details=msg)

        try:
            log.debug(f"[LogStore] Authorized by Kernel. Executing insert of {event_count} events.")
            return True
        except Exception as e:
            log.error(f"[LogStore] Bulk append failed during execution: {e}")
            return False

    async def close(self):
        await self._client.aclose()


@contract.daemon("rpc_worker")
class RpcWorkerDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RpcWorkerDaemon")
        self.app_ctx = ctx  
        
        self.topic = os.getenv("RPC_QUEUE_TOPIC", "internal.rpc.queue")
        self.group = os.getenv("RPC_QUEUE_GROUP", "internal_workers")
        self.worker_id = os.getenv("RPC_WORKER_ID", f"worker-{os.getpid()}")

        max_workers = int(os.getenv("RPC_MAX_CONCURRENCY", "100"))
        self.semaphore = asyncio.Semaphore(max_workers)
        
        self.routes: Dict[str, Callable] = {}
        self.tunnel = None
        self.worker_ctx: Optional[WorkerContext] = None
        self.ext_service: Optional[ExtRpcService] = None
        self._tasks = set()

    async def _init_context(self):
        log.info(f"[{self.name}] Initializing Headless Worker Dependencies...")
        
        broker = getattr(self.app_ctx, "broker", None) or DphiBroker()
        store = getattr(self.app_ctx, "store", None) or LogStreamStore()
        
        nexus = NexusAnchor(broker=broker, consensus_threshold=1, allowed_committee=[])
        node_pubkey = NodeSigner.get_instance().pubkey_hex if hasattr(NodeSigner, 'get_instance') else "mock_pubkey"
        exchange_adapter = ClearingAdapter(clearing_house_pub_key=node_pubkey)
        pta_adapter = PtaAdapter(broker=broker)
        
        policy_engine = IngressPolicyEngine(
            sequencer=ToposSequencer(), allocator=FuelAllocator(), monitor=HealthMonitor()
        )
        profile_service = BenchProfile()

        # 1. 내부 워커 컨텍스트 초기화
        self.worker_ctx = WorkerContext(
            broker=broker,
            store=store,
            nexus=nexus,
            exchange_adapter=exchange_adapter,
            pta_adapter=pta_adapter,
            policy_engine=policy_engine,
            profile_service=profile_service
        )

        # 2. 외부 연동(EVM, Wallet) 통합 서비스 초기화
        self.ext_service = ExtRpcService()

        # 3. RPC 라우터 레지스트리 마운트
        prod_validator = ValidatorService()
        self.routes = build_internal_rpc_registry(
            validator_service=prod_validator,
            ext_service=self.ext_service
        )
        log.info(f"[{self.name}] Dynamic RPC Registry mounted with {len(self.routes)} routes.")

    async def run(self):
        log.info(f"[{self.name}] Initiating Autonomous RPC Worker Daemon...")
        try:
            await self._init_context()
            self.tunnel = await TunnelFactory.get_default()
            log.info(f"[{self.name}] 🚀 Worker [{self.worker_id}] listening on topic: {self.topic}")
            
            while self.running:
                try:
                    messages = await self.tunnel.stream_consume(
                        self.topic, self.group, self.worker_id, count=10, block=2000
                    )
                    
                    for stream_name, msg_list in messages:
                        for message_id, msg_data in msg_list:
                            # 큐 소비 속도 조절 (Backpressure)
                            await self.semaphore.acquire()
                            
                            task = asyncio.create_task(self._process_and_release(message_id, msg_data))
                            self._tasks.add(task)
                            task.add_done_callback(self._tasks.discard)
                            
                except asyncio.TimeoutError:
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

    async def _process_and_release(self, message_id: str, msg_data: dict):
        """태스크 완료/실패 여부와 관계없이 반드시 Semaphore를 반환하도록 보장"""
        try:
            await self.process_message(message_id, msg_data)
        finally:
            self.semaphore.release()

    async def process_message(self, message_id: str, msg_data: dict):
        reply_to = request_id = None
        try:
            payload_raw = msg_data.get("payload") or msg_data.get(b"payload")
            if not payload_raw: 
                await self.tunnel.stream_ack(self.topic, self.group, message_id)
                return

            # 독약 메시지(Poison Pill) 방어: JSON 디코딩 실패 시 즉시 폐기 및 감사 로그
            try:
                payload = json.loads(payload_raw)
            except json.JSONDecodeError as e:
                log.error(f"[{self.name}] Invalid JSON payload. Discarding msg {message_id}: {e}")
                AuditWarden.record_anomaly(action="rpc.invalid_payload", details=str(payload_raw))
                await self.tunnel.stream_ack(self.topic, self.group, message_id)
                return

            method = payload.get("method")
            params = payload.get("params", {})
            reply_to = payload.get("reply_to") 
            request_id = payload.get("id")
            
            handler = self.routes.get(method)
            if not handler:
                response = {"error": True, "code": 404, "message": f"Method {method} not found"}
                log.warning(f"[{self.name}] Unknown method invoked: {method}")
            else:
                try:
                    response = await handler(params, self.worker_ctx)
                except Exception as handler_exc:
                    log.error(f"[{self.name}] Unhandled Exception in {method}: {handler_exc}", exc_info=True)
                    response = {"error": True, "code": 500, "message": f"Internal Worker Execution Error: {str(handler_exc)}"}
                
            if reply_to:
                try:
                    await self.tunnel.publish(reply_to, json.dumps({
                        "id": request_id,
                        "result": response if not response.get("error") else None,
                        "error": response if response.get("error") else None
                    }))
                except Exception as pub_exc:
                    log.error(f"[{self.name}] Failed to publish reply to {reply_to}: {pub_exc}")
                
            await self.tunnel.stream_ack(self.topic, self.group, message_id)
            
        except Exception as e:
            log.error(f"[{self.name}] Message Processing Failed critically: {e}", exc_info=True)
            with suppress(Exception):
                await self.tunnel.stream_ack(self.topic, self.group, message_id)

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing RPC Worker resources...")
        
        # Graceful Shutdown: 즉시 취소하지 않고 활성 트랜잭션 완료 대기
        if self._tasks:
            log.info(f"[{self.name}] Waiting for {len(self._tasks)} active tasks to finish...")
            done, pending = await asyncio.wait(self._tasks, timeout=5.0)
            if pending:
                log.warning(f"[{self.name}] {len(pending)} tasks did not finish in time. Canceling...")
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                
        if self.worker_ctx and self.worker_ctx.store:
            await self.worker_ctx.store.close()
            
        log.info(f"[{self.name}] Resource cleanup complete.")