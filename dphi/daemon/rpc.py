# fiber.dphi.daemon.rpc
import os
import json
import uuid
import httpx
import asyncio
from contextlib import suppress
from typing import Optional, Dict, Callable, List, Any

from fiber.dphi.rpc.registry import build_internal_rpc_registry
from fiber.dphi.rpc.handler import WorkerContext
from fiber.dphi.rpc.legacy.validator import AuthValidatorService
from fiber.dphi.edge.policy import IngressPolicyEngine, ToposSequencer, FuelAllocator, HealthMonitor

from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.state.ledger.gateway import StoreGateway

from xphi.arch.model.anchor.nexus import NexusAnchor
from xphi.arch.bound.adapter.settlement import ClearingAdapter
from xphi.kernel.space.sandbox.resolver import BenchProfile
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import PtaAdapter
from xphi.arch.bound.adapter.sign import NodeSigner
from xphi.arch.model.edge.receipt import LogstEvent
from xphi.watcher.receptor.warden import AuditWarden

from xphi.arch.model.edge.stream import (
    LogicStream, StreamMetadata, StreamIdentity, LogicPayload, ActionIntent, ProtocolSource
)

log = get_emitter("daemon.rpc")

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
        
        self.routes: Dict[str, Callable] = {}
        self.tunnel = None
        self.worker_ctx: Optional[WorkerContext] = None
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

        self.worker_ctx = WorkerContext(
            broker=broker,
            store=store,
            nexus=nexus,
            exchange_adapter=exchange_adapter,
            pta_adapter=pta_adapter,
            policy_engine=policy_engine,
            profile_service=profile_service
        )

        prod_validator = AuthValidatorService()
        self.routes = build_internal_rpc_registry(validator_service=prod_validator)
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
                            task = asyncio.create_task(self.process_message(message_id, msg_data))
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

    async def process_message(self, message_id: str, msg_data: dict):
        reply_to = request_id = None
        try:
            payload_str = msg_data.get("payload") or msg_data.get(b"payload")
            if not payload_str: 
                return

            payload = json.loads(payload_str)
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
                await self.tunnel.publish(reply_to, json.dumps({
                    "id": request_id,
                    "result": response if not response.get("error") else None,
                    "error": response if response.get("error") else None
                }))
                
            await self.tunnel.stream_ack(self.topic, self.group, message_id)
            
        except Exception as e:
            log.error(f"[{self.name}] Message Processing Failed critically: {e}", exc_info=True)
            with suppress(Exception):
                await self.tunnel.stream_ack(self.topic, self.group, message_id)

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing RPC Worker resources...")
        
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
                
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        if self.worker_ctx and self.worker_ctx.store:
            await self.worker_ctx.store.close()
            
        log.info(f"[{self.name}] Resource cleanup complete.")