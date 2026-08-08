# receptor.stream.store
## @lineage: kernel.phase.stream.store
import asyncio
import uuid
import httpx
import hashlib
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

from watcher.receptor.audit.warden import AuditWarden
from kernel.bind.state.spec import TransRule, NodeType
from kernel.dphi.ledger.consensus import KernelLedger
from watcher.receptor.mesh.gateway import ToposGateway
from kernel.phase.stream.schema import (
    LogicStream, 
    StreamMetadata, 
    StreamIdentity, 
    LogicPayload, 
    ActionIntent, 
    ProtocolSource
)
from watcher.receptor.contract.model import LogstEvent
from watcher.plane.emitter import get_emitter

log = get_emitter("log.store", phase="INGRESS")
_gateway_instance = ToposGateway()

@dataclass
class SurgentManifest:
    base_commit_hash: str
    head_commit_hash: str
    telemetry_pressure: Dict[str, Any]  
    proposed_rules: List[Dict[str, Any]] 

class GatekeeperEngine:
    @staticmethod
    def calculate_resonance_intensity(telemetry: Dict[str, Any]) -> float:
        leaks = telemetry.get("token_leaks", 0)
        timeouts = telemetry.get("node_lock_timeouts", 0)
        return (leaks * 0.1) + (timeouts * 0.5)

    @classmethod
    def simulate_evolutionary_rules(cls, telemetry: Dict[str, Any]) -> List[TransRule]:
        rules = []
        tension = cls.calculate_resonance_intensity(telemetry)
        
        if tension > 20.0:
            rules.append(TransRule(target_node="stable_core", new_node="legacy_symlink", kind=NodeType.SYMLINK))
        elif tension > 10.0:
            rules.append(TransRule(target_node="worker_pool", new_node="expanded_worker_pool", kind=NodeType.CORE))
            
        return rules

class PullRequestGatekeeper:
    def __init__(self, manifest_data: Dict[str, Any], store: Optional[KernelLedger] = None):
        self.manifest = SurgentManifest(
            base_commit_hash=manifest_data.get("base_commit_hash", ""),
            head_commit_hash=manifest_data.get("head_commit_hash", ""),
            telemetry_pressure=manifest_data.get("telemetry_pressure", {}),
            proposed_rules=manifest_data.get("proposed_rules", [])
        )
        self.store = store or KernelLedger()
        self.core_stream_id = "stream_core_infrastructure"

    def _generate_deterministic_hash(self, data: Any) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def execute_merkle_continuity_check(self) -> bool:
        latest_root = self.store.get_head_hash(self.core_stream_id) or "GENESIS_HASH"
        if self.manifest.base_commit_hash != latest_root:
            log.error(
                f"[Gatekeeper] Drop: Orphaned Chain. "
                f"PR Base ({self.manifest.base_commit_hash[:8]}) deviates from "
                f"Frozen Kernel Store ({latest_root[:8]})."
            )
            return False
            
        return True

    def execute_topological_flow_validation(self) -> bool:
        for rule_dict in self.manifest.proposed_rules:
            target = rule_dict.get("target_node") or rule_dict.get("source_name")
            if target in ["stable_core", "root_anchor", "kernel_vault"]:
                log.error(f"[Gatekeeper] Drop: Access Denied. Cannot invert or mutate physical Anchor node '{target}'.")
                return False

        simulated_rules = GatekeeperEngine.simulate_evolutionary_rules(self.manifest.telemetry_pressure)
        simulated_fingerprint = self._generate_deterministic_hash([asdict(r) for r in simulated_rules])
        proposed_fingerprint = self._generate_deterministic_hash(self.manifest.proposed_rules)
        if simulated_fingerprint != proposed_fingerprint:
            log.error("[Gatekeeper] Drop: Deviation detected. Modifications do not match deterministic suffering.")
            return False
            
        return True

    def process_pipeline(self) -> bool:
        if not self.execute_merkle_continuity_check():
            return False
        if not self.execute_topological_flow_validation():
            return False
        log.info(f"[Gatekeeper] Proof Verified. Kernel Head matches PR Base ({self.manifest.base_commit_hash[:8]}). Executing automatic bypass merge.")
        return True


class LogStreamStore:
    def __init__(self, gateway: ToposGateway = None, storage_endpoint: str = "http://internal-store:8000"):
        self.gateway = gateway or _gateway_instance
        self.storage_endpoint = storage_endpoint
        self._client = httpx.AsyncClient(base_url=self.storage_endpoint, timeout=5.0)

    async def bulk_append(self, stream_name: str, events: List[LogstEvent], metadata: Dict[str, Any] = None) -> bool:
        if not events:
            return True

        event_count = len(events)
        stream_uuid = uuid.uuid4()
        telemetry_pressure = await asyncio.to_thread(self._extract_telemetry_pressure, events)
        tension_score = GatekeeperEngine.calculate_resonance_intensity(telemetry_pressure)
        
        if metadata is None:
            metadata = {}
        metadata["telemetry_tension_score"] = tension_score

        payload_dict = {
            "stream_name": stream_name, 
            "count": event_count,
            "pressure": telemetry_pressure
        }
        
        # [핵심 수정] 무분별한 dict 전달이나 json 직렬화 대신 Schema(LogicStream) 구성
        logic_stream = LogicStream(
            meta=StreamMetadata(
                stream_id=stream_uuid,
                original_protocol=ProtocolSource.UNKNOWN, # 백엔드 내부 호출이므로
                content_length=len(str(payload_dict)),
                client_ip="internal_logstore"
            ),
            identity=StreamIdentity(
                is_authenticated=True,
                stateless_token_id="internal_logstore_agent",
                granted_scopes=["LOGSTREAM_BULK_INSERT"]
            ),
            payload=LogicPayload(
                # ActionIntent 열거형에 매핑 (INVOKE_TOOL을 시스템 툴 호출격으로 간주)
                intent=ActionIntent.INVOKE_TOOL, 
                parameters={"action": "LOGSTREAM_BULK_INSERT", "data": payload_dict, "meta": metadata}
            )
        )

        # Gateway에 단일 객체 전달
        is_authorized = await self.gateway.authorize_ingress(logic_stream)

        if not is_authorized:
            msg = f"Unauthorized bulk insert attempt to stream '{stream_name}' blocked by Kernel Store."
            log.warning(f"[LogtailStore] BLOCKED: {msg}")
            AuditWarden.record_anomaly(action="logstream.kernel_block", details=msg)
            return False

        if tension_score > 10.0:
            msg = f"High structural tension ({tension_score}) accepted by kernel in stream '{stream_name}'."
            log.error(f"[LogtailStore] TENSION ALERT: {msg}")
            AuditWarden.record_anomaly(action="logstream.high_tension_logged", details=msg)

        try:
            log.debug(f"[LogtailStore] Authorized by Kernel. Executing insert of {event_count} events.")
            return True
        except Exception as e:
            log.error(f"[LogtailStore] Bulk append failed during execution: {e}")
            return False

    def _extract_telemetry_pressure(self, events: List[LogstEvent]) -> Dict[str, int]:
        leaks = 0
        timeouts = 0
        for event in events:
            event_str = str(event).lower()
            if "leak" in event_str: leaks += 1
            if "timeout" in event_str: timeouts += 1
        return {"token_leaks": leaks, "node_lock_timeouts": timeouts}

    async def close(self):
        await self._client.aclose()

def get_logstream_store() -> LogStreamStore:
    return LogStreamStore(gateway=_gateway_instance)