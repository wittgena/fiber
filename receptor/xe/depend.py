# receptor.xe.depend
## @lineage: surface.receptor.xe.depend
from typing import Any

from fastapi import Request, HTTPException, status

from phase.dphi.adapter.anchor import NexusAnchor
from receptor.xe.profile import BenchProfile

from arch.topos.tunnel.subs import DistributedPubSub
from watcher.receptor.audit.secret import SecretAuditor
from receptor.ingress.gov.policy import (
    IngressPolicyEngine, 
    ToposSequencer, 
    FuelAllocator, 
    HealthMonitor
)
from arch.xor.parser.otlp import StrictOtlpExtractionEngine
from receptor.stream.store import LogStreamStore
from kernel.dphi.broker import DphiBroker
from kernel.dphi.adapter.exchange import ExchangeAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

log = get_emitter("xe.depend")

def _get_state_attr(request: Request, attr_name: str) -> Any:
    val = getattr(request.app.state, attr_name, None)
    if val is None:
        error_msg = f"Critical Service '{attr_name}' is not initialized in app.state."
        log.error(f"[DI Error] {error_msg}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=error_msg
        )
    return val

## WASM & Core Compute
async def get_wasm_broker(request: Request) -> DphiBroker:
    return _get_state_attr(request, "broker")

## Ledger & State Persistence
async def get_logstream_store(request: Request) -> LogStreamStore:
    """앱 구동 시 초기화된 Immutable Ledger Store 싱글톤 주입"""
    return _get_state_attr(request, "store")

## Consensus & Anchor
async def get_nexus_anchor(request: Request) -> NexusAnchor:
    """WASM 패리티 검증 및 Epoch Sealing을 전담하는 Anchor 주입"""
    broker = await get_wasm_broker(request)
    
    config = getattr(request.app.state, "config", None)
    allowed_committee = getattr(config, "committee_pubs", []) if config else []
    if not allowed_committee:
        try:
            allowed_committee = [NodeSigner.get_instance().pubkey_hex]
        except Exception:
            pass
            
    return NexusAnchor(broker=broker, consensus_threshold=1, allowed_committee=allowed_committee)

## DeFi & Financial Adapters
async def get_exchange_adapter(request: Request) -> ExchangeAdapter:
    """환전소/정산 영수증 발급 어댑터 주입"""
    try:
        node_pubkey = NodeSigner.get_instance().pubkey_hex
        return ExchangeAdapter(clearing_house_pub_key=node_pubkey)
    except Exception as e:
        log.error(f"[DI Error] Failed to initialize ExchangeAdapter: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Node Signer is not configured or keys are missing."
        )

## Profiling & Billing (A2A Compute 연산 전용)
async def get_bench_profile() -> BenchProfile:
    """A2A 연산에 대한 과금 검증 및 Cgroup 티어 산출/실행기 주입"""
    # 상태 의존성이 없는 독립 객체이므로 안전함
    return BenchProfile()

## Ingress Policy (D3Fi & Gateway)
async def get_ingress_policy(request: Request) -> IngressPolicyEngine:
    """거래 인입 시 위상(Topo), 자원(Fuel), 상태(Health)를 판단하는 단일 엔진"""
    return IngressPolicyEngine(
        sequencer=ToposSequencer(),
        allocator=FuelAllocator(),
        monitor=HealthMonitor()
    )

## Event Driven & Streaming (OTLP / Audit)
async def get_pubsub(request: Request) -> DistributedPubSub:
    """글로벌 브로드캐스트 및 이벤트 파이프라인 주입"""
    return _get_state_attr(request, "pubsub")

## Parsing & Extraction Engine
async def get_otlp_engine(request: Request) -> StrictOtlpExtractionEngine:
    return _get_state_attr(request, "otlp_engine")

## Ecosystem & Audit
async def get_secret_auditor(request: Request) -> SecretAuditor:
    """PII 마스킹 및 감사 로그 기록기 주입"""
    auditor = getattr(request.app.state, "secret_auditor", None)
    if not auditor:
        log.warning("[DI Warning] 'secret_auditor' not found in app.state. Using ephemeral SecretAuditor fallback.")
        return SecretAuditor()
    return auditor