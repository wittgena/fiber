# topos.xelog.depend
## @lineage: topos.ops.xelog.depend
## @lineage: ops.xelog.depend
from fastapi import Request

from topos.xelog.topos.tenant import TenantEco
from topos.xelog.audit.ledger import AuditLedger
from topos.xelog.topos.ingress.policy import (
    IngressPolicyEngine, 
    ToposSequencer, 
    FuelAllocator, 
    HealthMonitor
)
from topos.xelog.topos.log.store import LogStreamStore

from arch.topos.bound.interface.subs import DistributedPubSub
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.anchor import NexusAnchor
from watcher.dphi.adapter.exchange import D3fiExchangeAdapter
from watcher.dphi.adapter.sign import NodeSigner


## WASM & Core Compute
async def get_wasm_broker(request: Request) -> WasmBroker:
    return request.app.state.broker

## Ledger & State Persistence (신규 추가)
async def get_logstream_store(request: Request) -> LogStreamStore:
    """앱 구동 시 초기화된 Immutable Ledger Store 싱글톤 주입"""
    return request.app.state.store

## Consensus & Anchor
async def get_nexus_anchor(request: Request) -> NexusAnchor:
    """WASM 패리티 검증 및 Epoch Sealing을 전담하는 Anchor 주입"""
    broker = await get_wasm_broker(request)
    allowed_committee = getattr(request.app.state.config, "committee_pubs", [])
    return NexusAnchor(broker=broker, consensus_threshold=1, allowed_committee=allowed_committee)

## DeFi & Financial Adapters
async def get_exchange_adapter(request: Request) -> D3fiExchangeAdapter:
    """환전소/정산 영수증 발급 어댑터 주입"""
    node_pubkey = NodeSigner.get_instance().pubkey_hex
    return D3fiExchangeAdapter(clearing_house_pub_key=node_pubkey)

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
    return request.app.state.pubsub

## Ecosystem & Audit (OTLP / Audit)
async def get_tenant_eco(request: Request) -> TenantEco:
    """테넌트 별 Billing 및 토큰 리밋 관리를 위한 컨텍스트 주입"""
    return request.app.state.tenant_eco

async def get_audit_ledger(request: Request) -> AuditLedger:
    """PII 마스킹 및 감사 로그 기록기 주입"""
    return request.app.state.audit_ledger