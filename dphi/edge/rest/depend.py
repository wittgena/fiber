# fiber.dphi.edge.rest.depend
## @lineage: fiber.kernel.receptor.edge.rest.depend
## @lineage: fiber.kernel.receptor.dphi.depend
from typing import Any
from fastapi import Request, HTTPException, status

from fiber.dphi.rpc.client import InternalRpcClient
from fiber.phase.kernel.receptor.audit.secret import SecretAuditor

from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.kernel.dphi.broker import DphiBroker
from xphi.xor.parser.ruleset.otlp import StrictOtlpExtractionEngine
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("dphi.depend")

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

# =========================================================
# Gateway Dependencies (Only what the public edge needs)
# =========================================================

async def get_rpc_client() -> InternalRpcClient:
    """[NEW] 내부망 워커와 통신하기 위한 메시지 버스 기반 RPC 클라이언트"""
    return InternalRpcClient()

async def get_wasm_broker(request: Request) -> DphiBroker:
    """WASM 커널 제어 및 암호학적 증명(Fingerprint) 발급용 브로커"""
    return _get_state_attr(request, "broker")

async def get_pubsub(request: Request) -> DistributedPubSub:
    """글로벌 브로드캐스트 및 이벤트 파이프라인 주입"""
    return _get_state_attr(request, "pubsub")

async def get_otlp_engine(request: Request) -> StrictOtlpExtractionEngine:
    """텔레메트리 파싱 엔진"""
    return _get_state_attr(request, "otlp_engine")

async def get_secret_auditor(request: Request) -> SecretAuditor:
    """PII 마스킹 및 감사 로그 기록기 주입"""
    auditor = getattr(request.app.state, "secret_auditor", None)
    if not auditor:
        log.warning("[DI Warning] 'secret_auditor' not found in app.state. Using ephemeral fallback.")
        return SecretAuditor()
    return auditor

# [REMOVED] get_logstream_store, get_nexus_anchor, get_exchange_adapter, 
# get_utxo_adapter, get_bench_profile, get_ingress_policy
# -> 이들은 이제 Worker 데몬의 WorkerContext에서 직접 생성/관리됩니다.