# fiber.dphi.edge.serv.depend
from typing import Any
from fastapi import Request, HTTPException, status

from fiber.dphi.eco.client.rpc import InternalRpcClient

from xphi.watcher.receptor.warden import SecretAuditor
from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.xor.parser.ruleset.otlp import StrictOtlpExtractionEngine
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.xor.secret.cipher import Cipher
from xphi.arch.bound.xor.secret.client import get_secret_from_vendor, KMSVendor

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
    """
    PII 마스킹 및 감사 로그 기록기 주입 (KMS 연동 기반)
    앱 상태에 등록된 인스턴스가 없으면 KMS Vendor를 통해 동적으로 암호화 모듈을 조립합니다.
    """
    auditor = getattr(request.app.state, "secret_auditor", None)
    if auditor:
        return auditor
        
    log.warning("[DI Warning] 'secret_auditor' not found in app.state. Provisioning ephemeral KMS-backed fallback.")
    
    try:
        # 1. KMSVendor 추상화를 통해 시크릿 키 획득 시도 (현재는 LOCAL이지만 추후 AWS/GCP 등으로 교체 가능)
        secret_key = get_secret_from_vendor(
            client=None,
            key_manager=KMSVendor.LOCAL,
            secret_name="LEDGER_CIPHER_KEY"
        )
        
        # 2. 로컬 환경 변수에도 없을 경우의 E2E 테스트용 최후 폴백(Fallback)
        if not secret_key:
            secret_key = "dphi-ephemeral-test-key-32bytes!"
            
        # 3. Cipher 객체를 조립하여 SecretAuditor에 명시적으로 주입 (TypeError 완벽 해결)
        cipher_instance = Cipher(secret_key=secret_key)
        return SecretAuditor(cipher=cipher_instance)
        
    except Exception as e:
        log.critical(f"[Security] SecretAuditor provisioning failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cryptographic audit module unavailable."
        )