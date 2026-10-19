# fiber.dev.bridge.mcp.oob
## @lineage: fiber.gateway.mcp.bridge.oob
import os
import time
import hmac
import json
import hashlib
from typing import Dict, Any, Optional, Set

from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mcp.bridge.oob")

class OutOfBandApprovalBridge:
    """
    Zero-Trust Out-of-Band (Slack / Telegram / CLI) Security Bridge.
    코어 스크립트 수정 없이 외부 웹훅의 서명 검증, Replay 방어, State Token 무결성 검증을 수행한 뒤
    안전하게 정제된 RESUME / FORCE_ROLLBACK 인텐트만 내부 버스로 주입합니다.
    """
    MAX_TIMESTAMP_DRIFT_SEC = 300  # 5분 이상 지연된 웹훅 차단

    def __init__(self, rpc: Optional[InternalRpcClient] = None, slack_signing_secret: Optional[str] = None):
        self.rpc = rpc or InternalRpcClient()
        self.signer = NodeSigner.get_instance()
        # Slack Signing Secret이 없으면 노드 고유 공개키 기반으로 결정론적 테스트 시크릿 유도
        self.slack_secret = (
            slack_signing_secret
            or os.environ.get("FIBER_SLACK_SIGNING_SECRET")
            or hashlib.sha256(f"slack_oob:{self.signer.pubkey_hex}".encode()).hexdigest()
        ).encode("utf-8")

        # Replay 방어용 소비된 토큰 캐시 (운영 시 Redis NonceReplayProtector 연동 가능)
        self._consumed_tokens: Set[str] = set()
        # 외부 메신저 User ID -> 내부 SPIFFE ID 매핑 테이블
        self.identity_map: Dict[str, str] = {}

    def register_identity_mapping(self, external_user_id: str, spiffe_id: str) -> None:
        """Slack User ID(예: U012AB3CD)를 내부 감사 DB의 SPIFFE ID와 바인딩합니다."""
        self.identity_map[external_user_id] = spiffe_id

    # =========================================================================
    # 1. Egress: 외부 알림 발송용 암호화 State Token 발급 (Handle Hijacking 방지)
    # =========================================================================
    def mint_approval_state_token(
        self, handle_id: str, target_id: str, prompt_id: str, ttl_sec: int = 300
    ) -> Dict[str, Any]:
        """
        Slack 버튼의 `value` 필드에 주입할 위변조 방지 토큰을 생성합니다.
        공격자가 다른 handle_id로 바꿔치기하여 승인 버튼을 누르는 것을 원천 차단합니다.
        """
        exp = int(time.time()) + ttl_sec
        envelope = {
            "handle_id": handle_id,
            "target_id": target_id,
            "prompt_id": prompt_id,
            "exp": exp,
        }
        canonical = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        sig = self.signer.sign_payload(canonical)
        return {"envelope": envelope, "sig": sig}

    # =========================================================================
    # 2. Ingress: Slack Webhook HMAC-SHA256 검증 (Webhook Spoofing 방지)
    # =========================================================================
    def verify_slack_webhook_signature(self, timestamp: str, raw_body: bytes, signature: str) -> bool:
        """Slack 표준 v0 HMAC-SHA256 서명 및 Replay Timestamp Window를 검증합니다."""
        try:
            req_ts = int(timestamp)
        except (ValueError, TypeError):
            return False

        if abs(int(time.time()) - req_ts) > self.MAX_TIMESTAMP_DRIFT_SEC:
            log.warning(f"[OOB:Security] Webhook timestamp drift exceeded ({timestamp}). Possible replay.")
            return False

        basestring = f"v0:{timestamp}:".encode("utf-8") + raw_body
        expected_sig = "v0=" + hmac.new(self.slack_secret, basestring, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected_sig, signature)

    # =========================================================================
    # 3. Dispatch: 검증 완료 후 내부 Bus로 RESUME 또는 ROLLBACK 주입
    # =========================================================================
    async def handle_slack_interaction(
        self,
        timestamp: str,
        raw_body: bytes,
        slack_signature: str,
        interaction_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Slack Interactive Payload(모달 제출 또는 버튼 클릭)를 검증하고 Connector 큐로 인텐트를 발행합니다.
        """
        # Step 1: 채널(Slack) 진위 검증
        if not self.verify_slack_webhook_signature(timestamp, raw_body, slack_signature):
            raise PermissionError("INVALID_WEBHOOK_SIGNATURE")

        # Step 2: State Token 서명 및 만료 검증
        state_token = interaction_payload.get("state_token", {})
        envelope = state_token.get("envelope", {})
        sig = state_token.get("sig", "")

        canonical = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if not self.signer.verify_signature(canonical, sig):
            raise PermissionError("FORGED_STATE_TOKEN")

        if int(time.time()) > envelope.get("exp", 0):
            raise PermissionError("EXPIRED_STATE_TOKEN")

        # Step 3: 일회용 토큰 소비 처리 (버튼 연타 / Replay 차단)
        if sig in self._consumed_tokens:
            raise PermissionError("DUPLICATE_APPROVAL_REPLAY")
        self._consumed_tokens.add(sig)

        handle_id = envelope["handle_id"]
        target_id = envelope["target_id"]
        prompt_id = envelope["prompt_id"]
        decision = interaction_payload.get("decision", "APPROVE").upper()

        # Step 4: 거부(DENY) 버튼 클릭 시 즉시 강제 롤백 발행
        if decision != "APPROVE":
            await self.rpc.publish_intent(
                channel=f"mcp.intent.queue.{target_id}",
                payload={"handle_id": handle_id, "action": "FORCE_ROLLBACK"},
            )
            return {"status": "ROLLED_BACK", "handle_id": handle_id}

        # Step 5: 승인(APPROVE) 시 외부 유저 매핑 확인 및 RESUME 인텐트 발행
        external_user = interaction_payload.get("slack_user_id", "")
        mapped_spiffe = self.identity_map.get(external_user)
        if not mapped_spiffe:
            raise PermissionError(f"UNMAPPED_EXTERNAL_IDENTITY: {external_user}")

        otp_code = interaction_payload.get("otp_code", "")
        await self.rpc.publish_intent(
            channel=f"mcp.intent.queue.{target_id}",
            payload={
                "handle_id": handle_id,
                "action": "RESUME",
                "payload": {
                    "jsonrpc": "2.0",
                    "id": prompt_id,
                    "result": {
                        "value": otp_code,
                        "attested_user_id": mapped_spiffe,
                    },
                },
            },
        )
        log.info(f"[OOB:Approved] Dispatched verified RESUME for {handle_id} by {mapped_spiffe}")
        return {"status": "RESUMED", "handle_id": handle_id, "attested_by": mapped_spiffe}