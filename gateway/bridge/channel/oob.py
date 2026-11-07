# fiber.gateway.bridge.channel.oob
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
import time
from typing import Any, Dict, List, Optional, Set

from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("gateway.bridge.oob")

@dataclass
class ApprovalStateEnvelope:
    """Cryptographically signed state envelope embedded inside OOB interactive cards."""

    handle_id: str
    target_id: str
    prompt_id: str
    exp: int
    node_fp: Optional[str] = None
    bridge_id: Optional[str] = None
    require_totp: Optional[bool] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serializes only populated fields so canonical JSON matches legacy signatures byte-for-byte."""
        out: Dict[str, Any] = {
            "handle_id": self.handle_id,
            "target_id": self.target_id,
            "prompt_id": self.prompt_id,
            "exp": self.exp,
        }
        if self.node_fp is not None:
            out["node_fp"] = self.node_fp
        if self.bridge_id is not None:
            out["bridge_id"] = self.bridge_id
        if self.require_totp is not None:
            out["require_totp"] = bool(self.require_totp)
        if self.extra:
            out.update(self.extra)
        return out

    def to_canonical_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ApprovalStateEnvelope:
        known = {"handle_id", "target_id", "prompt_id", "exp", "node_fp", "bridge_id", "require_totp"}
        extra = {k: v for k, v in data.items() if k not in known}
        return cls(
            handle_id=str(data.get("handle_id", "")),
            target_id=str(data.get("target_id", "")),
            prompt_id=str(data.get("prompt_id", "")),
            exp=int(data.get("exp", 0)),
            node_fp=data.get("node_fp"),
            bridge_id=data.get("bridge_id"),
            require_totp=data.get("require_totp"),
            extra=extra,
        )

@dataclass
class AttestedResumeResult:
    """
    Verified human intervention payload injected back into the parked MCP worker.
    Optional extension slots (`override_args`, `lease_ttl_sec`, `quorum_sigs`, `comment`)
    are omitted on the wire unless explicitly provided, preserving 100% backward compatibility.
    """
    value: str
    attested_user_id: str
    override_args: Optional[Dict[str, Any]] = None  # In-flight payload steering / query modification
    lease_ttl_sec: Optional[int] = None             # Time-bounded sudo session lease
    quorum_sigs: Optional[List[str]] = None         # Multi-Sig / Quorum attestation chain
    comment: Optional[str] = None                   # Operator audit note or agent steering feedback

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "value": self.value,
            "attested_user_id": self.attested_user_id,
        }
        if self.override_args is not None:
            out["override_args"] = self.override_args
        if self.lease_ttl_sec is not None:
            out["lease_ttl_sec"] = self.lease_ttl_sec
        if self.quorum_sigs is not None:
            out["quorum_sigs"] = self.quorum_sigs
        if self.comment is not None:
            out["comment"] = self.comment
        return out

@dataclass
class McpIntentDispatch:
    """ Envelope published to `mcp.intent.queue.<target_id>` to unfreeze or abort a parked worker."""

    handle_id: str
    action: str  # "RESUME" | "FORCE_ROLLBACK"
    prompt_id: Optional[str] = None
    result: Optional[AttestedResumeResult] = None
    reason: Optional[str] = None

    @classmethod
    def for_resume(cls, handle_id: str, prompt_id: str, result: AttestedResumeResult) -> McpIntentDispatch:
        return cls(handle_id=handle_id, action="RESUME", prompt_id=prompt_id, result=result)

    @classmethod
    def for_rollback(cls, handle_id: str, reason: Optional[str] = None) -> McpIntentDispatch:
        return cls(handle_id=handle_id, action="FORCE_ROLLBACK", reason=reason)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "handle_id": self.handle_id,
            "action": self.action,
        }
        if self.action == "RESUME" and self.result is not None:
            out["payload"] = {
                "jsonrpc": "2.0",
                "id": self.prompt_id or f"prompt_{self.handle_id}",
                "result": self.result.to_dict(),
            }
        elif self.reason:
            out["reason"] = self.reason
        return out

class OutOfBandApprovalBridge:
    MAX_TIMESTAMP_DRIFT_SEC = 300  # Reject webhooks delayed by more than 5 minutes

    def __init__(self, rpc: Optional[InternalRpcClient] = None, slack_signing_secret: Optional[str] = None):
        self.rpc = rpc or InternalRpcClient()
        self.signer = NodeSigner.get_instance()
        self.slack_secret = (
            slack_signing_secret
            or hashlib.sha256(f"slack_oob:{self.signer.pubkey_hex}".encode()).hexdigest()
        ).encode("utf-8")

        self._consumed_tokens: Set[str] = set()
        self.identity_map: Dict[str, str] = {}

    def register_identity_mapping(self, external_user_id: str, spiffe_id: str) -> None:
        self.identity_map[external_user_id] = spiffe_id

    # Egress: Issue Ed25519-signed State Token for OOB notification cards (Prevents Handle Hijacking)
    def mint_approval_state_token(
        self,
        handle_id: str,
        target_id: str,
        prompt_id: str,
        ttl_sec: int = 300,
        *,
        node_fp: Optional[str] = None,
        bridge_id: Optional[str] = None,
        require_totp: Optional[bool] = None,
    ) -> Dict[str, Any]:
        env_model = ApprovalStateEnvelope(
            handle_id=handle_id,
            target_id=target_id,
            prompt_id=prompt_id,
            exp=int(time.time()) + ttl_sec,
            node_fp=node_fp,
            bridge_id=bridge_id,
            require_totp=require_totp,
        )
        sig = self.signer.sign_payload(env_model.to_canonical_bytes())
        return {"envelope": env_model.to_dict(), "sig": sig}

    # Ingress: Verify Slack Webhook HMAC-SHA256 signature & replay window
    def verify_slack_webhook_signature(self, timestamp: str, raw_body: bytes, signature: str) -> bool:
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

    # Dispatch: Verify cryptographic seal & publish RESUME or FORCE_ROLLBACK to internal bus
    async def handle_slack_interaction(
        self,
        timestamp: str,
        raw_body: bytes,
        slack_signature: str,
        interaction_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Step 1: Verify transport authenticity (HMAC-SHA256)
        if not self.verify_slack_webhook_signature(timestamp, raw_body, slack_signature):
            raise PermissionError("INVALID_WEBHOOK_SIGNATURE")

        # Step 2: Verify State Token Ed25519 signature & expiration
        state_token = interaction_payload.get("state_token", {})
        raw_envelope = state_token.get("envelope", {})
        sig = state_token.get("sig", "")

        # Verify against raw dict canonical bytes to guarantee compatibility with subclass-minted tokens
        canonical = json.dumps(raw_envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if not self.signer.verify_signature(canonical, sig):
            raise PermissionError("FORGED_STATE_TOKEN")

        env = ApprovalStateEnvelope.from_dict(raw_envelope)
        if int(time.time()) > env.exp:
            raise PermissionError("EXPIRED_STATE_TOKEN")

        # Step 3: Single-use token consumption (Prevents double-click / replay attacks)
        if sig in self._consumed_tokens:
            raise PermissionError("DUPLICATE_APPROVAL_REPLAY")
        self._consumed_tokens.add(sig)

        decision = str(interaction_payload.get("decision", "APPROVE")).upper()
        queue_channel = f"mcp.intent.queue.{env.target_id}"

        # Step 4: Immediate rollback on DENY
        if decision != "APPROVE":
            rollback_dispatch = McpIntentDispatch.for_rollback(
                handle_id=env.handle_id,
                reason=interaction_payload.get("reason"),
            )
            await self.rpc.publish_intent(channel=queue_channel, payload=rollback_dispatch.to_dict())
            return {"status": "ROLLED_BACK", "handle_id": env.handle_id}

        # Step 5: Resolve external user to SPIFFE ID and dispatch signed RESUME envelope
        external_user = str(interaction_payload.get("slack_user_id", ""))
        mapped_spiffe = self.identity_map.get(external_user)
        if not mapped_spiffe:
            raise PermissionError(f"UNMAPPED_EXTERNAL_IDENTITY: {external_user}")

        resume_result = AttestedResumeResult(
            value=str(interaction_payload.get("otp_code", "")),
            attested_user_id=mapped_spiffe,
            override_args=interaction_payload.get("override_args"),
            lease_ttl_sec=interaction_payload.get("lease_ttl_sec"),
            quorum_sigs=interaction_payload.get("quorum_sigs"),
            comment=interaction_payload.get("comment"),
        )
        resume_dispatch = McpIntentDispatch.for_resume(
            handle_id=env.handle_id,
            prompt_id=env.prompt_id,
            result=resume_result,
        )

        await self.rpc.publish_intent(channel=queue_channel, payload=resume_dispatch.to_dict())
        log.info(f"[OOB:Approved] Dispatched verified RESUME for {env.handle_id} by {mapped_spiffe}")
        return {"status": "RESUMED", "handle_id": env.handle_id, "attested_by": mapped_spiffe}