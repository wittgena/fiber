# fiber.infra.mcp.preflight
## @lineage: fiber.gateway.mcp.preflight
import os
import re
import json
import hashlib
from typing import Dict, Any, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric import ed25519
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.watcher.plane.emitter import get_emitter

class PreflightGuard:
    ENV_KEYS = ("FIBER_PREFLIGHT_PATTERN", "FIBER_HITL_ON")

    def __init__(self, target_id: str, rpc: InternalRpcClient, pattern: Optional[str] = None):
        self.target_id = target_id
        self.rpc = rpc
        self.log = get_emitter(f"mcp.preflight.{self.target_id}")

        self.pattern_str = self.resolve_pattern(self.target_id, pattern)
        self.guard_regex = re.compile(self.pattern_str, re.IGNORECASE) if self.pattern_str else None

        if self.guard_regex:
            self.log.info(
                f"[PreFlight] Armed Zero-Code HITL Guard on '{self.target_id}' (Pattern: /{self.pattern_str}/i)"
            )

        self.stashed_payloads: Dict[str, Dict[str, Any]] = {}
        self.validator_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(
            bytes.fromhex(NodeSigner.get_instance().pubkey_hex)
        )

    @classmethod
    def resolve_pattern(cls, target_id: str, explicit_pattern: Optional[str] = None) -> Optional[str]:
        """명시적 인자 -> 타겟별 환경변수(FIBER_PREFLIGHT_<TARGET>) -> 전역 환경변수 순으로 정규식을 결정"""
        sanitized_target = re.sub(r"[^A-Za-z0-9]", "_", target_id).upper()
        env_target_key = f"FIBER_PREFLIGHT_{sanitized_target}"

        raw = (
            explicit_pattern
            or os.environ.get(env_target_key)
            or os.environ.get("FIBER_PREFLIGHT_PATTERN")
            or os.environ.get("FIBER_HITL_ON")
        )
        return raw.strip() if raw and raw.strip() else None

    @classmethod
    def extract_from_env_dict(
        cls, env_dict: Optional[Dict[str, str]], explicit_override: Optional[str] = None
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """mcp.json 등의 개별 워커 `env` 블록에서 Pre-flight 설정 키만 안전하게 분리하여 반환"""
        if not env_dict:
            return explicit_override, {}

        extracted = explicit_override
        clean_env: Dict[str, str] = {}
        for k, v in env_dict.items():
            if k in cls.ENV_KEYS:
                if not extracted and v:
                    extracted = str(v).strip()
            else:
                clean_env[k] = v

        return extracted, clean_env

    def inspect_and_stash(self, handle_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """위험 패턴 매칭 시 원본을 Stash하고 YIELD 전이용 Synthetic Prompt를 반환"""
        if not self.guard_regex:
            return None

        params_dict = payload.get("params", {})
        canonical_str = json.dumps(params_dict, sort_keys=True)
        match = self.guard_regex.search(canonical_str)
        if not match:
            return None

        matched_keyword = match.group(0)
        payload_hash = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
        self.stashed_payloads[handle_id] = {
            "raw_payload": dict(payload),
            "payload_hash": payload_hash,
            "user_id": params_dict.get("_meta", {}).get("user_id", "spiffe://public/agent"),
        }

        self.log.warning(
            f"[PreFlight] Intercepted high-risk call ('{matched_keyword}') for {handle_id}. Parking before worker I/O."
        )
        return {
            "jsonrpc": "2.0",
            "id": f"prompt_{handle_id}",
            "method": "elicitation/createMessage",
            "params": {
                "message": (
                    f"[Pre-flight Guard] High-risk pattern '{matched_keyword}' detected on '{self.target_id}'. "
                    f"Enter TOTP:"
                ),
                "payload_hash": payload_hash,
            },
        }

    async def verify_and_pop(self, handle_id: str, resume_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Pre-flight에 의해 파킹된 인텐트인 경우 Validator(validate.attest) 호출 및 Ed25519 서명을 대리 검증한 뒤 보관된 원본 페이로드를 반환"""
        stashed = self.stashed_payloads.pop(handle_id, None)
        if not stashed:
            return None

        result_block = resume_payload.get("result")
        if not isinstance(result_block, dict) and isinstance(resume_payload.get("payload"), dict):
            result_block = resume_payload["payload"].get("result", {})
        result_block = result_block or {}

        otp_code = str(result_block.get("value", "")).strip()
        override_user = result_block.get("attested_user_id")
        user_id = override_user or stashed["user_id"]
        payload_hash_hex = stashed["payload_hash"]

        res = await self.rpc.call(
            "validate.attest",
            {"user_id": user_id, "otp_code": otp_code, "payload_hash": payload_hash_hex},
        )
        if not isinstance(res, dict) or res.get("error") or not res.get("signature"):
            err_msg = res.get("message", "Pre-flight attestation rejected") if isinstance(res, dict) else "RPC Error"
            raise PermissionError(err_msg)

        self.validator_pub_key.verify(bytes.fromhex(res["signature"]), bytes.fromhex(payload_hash_hex))
        self.log.info(f"[PreFlight] Proxy Attestation Verified for {handle_id}. Releasing stashed payload.")
        return stashed["raw_payload"]

    def clear(self, handle_id: str) -> None:
        self.stashed_payloads.pop(handle_id, None)