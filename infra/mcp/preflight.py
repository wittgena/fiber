# fiber.infra.mcp.preflight
import hashlib
import json
import os
import re
from typing import Any, Dict, Optional, Tuple

from cryptography.hazmat.primitives.asymmetric import ed25519
from fiber.dev.console.bridge.signal import UIKey, UIModal, UISlot, UISlotStatus
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.watcher.plane.emitter import get_emitter


class PreflightGuard:
    ENV_KEYS = ("FIBER_PREFLIGHT_PATTERN", "FIBER_HITL_ON")
    MODE_ENV_KEYS = ("FIBER_PREFLIGHT_MODE", "FIBER_HITL_MODE")
    BUTTON_MODE_ALIASES = ("button", "simple", "nototp", "1click")
    PREFIX_REGEX = re.compile(r"^(totp|button|simple|nototp|1click)\s*:\s*(.+)$", re.IGNORECASE)

    def __init__(
        self,
        target_id: str,
        rpc: InternalRpcClient,
        pattern: Optional[str] = None,
        require_totp: Optional[bool] = None,
    ):
        self.target_id = target_id
        self.rpc = rpc
        self.log = get_emitter(f"mcp.preflight.{self.target_id}", phase="DEFENSE")

        raw_pattern = self.resolve_pattern(self.target_id, pattern)
        self.pattern_str, self.require_totp = self._parse_mode_and_pattern(
            self.target_id, raw_pattern, require_totp
        )
        self.guard_regex = re.compile(self.pattern_str, re.IGNORECASE) if self.pattern_str else None

        if self.guard_regex:
            auth_badge = "TOTP" if self.require_totp else "BUTTON-ONLY"
            self.log.info(
                f"[PreFlight] Armed Zero-Code HITL Guard on '{self.target_id}' "
                f"(Pattern: /{self.pattern_str}/i, Mode: {auth_badge})",
                **{
                    UIKey.TARGET_ID.value: self.target_id,
                    "pattern": self.pattern_str,
                    "auth_mode": auth_badge,
                },
            )

        self.stashed_payloads: Dict[str, Dict[str, Any]] = {}
        self.validator_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(
            bytes.fromhex(NodeSigner.get_instance().pubkey_hex)
        )

    @staticmethod
    def _sanitize_target(target_id: str) -> str:
        return re.sub(r"[^A-Za-z0-9]", "_", target_id).upper()

    @classmethod
    def resolve_pattern(cls, target_id: str, explicit_pattern: Optional[str] = None) -> Optional[str]:
        """명시적 인자 -> 타겟별 환경변수(FIBER_PREFLIGHT_<TARGET>) -> 전역 환경변수 순으로 정규식을 결정"""
        sanitized_target = cls._sanitize_target(target_id)
        env_target_key = f"FIBER_PREFLIGHT_{sanitized_target}"

        raw = (
            explicit_pattern
            or os.environ.get(env_target_key)
            or os.environ.get("FIBER_PREFLIGHT_PATTERN")
            or os.environ.get("FIBER_HITL_ON")
        )
        return raw.strip() if raw and raw.strip() else None

    @classmethod
    def _parse_mode_and_pattern(
        cls,
        target_id: str,
        raw_pattern: Optional[str],
        explicit_require_totp: Optional[bool] = None,
    ) -> Tuple[Optional[str], bool]:
        """
        패턴 접두사(예: 'button:DROP|DELETE' 또는 'totp:DROP') 및 환경변수로부터
        순수 정규식과 TOTP 요구 여부(기본값 True)를 정밀 분리합니다.
        """
        sanitized_target = cls._sanitize_target(target_id)
        env_mode = (
            os.environ.get(f"FIBER_PREFLIGHT_MODE_{sanitized_target}")
            or os.environ.get("FIBER_PREFLIGHT_MODE")
            or os.environ.get("FIBER_HITL_MODE")
            or ""
        ).strip().lower()

        require_totp = explicit_require_totp if explicit_require_totp is not None else (
            env_mode not in cls.BUTTON_MODE_ALIASES
        )

        if not raw_pattern:
            return None, require_totp

        cleaned_pattern = raw_pattern.strip()
        prefix_match = cls.PREFIX_REGEX.match(cleaned_pattern)
        if prefix_match:
            mode_prefix, actual_regex = prefix_match.group(1).lower(), prefix_match.group(2).strip()
            if explicit_require_totp is None:
                require_totp = mode_prefix not in cls.BUTTON_MODE_ALIASES
            cleaned_pattern = actual_regex

        return (cleaned_pattern if cleaned_pattern else None), require_totp

    @classmethod
    def extract_from_env_dict(
        cls, env_dict: Optional[Dict[str, str]], explicit_override: Optional[str] = None
    ) -> Tuple[Optional[str], Dict[str, str]]:
        """
        mcp.json 등의 개별 워커 `env` 블록에서 Pre-flight 설정 키와 모드 키를 안전하게 추출합니다.
        별도 모드 키(FIBER_PREFLIGHT_MODE=button)가 있을 경우 패턴에 접두사로 결합하여 단일 문자열로 보존합니다.
        """
        if not env_dict:
            return explicit_override, {}

        extracted_pattern = explicit_override
        extracted_mode: Optional[str] = None
        clean_env: Dict[str, str] = {}

        for k, v in env_dict.items():
            if k in cls.ENV_KEYS:
                if not extracted_pattern and v:
                    extracted_pattern = str(v).strip()
            elif k in cls.MODE_ENV_KEYS:
                if v:
                    extracted_mode = str(v).strip().lower()
            else:
                clean_env[k] = v

        if extracted_pattern and extracted_mode in cls.BUTTON_MODE_ALIASES:
            if not cls.PREFIX_REGEX.match(extracted_pattern):
                extracted_pattern = f"button:{extracted_pattern}"

        return extracted_pattern, clean_env

    @staticmethod
    def _extract_tool_meta(payload: Dict[str, Any], params_dict: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """JSON-RPC MCP 호출 페이로드에서 실제 툴 이름과 인자 딕셔너리를 정밀 추출합니다."""
        tool_name = str(
            params_dict.get("name")
            or params_dict.get("tool")
            or payload.get("method")
            or "high_risk_tool"
        )
        raw_args = params_dict.get("arguments")
        if raw_args is None:
            raw_args = {k: v for k, v in params_dict.items() if k not in ("name", "tool", "_meta")}
        tool_args = dict(raw_args) if isinstance(raw_args, dict) else {"value": raw_args}
        return tool_name, tool_args

    def inspect_and_stash(self, handle_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """위험 패턴 매칭 시 원본을 Stash하고 콘솔 HITL 202 모달 시그널을 즉시 송출한 뒤 Synthetic Prompt를 반환"""
        if not self.guard_regex:
            return None

        params_dict = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        canonical_str = json.dumps(params_dict, sort_keys=True)
        match = self.guard_regex.search(canonical_str)
        if not match:
            return None

        matched_keyword = match.group(0)
        payload_hash = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
        tool_name, tool_args = self._extract_tool_meta(payload, params_dict)
        meta_block = params_dict.get("_meta") if isinstance(params_dict.get("_meta"), dict) else {}
        user_id = meta_block.get("user_id", "spiffe://public/agent")

        self.stashed_payloads[handle_id] = {
            "raw_payload": dict(payload),
            "payload_hash": payload_hash,
            "user_id": user_id,
            "require_totp": self.require_totp,
            "tool_name": tool_name,
        }

        mode_label = "TOTP" if self.require_totp else "BUTTON"
        action_prompt = "Enter TOTP:" if self.require_totp else "Approve or Reject execution:"

        # [핵심 개선] 배압 폴딩을 즉시 우회하는 .signal()과 Enum 계약(UIModal.HITL_202)을 결합하여 콘솔 모달 직결
        self.log.signal(
            f"[PreFlight:{mode_label}] Intercepted high-risk call '{tool_name}' (matched '{matched_keyword}') "
            f"for {handle_id}. Parking transaction (HTTP 202 YIELD).",
            **{
                UIKey.MODAL.value: UIModal.HITL_202.value,
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                UIKey.TARGET_ID.value: self.target_id,
                UIKey.TOOL_NAME.value: tool_name,
                UIKey.TOOL_ARGS.value: {
                    "matched_pattern": matched_keyword,
                    "require_totp": self.require_totp,
                    "arguments": tool_args,
                },
                UIKey.IDEM_KEY.value: handle_id,
            },
        )
        return {
            "jsonrpc": "2.0",
            "id": f"prompt_{handle_id}",
            "method": "elicitation/createMessage",
            "params": {
                "message": (
                    f"[Pre-flight Guard] High-risk pattern '{matched_keyword}' detected on '{self.target_id}'. "
                    f"{action_prompt}"
                ),
                "payload_hash": payload_hash,
                "require_totp": self.require_totp,
            },
        }

    async def verify_and_pop(self, handle_id: str, resume_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Pre-flight에 의해 파킹된 인텐트인 경우 Validator(validate.attest) 호출 및
        Ed25519 서명을 대리 검증한 뒤 보관된 원본 페이로드를 반환합니다.
        """
        stashed = self.stashed_payloads.get(handle_id)
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
        require_totp = stashed.get("require_totp", self.require_totp)
        tool_name = stashed.get("tool_name", "tool")

        res = await self.rpc.call(
            "validate.attest",
            {
                "user_id": user_id,
                "otp_code": otp_code,
                "payload_hash": payload_hash_hex,
                "require_totp": require_totp,
            },
        )
        if not isinstance(res, dict) or res.get("error") or not res.get("signature"):
            err_msg = res.get("message", "Pre-flight attestation rejected") if isinstance(res, dict) else "RPC Error"
            self.log.signal(
                f"[PreFlight] Attestation REJECTED for {handle_id} ('{tool_name}'): {err_msg}",
                **{
                    UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                    UIKey.TARGET_ID.value: self.target_id,
                    UIKey.TOOL_NAME.value: tool_name,
                    UIKey.IDEM_KEY.value: handle_id,
                },
            )
            raise PermissionError(err_msg)

        self.validator_pub_key.verify(bytes.fromhex(res["signature"]), bytes.fromhex(payload_hash_hex))
        self.stashed_payloads.pop(handle_id, None)
        mode_label = "TOTP" if require_totp else "Button"
        self.log.info(
            f"[PreFlight] Proxy Attestation ({mode_label}) Verified for {handle_id} ('{tool_name}'). Releasing stashed payload.",
            **{
                UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.TARGET_ID.value: self.target_id,
                UIKey.TOOL_NAME.value: tool_name,
                UIKey.IDEM_KEY.value: handle_id,
            },
        )
        return stashed["raw_payload"]

    def clear(self, handle_id: str) -> None:
        if self.stashed_payloads.pop(handle_id, None) is not None:
            self.log.info(
                f"[PreFlight] Cleared stashed transaction {handle_id} on '{self.target_id}'.",
                **{UIKey.TARGET_ID.value: self.target_id, UIKey.IDEM_KEY.value: handle_id},
            )