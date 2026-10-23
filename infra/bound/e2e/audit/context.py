# fiber.infra.bound.e2e.audit.context
## @lineage: fiber.dev.e2e.worker.fixture
import os
import time
import uuid
import hmac
import struct
import base64
import hashlib
from typing import Dict, Any, Optional

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from xphi.node.rpc.handler.validator import ValidatorService
import xphi.node.rpc.registry as rpc_registry
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.node.space.bind.resolver import resolve_path

class AuditContext:
    def __init__(
        self,
        spiffe_id: str = "spiffe://self/fiber",
        totp_secret: str = "JBSWY3DPEHPK3PXP",
        passphrase: Optional[str] = None,
    ):
        self.run_id = uuid.uuid4().hex[:8]
        base_id = spiffe_id.rstrip("/")
        self.spiffe_id = f"{base_id}_e2e_{self.run_id}"
        self.totp_secret = totp_secret
        self._explicit_passphrase = passphrase

        self.db_path = os.environ.get(
            "DPHI_AUDIT_DB_PATH",
            os.path.join(str(resolve_path("sign")), "deploy_audit.sqlite"),
        )
        self.validator_service: Optional[ValidatorService] = None
        self._original_rpc_builder = None

    @property
    def temp_db_path(self) -> str:
        """audit.py 로깅 호환성을 위한 프로퍼티 (표준 DB 경로 및 격리된 테스트 계정 표시)"""
        return f"{self.db_path} (Scoped ID: {self.spiffe_id})"

    def provision(self) -> ValidatorService:
        """NodeSigner 및 local_cipher.key 기반으로 ValidatorService를 초기화, 표준 DB에 격리된 일회용 테스트 계정을 안전하게 주입"""
        # ValidatorService 초기화
        self.validator_service = ValidatorService()
        active_passphrase = self._explicit_passphrase or self.validator_service.master_passphrase

        # 파생된 패스프레이즈로 Mock TOTP Secret을 AES-GCM 암호화
        salt = os.urandom(16)
        nonce = os.urandom(12)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        aesgcm = AESGCM(kdf.derive(active_passphrase.encode("utf-8")))
        ciphertext = aesgcm.encrypt(nonce, self.totp_secret.encode("utf-8"), None)

        # 표준 SQLite DB에 일회용 테스트 계정 삽입
        self.validator_service.conn.execute(
            "INSERT OR REPLACE INTO admin_users VALUES (?, ?, ?, ?)",
            (self.spiffe_id, salt.hex(), nonce.hex(), ciphertext.hex()),
        )
        self.validator_service.conn.commit()

        # Standalone 모드 실행 시 내부 RPC 워커가 동일한 인스턴스를 참조하도록 훅 연결
        self._original_rpc_builder = rpc_registry.build_internal_rpc_registry
        validator_ref = self.validator_service

        def _injected_builder(*args, **kwargs):
            kwargs["validator_service"] = validator_ref
            return self._original_rpc_builder(*args, **kwargs)

        rpc_registry.build_internal_rpc_registry = _injected_builder
        return self.validator_service

    def generate_totp(self) -> str:
        """현재 타임스탬프 기준 유효한 6자리 RFC 6238 TOTP 코드를 생성"""
        key = base64.b32decode(self.totp_secret, True)
        msg = struct.pack(">Q", int(time.time()) // 30)
        h = hmac.new(key, msg, hashlib.sha1).digest()
        o = h[19] & 15
        code = (struct.unpack(">I", h[o : o + 4])[0] & 0x7FFFFFFF) % 1000000
        return f"{code:06d}"

    def teardown(self):
        """DB 파일을 삭제하지 않고 이번 테스트에서 생성한 고유 계정(self.spiffe_id) 레코드만 정확히 제거"""
        if self._original_rpc_builder:
            rpc_registry.build_internal_rpc_registry = self._original_rpc_builder
            self._original_rpc_builder = None

        if self.validator_service and hasattr(self.validator_service, "conn"):
            try:
                self.validator_service.conn.execute(
                    "DELETE FROM admin_users WHERE user_id = ?", (self.spiffe_id,)
                )
                self.validator_service.conn.commit()
                self.validator_service.conn.close()
            except Exception:
                pass


class MCPGatewayTestClient:
    def __init__(self, base_url: str, target_id: str, spiffe_id: str):
        self.base_url = base_url.rstrip("/")
        self.target_id = target_id
        self.spiffe_id = spiffe_id
        self.invoke_url = f"{self.base_url}/v1/mcp-gateway/{self.target_id}/invoke"
        self.dpop = DPoPClientGenerator(key_size=2048)

    def _build_headers(self, idempotency_key: str, receipt: str = "valid_receipt") -> Dict[str, str]:
        nonce = uuid.uuid4().hex
        proof = self.dpop.generate_proof(url=self.invoke_url, method="POST", nonce=nonce)
        return {
            "x-idempotency-key": idempotency_key,
            "x-nonce": nonce,
            "Fuel-Receipt": receipt,
            "x-spiffe-id": self.spiffe_id,
            "DPoP": proof,
        }

    @staticmethod
    def build_migration_payload(
        req_id: int, service_name: str, target_env: str = "production", sql_script: str = "DROP TABLE"
    ) -> Dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": "tools/call",
            "params": {
                "name": "execute_db_migration",
                "arguments": {
                    "service_name": service_name,
                    "target_env": target_env,
                    "sql_script": sql_script,
                },
            },
        }

    @staticmethod
    def attach_totp_resume(base_payload: Dict[str, Any], prompt_id: str, totp_code: str) -> Dict[str, Any]:
        resumed = dict(base_payload)
        params_copy = dict(resumed.get("params", {}))
        params_copy["_meta"] = {
            "inputResponses": {
                "jsonrpc": "2.0",
                "id": prompt_id,
                "result": {"value": totp_code},
            }
        }
        resumed["params"] = params_copy
        return resumed

    async def invoke(
        self, payload: Dict[str, Any], idempotency_key: str, timeout: float = 10.0
    ) -> httpx.Response:
        headers = self._build_headers(idempotency_key=idempotency_key)
        async with httpx.AsyncClient(base_url=self.base_url, timeout=timeout) as client:
            return await client.post(self.invoke_url, json=payload, headers=headers)