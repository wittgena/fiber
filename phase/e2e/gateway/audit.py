# fiber.phase.e2e.gateway.audit
import os
import sys
import time
import uuid
import json
import asyncio
import sqlite3
import tempfile
import base64
import hmac
import hashlib
import struct
from typing import List

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from fiber.phase.e2e.infra.config import Phase, E2EConfig, TestResult
from fiber.phase.e2e.infra.bridge import BaseBridgePipeline, log
from fiber.agent.worker.connector import WorkerConnector

import fiber.agent.worker.legacy.sandbox as agent_deploy
import fiber.agent.worker.mcp.sentinel as agent_sentinel
from fiber.dphi.rpc.legacy.validator import AuthValidatorService
import fiber.dphi.rpc.registry as rpc_registry

from xphi.state.phase.reactor import PhaseReactor

class AuditSecurityPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config, 
            name="Stateful Security & Resilience Suite", 
            scope_name="MCP_AUDIT_SUITE"
        )

        self.deploy_id = "legacy-01"
        self.test_short_id = "fiber"
        self.test_spiffe_id = f"spiffe://self/{self.test_short_id}"
        self.mock_totp_secret = "JBSWY3DPEHPK3PXP"

        self.prompt_id = None
        self.idem_key_otp = None
        self.deploy_payload = None

        self.sentinel = None
        self._sentinel_task = None
        self.temp_db_path = None

        self.set_phases([
            Phase("Phase 7: Idempotency Fast-Path Defense (Trigger YIELD)", self.phase_idempotency_defense),
            Phase("Phase 8: MCP 2026-07-28 Stateless Re-issue & Resume", self.phase_stateless_otp_resume),
            Phase("Phase 9: Autonomous Reconciliation (Sentinel)", self.phase_sentinel_reconciliation)
        ])

    async def setup_custom_context(self):
        """환경 구축 및 E2E DB 프로비저닝, 그리고 안전한 Validator 인스턴스 주입"""
        print("\n" + "="*80)
        print("🔐 [Security Context] Zero-Trust Auth Validator Auto-Provisioning")
        
        self.test_passphrase = "test_master_passphrase_123"
        fd, self.temp_db_path = tempfile.mkstemp(suffix='_audit.sqlite')
        os.close(fd)
        
        salt = os.urandom(16)
        nonce = os.urandom(12)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        aesgcm = AESGCM(kdf.derive(self.test_passphrase.encode('utf-8')))
        ciphertext = aesgcm.encrypt(nonce, self.mock_totp_secret.encode('utf-8'), None)

        # 1. 샌드박스와 통신할 가짜 DB에 데이터 주입
        with sqlite3.connect(self.temp_db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_users (
                    user_id TEXT PRIMARY KEY, salt TEXT, nonce TEXT, ciphertext TEXT
                )
            """)
            conn.execute("INSERT INTO admin_users VALUES (?, ?, ?, ?)", 
                         (self.test_spiffe_id, salt.hex(), nonce.hex(), ciphertext.hex()))
            conn.commit()

        print(f"✅ Auto-injected Mock Secret into Temp DB: {self.temp_db_path}")
        print("="*80 + "\n")

        val_key = ed25519.Ed25519PrivateKey.generate()
        self.val_priv_hex = val_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption()).hex()
        self.val_pub_hex = val_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
        
        # 2. Validator 인스턴스화를 위한 환경 변수 세팅
        os.environ["DPHI_VALIDATOR_PRIVATE_KEY"] = self.val_priv_hex
        os.environ["DPHI_VALIDATOR_PUBLIC_KEY"] = self.val_pub_hex
        os.environ["DPHI_MASTER_PASSPHRASE"] = self.test_passphrase
        os.environ["DPHI_AUDIT_DB_PATH"] = self.temp_db_path
        
        # 3. 확실한 데이터 연결을 위해 여기서 Validator 인스턴스를 직접 만듭니다.
        self.validator_service = AuthValidatorService()
        
        # [Zero-Trust 격리] 메모리에 올렸으니 즉시 환경 변수에서 민감 정보 영구 삭제
        os.environ.pop("DPHI_VALIDATOR_PRIVATE_KEY", None)
        os.environ.pop("DPHI_MASTER_PASSPHRASE", None)
        os.environ.pop("DPHI_AUDIT_DB_PATH", None)
        # PUBLIC_KEY는 샌드박스의 서명 검증을 위해 남겨둡니다.

        # 4. [개선] 데몬이 우리가 만든 이 인스턴스를 사용하도록 동적 레지스트리에 주입 (Monkey Patch)
        original_builder = rpc_registry.build_internal_rpc_registry
        
        def safe_mock_registry_builder(*args, **kwargs):
            # args/kwargs에 뭐가 오든, 우리가 만든 E2E Validator를 강제로 물려줍니다.
            kwargs['validator_service'] = self.validator_service
            return original_builder(*args, **kwargs)
            
        rpc_registry.build_internal_rpc_registry = safe_mock_registry_builder
        
        log.info("[AuditPipeline] Ephemeral DB Provisioned and Validator Instance Injected.")

    async def setup_workers(self):
        """Worker Connector 및 Sentinel 데몬 기동"""
        # Deploy 샌드박스 부팅 (민감 정보가 지워진 안전한 os.environ 상속)
        deploy_cmd = f"{sys.executable} -m fiber.dphi.worker.deploy"
        self.connectors.append(
            WorkerConnector(target_id=self.deploy_id, legacy_command=deploy_cmd, mode="ephemeral")
        )

        # Sentinel 시작
        self.sentinel = agent_sentinel.AgentSentinel(ledger=self.mock_ledger, rpc_client=self.rpc, sweep_interval=1.0)
        self._sentinel_task = asyncio.create_task(self.sentinel.ignite())
        log.info("[AuditPipeline] Sentinel Autonomous Daemon & Worker Connectors Ignited. Environment Sanitized.")

    async def teardown_custom(self):
        if self.sentinel: self.sentinel.running = False
        if self._sentinel_task: self._sentinel_task.cancel()
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            
        # 닫히지 않은 Validator DB 커넥션 종료 (Lock 에러 방어)
        if hasattr(self, 'validator_service') and hasattr(self.validator_service, 'conn'):
            try: self.validator_service.conn.close()
            except: pass
            
        log.info("[AuditPipeline] Sentinel Autonomous Daemon Shutdown & Temp DB Cleared.")

    # =====================================================================
    # Test Phases (7 ~ 9)
    # =====================================================================
    async def phase_idempotency_defense(self):
        self.idem_key_otp = uuid.uuid4().hex
        self.deploy_payload = {
            "jsonrpc": "2.0", "id": 777, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "auth", "target_env": "production", "sql_script": "DROP TABLE"}}
        }
        headers = {
            "x-idempotency-key": self.idem_key_otp, 
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_x402",
            "x-spiffe-id": self.test_spiffe_id 
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res1 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=self.deploy_payload, headers=headers)
            if res1.status_code != 202: raise RuntimeError(f"Expected HTTP 202 (YIELD), got {res1.status_code} - {res1.text}")
            prompt_res = res1.json()
            self.prompt_id = prompt_res.get("id")

        headers["x-nonce"] = uuid.uuid4().hex 
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res2 = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=self.deploy_payload, headers=headers)
            if res2.status_code != 202: raise RuntimeError(f"Fast-Path failed. Expected 202, got {res2.status_code}")

        log.info("  └─ ✨ Idempotency Shield deflected duplicate request without crashing Sandbox.")

    async def phase_stateless_otp_resume(self):
        def _generate_totp(secret_b32: str) -> str:
            key = base64.b32decode(secret_b32, True)
            msg = struct.pack(">Q", int(time.time()) // 30)
            h = hmac.new(key, msg, hashlib.sha1).digest()
            o = h[19] & 15
            h = (struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000
            return f"{h:06d}"

        valid_totp_code = _generate_totp(self.mock_totp_secret)
        
        resume_payload = dict(self.deploy_payload)
        resume_payload["params"]["_meta"] = {
            "inputResponses": {"jsonrpc": "2.0", "id": self.prompt_id, "result": {"value": valid_totp_code}}
        }
        headers = {
            "x-idempotency-key": self.idem_key_otp,
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_x402",
            "x-spiffe-id": self.test_spiffe_id 
        }

        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=resume_payload, headers=headers)
            if res.status_code != 200:
                raise RuntimeError(f"Bridge failed to Resume Sandbox. Expected 200 OK, got {res.status_code} ({res.text})")

        final_result = res.json().get("result", {}).get("content", [{}])[0].get("text", "")
        if "successfully" not in final_result:
            raise RuntimeError(f"Resume succeeded, but payload failed: {final_result}")
        log.info("  └─ ✨ Stateless Resume -> Stateful Sentinel Execution -> 200 OK Resolution Verified.")

    async def phase_sentinel_reconciliation(self):
        self.mock_ledger.stale_timeout = 1.0
        idem_key_sentinel = uuid.uuid4().hex
        payload = {
            "jsonrpc": "2.0", "id": 888, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "billing", "target_env": "production", "sql_script": "DROP TABLE"}}
        }
        headers = {
            "x-idempotency-key": idem_key_sentinel, 
            "x-nonce": uuid.uuid4().hex, 
            "X-X402-Receipt": "valid_x402", 
            "x-spiffe-id": self.test_spiffe_id
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            await client.post(f"/v1/mcp-gateway/{self.deploy_id}/invoke", json=payload, headers=headers)

        handle_id = self.captured_handle_ids.get("latest")
        await asyncio.sleep(3.0)

        final_state = await self.mock_ledger.query_state(handle_id)
        if not final_state or final_state.metadata.get("status") != "FAULTED":
            raise RuntimeError("Sentinel failed to rollback.")
        log.info(f"  └─ ✨ Sentinel Autonomous Reconciliation Successful.")


class AuditSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await AuditSecurityPipeline(config=net_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("🛡️ [SECURITY & AUDIT BENCHMARK REPORT]")
        self.log.info("="*80)
        all_passed = all(r.passed for r in self.results)

        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(f"{status_icon} {idx:02d}. [{res.target}]".ljust(22) + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}")

        self.log.info("-" * 80)
        if all_passed: 
            self.log.info("🎉 SECURITY AUDIT PIPELINE VALIDATED SUCCESSFULLY.")
        else: 
            self.log.critical("💥 SECURITY FRACTURE DETECTED. Check logs for details.")
        self.log.info("="*80 + "\n")


def main(args_list: list[str] = None):
    app = AuditSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()