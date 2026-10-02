# fiber.dev.e2e.worker.audit
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

from fiber.infra.e2e.config import Phase, E2EConfig, TestResult
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log
import fiber.dev.ex.worker.deployer as worker_deployer
import fiber.dev.ex.worker.sentinel as worker_sentinel

from fiber.gateway.worker.connector import WorkerConnector
from fiber.infra.rpc.validator import ValidatorService
import fiber.infra.rpc.registry as rpc_registry

from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator

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
        
        # DPoP 클라이언트 인스턴스화
        self.dpop_client = DPoPClientGenerator(key_size=2048)

        self.set_phases([
            Phase("Phase 1: Idempotency Fast-Path Defense (Trigger YIELD)", self.phase_idempotency_defense),
            Phase("Phase 2: MCP 2026-07-28 Stateless Re-issue & Resume", self.phase_stateless_otp_resume),
            Phase("Phase 3: Autonomous Reconciliation (Sentinel)", self.phase_sentinel_reconciliation)
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
        
        os.environ["DPHI_VALIDATOR_PRIVATE_KEY"] = self.val_priv_hex
        os.environ["DPHI_VALIDATOR_PUBLIC_KEY"] = self.val_pub_hex
        os.environ["DPHI_MASTER_PASSPHRASE"] = self.test_passphrase
        os.environ["DPHI_AUDIT_DB_PATH"] = self.temp_db_path
        
        self.validator_service = ValidatorService()
        
        # [수정] 여기서 os.environ.pop 하던 3줄을 삭제하고 teardown_custom으로 이동시켰습니다.
        # 백엔드 데몬이 자체적으로 ValidatorService를 생성할 때 환경변수가 필요하기 때문입니다.

        original_builder = rpc_registry.build_internal_rpc_registry
        
        def safe_mock_registry_builder(*args, **kwargs):
            kwargs['validator_service'] = self.validator_service
            return original_builder(*args, **kwargs)
            
        rpc_registry.build_internal_rpc_registry = safe_mock_registry_builder
        
        log.info("[AuditPipeline] Ephemeral DB Provisioned and Validator Instance Injected.")

    async def setup_workers(self):
        deploy_cmd = f"{sys.executable} -m {worker_deployer.__name__}"
        self.connectors.append(
            WorkerConnector(target_id=self.deploy_id, execution_target=deploy_cmd, mode="ephemeral")
        )

        self.sentinel = worker_sentinel.AgentSentinel(ledger=self.mock_store, rpc_client=self.rpc, sweep_interval=1.0)
        self._sentinel_task = asyncio.create_task(self.sentinel.ignite())
        log.info("[AuditPipeline] Sentinel Autonomous Daemon & Worker Connectors Ignited. Environment Sanitized.")

    async def teardown_custom(self):
        if self.sentinel: self.sentinel.running = False
        if self._sentinel_task: self._sentinel_task.cancel()
        if self.temp_db_path and os.path.exists(self.temp_db_path):
            os.remove(self.temp_db_path)
            
        if hasattr(self, 'validator_service') and hasattr(self.validator_service, 'conn'):
            try: self.validator_service.conn.close()
            except: pass
            
        # [수정] 테스트 종료 시점에 환경변수들을 안전하게 삭제합니다.
        os.environ.pop("DPHI_VALIDATOR_PRIVATE_KEY", None)
        os.environ.pop("DPHI_MASTER_PASSPHRASE", None)
        os.environ.pop("DPHI_AUDIT_DB_PATH", None)
            
        log.info("[AuditPipeline] Sentinel Autonomous Daemon Shutdown & Temp DB Cleared.")

    ## Test Phases
    async def phase_idempotency_defense(self):
        self.idem_key_otp = uuid.uuid4().hex
        self.deploy_payload = {
            "jsonrpc": "2.0", "id": 777, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "auth", "target_env": "production", "sql_script": "DROP TABLE"}}
        }
        
        # 요청 1: DPoP 헤더 주입
        nonce1 = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.deploy_id}/invoke"
        dpop_proof1 = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce1)
        
        headers1 = {
            "x-idempotency-key": self.idem_key_otp, 
            "x-nonce": nonce1, 
            "X-X402-Receipt": "valid_x402",
            "x-spiffe-id": self.test_spiffe_id,
            "DPoP": dpop_proof1
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res1 = await client.post(url, json=self.deploy_payload, headers=headers1)
            if res1.status_code != 202: raise RuntimeError(f"Expected HTTP 202 (YIELD), got {res1.status_code} - {res1.text}")
            prompt_res = res1.json()
            self.prompt_id = prompt_res.get("id")

        # 요청 2(재시도 검증): 새로운 Nonce로 DPoP 재발급
        nonce2 = uuid.uuid4().hex
        dpop_proof2 = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce2)
        headers2 = dict(headers1)
        headers2.update({"x-nonce": nonce2, "DPoP": dpop_proof2})
        
        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res2 = await client.post(url, json=self.deploy_payload, headers=headers2)
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
        
        # Resume 요청용 DPoP 발급
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.deploy_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        
        headers = {
            "x-idempotency-key": self.idem_key_otp,
            "x-nonce": nonce, 
            "X-X402-Receipt": "valid_x402",
            "x-spiffe-id": self.test_spiffe_id,
            "DPoP": dpop_proof
        }

        async with httpx.AsyncClient(base_url=self.local_url, timeout=10.0) as client:
            res = await client.post(url, json=resume_payload, headers=headers)
            if res.status_code != 200:
                raise RuntimeError(f"Bridge failed to Resume Sandbox. Expected 200 OK, got {res.status_code} ({res.text})")

        final_result = res.json().get("result", {}).get("content", [{}])[0].get("text", "")
        if "successfully" not in final_result:
            raise RuntimeError(f"Resume succeeded, but payload failed: {final_result}")
        log.info("  └─ ✨ Stateless Resume -> Stateful Sentinel Execution -> 200 OK Resolution Verified.")

    # async def phase_sentinel_reconciliation(self):
    #     self.mock_ledger.stale_timeout = 1.0
    #     idem_key_sentinel = uuid.uuid4().hex
    #     payload = {
    #         "jsonrpc": "2.0", "id": 888, "method": "tools/call",
    #         "params": {"name": "execute_db_migration", "arguments": {"service_name": "billing", "target_env": "production", "sql_script": "DROP TABLE"}}
    #     }
        
    #     # Sentinel 강제 롤백 유도용 DPoP 발급
    #     nonce = uuid.uuid4().hex
    #     url = f"{self.local_url}/v1/mcp-gateway/{self.deploy_id}/invoke"
    #     dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        
    #     headers = {
    #         "x-idempotency-key": idem_key_sentinel, 
    #         "x-nonce": nonce, 
    #         "X-X402-Receipt": "valid_x402", 
    #         "x-spiffe-id": self.test_spiffe_id,
    #         "DPoP": dpop_proof
    #     }

    #     async with httpx.AsyncClient(base_url=self.local_url) as client:
    #         await client.post(url, json=payload, headers=headers)

    #     handle_id = self.captured_handle_ids.get("latest")
    #     await asyncio.sleep(3.0)

    #     final_state = await self.mock_ledger.query_state(handle_id)
    #     if not final_state or final_state.metadata.get("status") != "FAULTED":
    #         raise RuntimeError("Sentinel failed to rollback.")
    #     log.info(f"  └─ ✨ Sentinel Autonomous Reconciliation Successful.")
    
    async def phase_sentinel_reconciliation(self):
        self.mock_store.stale_timeout = 1.0
        idem_key_sentinel = uuid.uuid4().hex
        payload = {
            "jsonrpc": "2.0", "id": 888, "method": "tools/call",
            "params": {"name": "execute_db_migration", "arguments": {"service_name": "billing", "target_env": "production", "sql_script": "DROP TABLE"}}
        }
        
        nonce = uuid.uuid4().hex
        url = f"{self.local_url}/v1/mcp-gateway/{self.deploy_id}/invoke"
        dpop_proof = self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce)
        
        headers = {
            "x-idempotency-key": idem_key_sentinel, 
            "x-nonce": nonce, 
            "X-X402-Receipt": "valid_x402", 
            "x-spiffe-id": self.test_spiffe_id,
            "DPoP": dpop_proof
        }

        async with httpx.AsyncClient(base_url=self.local_url) as client:
            res = await client.post(url, json=payload, headers=headers)
            
        location = res.headers.get("Location", "")
        if "/" in location:
            handle_id = location.split("/")[-1]
        else:
            handle_id = self.captured_handle_ids.get("latest")

        # =========================================================================
        # [변경] 하드코딩된 sleep(3.0) 대신, 최대 10초간 0.5초 간격으로 Polling
        # =========================================================================
        max_retries = 20
        retry_interval = 0.5
        final_state = None
        
        for _ in range(max_retries):
            final_state = await self.mock_store.query_state(handle_id)
            if final_state and final_state.metadata.get("status") == "FAULTED":
                break  # Sentinel 롤백 완료됨!
            await asyncio.sleep(retry_interval)

        if not final_state or final_state.metadata.get("status") != "FAULTED":
            current_status = final_state.metadata.get('status') if final_state else 'None'
            raise RuntimeError(f"Sentinel failed to rollback within {max_retries * retry_interval}s. Current status: {current_status}")
            
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