# fiber.infra.mcp.worker.deployer
## @lineage: fiber.dev.worker.deployer
import os
import sys
import json
import logging
import hashlib
import asyncio
from typing import Dict, Any, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.exceptions import InvalidSignature

from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol
from xphi.arch.bound.adapter.pta import NodeSigner

log = logging.getLogger("worker.deployer")


class ExecutionDeployer(AsyncWorkerProtocol):
    def __init__(self):
        super().__init__(agent_name="execution.deployer")
        self.validator_pub_key: Optional[ed25519.Ed25519PublicKey] = self._load_validator_public_key()

        # 비동기 Elicitation(프롬프트) 및 RPC Delegation(대행) 응답을 대기하기 위한 Future 레지스트리
        self.pending_prompts: Dict[str, asyncio.Future] = {}

    def _load_validator_public_key(self) -> Optional[ed25519.Ed25519PublicKey]:
        """
        1순위: 명시적 환경변수 (DPHI_VALIDATOR_PUBLIC_KEY)
        2순위: NodeSigner 싱글톤의 결정론적 노드 공개키 (환경변수 노출 없이 자동 동기화)
        """
        pub_key_hex = os.environ.get("DPHI_VALIDATOR_PUBLIC_KEY")
        if not pub_key_hex:
            try:
                pub_key_hex = NodeSigner.get_instance().pubkey_hex
            except Exception as e:
                self.log.error(f"⚠️ Failed to resolve NodeSigner public key: {e}")
                return None

        try:
            return ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_key_hex.strip()))
        except Exception as e:
            self.log.error(f"⚠️ Invalid Ed25519 public key format: {e}")
            return None

    async def _route_request_async(self, req: Dict[str, Any]):
        # 1. Multiplex/Ephemeral Intent 제어 메시지 (RESUME) 언래핑 처리
        if req.get("action") == "RESUME":
            payload = req.get("payload", {})
            prompt_id = payload.get("id")

            target_id = prompt_id if prompt_id in self.pending_prompts else f"prompt_{prompt_id}"
            if target_id in self.pending_prompts:
                future = self.pending_prompts.pop(target_id)
                if not future.done():
                    future.set_result(payload)
            else:
                self.log.warning(f"Unmatched RESUME prompt_id: {prompt_id}")
            return

        # 2. 순수 JSON-RPC Response (RPC 대행 결과 및 Sentinel 강제 롤백 등의 에러 메시지) 처리
        if "method" not in req and ("result" in req or "error" in req):
            req_id = req.get("id")
            if req_id in self.pending_prompts:
                future = self.pending_prompts.pop(req_id)
                if not future.done():
                    future.set_result(req)
            else:
                # 호환성 방어: 상위 래퍼 없이 직접 prompt_id로 들어온 응답인 경우 매칭 시도
                alt_id = f"prompt_{req_id}"
                if alt_id in self.pending_prompts:
                    future = self.pending_prompts.pop(alt_id)
                    if not future.done():
                        future.set_result(req)
                else:
                    self.log.warning(f"Unmatched response received for unknown ID: {req_id}")
            return

        # 3. 일반 Request는 부모 프로토콜 엔진에 위임
        await super()._route_request_async(req)

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "execute_db_migration":
            user_id = meta.get("user_id") or arguments.get("user_id", "UNKNOWN_USER")
            await self._handle_migration(req_id, arguments, user_id)
        else:
            await self.send_error(req_id, -32601, f"Unknown tool: {tool_name}")

    async def _handle_migration(self, req_id: Any, args: Dict[str, Any], user_id: str):
        service = args.get("service_name")
        env = args.get("target_env")
        sql = args.get("sql_script", "").upper()

        self.log.info(f"Migration Request -> {service} [{env}] by {user_id}")

        is_destructive = "DROP" in sql or "TRUNCATE" in sql
        if is_destructive and env == "production":
            if not self.validator_pub_key:
                return await self.send_error(req_id, -500, "Validator Public Key is not initialized.")

            self.log.warning("⚠️ DESTRUCTIVE PAYLOAD. Halting execution and delegating validation.")

            canonical_payload = json.dumps({"service": service, "env": env, "sql": sql}, sort_keys=True).encode()
            payload_hash = hashlib.sha256(canonical_payload).digest()

            # 비동기 Await를 통해 트랜잭션을 안전하게 정지(Park)하고 OTP 요청
            otp_code = await self._request_user_otp_async(req_id, service)
            if not otp_code:
                return await self.send_error(req_id, -32000, "OTP Input Cancelled or Timed Out.")

            # Connector로 백엔드 Validator RPC 대행(Delegation) 요청
            delegate_req_id = f"delegate_{req_id}"
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self.pending_prompts[delegate_req_id] = future

            await self.send_request(
                req_id=delegate_req_id,
                method="rpc_delegate",
                params={
                    "target_method": "validate.attest",
                    "data": {
                        "user_id": user_id,
                        "otp_code": otp_code,
                        "payload_hash": payload_hash.hex(),
                    },
                },
            )
            self.log.info("[Deploy] Waiting for Validator attestation via Connector delegation...")

            try:
                delegate_res = await asyncio.wait_for(future, timeout=20.0)

                # Case A: JSON-RPC 레벨 에러 (Connector 대행 실패 등)
                if "error" in delegate_res:
                    err_info = delegate_res["error"]
                    code = err_info.get("code", -502) if isinstance(err_info, dict) else -502
                    msg = err_info.get("message", str(err_info)) if isinstance(err_info, dict) else str(err_info)
                    return await self.send_error(req_id, code, msg)

                rpc_response = delegate_res.get("result", {})

                # Case B: 핸들러(ValidatorService) 내부 도메인 에러 반환 처리
                if isinstance(rpc_response, dict) and rpc_response.get("error"):
                    return await self.send_error(
                        req_id,
                        rpc_response.get("code", -32000),
                        rpc_response.get("message", "Validator rejected attestation."),
                    )

                signature_hex = rpc_response.get("signature") if isinstance(rpc_response, dict) else None
                if not signature_hex:
                    return await self.send_error(req_id, -502, "Validator returned empty signature.")

            except asyncio.TimeoutError:
                self.pending_prompts.pop(delegate_req_id, None)
                return await self.send_error(req_id, -504, "Validator RPC delegation timed out.")
            except Exception as e:
                self.pending_prompts.pop(delegate_req_id, None)
                return await self.send_error(req_id, -500, f"Delegation processing failed: {str(e)}")

            # 로컬에서 NodeSigner Public Key로 Ed25519 서명 검증 (무결성 보장)
            try:
                self.validator_pub_key.verify(bytes.fromhex(signature_hex), payload_hash)
                self.log.info("🛡️ Cryptographic Attestation Verified. Authorization Granted.")
            except InvalidSignature:
                self.log.critical("🚨 SECURITY BREACH: Invalid Attestation Signature!")
                return await self.send_error(req_id, -32000, "Invalid Cryptographic Signature.")

        self.log.info(f"✅ Executing migration for {service}")
        await self.send_response(
            req_id,
            {"content": [{"type": "text", "text": "Migration executed successfully."}], "isError": False},
        )

    async def _request_user_otp_async(self, parent_req_id: Any, service: str) -> str:
        prompt_req_id = f"prompt_{parent_req_id}"

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_prompts[prompt_req_id] = future

        # Gateway로 Elicitation (YIELD 트리거) 요청 전송
        await self.send_request(
            req_id=prompt_req_id,
            method="elicitation/createMessage",
            params={"message": f"DANGER: Destructive migration on '{service}'. Enter TOTP token:"},
        )
        self.log.info("[BLOCKED] Waiting for TOTP input (Suspended execution context)...")

        try:
            client_res = await asyncio.wait_for(future, timeout=300.0)
            return client_res.get("result", {}).get("value", "")
        except asyncio.TimeoutError:
            self.log.error("OTP Timeout: Elicitation response not received.")
            self.pending_prompts.pop(prompt_req_id, None)
            return ""


def main():
    server = ExecutionDeployer()
    try:
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        log.info("Server shutting down.")


if __name__ == "__main__":
    main()