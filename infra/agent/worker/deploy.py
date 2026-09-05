# fiber.infra.agent.worker.deploy
## @lineage: fiber.a2a.worker.deploy
## @lineage: fiber.infra.worker.agent.deploy
import os
import sys
import json
import logging
import hashlib
from typing import Dict, Any

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.exceptions import InvalidSignature

from fiber.infra.agent.bridge.protocol import AgentProtocol
from fiber.infra.client.rpc import InternalRpcClient

log = logging.getLogger("agent.deploy")

class ExecutionDeployer(AgentProtocol):
    """
    [PEP: Policy Enforcement Point] 
    DB 접속 권한이나 마스터 키가 없습니다. Validator의 서명을 검증한 후 쿼리를 실행합니다.
    """
    def __init__(self):
        super().__init__(agent_name="agent.deploy")
        
        # Validator의 Public Key만 보유 (서명 검증용)
        pub_key_hex = os.environ.get("DPHI_VALIDATOR_PUBLIC_KEY")
        if not pub_key_hex:
            self.log.error("⚠️ DPHI_VALIDATOR_PUBLIC_KEY is missing. Execution will fail.")
        else:
            self.validator_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_key_hex))
            
        # A2A 통신을 위한 내부 RPC 클라이언트
        self.rpc_client = InternalRpcClient()

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "execute_db_migration":
            user_id = meta.get("user_id") or arguments.get("user_id", "UNKNOWN_USER")
            self._handle_migration(req_id, arguments, user_id)
        else:
            self.send_error(req_id, -32601, f"Unknown tool: {tool_name}")

    def _handle_migration(self, req_id: Any, args: Dict[str, Any], user_id: str):
        service = args.get("service_name")
        env = args.get("target_env")
        sql = args.get("sql_script", "").upper()
        
        self.log.info(f"Migration Request -> {service} [{env}] by {user_id}")

        is_destructive = "DROP" in sql or "TRUNCATE" in sql
        if is_destructive and env == "production":
            self.log.warning(f"⚠️ DESTRUCTIVE PAYLOAD. Halting execution and delegating validation.")
            
            # 1. Payload 고유 해시 생성 (무결성 보장)
            canonical_payload = json.dumps({"service": service, "env": env, "sql": sql}, sort_keys=True).encode()
            payload_hash = hashlib.sha256(canonical_payload).digest()

            # 2. 사용자에게 OTP 입력 요청 (YIELD)
            otp_code = self._request_user_otp_blocking(req_id, service)
            if not otp_code:
                return self.send_error(req_id, -32000, "OTP Input Cancelled.")

            # 3. [A2A 통신] Validator Agent에게 검증 및 서명 요청
            try:
                # 동기적 환경을 가정하여 RPC 처리 (프레임워크에 맞게 비동기/이벤트 구조로 조정 가능)
                rpc_response = self.rpc_client.call_sync(
                    target="agent.validator",
                    method="request_attestation",
                    params={
                        "user_id": user_id,
                        "otp_code": otp_code,
                        "payload_hash": payload_hash.hex()
                    }
                )
                signature_hex = rpc_response.get("signature")
                if not signature_hex:
                    return self.send_error(req_id, -32000, "Validation Rejected by Validator Agent.")
            except Exception as e:
                return self.send_error(req_id, -502, f"Failed to reach Validator Agent: {e}")

            # 4. 서명(Attestation) 로컬 검증
            try:
                self.validator_pub_key.verify(bytes.fromhex(signature_hex), payload_hash)
                self.log.info("🛡️ Cryptographic Attestation Verified. Authorization Granted.")
            except InvalidSignature:
                self.log.critical("🚨 SECURITY BREACH: Invalid Attestation Signature!")
                return self.send_error(req_id, -32000, "Invalid Cryptographic Signature.")
            except Exception:
                return self.send_error(req_id, -500, "Missing Validator Public Key.")

        # 5. 최종 실행 (PEP 역할)
        self.log.info(f"✅ Executing migration for {service}")
        self.send_response(req_id, {"content": [{"type": "text", "text": "Migration executed successfully."}], "isError": False})

    def _request_user_otp_blocking(self, parent_req_id: Any, service: str) -> str:
        prompt_req_id = f"prompt_{parent_req_id}"
        self.send_request(
            req_id=prompt_req_id,
            method="elicitation/createMessage",
            params={"message": f"DANGER: Destructive migration on '{service}'. Enter TOTP token:"}
        )
        self.log.info(f"[BLOCKED] Waiting for TOTP input...")
        
        response_line = sys.stdin.readline()
        if not response_line:
            return ""

        try:
            client_res = json.loads(response_line.strip())
            return client_res.get("result", {}).get("value", "")
        except json.JSONDecodeError:
            return ""

def main():
    server = ExecutionDeployer()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server shutting down.")

if __name__ == "__main__":
    main()