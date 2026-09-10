# fiber.agent.worker.legacy.sandbox
## @lineage: fiber.dphi.worker.deploy
import os
import sys
import json
import logging
import hashlib
import asyncio
from typing import Dict, Any

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.exceptions import InvalidSignature

from xphi.arch.contract.protocol.agent import AsyncAgentProtocol

log = logging.getLogger("worker.deploy")

class ExecutionDeployer(AsyncAgentProtocol):
    """
    [PEP: Policy Enforcement Point] 
    DB 접속 권한이나 마스터 키가 없습니다. Validator의 서명을 검증한 후 쿼리를 실행합니다.
    """
    def __init__(self):
        super().__init__(agent_name="agent.deploy")
        
        # [Zero-Trust] 오직 서명 검증을 위한 Public Key만 환경 변수에서 로드합니다.
        # Private Key 통신 등 백엔드 접근 기능은 모두 제거되었습니다. (Connector 위임)
        pub_key_hex = os.environ.get("DPHI_VALIDATOR_PUBLIC_KEY")
        if not pub_key_hex:
            self.log.error("⚠️ DPHI_VALIDATOR_PUBLIC_KEY is missing. Execution will fail.")
        else:
            self.validator_pub_key = ed25519.Ed25519PublicKey.from_public_bytes(bytes.fromhex(pub_key_hex))
            
        # 비동기 Elicitation(프롬프트) 및 RPC Delegation(대행) 응답을 대기하기 위한 Future 레지스트리
        self.pending_prompts: Dict[str, asyncio.Future] = {}

    async def _route_request_async(self, req: Dict[str, Any]):
        """
        부모 클래스의 라우터를 오버라이드하여, Gateway로부터 반환된 
        Multiplex 제어 메시지(RESUME) 및 순수 JSON-RPC Response를 가로채어 처리합니다.
        """
        # 1. Multiplex Intent 제어 메시지 (RESUME) 언래핑 처리
        if req.get("action") == "RESUME":
            payload = req.get("payload", {})
            prompt_id = payload.get("id")
            
            # ID Mismatch 해결: 원래 ID(예: 777)와 Elicitation ID(prompt_777) 양방향 호환 검사
            target_id = prompt_id if prompt_id in self.pending_prompts else f"prompt_{prompt_id}"
            
            if target_id in self.pending_prompts:
                future = self.pending_prompts.pop(target_id)
                if not future.done():
                    future.set_result(payload)  # 언래핑된 순수 MCP 응답을 Future로 전달
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
                # 롤백 시 id가 None으로 올 수 있으므로 경고만 남기고 무시
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
            self.log.warning(f"⚠️ DESTRUCTIVE PAYLOAD. Halting execution and delegating validation.")
            
            canonical_payload = json.dumps({"service": service, "env": env, "sql": sql}, sort_keys=True).encode()
            payload_hash = hashlib.sha256(canonical_payload).digest()

            # 비동기 Await를 통해 트랜잭션을 안전하게 정지(Park)하고 OTP 요청
            otp_code = await self._request_user_otp_async(req_id, service)
            if not otp_code:
                return await self.send_error(req_id, -32000, "OTP Input Cancelled or Timed Out.")

            # [백엔드 통신 개선] Validator RPC 직접 호출 제거 및 Connector로 대행(Delegation) 요청
            delegate_req_id = f"delegate_{req_id}"
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self.pending_prompts[delegate_req_id] = future
            
            # STDOUT을 통해 Connector에게 백엔드 RPC 호출을 위임 (Zero-Trust 격리 보장)
            await self.send_request(
                req_id=delegate_req_id,
                method="rpc_delegate",
                params={
                    "target_method": "validator.attest",  # 백엔드 핸들러 레지스트리에 등록된 타겟 메서드명
                    "data": {
                        "user_id": user_id,
                        "otp_code": otp_code,
                        "payload_hash": payload_hash.hex()
                    }
                }
            )
            self.log.info("[Deploy] Waiting for Validator attestation via Connector delegation...")

            try:
                # Connector가 서명을 받아 STDIN으로 다시 꽂아줄 때까지 20초간 대기
                delegate_res = await asyncio.wait_for(future, timeout=20.0)
                
                # 실패(에러) 응답 처리
                if "error" in delegate_res:
                    err_info = delegate_res["error"]
                    return await self.send_error(req_id, err_info.get("code", -502), err_info.get("message"))
                    
                # 성공 응답 추출
                rpc_response = delegate_res.get("result", {})
                signature_hex = rpc_response.get("signature")
                
                if not signature_hex:
                    return await self.send_error(req_id, -502, "Validator returned empty signature.")
                    
            except asyncio.TimeoutError:
                self.pending_prompts.pop(delegate_req_id, None)
                return await self.send_error(req_id, -504, "Validator RPC delegation timed out.")
            except Exception as e:
                return await self.send_error(req_id, -500, f"Delegation processing failed: {str(e)}")

            # 로컬에서 Public Key로 서명 검증 (무결성 보장)
            try:
                self.validator_pub_key.verify(bytes.fromhex(signature_hex), payload_hash)
                self.log.info("🛡️ Cryptographic Attestation Verified. Authorization Granted.")
            except InvalidSignature:
                self.log.critical("🚨 SECURITY BREACH: Invalid Attestation Signature!")
                return await self.send_error(req_id, -32000, "Invalid Cryptographic Signature.")

        self.log.info(f"✅ Executing migration for {service}")
        await self.send_response(req_id, {"content": [{"type": "text", "text": "Migration executed successfully."}], "isError": False})

    async def _request_user_otp_async(self, parent_req_id: Any, service: str) -> str:
        prompt_req_id = f"prompt_{parent_req_id}"
        
        # 1. 응답을 대기할 Future 객체 생성 및 레지스트리 등록
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_prompts[prompt_req_id] = future

        # 2. Gateway로 Elicitation (YIELD 트리거) 요청 전송
        await self.send_request(
            req_id=prompt_req_id,
            method="elicitation/createMessage",
            params={"message": f"DANGER: Destructive migration on '{service}'. Enter TOTP token:"}
        )
        self.log.info(f"[BLOCKED] Waiting for TOTP input (Suspended execution context)...")
        
        # 3. 메인 루프를 블로킹하지 않고 비동기로 대기
        try:
            # 타임아웃을 넉넉하게 주어 외부 Gateway나 사용자의 응답 대기 시간을 보장
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