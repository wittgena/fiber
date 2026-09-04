# fiber.infra.worker.agent.deploy
import sys
import json
import duckdb
import logging
import time
import hmac
import hashlib
import base64
import struct
from typing import Dict, Any

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [agent.deploy] %(message)s"
)
log = logging.getLogger("agent.deploy")

class TotpValidator:
    def __init__(self, base32_secret: str):
        self.secret = base32_secret

    def _get_totp_token(self, intervals_no: int) -> str:
        key = base64.b32decode(self.secret, True)
        msg = struct.pack(">Q", intervals_no)
        h = hmac.new(key, msg, hashlib.sha1).digest()
        o = h[19] & 15
        h = (struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000
        return f"{h:06d}"

    def verify(self, token: str, window: int = 1) -> bool:
        """현재 시간을 기준으로 앞뒤 1개의 윈도우(±30초)까지 허용하여 검증"""
        intervals = int(time.time()) // 30
        for i in range(-window, window + 1):
            if self._get_totp_token(intervals + i) == str(token).zfill(6):
                return True
        return False


class LegacyDeployer:
    def __init__(self):
        self.dba_totp = TotpValidator("JBSWY3DPEHPK3PXP") 
        
        self.conn = duckdb.connect("deploy_audit.duckdb")
        self._init_db()
        log.info("Enterprise Legacy Deploy Server Ignite. (DB: deploy_audit.duckdb)")

    def _init_db(self):
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_deploy_tx_id")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS deployment_logs (
                tx_id INTEGER DEFAULT nextval('seq_deploy_tx_id') PRIMARY KEY,
                service_name VARCHAR,
                target_env VARCHAR,
                status VARCHAR,
                otp_prompt_id VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def serve_forever(self):
        log.info("Listening for JSON-RPC payloads on stdin...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                self._dispatch(payload)
            except json.JSONDecodeError:
                self._send_error(None, -32700, "Parse error")
            except Exception as e:
                log.error(f"Internal Fracture: {e}", exc_info=True)
                self._send_error(None, -32603, f"Internal Server Error")

    def _dispatch(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            self._send_response(req_id, {"protocolVersion": "2024-11-05", "capabilities": {}})
        elif method == "tools/list":
            self._send_response(req_id, {
                "tools": [{
                    "name": "execute_db_migration",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "service_name": {"type": "string"},
                            "target_env": {"type": "string", "enum": ["staging", "production"]},
                            "sql_script": {"type": "string"}
                        },
                        "required": ["service_name", "target_env", "sql_script"]
                    }
                }]
            })
        elif method == "tools/call" and params.get("name") == "execute_db_migration":
            self._handle_migration(req_id, params.get("arguments", {}))
        else:
            self._send_error(req_id, -32601, f"Unknown method/tool: {method}")

    def _handle_migration(self, req_id: Any, args: Dict[str, Any]):
        service = args.get("service_name")
        env = args.get("target_env")
        sql = args.get("sql_script", "").upper()

        log.info(f"Migration Request -> {service} [{env}]")

        is_destructive = "DROP" in sql or "TRUNCATE" in sql
        if is_destructive and env == "production":
            log.warning("⚠️ DESTRUCTIVE PAYLOAD DETECTED. Locking transaction & demanding OTP.")
            
            result = self.conn.execute(
                "INSERT INTO deployment_logs (service_name, target_env, status) VALUES (?, ?, ?) RETURNING tx_id",
                [service, env, "PENDING_OTP"]
            ).fetchone()
            tx_id = result[0]

            otp_approved = self._request_user_otp_blocking(req_id, service)
            if not otp_approved:
                self.conn.execute("UPDATE deployment_logs SET status = 'REJECTED_OTP' WHERE tx_id = ?", [tx_id])
                log.error(f"Transaction {tx_id} aborted due to invalid OTP.")
                self._send_error(req_id, -32000, "Security Violation: Invalid or Expired TOTP token.")
                return

            self.conn.execute("UPDATE deployment_logs SET status = 'DEPLOYED' WHERE tx_id = ?", [tx_id])

        log.info(f"✅ Transaction successful for {service}")
        self._send_response(req_id, {"content": [{"type": "text", "text": "Migration executed successfully."}], "isError": False})

    def _request_user_otp_blocking(self, parent_req_id: Any, service: str) -> bool:
        prompt_req_id = f"prompt_{parent_req_id}"
        elicitation_msg = {
            "jsonrpc": "2.0",
            "id": prompt_req_id,
            "method": "elicitation/createMessage",
            "params": {
                "message": f"DANGER: Destructive migration on '{service}'. Enter 6-digit TOTP token:"
            }
        }
        
        sys.stdout.write(json.dumps(elicitation_msg) + "\n")
        sys.stdout.flush()

        log.info(f"[BLOCKED] Waiting for TOTP input on stdin for prompt_id: {prompt_req_id}...")
        
        response_line = sys.stdin.readline()
        if not response_line:
            return False

        try:
            client_res = json.loads(response_line.strip())
            
            if client_res.get("id") != prompt_req_id:
                log.error("JSON-RPC ID mismatch in OTP response.")
                return False

            otp_code = client_res.get("result", {}).get("value", "")
            log.info(f"TOTP received. Verifying...")
            
            return self.dba_totp.verify(otp_code)
            
        except Exception as e:
            log.error(f"Failed to parse or verify TOTP: {e}")
            return False

    def _send_response(self, req_id: Any, result: Dict[str, Any]):
        resp = {"jsonrpc": "2.0", "id": req_id, "result": result}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()

    def _send_error(self, req_id: Any, code: int, message: str):
        resp = {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()

def main():
    server = LegacyDeployer()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server shutting down.")

if __name__ == "__main__":
    main()