# bound.watcher.server.gatekeeper
## @lineage: xphi.proxy.pypi.gatekeeper
# xphi/proxy/pypi/gatekeeper.py
import sys
import base64
import json
import time
import asyncio
from bound.adapter.bridge.ledger import LedgerBridge
from watcher.plane.emitter import get_emitter

log = get_emitter("pypi.gatekeeper")

# 전역 상태 (Server에서 MCP를 통해 업데이트 가능)
bridge = LedgerBridge()
quarantine_db = {}  
auth_policy = {"strict_mode": True, "expected_token": "temp_oidc_token_12345"}

def emit_ledger_event(event_type: str, severity: str, details: dict):
    """Emits raw telemetry for operations that bypass or precede the Ledger."""
    event = {
        "timestamp": time.time(),
        "plane": "anchor.membrane",
        "type": event_type,
        "severity": severity,
        "payload": details
    }
    log.info(json.dumps(event), file=sys.stderr)


class GatekeeperRejection(Exception):
    """통제 로직(Gatekeeper)에서 발생한 차단 이벤트를 처리하기 위한 예외 클래스"""
    def __init__(self, http_code: int, message: str):
        self.http_code = http_code
        self.message = message


class TrafficGatekeeper:
    """트래픽 통제 및 보안 검증을 전담하는 클래스 (1차 방어선)"""
    def __init__(self, bridge_instance, policy, q_db):
        self.bridge = bridge_instance
        self.auth_policy = policy
        self.quarantine_db = q_db

    async def evaluate_request(self, path: str, headers: dict, client_ip: str, package_name: str):
        """요청에 대한 모든 보안 게이트를 순차적으로 검증합니다."""
        await self._check_uri_length(path, client_ip)
        self._check_authentication(headers, client_ip, path)
        if package_name:
            await self._check_ledger_authorization(package_name, client_ip, path)

    async def _check_uri_length(self, path: str, client_ip: str):
        """[Gatekeep 1] HTTP URI length limits"""
        if len(path) > 2048:
            await self.bridge.authorize(
                action_id="proxy.uri_violation",
                payload={"ip": client_ip, "path_length": len(path)},
                metadata={"severity": "CRITICAL", "type": "URI_TOO_LONG"}
            )
            raise GatekeeperRejection(414, "URI Too Long")

    def _check_authentication(self, headers: dict, client_ip: str, path: str):
        """[Gatekeep 2] Zero Trust Authentication"""
        if self.auth_policy.get("strict_mode"):
            auth_header = headers.get('Authorization')
            expected_auth = b"Basic " + base64.b64encode(b"__token__:" + self.auth_policy["expected_token"].encode())
            if not auth_header or auth_header.encode() != expected_auth:
                emit_ledger_event("AUTH_FAILED", "WARNING", {"ip": client_ip, "path": path})
                raise GatekeeperRejection(401, "Brane Secure Boundary: Authentication Failed")

    async def _check_ledger_authorization(self, package_name: str, client_ip: str, path: str):
        """[Gatekeep 3] Dynamic Kernel Ledger Authorization"""
        policy = self.quarantine_db.get(package_name)
        is_authorized = await self.bridge.authorize(
            action_id=f"proxy.fetch.{package_name}",
            payload={"target": package_name, "ip": client_ip, "path": path, "threat_intel": policy},
            metadata={"severity": "INFO", "type": "PACKAGE_REQUEST"}
        )

        if not is_authorized:
            log.warning(f"[Proxy] Blocked by LedgerBridge: {package_name} failed topological tension evaluation.")
            raise GatekeeperRejection(403, "Brane Security: Blocked by Topological Ledger")

# 싱글톤 인스턴스 내보내기
gatekeeper = TrafficGatekeeper(bridge, auth_policy, quarantine_db)