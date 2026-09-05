# fiber.infra.worker.protocol
import sys
import json
import logging
from typing import Dict, Any, Optional

"""Module-Level I/O Hijacking (STDOUT 원천 봉쇄)"""
_REAL_STDOUT = sys.stdout
sys.stdout = sys.stderr

class AgentProtocol:
    """@desc: Fiber 네트워크와 레거시 프로세스 간의 IPC 통신을 규격화하는 베이스 클래스 - JSON-RPC 2.0 규격"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        
        logging.basicConfig(
            stream=sys.stderr, 
            level=logging.INFO, 
            format=f"%(asctime)s [%(levelname)s] [{self.agent_name}] %(message)s"
        )
        self.log = logging.getLogger(self.agent_name)

    """Single Point of Egress"""
    def _emit_rpc_message(self, message: Dict[str, Any]):
        """모든 메시지에 JSON-RPC 2.0 봉투를 씌우고 안전하게 직렬화하여 배출합니다."""
        # 1. 스펙 강제화: 어떤 메시지든 jsonrpc 버전은 2.0으로 고정
        message["jsonrpc"] = "2.0"
        
        try:
            raw_out = json.dumps(message) + "\n"
            _REAL_STDOUT.write(raw_out)
            _REAL_STDOUT.flush()
        except TypeError as e:
            # 에이전트가 JSON으로 변환할 수 없는 객체를 넘긴 경우 (직렬화 실패 방어)
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message: # 에러 루프 방지
                self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    """표준 JSON-RPC Message Builders"""
    def send_response(self, req_id: Any, result: Any):
        """정상 결과 응답 (Result Response)"""
        self._emit_rpc_message({"id": req_id, "result": result})

    def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        """에러 응답 (Error Response)"""
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data  # JSON-RPC 2.0 표준 부가 데이터 필드
        self._emit_rpc_message({"id": req_id, "error": err_obj})

    def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        """서버 주도형 요청 발송 (Request/Notification) - Elicitation/YIELD 등"""
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self._emit_rpc_message(msg)

    def serve_forever(self):
        """무한 입력 대기 루프"""
        self.log.info(f"Agent '{self.agent_name}' Ignited. Listening on stdin...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                self._route_request(payload)
            except json.JSONDecodeError:
                self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Internal Fracture: {e}", exc_info=True)
                self.send_error(payload.get("id") if isinstance(payload, dict) else None, -32603, "Internal Server Error")

    def _route_request(self, req: Dict[str, Any]):
        """JSON-RPC 스펙 라우팅"""
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
        elif method == "tools/list":
            self.handle_tools_list(req_id)
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            meta = params.get("_meta", {})
            
            try:
                self.handle_tools_call(req_id, tool_name, arguments, meta)
            except Exception as e:
                self.log.error(f"Execution Fault in '{tool_name}': {e}", exc_info=True)
                self.send_error(req_id, -32000, str(e))
        else:
            self.send_error(req_id, -32601, f"Unknown method: {method}")

    """Abstract Handlers"""
    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": []})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")