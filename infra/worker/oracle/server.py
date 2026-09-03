# fiber.infra.worker.oracle.server
import sys
import json
import logging
import traceback
from typing import Dict, Any, List

from fiber.infra.observer.oracle.receptor import OracleReceptor
from xphi.watcher.plane.emitter import get_emitter

logging.basicConfig(
    stream=sys.stderr, 
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] oracle.server - %(message)s"
)
log = logging.getLogger("oracle.server")

# =====================================================================
# 2. The MCP Server (A2A Node Wrapper)
# =====================================================================
class OracleMcpServer:
    def __init__(self):
        log.info("Initializing Deterministic Oracle Receptor...")
        try:
            # 외부 API 페치, 교차 검증, 서명(Sealing)을 담당하는 코어 모듈
            self.receptor = OracleReceptor()
            log.info("Oracle Receptor successfully mounted. Ready for A2A Intents.")
        except Exception as e:
            log.critical(f"Failed to mount Oracle Receptor: {e}", exc_info=True)
            sys.exit(1)

    def serve_forever(self):
        """커넥터(Sidecar)로부터 전달되는 JSON-RPC 인텐트를 무한 루프(Event Loop)로 대기합니다."""
        log.info("Listening for JSON-RPC payloads on stdin...")
        
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            try:
                request = json.loads(line)
                self._handle_request(request)
            except json.JSONDecodeError:
                log.error(f"Malformed JSON payload received: {line}")
                self._send_error(req_id=None, code=-32700, message="Parse error: Invalid JSON")
            except Exception as e:
                log.error(f"Unhandled server fracture: {e}", exc_info=True)
                self._send_error(req_id=request.get("id") if 'request' in locals() else None, 
                                 code=-32603, message="Internal Server Error")

    def _handle_request(self, request: Dict[str, Any]):
        """JSON-RPC 메서드 라우터"""
        req_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})

        log.info(f"Received Intent: {method} (ID: {req_id})")

        # MCP 2026-07-28 Spec: tools/list
        if method == "tools/list":
            self._send_response(req_id, {
                "tools": [{
                    "name": "fetch_aggregated_kline",
                    "description": "Fetch and cryptographically seal multi-exchange (Binance, Coinbase) Kline data.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "symbol": {"type": "string", "description": "Trading pair (e.g., BTCUSDT)"},
                            "strategy": {"type": "string", "description": "Aggregation strategy (mean, median)", "default": "mean"}
                        },
                        "required": ["symbol"]
                    }
                }]
            })

        # MCP 2026-07-28 Spec: tools/call
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name == "fetch_aggregated_kline":
                self._execute_kline_fetch(req_id, arguments)
            else:
                self._send_error(req_id, code=-32601, message=f"Method not found: Unknown tool '{tool_name}'")
                
        else:
            self._send_error(req_id, code=-32601, message=f"Method not found: {method}")

    def _execute_kline_fetch(self, req_id: Any, arguments: Dict[str, Any]):
        """오라클 비즈니스 로직 실행 및 응답 (블로킹 I/O 포함)"""
        symbol = arguments.get("symbol", "BTCUSDT")
        strategy = arguments.get("strategy", "mean")
        
        # 기본 타겟 ARN (바이낸스와 코인베이스의 결정론적 어댑터)
        target_arns = [
            "arn:bound:oracle:binance:kline:v1.0.0",
            "arn:bound:oracle:coinbase:kline:v1.0.0"
        ]

        try:
            log.info(f"Executing Oracle Policy -> Symbol: {symbol}, Strategy: {strategy}")
            
            # [블로킹 I/O 발생 지점] 
            # 외부망 통신이 지연되더라도, 이 프로세스만 블로킹될 뿐 DPHI 코어망(Ledger)은 전혀 영향을 받지 않음
            result = self.receptor.fetch_and_seal(symbol=symbol, target_arns=target_arns, strategy=strategy)
            
            # 성공 응답을 Connector에게 반환 (이 데이터가 LogicStream으로 원장에 RESOLVED 씰링됨)
            self._send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(result)}],
                "isError": False
            })
            
        except Exception as e:
            log.error(f"Oracle Execution Failed for {symbol}: {str(e)}", exc_info=True)
            # MCP 스펙상 도구 실행 실패는 isError=True 로 반환하거나 JSON-RPC 레벨 에러로 반환 가능
            # Connector가 FAULTED 상태 전이를 명확히 인지하게끔 명시적 JSON-RPC 에러 객체 생성
            self._send_error(req_id, code=-32000, message=f"Oracle Tool Execution Failed: {str(e)}")

    # =====================================================================
    # 3. IPC Communicators (Stdout Writers)
    # =====================================================================
    def _send_response(self, req_id: Any, result: Dict[str, Any]):
        """Connector로 성공 결과 전송. flush()를 통해 파이프라인 버퍼링으로 인한 교착 상태 방지."""
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": result
        }
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush() 

    def _send_error(self, req_id: Any, code: int, message: str):
        """Connector로 에러 결과 전송."""
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {
                "code": code,
                "message": message
            }
        }
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()

# =====================================================================
# 4. Entrypoint
# =====================================================================
def main():
    server = OracleMcpServer()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()