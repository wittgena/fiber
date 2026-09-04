# fiber.infra.worker.agent.oracle
import sys
import json
import time
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
log = logging.getLogger("agent.oracle")

class OracleMcpServer:
    """
    [A2A Node Wrapper - Deterministic Oracle]
    외부 데이터를 페치하고 암호학적 서명을 수행하는 코어 모듈(Receptor)의 래퍼입니다.
    초고동시성 환경에서 외부 API 비용을 방어하기 위해 'Sealed Payload' 스마트 캐싱을 수행합니다.
    """
    def __init__(self):
        log.info("Initializing Deterministic Oracle Receptor...")
        try:
            # 외부 API 페치, 교차 검증, 서명(Sealing)을 담당하는 코어 모듈
            self.receptor = OracleReceptor()
            log.info("Oracle Receptor successfully mounted. Ready for A2A Intents.")
        except Exception as e:
            log.critical(f"Failed to mount Oracle Receptor: {e}", exc_info=True)
            sys.exit(1)

        # [정렬 됨] 코어 엔진이 씰링(Sealed)한 최종 증명 객체 자체를 캐싱하는 메모리
        # 동일한 1초 내에 쏟아지는 수백 개의 에이전트 요청을 단 1회의 외부 API 호출로 방어합니다.
        self._sealed_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 1.0  # 1.0초의 국지적 합의 윈도우 (Local Consensus Window)

    def serve_forever(self):
        """커넥터(Sidecar)로부터 전달되는 JSON-RPC 인텐트를 무한 루프(Event Loop)로 대기합니다."""
        log.info("Listening for JSON-RPC payloads on stdin (Daemon Mode)...")
        
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
                self._send_error(
                    req_id=request.get("id") if 'request' in locals() else None, 
                    code=-32603, 
                    message="Internal Server Error"
                )

    def _handle_request(self, request: Dict[str, Any]):
        """JSON-RPC 메서드 라우터"""
        req_id = request.get("id")
        method = request.get("method")
        params = request.get("params", {})

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
                
        elif method == "initialize":
            self._send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            
        else:
            self._send_error(req_id, code=-32601, message=f"Method not found: {method}")

    def _execute_kline_fetch(self, req_id: Any, arguments: Dict[str, Any]):
        """
        [핵심 정렬 로직] 오라클 비즈니스 로직 실행 및 응답
        외부 I/O를 최소화하면서도, 합의 가능한 완벽한 무결성 객체(Attestation)를 반환합니다.
        """
        symbol = arguments.get("symbol", "BTCUSDT")
        strategy = arguments.get("strategy", "mean")
        
        cache_key = f"{symbol}_{strategy}"
        now = time.time()

        try:
            # 1. 국지적 합의 윈도우(1초) 이내의 캐시 히트 (연산/네트워크 I/O 비용 Zero)
            # -> 이전에 Receptor가 서명까지 모두 완료한 '통짜' 데이터를 그대로 반환합니다.
            cached_item = self._sealed_cache.get(cache_key)
            if cached_item and (now - cached_item['ts']) < self._cache_ttl:
                sealed_payload = cached_item['payload']
                log.debug(f"[Oracle] Cache Hit for {cache_key}. Reusing attestation payload.")
            else:
                # 2. 캐시 미스 시에만 하부 코어 망을 통해 외부 데이터 페치 및 암호학적 씰링(Sealing) 수행
                log.info(f"Executing Oracle Policy -> Symbol: {symbol}, Strategy: {strategy}")
                target_arns = [
                    "arn:bound:oracle:binance:kline:v1.0.0",
                    "arn:bound:oracle:coinbase:kline:v1.0.0"
                ]
                
                # 블로킹 I/O 발생 지점
                sealed_payload = self.receptor.fetch_and_seal(symbol=symbol, target_arns=target_arns, strategy=strategy)
                
                # 서명된 결과를 메모리에 갱신
                self._sealed_cache[cache_key] = {'payload': sealed_payload, 'ts': now}

            # 3. 에이전트는 어떠한 가짜 영수증도 만들지 않고, 순수하게 증명된 데이터만 반환합니다.
            # 이 페이로드에 대한 과금(UTXO) 및 검증은 rpc.handler가 담당합니다.
            self._send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(sealed_payload)}],
                "isError": False
            })
            
        except Exception as e:
            log.error(f"Oracle Execution Failed for {symbol}: {str(e)}", exc_info=True)
            self._send_error(req_id, code=-32000, message=f"Oracle Tool Execution Failed: {str(e)}")

    # =====================================================================
    # IPC Communicators (Stdout Writers)
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


def main():
    server = OracleMcpServer()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()