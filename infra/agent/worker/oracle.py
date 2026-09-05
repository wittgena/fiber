# fiber.infra.agent.worker.oracle
## @lineage: fiber.a2a.worker.oracle
## @lineage: fiber.infra.worker.agent.oracle
import sys
import json
import time
from typing import Dict, Any

from fiber.infra.agent.observer.oracle.receptor import OracleReceptor

# [핵심] STDOUT 오염 방지 및 JSON-RPC 표준 루프를 담당하는 베이스 프로토콜 임포트
from fiber.infra.agent.bridge.protocol import AgentProtocol

class OracleMcpServer(AgentProtocol):
    def __init__(self):
        # [적용] 부모 클래스 초기화 - 자동으로 stdout 격리 및 로깅 설정이 적용됩니다.
        super().__init__(agent_name="agent.oracle")
        
        self.log.info("Initializing Deterministic Oracle Receptor...")
        try:
            # 외부 API 페치, 교차 검증, 서명(Sealing)을 담당하는 코어 모듈
            self.receptor = OracleReceptor()
            self.log.info("Oracle Receptor successfully mounted. Ready for A2A Intents.")
        except Exception as e:
            self.log.critical(f"Failed to mount Oracle Receptor: {e}", exc_info=True)
            sys.exit(1)

        # 코어 엔진이 씰링(Sealed)한 최종 증명 객체 자체를 캐싱하는 메모리
        # 동일한 1초 내에 쏟아지는 수백 개의 에이전트 요청을 단 1회의 외부 API 호출로 방어합니다.
        self._sealed_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 1.0  # 1.0초의 국지적 합의 윈도우 (Local Consensus Window)

    # [삭제됨] 지저분했던 serve_forever, _handle_request 메서드는 모두 부모 클래스(AgentProtocol)로 이관되었습니다.

    # =====================================================================
    # AgentProtocol 추상 메서드 구현
    # =====================================================================
    def handle_tools_list(self, req_id: Any):
        """MCP 2026 규격의 tools/list 요청 처리"""
        tools = [{
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
        self.send_response(req_id, {"tools": tools})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        """
        MCP 2026 규격의 tools/call 요청 처리
        """
        if tool_name == "fetch_aggregated_kline":
            self._execute_kline_fetch(req_id, arguments)
        else:
            self.send_error(req_id, code=-32601, message=f"Method not found: Unknown tool '{tool_name}'")

    # =====================================================================
    # 순수 비즈니스 로직 (Oracle Sealing & Caching)
    # =====================================================================
    def _execute_kline_fetch(self, req_id: Any, arguments: Dict[str, Any]):
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
                self.log.debug(f"[Oracle] Cache Hit for {cache_key}. Reusing attestation payload.")
            else:
                # 2. 캐시 미스 시에만 하부 코어 망을 통해 외부 데이터 페치 및 암호학적 씰링(Sealing) 수행
                self.log.info(f"Executing Oracle Policy -> Symbol: {symbol}, Strategy: {strategy}")
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
            self.send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(sealed_payload)}],
                "isError": False
            })
            
        except Exception as e:
            self.log.error(f"Oracle Execution Failed for {symbol}: {str(e)}", exc_info=True)
            self.send_error(req_id, code=-32000, message=f"Oracle Tool Execution Failed: {str(e)}")


def main():
    server = OracleMcpServer()
    try:
        # [적용] 베이스 클래스(AgentProtocol)의 견고한 I/O 무한 루프 실행
        server.serve_forever()
    except KeyboardInterrupt:
        server.log.info("Server shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()