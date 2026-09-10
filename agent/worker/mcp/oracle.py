# fiber.agent.worker.mcp.oracle
## @lineage: fiber.dphi.worker.oracle
import sys
import json
import time
import asyncio
from typing import Dict, Any

from xphi.bound.oracle.receptor import OracleReceptor
from xphi.arch.contract.protocol.agent import AsyncAgentProtocol

class OracleMcpServer(AsyncAgentProtocol):
    def __init__(self):
        super().__init__(agent_name="agent.oracle")
        
        self.log.info("Initializing Async Deterministic Oracle Receptor...")
        try:
            self.receptor = OracleReceptor()
            self.log.info("Async Oracle Receptor successfully mounted. Ready for A2A Intents.")
        except Exception as e:
            self.log.critical(f"Failed to mount Async Oracle Receptor: {e}", exc_info=True)
            sys.exit(1)

        self._sealed_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 1.0  # 1.0초의 국지적 합의 윈도우 (Local Consensus Window)

    async def handle_tools_list(self, req_id: Any):
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
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "fetch_aggregated_kline":
            # 이벤트 루프 안에서 코루틴으로 비동기 실행됨
            await self._execute_kline_fetch(req_id, arguments)
        else:
            await self.send_error(req_id, code=-32601, message=f"Method not found: Unknown tool '{tool_name}'")

    async def _execute_kline_fetch(self, req_id: Any, arguments: Dict[str, Any]):
        symbol = arguments.get("symbol", "BTCUSDT")
        strategy = arguments.get("strategy", "mean")
        
        cache_key = f"{symbol}_{strategy}"
        now = time.time()

        try:
            cached_item = self._sealed_cache.get(cache_key)
            if cached_item and (now - cached_item['ts']) < self._cache_ttl:
                sealed_payload = cached_item['payload']
                self.log.debug(f"[Oracle] Cache Hit for {cache_key}. Reusing attestation payload.")
            else:
                self.log.info(f"Executing Async Oracle Policy -> Symbol: {symbol}, Strategy: {strategy}")
                target_arns = [
                    "arn:bound:oracle:binance:kline:v1.0.0",
                    "arn:bound:oracle:coinbase:kline:v1.0.0"
                ]
                sealed_payload = await self.receptor.fetch_and_seal(
                    symbol=symbol, target_arns=target_arns, strategy=strategy
                )
                self._sealed_cache[cache_key] = {'payload': sealed_payload, 'ts': time.time()}

            # 에이전트는 어떠한 가짜 영수증도 만들지 않고, 순수하게 증명된 데이터만 반환
            await self.send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(sealed_payload)}],
                "isError": False
            })
            
        except Exception as e:
            self.log.error(f"Async Oracle Execution Failed for {symbol}: {str(e)}", exc_info=True)
            await self.send_error(req_id, code=-32000, message=f"Oracle Tool Execution Failed: {str(e)}")

def main():
    server = OracleMcpServer()
    try:
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        server.log.info("Server shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()