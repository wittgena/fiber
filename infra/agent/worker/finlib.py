# fiber.infra.agent.worker.finlib
## @lineage: fiber.a2a.worker.finlib
## @lineage: fiber.infra.worker.agent.finlib
import sys
import json
import logging
import ast
import operator
import math
import hashlib
from typing import Dict, Any, List

# [핵심] STDOUT 오염 방지 및 JSON-RPC 표준 루프를 담당하는 베이스 프로토콜 임포트
from fiber.infra.agent.bridge.protocol import AgentProtocol

log = logging.getLogger("agent.finlib")

# =====================================================================
# [선택적 의존성 로딩 구조]
# 무거운 C-binding 패키지(talib, QuantLib)가 없어도 데몬이 실행되도록 처리
# =====================================================================
try:
    import numpy as np
    import talib
    HAS_TALIB = True
except ImportError:
    HAS_TALIB = False
    log.warning("Optional dependency 'talib' or 'numpy' not found. 'calc_indicators_batch' will use MOCK mode.")

try:
    import QuantLib as ql
    HAS_QL = True
except ImportError:
    HAS_QL = False
    log.warning("Optional dependency 'QuantLib' not found. 'resolve_dates' will use MOCK mode.")
# =====================================================================


class FinLib(AgentProtocol):
    def __init__(self):
        # [적용] 부모 클래스 초기화 - 자동으로 stdout이 격리(Barrier)되고 로깅 환경이 설정됩니다.
        super().__init__(agent_name="agent.finlib")
        
        self.tick_sizes = {"BTCUSD": 0.5, "ETHUSD": 0.01, "SOLUSD": 0.001}
        self.lot_sizes = {"BTCUSD": 0.001, "ETHUSD": 0.01, "SOLUSD": 0.1}
        
        # QuantLib 캘린더 엔진 캐싱
        self.target_calendar = ql.TARGET() if HAS_QL else None
        
        # 금융 공학 연산을 위한 AST 화이트리스트 (Injection 방어)
        self.allowed_math_funcs = {
            "log": math.log, 
            "exp": math.exp, 
            "sqrt": math.sqrt, 
            "pow": math.pow
        }

    # =====================================================================
    # AgentProtocol 추상 메서드 구현 (도구 목록 및 실행 라우팅)
    # =====================================================================
    def handle_tools_list(self, req_id: Any):
        """MCP 2026 규격의 tools/list 요청 처리"""
        tools = [
            {
                "name": "resolve_dates",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "base_date": {"type": "string", "description": "YYYY-MM-DD format"},
                        "offset_business_days": {"type": "integer"}
                    },
                    "required": ["base_date", "offset_business_days"]
                }
            },
            {
                "name": "normalize_order",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                        "raw_price": {"type": "number"},
                        "raw_amount": {"type": "number"}
                    },
                    "required": ["symbol", "raw_price", "raw_amount"]
                }
            },
            {
                "name": "calc_indicators_batch",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "prices_matrix": {
                            "type": "array", 
                            "items": {"type": "array", "items": {"type": "number"}},
                            "description": "List of price arrays for batch processing"
                        },
                        "period": {"type": "integer"}
                    },
                    "required": ["prices_matrix", "period"]
                }
            },
            {
                "name": "eval_math",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "expression": {
                            "type": "string",
                            "description": "Supported ops: +, -, *, /, log, exp, sqrt, pow"
                        }
                    },
                    "required": ["expression"]
                }
            },
            {
                "name": "generate_fingerprint",
                "inputSchema": {
                    "type": "object",
                    "properties": {"payload": {"type": "object"}},
                    "required": ["payload"]
                }
            }
        ]
        self.send_response(req_id, {"tools": tools})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        """
        MCP 2026 규격의 tools/call 요청 처리
        코어에서 주입한 _meta와 레거시가 필요한 arguments가 분리되어 전달됩니다.
        """
        try:
            if tool_name == "resolve_dates":
                res = self._tool_resolve_dates(arguments)
            elif tool_name == "normalize_order":
                res = self._tool_normalize_order(arguments)
            elif tool_name == "calc_indicators_batch":
                res = self._tool_calc_indicators_batch(arguments)
            elif tool_name == "eval_math":
                res = self._tool_eval_math(arguments)
            elif tool_name == "generate_fingerprint":
                res = self._tool_generate_fingerprint(arguments)
            else:
                self.send_error(req_id, -32601, f"Unknown tool: {tool_name}")
                return
            
            # 정상 결과 전송 (AgentProtocol의 메서드 활용)
            self.send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(res)}], 
                "isError": False
            })
            
        except (ValueError, TypeError, KeyError) as e:
            self.log.warning(f"Invalid Parameters for {tool_name}: {e}")
            # [고의적 에러 테스트 라우팅] (Phase 5: Precise Error Routing 연동)
            self.send_error(req_id, -32602, f"Invalid params: {str(e)}")

    # =====================================================================
    # 순수 비즈니스 로직 (도구 구현체들)
    # =====================================================================
    def _tool_resolve_dates(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """QuantLib C++ 엔진을 활용한 초고속 휴일/영업일 계산 (또는 Mock)"""
        if not HAS_QL:
            return {"resolved_date": "2026-09-10", "mode": "mocked"}

        try:
            base_date = ql.DateParser.parseFormatted(args["base_date"], "%Y-%m-%d")
        except Exception:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got '{args.get('base_date')}'")

        advanced_date = self.target_calendar.advance(
            base_date, 
            int(args.get("offset_business_days", 0)), 
            ql.Days
        )
        return {"resolved_date": f"{advanced_date.year()}-{advanced_date.month():02d}-{advanced_date.dayOfMonth():02d}"}

    def _tool_normalize_order(self, args: Dict[str, Any]) -> Dict[str, Any]:
        sym = str(args["symbol"]).upper()
        tick = self.tick_sizes.get(sym, 0.01)
        lot = self.lot_sizes.get(sym, 0.001)
        
        norm_p = round(round(float(args["raw_price"]) / tick) * tick, 8)
        norm_a = round(round(float(args["raw_amount"]) / lot) * lot, 8)
        
        return {"symbol": sym, "normalized_price": norm_p, "normalized_amount": norm_a}

    def _tool_calc_indicators_batch(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """다차원 배열 처리를 통한 O(1) IPC 통신 달성 (또는 Mock)"""
        prices_matrix = args.get("prices_matrix", [])
        period = int(args.get("period", 14))

        if not isinstance(prices_matrix, list):
            raise ValueError("prices_matrix must be a list of arrays")
        
        if not HAS_TALIB:
            return {"indicator": "SMA", "values": [999.99] * len(prices_matrix), "mode": "mocked"}
        
        results: List[Any] = []
        for prices in prices_matrix:
            arr = np.array(prices, dtype=np.float64)
            if len(arr) < period:
                results.append(None)
                continue
            
            sma = talib.SMA(arr, timeperiod=period)
            results.append(round(float(sma[-1]), 8))
            
        return {"indicator": "SMA", "values": results}

    def _tool_eval_math(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """AST 화이트리스트 기반 안전한 수학 표현식 실행"""
        expr = args.get("expression", "")
        
        def _eval(node):
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)): 
                return node.value
            elif isinstance(node, ast.Num): 
                return node.n
            elif isinstance(node, ast.BinOp):
                ops = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv}
                return ops[type(node.op)](_eval(node.left), _eval(node.right))
            elif isinstance(node, ast.Call):
                if hasattr(node.func, 'id'):
                    func_name = node.func.id
                    if func_name in self.allowed_math_funcs:
                        eval_args = [_eval(a) for a in node.args]
                        return self.allowed_math_funcs[func_name](*eval_args)
                raise ValueError(f"Unsupported or dangerous math function call.")
                
            raise ValueError("Illegal AST node detected.")

        try:
            res = _eval(ast.parse(expr, mode='eval').body)
            return {"result": float(res)}
        except SyntaxError:
            raise ValueError(f"Syntax error in expression: {expr}")

    def _tool_generate_fingerprint(self, args: Dict[str, Any]) -> Dict[str, Any]:
        payload = args.get("payload", {})
        canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        fingerprint = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
        return {"fingerprint": fingerprint, "status": "DETERMINISTIC"}


def main():
    server = FinLib()
    try:
        # [적용] 부모 클래스(AgentProtocol)의 강력한 루프 실행
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("FinLib Oracle Terminated by Interrupt.")

if __name__ == "__main__":
    main()