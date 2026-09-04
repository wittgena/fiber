# fiber.infra.worker.agent.finlib
import sys
import json
import logging
import ast
import operator
import math
import hashlib
import numpy as np
import talib
import QuantLib as ql
from typing import Dict, Any, List

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [finlib] %(message)s"
)
log = logging.getLogger("agent.finlib")

class FinLib:
    """
    [Deterministic Compute Oracle]
    A2A 생태계에서 순수 수학 및 금융 공학 연산을 담당하는 무상태(Stateless) 데몬입니다.
    과금(Fuel/L402) 및 상태 합의(Consensus)는 모두 상위 코어망(dphi.broker)에 위임하며,
    오직 C/C++ 바인딩(TA-Lib, QuantLib)의 연산 오버헤드를 제로(0)로 만드는 것에 집중합니다.
    """
    def __init__(self):
        # [정렬 포인트 1] 물리적 메모리 초기화 1회 한정 (Init Tax 제거)
        self.tick_sizes = {"BTCUSD": 0.5, "ETHUSD": 0.01, "SOLUSD": 0.001}
        self.lot_sizes = {"BTCUSD": 0.001, "ETHUSD": 0.01, "SOLUSD": 0.1}
        
        # QuantLib 캘린더 엔진 캐싱 (호출 시마다 재생성되는 병목 제거)
        self.target_calendar = ql.TARGET() 
        
        # 금융 공학 연산을 위한 AST 화이트리스트 (Injection 방어)
        self.allowed_math_funcs = {
            "log": math.log, 
            "exp": math.exp, 
            "sqrt": math.sqrt, 
            "pow": math.pow
        }

    def serve_forever(self):
        """다중화(Multiplexing)를 지원하는 표준 입출력 이벤트 루프"""
        log.info("Unified FinStdLib Oracle Ignited (Daemon Mode). Listening on stdin.")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                self._dispatch(json.loads(line))
            except json.JSONDecodeError:
                self._send_error(None, -32700, "Parse error: Invalid JSON payload")
            except Exception as e:
                log.error(f"Daemon Loop Fracture: {e}", exc_info=True)
                self._send_error(None, -32603, "Internal Server Error")

    def _dispatch(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            self._send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
        elif method == "tools/list":
            self._handle_tools_list(req_id)
        elif method == "tools/call":
            self._handle_tools_call(req_id, params)
        else:
            self._send_error(req_id, -32601, f"Unknown method: {method}")

    def _handle_tools_list(self, req_id: Any):
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
                # 다차원 배열(Matrix) 지원을 통한 IPC 직렬화/역직렬화 비용 상각(Amortization)
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
        self._send_response(req_id, {"tools": tools})

    def _handle_tools_call(self, req_id: Any, params: Dict[str, Any]):
        tool_name = params.get("name")
        args = params.get("arguments", {})
        
        try:
            if tool_name == "resolve_dates":
                res = self._tool_resolve_dates(args)
            elif tool_name == "normalize_order":
                res = self._tool_normalize_order(args)
            elif tool_name == "calc_indicators_batch":
                res = self._tool_calc_indicators_batch(args)
            elif tool_name == "eval_math":
                res = self._tool_eval_math(args)
            elif tool_name == "generate_fingerprint":
                res = self._tool_generate_fingerprint(args)
            else:
                self._send_error(req_id, -32601, f"Unknown tool: {tool_name}")
                return
            
            # [정렬 포인트 2] MCP Spec 준수. 순수 결과값만을 반환.
            # 이 결과물을 바탕으로 영수증을 발행하고 서명하는 것은 상위 레이어의 몫.
            self._send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(res)}], 
                "isError": False
            })
            
        except (ValueError, TypeError, KeyError) as e:
            # 사용자/LLM 파라미터 오류: -32602 (Invalid Params) 매핑 -> 상위 에이전트 자가 복구 유도
            log.warning(f"Invalid Parameters for {tool_name}: {e}")
            self._send_error(req_id, -32602, f"Invalid params: {str(e)}")
        except Exception as e:
            # C-엔진 충돌 및 메모리 에러: -32000 (Execution Fault) 매핑 -> Sentinel 강제 롤백 대상
            log.error(f"Execution Fault in {tool_name}: {e}", exc_info=True)
            self._send_error(req_id, -32000, f"Execution Fault: {str(e)}")

    def _tool_resolve_dates(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """QuantLib C++ 엔진을 활용한 초고속 휴일/영업일 계산"""
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
        """다차원 배열 처리를 통한 O(1) IPC 통신 달성"""
        prices_matrix = args.get("prices_matrix", [])
        period = int(args.get("period", 14))
        
        results: List[Any] = []
        for prices in prices_matrix:
            arr = np.array(prices, dtype=np.float64)
            if len(arr) < period:
                results.append(None)
                continue
            
            sma = talib.SMA(arr, timeperiod=period)
            # numpy float64를 순수 Python float로 캐스팅하여 JSON 직렬화 에러 방지
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

    # =====================================================================
    # IPC Communicators (Stdout Writers)
    # =====================================================================
    def _send_response(self, req_id: Any, result: Dict[str, Any]):
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\n")
        sys.stdout.flush()

    def _send_error(self, req_id: Any, code: int, message: str):
        sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}) + "\n")
        sys.stdout.flush()


def main():
    server = FinLib()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("FinLib Oracle Terminated by Interrupt.")

if __name__ == "__main__":
    main()