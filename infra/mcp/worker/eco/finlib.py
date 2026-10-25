# fiber.infra.mcp.worker.eco.finlib
"""
[MCP Worker Authoring Rules (ToolBinderMixin)]
1. Method Naming & Signature:
   - Define exposed MCP tools with the `tool_<name>` prefix (e.g., `def tool_resolve_dates(self, base_date: str, offset_business_days: int)`).
   - Type hints (`str`, `int`, `float`, `bool`, `list`, `dict`, `Literal`, `Optional`) and default values automatically synthesize the JSON `inputSchema` and `required` list.
2. Docstring Convention (`\"\"\"`):
   - First line: Extracted as the tool-level `description`.
   - Subsequent lines prefixed with `- <param_name>: <desc>` (or `:param <param_name>: <desc>`): Extracted as individual parameter `description` fields.
3. Return & Error Handling:
   - Return native Python `dict`, `list`, or `str` directly; the base protocol wraps and serializes them into MCP `content` envelopes (`isError: False`).
   - Raise `ValueError` / `TypeError` / `KeyError` for invalid arguments (mapped to JSON-RPC `-32602`).
   - Raise `ToolError` for domain-level negative results (mapped to MCP `content` envelope with `isError: True`).
"""
import json
import logging
import ast
import operator
import math
import hashlib
from typing import Dict, Any, List

from xphi.arch.contract.protocol.worker import WorkerProtocol

log = logging.getLogger("eco.finlib")

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


class FinLib(WorkerProtocol):
    def __init__(self):
        self.tick_sizes = {"BTCUSD": 0.5, "ETHUSD": 0.01, "SOLUSD": 0.001}
        self.lot_sizes = {"BTCUSD": 0.001, "ETHUSD": 0.01, "SOLUSD": 0.1}
        self.target_calendar = ql.TARGET() if HAS_QL else None
        self.allowed_math_funcs = {
            "log": math.log,
            "exp": math.exp,
            "sqrt": math.sqrt,
            "pow": math.pow,
        }
        super().__init__(agent_name="eco.finlib")

    def tool_resolve_dates(self, base_date: str, offset_business_days: int) -> Dict[str, Any]:
        """Resolve target business date from base_date and business day offset.
        - base_date: YYYY-MM-DD format
        - offset_business_days: Business days to advance
        """
        if not HAS_QL:
            return {"resolved_date": "2026-09-10", "mode": "mocked"}

        try:
            parsed_date = ql.DateParser.parseFormatted(base_date, "%Y-%m-%d")
        except Exception:
            raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got '{base_date}'")

        advanced_date = self.target_calendar.advance(
            parsed_date,
            offset_business_days,
            ql.Days,
        )
        return {"resolved_date": f"{advanced_date.year()}-{advanced_date.month():02d}-{advanced_date.dayOfMonth():02d}"}

    def tool_normalize_order(self, symbol: str, raw_price: float, raw_amount: float) -> Dict[str, Any]:
        """Normalize raw order price and amount according to symbol tick and lot sizes."""
        sym = symbol.upper()
        tick = self.tick_sizes.get(sym, 0.01)
        lot = self.lot_sizes.get(sym, 0.001)

        norm_p = round(round(raw_price / tick) * tick, 8)
        norm_a = round(round(raw_amount / lot) * lot, 8)

        return {"symbol": sym, "normalized_price": norm_p, "normalized_amount": norm_a}

    def tool_calc_indicators_batch(self, prices_matrix: List[List[float]], period: int) -> Dict[str, Any]:
        """다차원 배열 처리를 통한 O(1) IPC 통신 달성 (또는 Mock)
        - prices_matrix: List of price arrays for batch processing
        """
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

    def tool_eval_math(self, expression: str) -> Dict[str, Any]:
        """AST 화이트리스트 기반 안전한 수학 표현식 실행
        - expression: Supported ops: +, -, *, /, log, exp, sqrt, pow
        """
        def _eval(node):
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
                return node.value
            elif isinstance(node, ast.Num):
                return node.n
            elif isinstance(node, ast.BinOp):
                ops = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv}
                return ops[type(node.op)](_eval(node.left), _eval(node.right))
            elif isinstance(node, ast.Call):
                if hasattr(node.func, "id"):
                    func_name = node.func.id
                    if func_name in self.allowed_math_funcs:
                        eval_args = [_eval(a) for a in node.args]
                        return self.allowed_math_funcs[func_name](*eval_args)
                raise ValueError("Unsupported or dangerous math function call.")

            raise ValueError("Illegal AST node detected.")

        try:
            res = _eval(ast.parse(expression, mode="eval").body)
            return {"result": float(res)}
        except SyntaxError:
            raise ValueError(f"Syntax error in expression: {expression}")

    def tool_generate_fingerprint(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a deterministic SHA-256 fingerprint for a JSON payload."""
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return {"fingerprint": fingerprint, "status": "DETERMINISTIC"}

def main():
    server = FinLib()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("FinLib Oracle Terminated by Interrupt.")

if __name__ == "__main__":
    main()