# bound.exchange.capital.binance.kline
## @lineage: bound.capital.oracle.binance.kline
"""
@arn: arn:bound:oracle:binance:kline:v1.0.0
@desc: Deterministic data adapter and validator for Binance K-line (Candlestick) data.
@security: The raw bytes of this source file (.py) are cryptographically hashed for execution integrity.
@constraint: Do not modify whitespace, comments, or logic after network deployment.
"""
from typing import Dict, Any, List, TypedDict

BASE_URL = "https://api.binance.com/api/v3/klines"

class Candle(TypedDict):
    ts: float
    o: float
    h: float
    l: float
    c: float
    v: float

def build_intent_params(symbol: str, interval: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """Constructs deterministic HTTP request parameters with basic bound checks."""
    if limit <= 0 or limit > 1000:
        raise ValueError("Limit must be strictly between 1 and 1000")
        
    return {
        "url": BASE_URL,
        "method": "GET",
        "query": {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            "endTime": end_time_ms
        }
    }

def parse_observation(raw_data: List[List[Any]]) -> List[Candle]:
    """@desc: Parses the raw exchange response, minifies it, and validates the integrity of the financial data to prevent State Poisoning on the ledger"""
    parsed: List[Candle] = []
    for candle in raw_data:
        ## @phase.1: Type casting
        ts = float(candle[0])
        o, h, l, c, v = map(float, candle[1:6])
        
        ## @phase.2: Financial Logic Validation (Prevent anomalous data injection)
        if h < l:
            raise ValueError(f"Anomalous data: High price ({h}) cannot be lower than Low price ({l})")
        
        if any(val < 0 for val in (o, h, l, c, v)):
            raise ValueError("Anomalous data: Negative prices or volume detected")
            
        ## @step.3: Serialize to canonical schema
        parsed.append({
            "ts": ts,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
            "v": v
        })
        
    return parsed