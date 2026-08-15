# bound.exchange.capital.coinbase.kline
## @lineage: bound.capital.oracle.coinbase.kline
"""
@arn: arn:bound:oracle:coinbase:kline:v1.0.0
@desc: Deterministic data adapter and validator for Coinbase Advanced Trade K-line data.
@security: The raw bytes of this source file (.py) are cryptographically hashed for execution integrity.
@constraint: Do not modify whitespace, comments, or logic after network deployment.
"""
import datetime
from typing import Dict, Any, List, TypedDict

BASE_URL = "https://api.exchange.coinbase.com/products"

class Candle(TypedDict):
    ts: float
    o: float
    h: float
    l: float
    c: float
    v: float

def build_intent_params(symbol: str, interval_sec: int, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """
    Constructs deterministic HTTP request parameters.
    Note: Coinbase expects target product in URL path and uses ISO-8601 or seconds for time.
    """
    valid_intervals = {60, 300, 900, 3600, 21600, 86400}
    if interval_sec not in valid_intervals:
        raise ValueError(f"Invalid granularity. Must be one of {valid_intervals}")
        
    if limit <= 0 or limit > 300:
        raise ValueError("Coinbase limit must be strictly between 1 and 300")
        
    end_time_sec = end_time_ms // 1000
    start_time_sec = end_time_sec - (interval_sec * limit)
    
    # 코인베이스는 심볼 포맷이 다릅니다. (예: BTCUSDT -> BTC-USDT)
    formatted_symbol = symbol.replace("USDT", "-USDT").replace("USD", "-USD")
    
    return {
        "url": f"{BASE_URL}/{formatted_symbol}/candles",
        "method": "GET",
        "query": {
            "granularity": interval_sec,
            "start": str(start_time_sec),
            "end": str(end_time_sec)
        }
    }

def parse_observation(raw_data: List[List[Any]]) -> List[Candle]:
    """
    @desc: Parses Coinbase response and normalizes it to the Canonical schema.
    @warning: Coinbase array order is [time, low, high, open, close, volume] 
              which differs from Binance (OHLC). This order mapping is critical.
    """
    parsed: List[Candle] = []
    for candle in raw_data:
        ## @phase.1: Type casting & Order Normalization (Low-High-Open-Close -> OHLC)
        ts = float(candle[0]) * 1000  # Convert to milliseconds for cross-exchange consistency
        l = float(candle[1])
        h = float(candle[2])
        o = float(candle[3])
        c = float(candle[4])
        v = float(candle[5])
        
        ## @phase.2: Financial Logic Validation (Prevent anomalous data injection)
        if h < l:
            raise ValueError(f"Anomalous data: High price ({h}) cannot be lower than Low price ({l})")
        
        if any(val < 0 for val in (o, h, l, c, v)):
            raise ValueError("Anomalous data: Negative prices or volume detected")
            
        ## @step.3: Serialize to canonical schema (Must match Binance Oracle output)
        parsed.append({
            "ts": ts,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
            "v": v
        })
        
    return parsed