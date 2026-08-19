# bound.observer.proof.coinbase.kline
## @lineage: bound.proof.coinbase.kline
"""
@arn: arn:bound:oracle:coinbase:kline:v1.0.0
@desc: Deterministic data adapter and validator for Coinbase Advanced Trade K-line data
@security: The raw bytes of this source file are cryptographically hashed for execution integrity
@constraint: Do not modify whitespace, comments, or logic after network deployment
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
    """@desc: Constructs deterministic HTTP request parameters by abstracting Coinbase-specific temporal constraints into the standard protocol format"""
    valid_intervals = {60, 300, 900, 3600, 21600, 86400}
    if interval_sec not in valid_intervals:
        raise ValueError(f"Invalid granularity Must be one of {valid_intervals}")
        
    if limit <= 0 or limit > 300:
        raise ValueError("Coinbase limit must be strictly between 1 and 300")
        
    end_time_sec = end_time_ms // 1000
    start_time_sec = end_time_sec - (interval_sec * limit)
    
    ## @desc: Reformat the standard symbol string to match the Coinbase-specific API requirements to ensure successful routing
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
    """@desc: Parses and normalizes the Coinbase-specific data array into the canonical OHLC schema to guarantee uniformity across multi-source attestations"""
    parsed: List[Candle] = []
    for candle in raw_data:
        ## @phase.1: Realign the unique Coinbase array order and temporal scale to match the global network standard
        ts = float(candle[0]) * 1000  
        l = float(candle[1])
        h = float(candle[2])
        o = float(candle[3])
        c = float(candle[4])
        v = float(candle[5])
        
        ## @phase.2: Enforce market invariants to reject anomalous data injections before they reach the execution router
        if h < l:
            raise ValueError(f"Anomalous data High price ({h}) cannot be lower than Low price ({l})")
        
        if any(val < 0 for val in (o, h, l, c, v)):
            raise ValueError("Anomalous data Negative prices or volume detected")
            
        ## @phase.3: Serialize to the canonical schema to ensure cross-exchange compatibility in downstream receptor logic
        parsed.append({
            "ts": ts,
            "o": o,
            "h": h,
            "l": l,
            "c": c,
            "v": v
        })
        
    return parsed