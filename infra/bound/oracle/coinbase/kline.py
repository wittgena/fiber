# fiber.infra.bound.oracle.coinbase.kline
## @lineage: xphi.arch.bound.xor.oracle.coinbase.kline
## @lineage: xphi.bound.xor.oracle.coinbase.kline
## @lineage: xphi.bound.oracle.coinbase.kline
"""
@arn: arn:bound:oracle:coinbase:kline:v1.0.1
@desc: Deterministic data adapter and validator for Coinbase Advanced Trade K-line data
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

def _normalize_coinbase_symbol(symbol: str) -> str:
    clean_symbol = symbol.replace("-", "").replace("_", "").upper()
    if clean_symbol.endswith("USDT"):
        return f"{clean_symbol[:-4]}-USDT"
    elif clean_symbol.endswith("USDC"):
        return f"{clean_symbol[:-4]}-USDC"
    elif clean_symbol.endswith("USD"):
        return f"{clean_symbol[:-3]}-USD"
    elif clean_symbol.endswith("EUR"):
        return f"{clean_symbol[:-3]}-EUR"
    return symbol

def build_intent_params(symbol: str, interval_sec: int, limit: int, end_time_ms: int) -> Dict[str, Any]:
    valid_intervals = {60, 300, 900, 3600, 21600, 86400}
    if interval_sec not in valid_intervals:
        raise ValueError(f"Invalid granularity Must be one of {valid_intervals}")
        
    if limit <= 0 or limit > 300:
        raise ValueError("Coinbase limit must be strictly between 1 and 300")
        
    # [핵심 개선] Coinbase API 특성에 맞게 타임스탬프를 granularity 배수로 정확히 정렬(Align)
    end_time_sec = end_time_ms // 1000
    aligned_end_time = (end_time_sec // interval_sec) * interval_sec
    aligned_start_time = aligned_end_time - (interval_sec * limit)
    
    formatted_symbol = _normalize_coinbase_symbol(symbol)
    
    return {
        "url": f"{BASE_URL}/{formatted_symbol}/candles",
        "method": "GET",
        "query": {
            "granularity": interval_sec,
            "start": str(aligned_start_time),
            "end": str(aligned_end_time)
        }
    }

def parse_observation(raw_data: List[List[Any]]) -> List[Candle]:
    parsed: List[Candle] = []
    for candle in raw_data:
        ts = float(candle[0]) * 1000  
        l = float(candle[1])
        h = float(candle[2])
        o = float(candle[3])
        c = float(candle[4])
        v = float(candle[5])
        
        if h < l:
            raise ValueError(f"Anomalous data High price ({h}) cannot be lower than Low price ({l})")
        if any(val < 0 for val in (o, h, l, c, v)):
            raise ValueError("Anomalous data Negative prices or volume detected")
            
        parsed.append({
            "ts": ts, "o": o, "h": h, "l": l, "c": c, "v": v
        })
    return parsed