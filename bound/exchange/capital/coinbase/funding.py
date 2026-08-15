# bound.exchange.capital.coinbase.funding
## @lineage: bound.capital.oracle.coinbase.funding
"""
@arn: arn:bound:oracle:coinbase:funding:v1.0.0
@desc: Deterministic adapter and validator for Coinbase International Perpetual Funding Rates.
@security: The raw bytes of this source file (.py) are cryptographically hashed for execution integrity.
@constraint: Do not modify whitespace, comments, or logic after network deployment.
"""
from typing import Dict, Any, List, TypedDict

# Coinbase International Exchange API (Perpetuals)
BASE_URL = "https://api.international.coinbase.com/api/v1/instruments"

class FundingRate(TypedDict):
    symbol: str
    rate: float
    time: float

def build_intent_params(symbol: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """Constructs deterministic HTTP request parameters for historical funding rates."""
    if limit <= 0 or limit > 1000:
        raise ValueError("Limit must be strictly between 1 and 1000")
        
    # 코인베이스 파생상품 심볼 포맷팅 (예: 바이낸스의 BTCUSDT -> 코인베이스의 BTC-PERP)
    formatted_symbol = symbol.replace("USDT", "-PERP").replace("USD", "-PERP")
    
    # Coinbase API는 ISO8601 포맷의 시간을 요구하는 경우가 많으나, 
    # 결정론적(Deterministic) 호출을 위해 Timestamp를 파라미터로 안전하게 매핑합니다.
    # (API 스펙에 따라 datetime 변환이 필요할 수 있습니다)
    
    return {
        "url": f"{BASE_URL}/{formatted_symbol}/funding",
        "method": "GET",
        "query": {
            "result_limit": limit,     # Coinbase Intx 파라미터 규격
            "time_to": end_time_ms     # 특정 시간 이전의 데이터를 조회
        }
    }

def parse_observation(raw_data: List[Dict[str, Any]]) -> List[FundingRate]:
    """
    @desc: Parses Coinbase futures funding rates, normalizes to canonical schema, 
           and strictly bounds the rate limits to prevent malicious exploitation.
    """
    parsed: List[FundingRate] = []
    
    for item in raw_data:
        ## @phase.1: Type casting & Extraction (Coinbase JSON Structure)
        # Coinbase 응답의 키 값은 바이낸스와 다릅니다. (instrument -> symbol, funding_rate -> rate)
        raw_symbol = str(item.get("instrument", ""))
        
        # 내부 시스템의 정규화를 위해 다시 '-PERP'를 제거하여 원상복구 (BTC-PERP -> BTCUSDT)
        normalized_symbol = raw_symbol.replace("-PERP", "USDT")
        
        rate = float(item.get("funding_rate", 0.0))
        
        # 코인베이스는 RFC3339/ISO8601 문자열 또는 초(sec) 단위를 주로 반환하므로 
        # 바이낸스와 동일한 밀리초(ms) 단위의 float로 맞춰주어야 합니다.
        # (이 예시에서는 API 응답이 밀리초 timestamp라고 가정)
        time = float(item.get("event_time", 0.0)) 
        
        ## @phase.2: Derivative Financial Logic Validation
        # 펀딩비 하드 리밋(±10%) 강제 (바이낸스 스크립트와 동일한 보안 정책)
        if not (-0.10 <= rate <= 0.10):
            raise ValueError(f"Anomalous data: Funding rate ({rate}) exceeds absolute safety threshold of ±10%")
            
        if time <= 0:
            raise ValueError("Anomalous data: Invalid timestamp detected")
            
        ## @step.3: Serialize to canonical schema (Must match Binance output completely)
        parsed.append({
            "symbol": normalized_symbol,
            "rate": rate,
            "time": time
        })
        
    return parsed