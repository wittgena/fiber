# bound.proof.binance.funding
## @lineage: bound.exchange.capital.binance.funding
## @lineage: bound.capital.oracle.binance.funding
"""
@arn: arn:bound:oracle:binance:funding:v1.0.0
@desc: Deterministic adapter and validator for Binance USD(S)-M Futures Funding Rates.
@security: The raw bytes of this source file (.py) are cryptographically hashed for execution integrity.
@constraint: Do not modify whitespace, comments, or logic after network deployment.
"""
from typing import Dict, Any, List, TypedDict

# Binance USD-M Futures API
BASE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

class FundingRate(TypedDict):
    symbol: str
    rate: float
    time: float

def build_intent_params(symbol: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """Constructs deterministic HTTP request parameters for historical funding rates."""
    if limit <= 0 or limit > 1000:
        raise ValueError("Limit must be strictly between 1 and 1000")
        
    return {
        "url": BASE_URL,
        "method": "GET",
        "query": {
            "symbol": symbol,
            "limit": limit,
            "endTime": end_time_ms
        }
    }

def parse_observation(raw_data: List[Dict[str, Any]]) -> List[FundingRate]:
    """
    @desc: Parses futures funding rates and strictly bounds the rate limits 
           to prevent malicious exploitation of delta-neutral vaults or synthetic AMMs.
    """
    parsed: List[FundingRate] = []
    
    for item in raw_data:
        ## @phase.1: Type casting & Extraction
        symbol = str(item.get("symbol", ""))
        rate = float(item.get("fundingRate", 0.0))
        time = float(item.get("fundingTime", 0.0))
        
        ## @phase.2: Derivative Financial Logic Validation (Prevent anomalous state injection)
        # 펀딩비는 현물 가격과 달리 음수가 가능하지만, 비정상적인 스퀴즈(Squeeze) 시 
        # 과도한 청산(Liquidation)을 유발하므로 하드 리밋(예: ±10%)을 강제합니다.
        if not (-0.10 <= rate <= 0.10):
            raise ValueError(f"Anomalous data: Funding rate ({rate}) exceeds absolute safety threshold of ±10%")
            
        if time <= 0:
            raise ValueError("Anomalous data: Invalid timestamp detected")
            
        ## @step.3: Serialize to canonical schema
        parsed.append({
            "symbol": symbol,
            "rate": rate,
            "time": time
        })
        
    return parsed