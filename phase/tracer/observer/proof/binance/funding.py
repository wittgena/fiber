# fiber.phase.tracer.observer.proof.binance.funding
## @lineage: phase.tracer.observer.proof.binance.funding
## @lineage: bound.observer.proof.binance.funding
## @lineage: dphi.observer.proof.binance.funding
## @lineage: eco.observer.proof.binance.funding
## @lineage: bound.proof.binance.funding
"""
@arn: arn:bound:oracle:binance:funding:v1.0.0
@desc: Deterministic adapter and validator for Binance USD(S)-M Futures Funding Rates
@security: The raw bytes of this source file are cryptographically hashed for execution integrity
@constraint: Do not modify whitespace, comments, or logic after network deployment
"""
from typing import Dict, Any, List, TypedDict

BASE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

class FundingRate(TypedDict):
    symbol: str
    rate: float
    time: float

def build_intent_params(symbol: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """@desc: Constructs deterministic HTTP request parameters with strict bound constraints to prevent resource exhaustion during node execution"""
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
    """@desc: Parses futures funding rates and enforces strict bounds to prevent malicious exploitation of delta-neutral vaults or synthetic AMMs"""
    parsed: List[FundingRate] = []
    
    for item in raw_data:
        ## @phase.1: Extract and coerce raw fields into strict types to guarantee deterministic cryptographic hashing
        symbol = str(item.get("symbol", ""))
        rate = float(item.get("fundingRate", 0.0))
        time = float(item.get("fundingTime", 0.0))
        
        ## @phase.2: Enforce absolute hard limits on funding rates to prevent cascading liquidations during anomalous market squeezes
        if not (-0.10 <= rate <= 0.10):
            raise ValueError(f"Anomalous data Funding rate ({rate}) exceeds absolute safety threshold of ±10%")
            
        if time <= 0:
            raise ValueError("Anomalous data Invalid timestamp detected")
            
        ## @phase.3: Serialize to the canonical schema to ensure cross-exchange compatibility in downstream receptor logic
        parsed.append({
            "symbol": symbol,
            "rate": rate,
            "time": time
        })
        
    return parsed