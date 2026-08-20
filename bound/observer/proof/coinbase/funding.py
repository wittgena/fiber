# bound.observer.proof.coinbase.funding
## @lineage: eco.observer.proof.coinbase.funding
## @lineage: bound.proof.coinbase.funding
"""
@arn: arn:bound:oracle:coinbase:funding:v1.0.0
@desc: Deterministic adapter and validator for Coinbase International Perpetual Funding Rates
@security: The raw bytes of this source file are cryptographically hashed for execution integrity
@constraint: Do not modify whitespace, comments, or logic after network deployment
"""
from typing import Dict, Any, List, TypedDict

BASE_URL = "https://api.international.coinbase.com/api/v1/instruments"

class FundingRate(TypedDict):
    symbol: str
    rate: float
    time: float

def build_intent_params(symbol: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """@desc: Constructs deterministic HTTP request parameters by abstracting Coinbase-specific format constraints into the standard protocol"""
    if limit <= 0 or limit > 1000:
        raise ValueError("Limit must be strictly between 1 and 1000")
        
    ## @desc: Reformat the standard symbol string to match Coinbase perpetual specifications
    formatted_symbol = symbol.replace("USDT", "-PERP").replace("USD", "-PERP")
    
    ## @desc: Map canonical timestamps safely to prevent non-deterministic ISO8601 string conversions
    return {
        "url": f"{BASE_URL}/{formatted_symbol}/funding",
        "method": "GET",
        "query": {
            "result_limit": limit,
            "time_to": end_time_ms
        }
    }

def parse_observation(raw_data: List[Dict[str, Any]]) -> List[FundingRate]:
    """@desc: Parses Coinbase futures funding rates and normalizes to the canonical schema while bounding rate limits to prevent exploitation"""
    parsed: List[FundingRate] = []
    
    for item in raw_data:
        ## @phase.1: Realign unique Coinbase response keys and normalize the symbol back to the global network standard
        raw_symbol = str(item.get("instrument", ""))
        normalized_symbol = raw_symbol.replace("-PERP", "USDT")
        rate = float(item.get("funding_rate", 0.0))
        time = float(item.get("event_time", 0.0))
        
        ## @phase.2: Enforce absolute hard limits on funding rates to prevent cascading liquidations during anomalous market squeezes
        if not (-0.10 <= rate <= 0.10):
            raise ValueError(f"Anomalous data Funding rate ({rate}) exceeds absolute safety threshold of ±10%")
            
        if time <= 0:
            raise ValueError("Anomalous data Invalid timestamp detected")
            
        ## @phase.3: Serialize to the canonical schema to ensure cross-exchange compatibility in downstream receptor logic
        parsed.append({
            "symbol": normalized_symbol,
            "rate": rate,
            "time": time
        })
        
    return parsed