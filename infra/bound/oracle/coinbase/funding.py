# fiber.infra.bound.oracle.coinbase.funding
## @lineage: xphi.arch.bound.xor.oracle.coinbase.funding
## @lineage: xphi.bound.xor.oracle.coinbase.funding
## @lineage: xphi.bound.oracle.coinbase.funding
"""
@arn: arn:bound:oracle:coinbase:funding:v1.0.1
@desc: Deterministic adapter and validator for Coinbase International Perpetual Funding Rates
"""
from typing import Dict, Any, List, TypedDict

BASE_URL = "https://api.international.coinbase.com/api/v1/instruments"

class FundingRate(TypedDict):
    symbol: str
    rate: float
    time: float

def _format_coinbase_symbol(symbol: str) -> str:
    """
    @desc: Safely extracts the base asset and formats it to Coinbase Perpetual standards.
    Prevents double-dash formatting errors (e.g., BTC-USDT -> BTC--PERP).
    """
    clean_symbol = symbol.replace("-", "").replace("_", "").upper()
    if clean_symbol.endswith("USDT"):
        base = clean_symbol[:-4]
    elif clean_symbol.endswith("USD"):
        base = clean_symbol[:-3]
    else:
        base = clean_symbol
    return f"{base}-PERP"

def _normalize_global_symbol(raw_symbol: str) -> str:
    """
    @desc: Restores the Coinbase specific symbol back to the global network standard.
    Ensures safe restoration even if the exchange alters its suffix format.
    """
    if raw_symbol.endswith("-PERP"):
        base = raw_symbol[:-5]
    else:
        base = raw_symbol.replace("-", "")
    return f"{base}USDT"

def build_intent_params(symbol: str, limit: int, end_time_ms: int) -> Dict[str, Any]:
    """@desc: Constructs deterministic HTTP request parameters by abstracting Coinbase-specific format constraints into the standard protocol"""
    if limit <= 0 or limit > 1000:
        raise ValueError("Limit must be strictly between 1 and 1000")
        
    ## @desc: Reformat the standard symbol string safely to match Coinbase perpetual specifications
    formatted_symbol = _format_coinbase_symbol(symbol)
    
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
        ## @phase.1: Realign unique Coinbase response keys and safely normalize the symbol back to the global network standard
        raw_symbol = str(item.get("instrument", ""))
        normalized_symbol = _normalize_global_symbol(raw_symbol)
        
        # Type safety enforcement: Catch unexpected nulls or string injection from the exchange API
        try:
            rate = float(item.get("funding_rate", 0.0))
            event_time = float(item.get("event_time", 0.0))
        except (ValueError, TypeError):
            raise ValueError("Anomalous data: Invalid rate or timestamp format received from Coinbase")
        
        ## @phase.2: Enforce absolute hard limits on funding rates to prevent cascading liquidations during anomalous market squeezes
        if not (-0.10 <= rate <= 0.10):
            raise ValueError(f"Anomalous data: Funding rate ({rate}) exceeds absolute safety threshold of ±10%")
            
        if event_time <= 0:
            raise ValueError("Anomalous data: Invalid timestamp detected")
            
        ## @phase.3: Serialize to the canonical schema to ensure cross-exchange compatibility in downstream receptor logic
        parsed.append({
            "symbol": normalized_symbol,
            "rate": rate,
            "time": event_time
        })
        
    return parsed