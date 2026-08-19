# phase.dphi.adapter.exchange
## @lineage: kernel.dphi.adapter.exchange
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.xor.surge.model import DynamicSurgeModel
from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter
from kernel.dphi.exchange.config import billing_config
from kernel.dphi.cgroup import Tier

log = get_emitter("adapter.exchange")

class TransactionReceipt(DynamicSurgeModel):
    job_id: str
    topos_id: str
    parity_hash: str
    fuel_consumed: int
    settlement_status: str

class SettlementPayload(DynamicSurgeModel):
    batch_id: str
    state_root: str
    attestations: List[str] 
    gas_used: int
    timestamp: int

class ExchangeAdapter:
    def __init__(self, clearing_house_pub_key: str):
        self.clearing_house_pub = clearing_house_pub_key

    def _quantize_fuel_cost(self, fuel_consumed: int, tier: Tier) -> float:
        base_cost = (fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
        multiplier = 1.5 if tier == Tier.SYSTEM else 1.0
        return base_cost * multiplier

    def finalize_settlement(
        self, 
        entangled_state: dict, 
        cost_metrics: dict, 
        signatures: Optional[List[str]] = None, # Maintained gracefully for Workflow.py backward compatibility
        tier: Tier = Tier.SYSTEM
    ) -> TransactionReceipt:
        parity = entangled_state.get("parity", {})
        parity_topos_id = parity.get("topos_id", f"unknown_batch_{int(time.time())}")
        parity_phase_id = parity.get("phase_id", 0) 
        
        fuel_consumed = cost_metrics.get("fuel_consumed", 0)
        estimated_usd = self._quantize_fuel_cost(fuel_consumed, tier)
        log.info(f"[Exchange Adapter] Settlement Finalized. Topos: {parity_topos_id}, Parity: {parity_phase_id}, Cost: ${estimated_usd:.6f}")
        
        return TransactionReceipt(
            job_id=str(parity_topos_id),
            topos_id=str(parity_topos_id),
            parity_hash=str(entangled_state.get("state_hash", parity_phase_id)), 
            fuel_consumed=fuel_consumed,
            settlement_status="COMMITTED_TO_NEXUS"
        )
        
    def generate_settlement_payload(
        self, 
        receipt: TransactionReceipt,
        attestations: Optional[List[str]] = None
    ) -> SettlementPayload:
        """@desc: Translates a pure TransactionReceipt into a strict external payload wrapped with Notary Attestations."""
        return SettlementPayload(
            batch_id=receipt.job_id,
            state_root=receipt.parity_hash,
            attestations=attestations or [],
            gas_used=receipt.fuel_consumed,
            timestamp=int(time.time())
        )