# fiber.dphi.infra.observer.oracle.receptor
## @lineage: fiber.dphi.observer.oracle.receptor
## @lineage: fiber.phase.tracer.observer.oracle.receptor
## @lineage: phase.tracer.observer.oracle.receptor
## @lineage: bound.observer.oracle.receptor
## @lineage: dphi.observer.oracle.receptor
## @lineage: eco.observer.oracle.receptor
## @lineage: bound.proof.oracle.receptor
## @lineage: bound.exchange.capital.receptor
import time
import hashlib
from typing import Dict, Any, List, Optional

from fiber.dphi.infra.observer.oracle.aggregator import ProvableOracleAggregator
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import NodeSigner
from xphi.watcher.plane.emitter import get_emitter

class OracleReceptor:
    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.universal")
        self.aggregator = ProvableOracleAggregator()

    def fetch_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        fetch_time = int(time.time())
        self.log.info(f"[Receptor] Executing policy for {symbol} | Sources: {len(target_arns)} | Strategy: {strategy}")

        aggregator_result = self.aggregator.execute_policy(symbol, target_arns, strategy, fetch_time)
        context = {
            "oracle_id": "universal_receptor_v2.0",
            "timestamp": fetch_time,
            "signer_pubkey": getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        }

        recipe = {
            "strategy": strategy,
            "aggregator_code_hash": aggregator_result["aggregator_code_hash"],
            "sources": aggregator_result["composite_sources"]
        }

        observation = {
            "individual_hashes": aggregator_result["individual_hashes"],
            "payload": aggregator_result["aggregated_data"],
            "aggregated_hash": aggregator_result["aggregated_hash"]
        }

        recipe_root_bytes = StateAdapter.to_canonical_bytes(recipe)
        attestation_payload = {
            "context": context,
            "recipe_root": hashlib.sha256(recipe_root_bytes).hexdigest(),
            "observation_root": observation["aggregated_hash"]
        }
        
        canonical_root = StateAdapter.to_canonical_bytes(attestation_payload)
        signature = self.signer.sign_payload(canonical_root)
        self.log.info(f"  └─ [Universal Seal] Sig: {signature[:12]}... | Root: {hashlib.sha256(canonical_root).hexdigest()[:8]}")
        return {
            "context": context,
            "recipe": recipe,
            "observation": observation,
            "attestation": {
                "canonical_root": hashlib.sha256(canonical_root).hexdigest(),
                "signature": signature
            }
        }