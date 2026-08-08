# receptor.ingress.billing.verification
## @lineage: receptor.arch.billing
import json
from typing import Any, List
from kernel.dphi.broker import DphiBroker, DphiMethod
from watcher.tracer.scope import scope_trace

class VerificationError(Exception):
    pass

class BillingVerifier:
    def __init__(self, target_context: str, max_errors: int = 1):
        self.target_context = target_context
        self.max_errors = max_errors
        self.mapped_state: List[str] = []
        self.broker = DphiBroker()

    async def verify_mapping(self, target_nodes: List[str]) -> str:
        observations = []
        error_count = 0
        
        for i, node_id in enumerate(target_nodes):
            async with scope_trace(name=f"verify_node_{i}", facet="logical"):
                try:
                    # 1. Fetch Data
                    payload = json.dumps({"target_node": node_id, "action": "verify_billing"})
                    res = await self.broker.invoke(target_func=DphiMethod.VERIFY_PACKET, payload=payload)
                    
                    if not res.success:
                        raise VerificationError(f"WASM Kernel rejected billing validation: {res.error}")
                    
                    # 2. Parse Data
                    try:
                        output = json.loads(res.output)
                    except json.JSONDecodeError:
                        output = {"raw": res.output}

                    # 3. Calculate Metric & Evaluate
                    if output.get("is_valid", False):
                        valid_obs = f"WASM consensus verified billing for node: {node_id} (Tx: {output.get('tx_id')})"
                        observations.append(valid_obs)
                        self.mapped_state.append(valid_obs)
                    else:
                        error_count += 1
                        if error_count >= self.max_errors:
                            raise VerificationError("Unverified demands exceeded logical tolerance.")
                        
                except VerificationError:
                    # Rollback State on Failure
                    self.mapped_state.clear()
                    raise
                    
        # 4. Synthesize Report
        report_body = "\n".join(observations)
        return f"WASM Billing Verification Report:\n{report_body}"

async def execute_billing_verification(target_nodes: List[str], expected_billing_id: str) -> str:
    verifier = BillingVerifier(target_context=expected_billing_id, max_errors=1)
    return await verifier.verify_mapping(target_nodes)