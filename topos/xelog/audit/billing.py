# topos.xelog.audit.billing
import json
from typing import Any, Tuple, List
from watcher.dphi.broker import WasmBroker, WasmMethod
from abc import ABC, abstractmethod
from typing import List, Any, Tuple
from watcher.tracer.scope import scope_trace

class FrameCollapseError(Exception):
    pass

class FrameVerifier(ABC):
    def __init__(self, target_context: Any, max_errors: int = 1):
        self.target_context = target_context
        self.max_errors = max_errors
        self.mapped_state: List[str] = []

    @abstractmethod
    async def fetch_data(self, node_id: str) -> str:
        pass

    @abstractmethod
    def parse_data(self, raw_data: str) -> Any:
        pass

    @abstractmethod
    def calculate_metric(self, node_id: str, output: Any) -> Tuple[float, str]:
        pass

    @abstractmethod
    def synthesize_report(self, observations: str) -> str:
        pass

    async def verify_mapping(self, target_nodes: List[str]) -> str:
        observations = ""
        error_count = 0
        
        for i, node_id in enumerate(target_nodes):
            async with scope_trace(name=f"verify_node_{i}", facet="logical"):
                try:
                    raw_data = await self.fetch_data(node_id)
                    output = self.parse_data(raw_data)
                    score, valid_obs = self.calculate_metric(node_id, output)
                    
                    if score == 0.0:
                        error_count += 1
                        if error_count >= self.max_errors:
                            raise FrameCollapseError("Unverified demands exceeded logical tolerance.")
                        continue
                        
                    observations += valid_obs + "\n"
                    self.mapped_state.append(valid_obs)
                except FrameCollapseError:
                    self._rollback_state()
                    raise
                    
        return self.synthesize_report(observations)

    def _rollback_state(self):
        self.mapped_state.clear()

class BillingVerifier(FrameVerifier):
    async def fetch_data(self, node_id: str) -> str:
        broker = WasmBroker()
        payload = json.dumps({"target_node": node_id, "action": "verify_billing"})
        res = await broker.invoke(target_func=WasmMethod.VERIFY_PACKET, payload=payload)
        if not res.success:
            raise FrameCollapseError(f"WASM Kernel rejected billing validation: {res.error.message}")
        return res.output

    def parse_data(self, raw_data: str) -> Any:
        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return {"raw": raw_data}

    def calculate_metric(self, node_id: str, output: Any) -> Tuple[float, str]:
        if output.get("is_valid", False):
            return (1.0, f"WASM consensus verified billing for node: {node_id} (Tx: {output.get('tx_id')})")
        return (0.0, f"WASM consensus rejected billing for node: {node_id}")

    def synthesize_report(self, observations: str) -> str:
        return f"WASM Billing Verification Report:\n{observations.strip()}"

async def execute_billing_verification(target_nodes: List[str], expected_billing_id: str) -> str:
    verifier = BillingVerifier(target_context=expected_billing_id, max_errors=1)
    return await verifier.verify_mapping(target_nodes)