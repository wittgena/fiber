# topos.xelog.audit.billing
## @lineage: topos.ops.xelog.audit.billing
## @lineage: ops.xelog.audit.billing
import json
from typing import List, Any, Callable, Tuple, Awaitable
from pathlib import Path

from phase.bind.resolver import find_current_self
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.tracer.scope import scope_trace
from watcher.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
log = get_emitter("audit.billing", phase="gov.gateway")

class FrameCollapseError(Exception):
    pass

class FrameVerifier:
    def __init__(
        self, 
        target_context: Any,
        tool_func: Callable[[str], Awaitable[str]],
        parser_func: Callable[[str, Any], Any],
        metric_func: Callable[[str, Any], Tuple[float, str]],
        synthesizer_func: Callable[[str], str],
        max_errors: int = 1
    ):
        self.target_context = target_context
        self.tool_func = tool_func
        self.parser_func = parser_func
        self.metric_func = metric_func
        self.synthesizer_func = synthesizer_func
        self.max_errors = max_errors
        self.mapped_state: List[str] = []

    async def verify_mapping(self, target_nodes: List[str]) -> str:
        observations = ""
        error_count = 0
        
        for i, node_id in enumerate(target_nodes):
            async with scope_trace(name=f"verify_node_{i}", facet="logical"):
                try:
                    ## Mock 대신 실제 비동기 WASM I/O 실행
                    raw_data = await self.tool_func(node_id)
                    output = self.parser_func(raw_data, self.target_context)
                    score, valid_obs = self.metric_func(node_id, output)
                    
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
        return self.synthesizer_func(observations)

    def _rollback_state(self):
        self.mapped_state.clear()


async def _wasm_billing_tool(node_id: str) -> str:
    broker = WasmBroker()
    payload = json.dumps({"target_node": node_id, "action": "verify_billing"})
    
    res = await broker.invoke(target_func=WasmMethod.VERIFY_PACKET, payload=payload)
    if not res.success:
        raise FrameCollapseError(f"WASM Kernel rejected billing validation: {res.error.message}")
    
    return res.output

def _wasm_billing_parser(raw_data: str, target_context: Any) -> Any:
    try:
        return json.loads(raw_data)
    except json.JSONDecodeError:
        return {"raw": raw_data}

def _wasm_billing_metric(node_id: str, output: Any) -> Tuple[float, str]:
    is_valid = output.get("is_valid", False)
    if is_valid:
        return (1.0, f"WASM consensus verified billing for node: {node_id} (Tx: {output.get('tx_id')})")
    return (0.0, f"WASM consensus rejected billing for node: {node_id}")

def _wasm_billing_synthesizer(observations: str) -> str:
    return f"WASM Billing Verification Report:\n{observations.strip()}"

async def execute_billing_verification(target_nodes: List[str], expected_billing_id: str) -> str:
    verifier = FrameVerifier(
        target_context=expected_billing_id,
        tool_func=_wasm_billing_tool,
        parser_func=_wasm_billing_parser,
        metric_func=_wasm_billing_metric,
        synthesizer_func=_wasm_billing_synthesizer,
        max_errors=1
    )
    return await verifier.verify_mapping(target_nodes)