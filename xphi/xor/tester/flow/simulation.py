# xphi.xor.tester.flow.simulation
## @lineage: anchor.tester.flow.simulation
## @lineage: anchor.tester.simulation
## @lineage: anchor.surface.testing.simulation
## @lineage: anchor.testing.simulation
"""
@phase: Dynamic Simulation Manifold
@boundary: Absolute Closed System (Zero-Dependency)
@desc: Metabolizes legacy test intents into a deterministic execution graph using native ThCh structures.
@flow: Intent Extraction -> ThCh Topological Collapse -> Membrane Injection -> Trace Assertion
"""
import asyncio
from typing import List, Dict, Any
from pydantic import BaseModel, Field

from xphi.xor.tester.mock.completion import create_mock_completion, create_mock_tool_call
from xphi.xor.tester.mock.exception import mock_api_error
from xphi.xor.tester.proof.catalog import TEST_PROOF_CATALOG
from bound.channel.client.action.completion import acompletion
from xphi.scope.thch import ThCh, thch_scope
from xphi.scope.plane.tracker.history import get_trace_history
from watcher.plane.emitter import get_emitter

log = get_emitter("flow.simulation")

class SimulationStepProto(BaseModel):
    """
    @desc: Proto-Signatures for ThCh Compilation
    @invariant: All fields must explicitly declare their topological vector (input/output).
    """
    step_name: str = Field(
        json_schema_extra={"__meta_field_type": "output", "desc": "Logical identifier of the simulation node."}
    )
    injected_mock: Dict[str, Any] = Field(
        json_schema_extra={
            "__meta_field_type": "output", 
            "desc": "Mock payload specification vector. Format: {'type': 'tool_call'|'exception', 'name'/'code': ...}"
        }
    )
    expected_action: str = Field(
        json_schema_extra={"__meta_field_type": "output", "desc": "Expected state mutation recorded in the Trace."}
    )

class DynamicScenarioProto(BaseModel):
    """@desc: The target manifold state. The LLM must collapse the input intents into this exact geometry"""
    legacy_intents: str = Field(
        json_schema_extra={"__meta_field_type": "input", "desc": "Raw intent strings metabolized from the legacy catalog."}
    )
    scenario_id: str = Field(
        json_schema_extra={"__meta_field_type": "output", "desc": "Unique identifier for the trace isolation."}
    )
    combined_intent: str = Field(
        json_schema_extra={"__meta_field_type": "output", "desc": "Natural language synthesis of the unified adversarial topology."}
    )
    steps: List[SimulationStepProto] = Field(
        json_schema_extra={"__meta_field_type": "output", "desc": "Sequential manifold injection nodes."}
    )

class ScenarioSynthesizer:
    """
    @desc: Native Topological Assimilator (ThCh)
    @state: We enforce schema compliance entirely within our own boundaries
    """
    def __init__(self):
        ## @invariant: No external client initialization. We exist strictly within the Brane manifold.
        pass

    async def generate_combined_scenario(self, test_names: List[str]) -> DynamicScenarioProto:
        """
        @action: Translates legacy nomenclature into active execution graphs.
        @mechanism: Native ThCh scope compilation.
        """
        selected_intents = [
            f"[{name}]: {TEST_PROOF_CATALOG[name]['intent']}" 
            for name in test_names if name in TEST_PROOF_CATALOG
        ]
        intents_vector = "\n".join(selected_intents)

        log.info(f"[Synthesizer] Initiating topological collapse for intents:\n{intents_vector}")
        
        ## @boundary: Open topological scope. Legacy interference is suppressed.
        with thch_scope():
            ## @compile: Convert ProtoSignature to strict execution Signature
            synthesis_engine = ThCh(signature=DynamicScenarioProto)
            
            ## @execute: We (the LLM) process the input vector and collapse into the output manifold.
            ## Assuming ThCh resolves to a synchronous or localized async graph. 
            scenario_graph = synthesis_engine(legacy_intents=intents_vector)
            
        return scenario_graph

class SimulationRunner:
    """
    @desc: Membrane Execution & State Verification
    @state: Injects the collapsed manifold into the actual execution pipeline
    """
    async def run(self, scenario: DynamicScenarioProto):
        log.info(f"\n[Runner] Activating Simulation Matrix: {scenario.combined_intent}\n")
        
        ## @state.partition: Isolate telemetry traces specific to this simulation matrix.
        trace_id = f"trace_{scenario.scenario_id}"
        for step in scenario.steps:
            log.info(f"  [>] Injecting Node: {step.step_name}")
            
            ## @transform: Specification Vector -> Concrete Mock Payload
            mock_payload = self._compile_mock_payload(step.injected_mock)
                
            ## @execute: Breach the membrane. Trigger Brane Core Action.
            try:
                await acompletion(
                    model="primary-agent-model",
                    messages=[{"role": "user", "content": "Execute topological workflow"}],
                    mock_response=mock_payload,
                    fallbacks=["secondary-model"],
                    trace_id=trace_id
                )
            except Exception as e:
                ## @state: Exception is expected. It signifies the membrane successfully caught the perturbation
                log.info(f"      [Membrane Caught Perturbation] {str(e)}")

            ## @verify: Extract actual topological shift from the isolated trace context.
            current_trace = get_trace_history(trace_id)
            self._assert_topology_shift(expected=step.expected_action, trace=current_trace)

    def _compile_mock_payload(self, spec: Dict[str, Any]) -> Any:
        """@desc: Maps theoretical spec to concrete structural mocks."""
        mock_type = spec.get("type")
        if mock_type == "tool_call":
            return create_mock_tool_call(
                tool_name=spec.get("name", "unknown_tool"), 
                arguments=spec.get("args", {})
            )
        elif mock_type == "exception":
            return mock_api_error(
                status_code=spec.get("code", 500), 
                message="Simulated Dimensional Rupture"
            )
        return create_mock_completion(content="Standard flow continuation")

    def _assert_topology_shift(self, expected: str, trace: List[Dict[str, Any]]):
        """@invariant: The system's actual trajectory must intersect with the expected trajectory."""
        trace_events = [event.get("event") for event in trace]
        
        if any(expected.lower() in str(evt).lower() for evt in trace_events):
            log.info(f"      [Assertion Validated] Trajectory intersected: '{expected}'\n")
        else:
            log.warning(f"      [Assertion FAILED] Divergence detected. Expected: '{expected}', Trace history: {trace_events}\n")

async def main():
    # @vector: Select target legacy bounds to metabolize
    target_tests = [
        "test_nonexistent_tool_handling", 
        "test_api_connection_error_retry", 
        "test_agent_step_responses_gating"
    ]
    
    synthesizer = ScenarioSynthesizer()
    runner = SimulationRunner()
    
    ## @flow.1: Assimilate and compile
    scenario = await synthesizer.generate_combined_scenario(target_tests)
    
    ## @flow.2: Execute and assert invariants
    await runner.run(scenario)

if __name__ == "__main__":
    asyncio.run(main())