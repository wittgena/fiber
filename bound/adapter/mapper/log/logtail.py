# bound.adapter.mapper.log.logtail
"""
@problem.state:
- Scalar metrics inherently lack spatial and semantic context.
- Static SVM routing cannot explain *why* an anomaly maps to a specific topology.
@flow: absorb_signal -> manifold_projection (LLM/Signature) -> materialize_spec
@desc:
- Ingests sparse anomaly payloads and uses `abcd.manifold`
- to dynamically project and route signals to topological downstream engines with verifiable evidence.
"""
import json
import asyncio
import argparse
from pathlib import Path
from typing import Dict, Any

from anchor.registry.router.workflow import Workflow, step, Event, StartEvent, StopEvent
from xor.opt.dsp.lm import BaseLM
from xor.opt.adapter.base import Adapter
from xor.opt.manifold.model.reasoning import Reasoning

from arch.xor.sign.signature import Signature
from arch.xor.sign.field import InputField, OutputField
from phase.bind.resolver import find_current_self, resolve_path
from phase.bind.folding import folding
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("log.logtail", phase="log")

SPEC_ROOT = resolve_path('surface') / "map"

class ToposRouteSignature(Signature):
    """
    Analyzes an infrastructure anomaly payload and routes it to the appropriate topological archetype.
    Must provide step-by-step reasoning from the raw payload as evidence.
    """
    raw_payload: str = InputField(desc="JSON string containing metrics and symptom diagnosis.")
    available_archetypes: str = InputField(desc="List of available target archetypes and their descriptions.")
    
    reasoning: Reasoning = OutputField(desc="Step-by-step logic explaining why this payload fits the target archetype.")
    target_archetype: str = OutputField(desc="The selected track_id (e.g., trk_cluster_collapse, trk_flow_asymmetry).")
    confidence_score: float = OutputField(desc="Confidence score between 0.0 and 1.0.")


class SignalAbsorbedEvent(Event):
    """@event: Raw sparse anomaly payload decoded from ingest pipeline."""
    entity_id: str
    raw_payload: str
    flow_id: str

class TrackRoutedEvent(Event):
    """@event: Execution specification containing LLM-verified Reasoning."""
    entity_id: str
    target_archetype: str
    reasoning: Reasoning
    confidence: float
    downstream_target: str


class ToposMappingWorkflow(Workflow):
    def __init__(self, lm_client: BaseLM, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lm_client = lm_client
        self.adapter = Adapter(use_native_function_calling=True)
        
        # @latent.archetypes: Definitions provided to the LLM
        self.archetypes = {
            "trk_cluster_collapse": "Downstream: engine.graph.centrality_mapper. Use when nodes fail simultaneously or lose connectivity.",
            "trk_flow_asymmetry": "Downstream: engine.graph.flow_dynamics. Use when volume metrics decouple from active session states.",
            "trk_state_desync": "Downstream: engine.state.consensus_verifier. Use for state drift, consensus failures, or rollback vectors."
        }

    @step
    async def absorb_signal(self, ev: StartEvent) -> SignalAbsorbedEvent:
        # [Refactor] Transitioned from dictionary access (ev.get) to explicit attribute access (getattr)
        # to strictly conform to the expected Event schema representations.
        raw_json = getattr(ev, "raw_data", None)
        
        if not raw_json:
            raise ValueError("Payload missing: 'raw_data' required.")
            
        data = json.loads(raw_json)
        entity_id = data.get("contract_id", data.get("entity_id", "unknown_entity"))
        flow_id = data.get("trace_id", f"flow-{entity_id}")
        
        log.info("Ingestion: Decoding structural fracture payload", entity_id=entity_id)
        
        return SignalAbsorbedEvent(
            entity_id=entity_id,
            raw_payload=raw_json,
            flow_id=flow_id
        )

    @step
    async def manifold_projection(self, ev: SignalAbsorbedEvent) -> TrackRoutedEvent:
        """@step.2: Manifold Signature Projection (LLM Reasoning & Routing)"""
        with flow_scope(flow_id=ev.flow_id, phase="manifold_projection") as ctx:
            log.trace("Commencing LLM projection via Manifold Adapter", entity_id=ev.entity_id)
            
            archetype_desc = "\n".join([f"- {k}: {v}" for k, v in self.archetypes.items()])
            inputs = {
                "raw_payload": ev.raw_payload,
                "available_archetypes": archetype_desc
            }
            
            try:
                responses = await self.adapter.acall(
                    lm=self.lm_client,
                    lm_kwargs={"temperature": 0.1},
                    signature=ToposRouteSignature,
                    demos=[],
                    inputs=inputs
                )
                output = responses[0]
                
                target_archetype = output["target_archetype"]
                confidence = output["confidence_score"]
                reasoning = output["reasoning"]
                
                log.signal(
                    "Topos Routed Successfully",
                    entity_id=ev.entity_id,
                    target_archetype=target_archetype,
                    confidence=confidence,
                    reasoning_steps=len(reasoning.steps) if reasoning else 0
                )
                
                if confidence < 0.65:
                    log.warning("Signal attenuation detected. Dropping payload.", confidence=confidence)
                    return StopEvent(result=None)
                
                downstream_target = self.archetypes.get(target_archetype, "engine.default").split(". Use")[0].replace("Downstream: ", "")
                
                return TrackRoutedEvent(
                    entity_id=ev.entity_id,
                    target_archetype=target_archetype,
                    reasoning=reasoning,
                    confidence=confidence,
                    downstream_target=downstream_target
                )
                
            except Exception as e:
                log.error("Manifold projection failed", exc_info=True, entity_id=ev.entity_id)
                raise

    @step
    async def materialize_spec(self, ev: TrackRoutedEvent) -> StopEvent:
        """@step.3: Materialize Execution Spec with Verified Evidence"""
        
        spec_payload = {
            "entity_id": ev.entity_id,
            "topological_track": ev.target_archetype,
            "downstream_target": ev.downstream_target,
            "confidence": ev.confidence,
            "evidence": {
                "reasoning": str(ev.reasoning)
            }
        }
        
        out_file = SPEC_ROOT / f"manifold_spec_{ev.entity_id}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(spec_payload, f, indent=2, ensure_ascii=False)
            
        log.info("Topological execution spec materialized", 
                 file=out_file.name, 
                 downstream=ev.downstream_target)
                 
        return StopEvent(result={
            "status": "success",
            "entity_id": ev.entity_id,
            "mapped_track": ev.target_archetype,
            "spec_file": str(out_file)
        })


async def run_pipeline(json_payload: str):
    mock_lm_client = BaseLM(model="gpt-4o") 
    workflow = ToposMappingWorkflow(lm_client=mock_lm_client, timeout=120.0)
    
    # The `folding` wrapper establishes resilient execution boundaries.
    # It maintains structural compatibility by intrinsically delegating to the async .run() method of the wrapped Workflow.
    with folding(workflow, re_entry_limit=5) as b_workflow:
        log.info("Execution boundaries established. Commencing agentic manifold projection.")
        
        # Inject `raw_data` as a keyword argument to ensure seamless binding to the underlying StartEvent properties.
        result = await b_workflow.run(raw_data=json_payload)
        
    log.info("\n" + "="*60)
    if result:
        log.info("[Agentic Manifold Projection Complete]")
        log.info(f"- Mapped Archetype : {result.get('mapped_track')}")
        log.info(f"- Output Spec Path : {result.get('spec_file')}")
    else:
        log.info("[Pipeline Halted] Signal fell below boundary threshold.")
    log.info("="*60)


def main():
    parser = argparse.ArgumentParser(description="Topos Mapper using abcd.manifold")
    mock_input = json.dumps({
        "entity_id": "node-alpha-1779391292624",
        "trace_id": "trc-a1b2c3d4",
        "namespace": "cluster_net",
        "metrics": {"volume": 5000000, "anomaly_rate": 0.85},
        "diagnosis": {
            "symptom": "Asymmetric decoupling between traffic volume and active sessions",
            "cause": "State space saturation by coordinated low-dimensional actors"
        }
    })
    
    parser.add_argument("--test-input", default=mock_input, help="JSON string payload")
    args = parser.parse_args()
    try:
        asyncio.run(run_pipeline(args.test_input))
    except Exception as e:
        log.critical("Unhandled Daemon Collapse", exc_info=True)


if __name__ == "__main__":
    main()