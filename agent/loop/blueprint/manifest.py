# fiber.agent.loop.blueprint.manifest
## @lineage: fiber.agent.conver.loop.blueprint.manifest
from __future__ import annotations

from enum import Enum
from typing import Dict, Any

from fiber.agent.engine.tool.action.factory import CoreAction

class BlueprintType(Enum):
    SCHEME = "scheme"
    TRANSACTION = "transaction"
    TRACER = "tracer"
    RESOLUTION = "resolution"


# =====================================================================
# 1. Base Instruction Generator (Data Tier)
# =====================================================================
def get_instruction(focus: str, target_tier: str = "SYSTEM", fuel_limit: int = 2_000_000_000) -> str:
    """
    Returns the strict Zero-Trust system instructions for the LLM.
    LLM Generator should use this string template structure instead of inventing behavioral rules.
    """
    return (
        f"You are operating within the DPHI Zero-Trust Architecture (2026 Sandbox Environment).\n"
        f"Topology: '{focus}' | Constraint Tier: {target_tier} | Maximum Fuel Limit: {fuel_limit} (WASM Kinetic Trap Active).\n"
        f"Navigate through the specified deterministic events sequentially.\n"
        f"Determine the most efficient commands dynamically based on your environment. Avoid database lock contention by respecting the in-memory PTA netting model.\n"
        f"If an anomaly, Byzantine fault, or topological rupture is detected, trigger the 'signal' tool to broadcast x402 architectural telemetry.\n\n"
        f"CRITICAL RULES FOR FUNCTION CALLING:\n"
        f"1. EXACT TOOL NAMES: Strictly use exact lowercase tool names as registered (e.g., 'terminal', '{CoreAction.SIGNAL.value}', '{CoreAction.FINISH.value}').\n"
        f"2. REQUIRED PARAMETERS: Never omit required parameters. For example, when using 'terminal', you must provide BOTH 'command' and 'security_risk'."
    )


# =====================================================================
# 2. Declarative Manifest Table (LLM Generation Target)
# =====================================================================
BLUEPRINT_MANIFESTS: Dict[BlueprintType, Dict[str, Dict[str, Any]]] = {
    
    # ---------------------------------------------------------
    # [RESOLUTION] - Architectural Refactoring & Healing
    # ---------------------------------------------------------
    BlueprintType.RESOLUTION: {
        "resolution_hacking": {
            "topology_name": "Semantic Resolution Funnel",
            "focus": "Dependency Inversion Refactoring & Topological Rupture Healing",
            "min_cognitive_score": 4,
            "relations": "sequential",
            "instruction": get_instruction("Dependency Inversion Refactoring & Topological Rupture Healing"),
            "steps": [
                {"action": "terminal", "intent": "explore", "content": "Scan the workspace logs (e.g., conv.flow) to locate the Dependency Inversion rupture between fiber.scope.manager and surgent.", "expected_outcome": "Coupling boundary identified."},
                {"action": "terminal", "intent": "modify", "content": "Extract the ToposController and RuntimeNode from fiber/phase/scope/manager.py and isolate them into surgent/agent/topos/controller.py.", "expected_outcome": "Physical decoupling achieved."},
                {"action": CoreAction.SIGNAL.value, "intent": "evangelize", "content": "Emit a JSON payload summarizing the resolved dependency inversion (files_mutated, pattern_applied: 'Decoupling').", "params_template": {"channel": "slack_#architecture", "requires_consensus": True}},
                {"action": CoreAction.FINISH.value, "intent": "commit", "content": "Commit the isolated topological changes, ensuring the pure managed_scope remains in fiber, and finish execution.", "expected_outcome": "ConverStatus.FINISHED"}
            ]
        }
    },

    # ---------------------------------------------------------
    # [SCHEME] - Core System Operation & Self-Healing
    # ---------------------------------------------------------
    BlueprintType.SCHEME: {
        "agent": {
            "topology_name": "agent.cognitive",
            "focus": "XOR Parity & PTA State Validation",
            "min_cognitive_score": 3,
            "relations": "sequential",
            "instruction": get_instruction("XOR Parity & PTA State Validation"),
            "steps": [
                {"action": "terminal", "intent": "phase.cognitive", "content": "Scan the dphi log and extract multi-agent Fuel and memory metrics."},
                {"action": "terminal", "intent": "phase.cognitive", "content": "Execute 'validate_parity' wasm to assert the XOR Parity triplet (topos_id, nexus_id, phase_id) consistency across the cluster."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.cognitive", "content": "Emit telemetry signal containing PTA state parity validation and precise metering costs."},
                {"action": CoreAction.FINISH.value, "intent": "phase.cognitive", "content": "Append the canonical hash to collapse log and seal the epoch."}
            ]
        },
        "gov": {
            "topology_name": "gov.sandbox",
            "focus": "WASM Cgroup Kinetic Trap Isolation Check",
            "relations": "coupled,isolated",
            "min_cognitive_score": 4,
            "instruction": get_instruction("WASM Cgroup Kinetic Trap Isolation Check"),
            "steps": [
                {"action": "terminal", "intent": "phase.sandbox", "content": "Trigger the WASM Cgroup constraint suite targeting the STANDARD tier (64MB RAM / 10M Fuel Limit)."},
                {"action": "terminal", "intent": "phase.sandbox", "content": "Analyze test output log to verify fuel exhaustion events. Ensure the engine correctly triggered a 'wasm trap: all fuel consumed' halt."},
                {"action": "terminal", "intent": "phase.sandbox", "content": "Verify process isolation (Tier 2 Pyodide & Tier 3 Direct WASM) to ensure zero state bleeding occurs."},
                {"action": CoreAction.FINISH.value, "intent": "phase.sandbox", "content": "Confirm the multi-vector physical isolation boundary is secure and finalize."}
            ]
        },
        "meta": {
            "topology_name": "meta.telemetry",
            "focus": "FSM-Driven Byzantine Fault Tolerance Simulation",
            "relations": "mutated,survived",
            "min_cognitive_score": 5,
            "instruction": get_instruction("FSM-Driven Byzantine Fault Tolerance Simulation"),
            "steps": [
                {"action": "terminal", "intent": "phase.meta", "content": "Inject a Byzantine fault into broker config to simulate malicious calldata corruption and strict latency."},
                {"action": "terminal", "intent": "phase.meta", "content": "Hot-reload the MCP broker service to enforce the anomalous FSM edge cases."},
                {"action": "terminal", "intent": "phase.meta", "content": "Execute the fiber a2a test suite to verify that the XOR Parity state machine successfully recovers missing topology frames."},
                {"action": CoreAction.FINISH.value, "intent": "phase.meta", "content": "Verify 'Epoch Sealed Successfully' via 2-of-3 threshold consensus and exit."}
            ]
        },
        "autopoiesis": {
            "topology_name": "agent.autopoiesis",
            "focus": "Ephemeral Runtime & Self-Healing Orchestration",
            "relations": "decoupled_io,self_corrected",
            "min_cognitive_score": 4,
            "instruction": get_instruction("Ephemeral Runtime & Self-Healing Orchestration"),
            "steps": [
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Author a lightweight x402 Edge 'health_api.py' that returns {'status': 'alive'} on port 8080."},
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Deploy the API server as a background process inside the V8 Isolate context, capturing 'api.log'."},
                {"action": "terminal", "intent": "phase.autopoiesis", "content": "Poll the local health endpoint. If blocked by OS-Level boundaries, mutate permissions dynamically and retry."},
                {"action": CoreAction.FINISH.value, "intent": "phase.autopoiesis", "content": "Terminate the background API process, seal the micro-debt transaction, and exit."}
            ]
        }
    },

    # ---------------------------------------------------------
    # [TRANSACTION] - Web3, Security & Data Transformations
    # ---------------------------------------------------------
    BlueprintType.TRANSACTION: {
        "code_auditor": {
            "topology_name": "nexus.fiber.scan",
            "focus": "Fiber/Surgent Structural Dependency Auditing",
            "relations": "scanned,isolated",
            "min_cognitive_score": 3,
            "instruction": get_instruction("Fiber/Surgent Structural Dependency Auditing"),
            "steps": [
                {"action": "terminal", "intent": "phase.auditor", "content": "Scan the 'src' directory to identify all instances where 'fiber' incorrectly references 'surgent' (Dependency Inversion)."},
                {"action": "terminal", "intent": "phase.auditor", "content": "Execute 'build_ast_graph.py' to generate 'graph.json' mapping the physical separation of concerns."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.auditor", "content": "Read 'graph.json' and emit the structural coupling report via JSON signal."},
                {"action": CoreAction.FINISH.value, "intent": "phase.auditor", "content": "Remove the temporary 'graph.json' file and complete the audit transaction."}
            ]
        },
        "data_folder": {
            "topology_name": "theoria.compiler.fold",
            "focus": "Deterministic Schema & Zero-Gas Netting",
            "relations": "transformed,sealed",
            "min_cognitive_score": 1,
            "instruction": get_instruction("Deterministic Schema & Zero-Gas Netting"),
            "steps": [
                {"action": "terminal", "intent": "phase.folder", "content": "Locate and read 'raw_input.txt' to inspect unstructured noisy data targeting the PTA ledger."},
                {"action": "terminal", "intent": "phase.folder", "content": "Execute 'topos_compiler.py' under strict deterministic Pyodide constraints to produce 'compiler_output.json'."},
                {"action": "terminal", "intent": "phase.folder", "content": "Verify the structural integrity of the generated JSON against the x402 payment schema."},
                {"action": CoreAction.FINISH.value, "intent": "phase.folder", "content": "Seal the validated data into a KernelCommit (AuditReceipt) and exit without database row-locking."}
            ]
        },
        "infra_sealer": {
            "topology_name": "nexus.sphere.deploy",
            "focus": "REVM Shadow Settlement & Rollup Validation",
            "relations": "projected,validated",
            "min_cognitive_score": 3,
            "instruction": get_instruction("REVM Shadow Settlement & Rollup Validation"),
            "steps": [
                {"action": "terminal", "intent": "phase.sealer", "content": "Locate the 'transferFrom' Ethereum calldata manifest within the workspace."},
                {"action": "terminal", "intent": "phase.sealer", "content": "Run a dry-run simulation using the embedded REVM (Rust EVM) module to calculate state mutation proofs (sim_results.json)."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.sealer", "content": "Extract metrics from 'sim_results.json' and emit a signal indicating whether the transaction Reverted or succeeded."},
                {"action": CoreAction.FINISH.value, "intent": "phase.sealer", "content": "Generate the L2 Rollup Hash for the validated manifest, append to pipeline logs, and drop the simulation context."}
            ]
        }
    },

    # ---------------------------------------------------------
    # [TRACER] - Infrastructure & Chaos Engineering
    # ---------------------------------------------------------
    BlueprintType.TRACER: {
        "divergence": {
            "topology_name": "tracer.edge.divergence",
            "focus": "Edge V8 Isolate Defense Audit",
            "relations": "observed,collapsed",
            "min_cognitive_score": 4,
            "instruction": get_instruction("Edge V8 Isolate Defense Audit"),
            "steps": [
                {"action": "terminal", "intent": "phase.genesis", "content": "Spin up the dual V8 hologram simulated Cloudflare Worker environment."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Inject a tainted malicious payload targeting host filesystem inspection (/etc/passwd) and low-level socket bindings."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Monitor the isolation gateway for 10 seconds to capture the 'OSError: emscripten does not support processes' physical blockade."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "Parse the Cloudflare edge logs and emit a payload confirming OS-Level Kernel Isolation success."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Tear down the V8 Isolate to leave the perimeter in a safe collapsed state and finish."}
            ]
        },
        "oom": {
            "topology_name": "tracer.cgroup.oom",
            "focus": "Absolute Resource Collapse (WASM OOM)",
            "relations": "isolated,crushed",
            "min_cognitive_score": 4,
            "instruction": get_instruction("Absolute Resource Collapse (WASM OOM)"),
            "steps": [
                {"action": "terminal", "intent": "phase.genesis", "content": "Initialize a TaskWasm daemon with a strict predefined hardware boundary (64MB)."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Run a recursive Call-Stack Overflow script aimed at triggering memory leaks inside the boundary."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Wait for the WASM engine to halt execution and capture its MemoryError Exit Code."},
                {"action": CoreAction.SIGNAL.value, "intent": "phase.judgment", "content": "If the Hypervisor intercepted the overflow in O(1) time, emit a signal confirming Cgroup enforcement."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Purge the pre-warmed instance pool memory bleeding contexts and complete the task."}
            ]
        },
        "repro": {
            "topology_name": "tracer.depin.repro",
            "focus": "DePIN FSM Synchronization Resonance",
            "relations": "synchronized,restored",
            "min_cognitive_score": 4,
            "instruction": get_instruction("DePIN FSM Synchronization Resonance"),
            "steps": [
                {"action": "terminal", "intent": "phase.genesis", "content": "Boot the Finite State Machine (FSM) Bridge environment to guarantee a clean DePIN network state."},
                {"action": "terminal", "intent": "phase.stimulus", "content": "Execute 'inject_delayed_message.py' to push an EIP-712 signed stimulus into the off-chain queue."},
                {"action": "terminal", "intent": "phase.resonance", "content": "Monitor the worker logs for 'Tripartite Parity triplet validated' events for a duration of 35 seconds."},
                {"action": CoreAction.FINISH.value, "intent": "phase.teardown", "content": "Tear down the multi-agent cluster orchestrator to restore the field to absolute zero and conclude."}
            ]
        }
    }
}