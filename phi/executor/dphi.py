# phi.executor.dphi
## @lineage: swarm.phi.wasm.executor
## @lineage: topos.phi.wasm.executor
import json
import ast
import uuid
import time
from dataclasses import dataclass, field
from typing import Any, Dict, AsyncGenerator

from arch.xor.parser.block.contract import Contract, CoherenceState
from phase.bind.client.engine.local import LLMEngine
from watcher.dphi.broker import WasmBroker, WasmMethod
from watcher.plane.emitter import get_emitter
from watcher.dphi.cgroup import Tier

log = get_emitter("wasm.executor")

@dataclass
class TaskContext:
    """Standardized unit of work passed to the execution engine."""
    payload: Dict[str, Any]
    task_type: str = "default"
    tier: str = Tier.STANDARD.value  ## Maps to CgroupPolicy limits (e.g., SYSTEM or STANDARD)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])


SOURCE_NAME = "wasm_executor"
HANDLER_LLM = "llm_chat"
HANDLER_AST = "ast_parse"

WASM_KIND_INFO = "INFO"
WASM_KIND_TRANSITION = "TRANSITION"
WASM_MSG_IO_REQUIRED = "IO_REQUIRED"

KEY_RAW_INPUT = "raw_input"
KEY_TARGET_FILE = "target_file"
DEFAULT_DUMMY_FILE = "./dummy_source.py"


def generate_topos_id() -> str:
    """Helper to generate a numeric string satisfying ToposId constraints."""
    return str(time.time_ns())


class LLMHandler:
    """Handler for external network I/O (LLM inference)."""
    
    def __init__(self):
        self.llm_client = LLMEngine()

    async def execute(self, payload: Dict[str, Any], instruction: str) -> Dict[str, Any]:
        log.info("[LLMHandler] Executing LLM inference for WASM side-effect...")
        
        system_prompt = instruction
        input_data = payload.get(KEY_RAW_INPUT) or payload
        user_prompt = f"Input Context:\n{json.dumps(input_data, indent=2, ensure_ascii=False)}"

        try:
            response = self.llm_client.chat(
                system_prompt=system_prompt, 
                user_prompt=user_prompt,
                timeout=60
            )
            log.info("[LLMHandler] LLM Inference successful.")
        except Exception as e:
            log.error(f"[LLMHandler] Inference failed: {e}")
            response = f"ERROR_DURING_INFERENCE: {str(e)}"
            
        return { **payload, "llm_output": response }


class ASTHandler:
    """Handler for local file system operations and AST parsing."""
    
    async def execute(self, payload: Dict[str, Any], instruction: str) -> Dict[str, Any]:
        target_file = payload.get(KEY_TARGET_FILE, DEFAULT_DUMMY_FILE)
        log.info(f"[ASTHandler] Initiating AST Parsing on {target_file}...")
        
        try:
            with open(target_file, "r", encoding="utf-8") as f:
                source = f.read()
            tree = ast.parse(source)
            
            ## Execute AST processing logic
            generated_topology = {"entry": "auto_extracted", "nodes": {}}
            generated_code = f"# Auto-generated from {target_file}\nXPHI = {generated_topology}"
            
            log.info("[ASTHandler] Parse successful.")
        except Exception as e:
            log.error(f"[ASTHandler] AST parse failed: {e}")
            generated_code = f"ERROR: {e}"

        return { **payload, "transcribed_code": generated_code }


class WasmExecutor:
    """Executor for WASM kernel state loop and I/O delegation."""
    
    def __init__(self):
        self.broker = WasmBroker()
        self.io_handlers = {
            HANDLER_LLM: LLMHandler(),
            HANDLER_AST: ASTHandler()
        }

    async def execute_stream(self, context: TaskContext) -> AsyncGenerator[Contract, None]:
        log.info(f"[{SOURCE_NAME}] Dispatching task '{context.task_type}' (Tier: {context.tier}, ID: {context.task_id}) to WASM Kernel")
        current_payload = context.payload
        
        while True:
            ## Inject tier into request data to enforce Cgroup limitations at the kernel level
            request_data = {
                "action": context.task_type,
                "tier": context.tier,
                "payload": current_payload
            }

            exec_result = await self.broker.invoke(
                target_func=WasmMethod.EXECUTE_TRANSITION, 
                payload=json.dumps(request_data)
            )
            
            ## 1. Handle WASM execution errors (e.g., OOM, Fuel Exhaustion)
            if not exec_result.success:
                log.error(f"[{SOURCE_NAME}] WASM Execution Fault: {exec_result.error}")
                yield Contract(
                    id=generate_topos_id(),
                    kind="wasm_execution_fault",
                    source=SOURCE_NAME,
                    state=CoherenceState.FRAGMENTED,
                    payload={"task_id": context.task_id, "error": str(exec_result.error)}
                )
                break
                
            res = json.loads(exec_result.output)
            
            ## 2. Handle kernel firewall blocks
            if not res.get("is_authorized", True):
                log.error(f"[{SOURCE_NAME}] Kernel Firewall Blocked: {res.get('error_msg')}")
                yield Contract(
                    id=generate_topos_id(),
                    kind="firewall_blocked",
                    source=SOURCE_NAME,
                    state=CoherenceState.FRAGMENTED,
                    payload={"task_id": context.task_id, "error": res.get("error_msg")}
                )
                break
                
            ## 3. Emit intermediate state transitions (Includes Fuel Metrics)
            yield Contract(
                id=generate_topos_id(),
                kind="state_transition",
                source=SOURCE_NAME,
                state=CoherenceState.STREAMING,
                payload={"task_id": context.task_id, "data": res}
            )
            
            residues = res.get("all_residues", [])
            
            ## Identify external I/O requests (Side-effects) from WASM
            io_request = next(
                (r.get("msg") for r in residues if r.get("kind") == WASM_KIND_INFO and WASM_MSG_IO_REQUIRED in r.get("msg")), 
                None
            )
            
            if io_request:
                handler_key = HANDLER_LLM if "LLM" in io_request else HANDLER_AST
                handler = self.io_handlers.get(handler_key)
                
                if handler:
                    ## Process I/O at Python level, update payload, and re-enter loop
                    current_payload = await handler.execute(current_payload, instruction=io_request)
                    continue 
                    
            ## 4. Finalize execution if no further transitions exist
            if not any(r.get("kind") == WASM_KIND_TRANSITION for r in residues):
                log.info(f"[{SOURCE_NAME}] Task {context.task_id} Execution Complete.")
                yield Contract(
                    id=generate_topos_id(),
                    kind="execution_complete",
                    source=SOURCE_NAME,
                    state=CoherenceState.COHERENT,
                    payload={
                        "task_id": context.task_id, 
                        "final_state": res.get("final_root"),
                        "fuel_consumed": res.get("fuel_consumed", 0)
                    }
                )
                break