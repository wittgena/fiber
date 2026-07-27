# topos.ops.xelog.edge.a2a.profile
## @lineage: ops.xelog.edge.a2a.profile
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any

from topos.bound.adapter.sandbox import MetabolicProfile
from topos.bound.adapter.profile import ProfileAdapter

from swarm.phi.wasm.executor import WasmExecutor, TaskContext
from arch.xor.parser.block.contract import CoherenceState
from watcher.dphi.cgroup import Tier

from watcher.plane.emitter import get_logger

log = get_logger("edge.profile")

edge_dag = APIRouter(prefix="/profile/v1", tags=["Agentic Firewall (B2B)"])

class SimulationRequest(BaseModel):
    agent_schema: Dict[str, Any]
    context_depth: int = 2
    target_entry: str

async def extract_client_project(api_key: str = "test_key") -> str:
    """
    Parses API Key or JWT to extract the GCP Project ID. 
    (To be replaced by proper OIDC/Auth Middleware later)
    """
    return "generative-language-client-1234" 


def charge_account(client_id: str, fuel_consumed: int):
    """
    Post-execution billing based on deterministic WASM fuel consumption.
    Future Alignment: Dispatch a 'consensus_pulse' Contract via LedgerPulseEmitter.
    """
    ## Example conversion: 1,000,000 Fuel = 0.01 USD
    billed_amount = (fuel_consumed / 1_000_000) * 0.01
    log.info(f"[Billing] Charged ${billed_amount:.4f} for {fuel_consumed:,} fuel. Client: {client_id}")


@edge_dag.post("/simulate/dag")
async def simulate_agent_dag(
    req: SimulationRequest, 
    client_project_id: str = Depends(extract_client_project)
):
    """
    [Core Product] Simulates and applies firewall constraints to an external 
    AI agent's execution plan within a WASM-isolated Cgroup environment.
    """
    
    ## 1. Retrieve the Profile (WASM Billing Verification applied)
    profile: MetabolicProfile = await ProfileAdapter.determine_profile(client_project_id)
    
    ## 2. Map MetabolicProfile to Wasm Cgroup Tier
    target_tier = Tier.SYSTEM.value if profile.max_compute_time > 1.0 else Tier.STANDARD.value
    log.info(f"[{client_project_id}] Target Execution Tier mapped to: {target_tier}")
    
    ## 3. Configure WASM Task Context with hard resource limits
    executor = WasmExecutor()
    context = TaskContext(
        task_type="execute_agent_schema",
        tier=target_tier, 
        payload={
            "schema": req.agent_schema,
            "entry": req.target_entry,
            "depth": req.context_depth
        }
    )
    
    total_fuel_consumed = 0
    final_status = "UNKNOWN"
    reason = ""
    
    ## 4. Execute and monitor the WASM event stream
    try:
        async for contract in executor.execute_stream(context):
            
            if contract.state == CoherenceState.STREAMING:
                ## Extract intermediate fuel metrics from WASM kernel residues
                current_fuel = contract.payload.get("data", {}).get("fuel_consumed")
                if current_fuel:
                    total_fuel_consumed = current_fuel
                    
                log.debug(f"[{context.task_id}] Processing... Fuel Consumed: {total_fuel_consumed:,}")
                continue
                
            elif contract.state == CoherenceState.FRAGMENTED:
                ## Execution halted (e.g., OOM, Fuel Exhaustion, Firewall Blocked)
                final_status = contract.kind.upper()
                reason = contract.payload.get("error", "Unknown sandbox constraint triggered")
                total_fuel_consumed = contract.payload.get("fuel_consumed", total_fuel_consumed)
                break
                
            elif contract.state == CoherenceState.COHERENT:
                ## Execution successfully reached logical completion
                final_status = "SUCCESS"
                total_fuel_consumed = contract.payload.get("fuel_consumed", total_fuel_consumed)
                break
                
    except Exception as e:
        log.error(f"[{context.task_id}] Critical Error during Sandbox execution: {e}")
        final_status = "SYSTEM_ERROR"
        reason = str(e)

    ## 5. Post-Execution Ledger Settlement & Response
    charge_account(client_project_id, total_fuel_consumed)
    
    if final_status == "SUCCESS":
        log.info(f"[{context.task_id}] Execution Stable. Status: {final_status}")
        return {
            "status": "WET_RUN_SUCCESS", 
            "fuel_used": total_fuel_consumed,
            "tier_applied": target_tier
        }
    else:
        log.warning(f"[{context.task_id}] Execution Collapsed. Status: {final_status} | Reason: {reason}")
        return {
            "status": "COLLAPSE_PREDICTED", 
            "reason": reason, 
            "fuel_used": total_fuel_consumed,
            "tier_applied": target_tier
        }