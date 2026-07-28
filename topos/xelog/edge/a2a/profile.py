# topos.xelog.edge.a2a.profile
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, Optional
from topos.xelog.bench.profile import ProfileServ

profile_edge = APIRouter()

class SimulationRequest(BaseModel):
    agent_schema: Dict[str, Any]
    context_depth: int = 2
    target_entry: str

class SimulationResponse(BaseModel):
    status: str
    cycles_used: int
    tier_applied: str
    reason: Optional[str] = None

class ProfileSimulationState:
    SUCCESS = "WET_RUN_SUCCESS"
    COLLAPSED = "COLLAPSE_PREDICTED"

async def extract_client_project(api_key: str = "test_key") -> str:
    return "generative-language-client-1234" 

def get_simulator_service() -> ProfileServ:
    """FastAPI Depends를 위한 서비스 인스턴스 주입기"""
    return ProfileServ()

@profile_edge.post(
    "/bench/sandbox", 
    summary="A2A Agent DAG sandbox",
    response_model=SimulationResponse
)
async def simulate_agent_dag(
    req: SimulationRequest, 
    client_project_id: str = Depends(extract_client_project),
    simulator: ProfileServ = Depends(get_simulator_service)
):
    try:
        result = await simulator.execute(
            client_project_id=client_project_id,
            schema=req.agent_schema,
            entry=req.target_entry,
            depth=req.context_depth
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
        
    is_success = (result.status == "COHERENCE")
    api_status = ProfileSimulationState.SUCCESS if is_success else ProfileSimulationState.COLLAPSED
    return SimulationResponse(
        status=api_status, 
        cycles_used=result.cycles_used, 
        tier_applied=result.tier_applied,
        reason=result.reason
    )