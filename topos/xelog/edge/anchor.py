# topos.xelog.edge.anchor
## @lineage: topos.ops.xelog.edge.anchor
## @lineage: ops.xelog.edge.anchor
from fastapi import APIRouter, Depends, HTTPException, status

from topos.xelog.depend import get_nexus_anchor
from topos.xelog.topos.state.edge import (
    EdgeState,
    ParityTripletSchema,
    AnchorProposalRequest,
    AnchorSealResponse
)
from watcher.dphi.adapter.anchor import NexusAnchor, AnchorProposal

anchor_edge = APIRouter(prefix="/anchor/v1", tags=["Anchor (Consensus)"])

@anchor_edge.post(
    "/seal", 
    summary="상태 합의 및 영수증 방출 (Seal Epoch)",
    response_model=AnchorSealResponse
)
async def seal_state(
    req: AnchorProposalRequest,
    nexus: NexusAnchor = Depends(get_nexus_anchor)
):
    proposal = AnchorProposal(
        receptor_id=req.receptor_id,
        proposed_parity=req.proposed_parity.model_dump(),
        parent_nexus_id=req.parent_nexus_id,
        repos=req.repos,
        signers=req.signers,
        signatures=req.signatures,
        timestamp=req.timestamp
    )
    result = await nexus.anchor_state(proposal)
    if not result.is_sealed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Topology Ruptured or Consensus Failed: {result.rupture_reason}"
        )
        
    return AnchorSealResponse(
        status=EdgeState.SEALED_AND_COMMITTED,
        nexus_id=result.nexus_id,
        commit_hash=result.commit_hash,
        receipt=result.receipt.__dict__ if hasattr(result.receipt, "__dict__") else dict(result.receipt)
    )