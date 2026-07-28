# topos.xelog.edge.a2a.router
from fastapi import APIRouter

from topos.xelog.edge.a2a.compute import compute_edge
from topos.xelog.edge.a2a.exchange import exchange_edge
from topos.xelog.edge.a2a.profile import profile_edge

a2a_router = APIRouter(prefix="/v1/a2a")
a2a_router.include_router(compute_edge, prefix="/compute", tags=["A2A Compute"])
a2a_router.include_router(exchange_edge, prefix="/exchange", tags=["A2A Exchange"])
a2a_router.include_router(profile_edge, prefix="/profile", tags=["A2A Profile"])