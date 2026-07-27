# topos.bound.adapter.profile
## @lineage: topos.gov.adapter.profile
import asyncio
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any

from atoa.secure.secret.manager import get_secret_str
from topos.ops.xelog.audit.billing import execute_billing_verification, FrameCollapseError
from topos.bound.adapter.sandbox import RegulatedSandbox, MetabolicProfile

from watcher.plane.emitter import get_logger
from watcher.dphi.cgroup import CgroupPolicy, Tier

log = get_logger("adapter.profile")

class ProfileAdapter:
    @staticmethod
    async def determine_profile(client_project_id: str) -> MetabolicProfile:
        log.info(f"[Adapter] Requesting WASM-backed billing verification for project: {client_project_id}")
        
        ## Dynamically fetch the expected billing ID using Secret Manager
        ## Falls back to the placeholder if not found in Env/KMS/OIDC
        expected_billing_id = get_secret_str(
            secret_name="EXPECTED_BILLING_ID", 
            default_value="01XXXX-EXXXXX-XXXXXX"
        )
        
        if not expected_billing_id:
            log.error("[Adapter] Critical: EXPECTED_BILLING_ID is entirely missing or unresolvable.")
            raise HTTPException(status_code=500, detail="Internal configuration error.")
            
        try:
            verification_report = await execute_billing_verification(
                [client_project_id], 
                expected_billing_id
            )
            log.info("[Adapter] Verification Successful. Assigning SYSTEM (PREMIUM) Policy.")
            policy = CgroupPolicy.system()
            
            return MetabolicProfile(
                max_threads=4, 
                max_compute_time=float(policy.cpu_fuel_quota / 100_000_000), 
                max_node_capacity=50,
                max_simulation_ticks=1000
            )
            
        except FrameCollapseError as e:
            log.warning(f"[Adapter] Verification Collapsed: {e}. Assigning STANDARD (DEGRADED) Policy.")
            policy = CgroupPolicy.standard()
            
            return MetabolicProfile(
                max_threads=1, 
                max_compute_time=float(policy.cpu_fuel_quota / 100_000_000),
                max_node_capacity=3, 
                max_simulation_ticks=50 
            )
            
        except Exception as e:
            log.error(f"[Adapter] Unexpected Error during verification: {e}")
            policy = CgroupPolicy.custom(mem_mb=16, fuel=1_000_000) 
            
            return MetabolicProfile(
                max_threads=1, 
                max_compute_time=0.01, 
                max_node_capacity=1,
                max_simulation_ticks=10
            )