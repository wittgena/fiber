# surface.receptor.xe.profile
## @lineage: watcher.receptor.xe.profile
import os
from dataclasses import dataclass
from typing import Dict, Any, Optional

from arch.contract.resolver.sandbox import MetabolicProfile, SandboxResolver
from arch.gov.billing import execute_billing_verification, VerificationError
from arch.xor.parser.block.contract import CoherenceState
from arch.gov.billing import execute_billing_verification, VerificationError

from kernel.dphi.sandbox.executor import SandboxExecutor, TaskContext, SandboxEnv
from kernel.dphi.cgroup import CgroupPolicy, Tier
from kernel.dphi.exchange.config import billing_config, tier_config

from watcher.plane.emitter import get_logger

log = get_logger("bench.profile")

@dataclass
class BenchResult:
    status: str
    fuel_consumed: int
    tier_applied: str
    reason: Optional[str] = None

class BenchProfile:
    def __init__(self, expected_billing_id: Optional[str] = None):
        self.expected_billing_id = (
            expected_billing_id or 
            getattr(billing_config, "expected_billing_id", None) or 
            os.getenv("EXPECTED_BILLING_ID", "01XXXX-EXXXXX-XXXXXX")
        )

    async def _resolve_profile(self, client_project_id: str) -> MetabolicProfile:
        log.info(f"[Profile] Requesting WASM-backed billing verification for project: {client_project_id}")
        if not self.expected_billing_id:
            log.error("[Profile] Critical: expected_billing_id is entirely missing or unresolvable.")
            raise ValueError("Internal configuration error.")
            
        try:
            await execute_billing_verification([client_project_id], self.expected_billing_id)
            log.info("[Profile] Verification Successful. Assigning SYSTEM (PREMIUM) Policy.")
            policy = CgroupPolicy.system()
            
            return MetabolicProfile(
                cgroup_policy=policy,
                max_threads=tier_config.system_max_threads, 
                max_compute_time=float(policy.cpu_fuel_quota / tier_config.fuel_to_seconds_ratio), 
                max_node_capacity=tier_config.system_max_node_capacity,
                max_simulation_ticks=tier_config.system_max_simulation_ticks
            )
        except VerificationError as e:
            log.warning(f"[Profile] Verification Collapsed: {e}. Assigning STANDARD (DEGRADED) Policy.")
            policy = CgroupPolicy.standard()
            
            return MetabolicProfile(
                cgroup_policy=policy,
                max_threads=tier_config.standard_max_threads, 
                max_compute_time=float(policy.cpu_fuel_quota / tier_config.fuel_to_seconds_ratio),
                max_node_capacity=tier_config.standard_max_node_capacity, 
                max_simulation_ticks=tier_config.standard_max_simulation_ticks 
            )
        except Exception as e:
            log.error(f"[Profile] Unexpected Error during verification: {e}")
            policy = CgroupPolicy.custom(mem_mb=tier_config.fallback_mem_mb, fuel=tier_config.fallback_fuel)
            return MetabolicProfile(
                cgroup_policy=policy,
                max_threads=tier_config.fallback_max_threads, 
                max_compute_time=tier_config.fallback_max_compute_time, 
                max_node_capacity=tier_config.fallback_max_node_capacity, 
                max_simulation_ticks=tier_config.fallback_max_simulation_ticks
            )

    def _charge_account(self, client_id: str, fuel_consumed: int):
        billed_amount = (fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
        log.info(f"[Billing] Charged ${billed_amount:.4f} for {fuel_consumed:,} fuel units. Client: {client_id}")

    async def execute(self, client_project_id: str, schema: Dict[str, Any], entry: str, depth: int, dry_run: bool = False) -> BenchResult:
        profile = await self._resolve_profile(client_project_id)
        
        target_tier = profile.cgroup_policy.tier
        log.info(f"[{client_project_id}] Target Execution Tier mapped to: {target_tier.value}")
        
        sandbox_resolver = SandboxResolver(profile=profile)
        executor = SandboxExecutor(resolvers={"SANDBOX": sandbox_resolver}) 
        
        context = TaskContext(
            task_type="execute_agent_schema",
            tier=target_tier.value, 
            sandbox_env=SandboxEnv.DENO,
            payload={"schema": schema, "entry": entry, "depth": depth}
        )
        
        latest_contract = None
        try:
            async for contract in executor.execute_stream(context):
                latest_contract = contract
                log.debug(f"[{contract.topos_id}] Event Stream -> State: {contract.state.name} | Kind: {contract.kind}")
                
        except Exception as e:
            log.error(f"[{context.topos_id}] Host Crash during execution: {e}")
            return BenchResult("HOST_DIVERGENCE", 0, target_tier.value, str(e))

        if not latest_contract:
            return BenchResult("NO_OUTPUT", 0, target_tier.value, "Stream yielded no contracts.")

        payload_data = latest_contract.payload.get("data", latest_contract.payload)
        
        sandbox_report = payload_data.get("sandbox_report", {})
        cgroup_fuel = sandbox_report.get("cgroup_metrics", {}).get("fuel_consumed", 0)
        dag_cycles = payload_data.get("cycles", 0)
        
        final_fuel_consumed = cgroup_fuel if cgroup_fuel > 0 else dag_cycles
        final_status = latest_contract.kind.upper()
        reason = payload_data.get("reason") or payload_data.get("detail") or "Execution completed successfully"
        
        if not dry_run:
            self._charge_account(client_project_id, final_fuel_consumed)
        else:
            log.info(f"[Billing] Dry-run complete. Estimated {final_fuel_consumed:,} units for Client: {client_project_id}")
        
        return BenchResult(
            status=final_status, 
            fuel_consumed=final_fuel_consumed, 
            tier_applied=target_tier.value,
            reason=reason
        )