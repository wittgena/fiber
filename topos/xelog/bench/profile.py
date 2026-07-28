# topos.xelog.bench.profile
from dataclasses import dataclass
from typing import Dict, Any, Optional

from atoa.secure.secret.manager import get_secret_str

from topos.xelog.audit.billing import execute_billing_verification, FrameCollapseError
from topos.bound.adapter.sandbox import MetabolicProfile, SandboxResolver

from arch.xor.parser.block.contract import CoherenceState
from phase.wasm.executor import WasmExecutor, TaskContext
from watcher.dphi.cgroup import CgroupPolicy, Tier
from watcher.plane.emitter import get_logger

log = get_logger("bench.profile")

@dataclass
class SimulationResult:
    status: str
    cycles_used: int
    tier_applied: str
    reason: Optional[str] = None

class ProfileServ:
    async def _resolve_profile(self, client_project_id: str) -> MetabolicProfile:
        log.info(f"[Profile] Requesting WASM-backed billing verification for project: {client_project_id}")
        expected_billing_id = get_secret_str(secret_name="EXPECTED_BILLING_ID", default_value="01XXXX-EXXXXX-XXXXXX")
        
        if not expected_billing_id:
            log.error("[Profile] Critical: EXPECTED_BILLING_ID is entirely missing or unresolvable.")
            raise ValueError("Internal configuration error.")
            
        try:
            await execute_billing_verification([client_project_id], expected_billing_id)
            log.info("[Profile] Verification Successful. Assigning SYSTEM (PREMIUM) Policy.")
            policy = CgroupPolicy.system()
            
            return MetabolicProfile(
                max_threads=4, 
                max_compute_time=float(policy.cpu_fuel_quota / 100_000_000), 
                max_node_capacity=50,
                max_simulation_ticks=1000
            )
            
        except FrameCollapseError as e:
            log.warning(f"[Profile] Verification Collapsed: {e}. Assigning STANDARD (DEGRADED) Policy.")
            policy = CgroupPolicy.standard()
            return MetabolicProfile(
                max_threads=1, 
                max_compute_time=float(policy.cpu_fuel_quota / 100_000_000),
                max_node_capacity=3, 
                max_simulation_ticks=50 
            )
            
        except Exception as e:
            log.error(f"[Profile] Unexpected Error during verification: {e}")
            return MetabolicProfile(
                max_threads=1, max_compute_time=0.01, max_node_capacity=1, max_simulation_ticks=10
            )

    def _charge_account(self, client_id: str, cycles_consumed: int):
        billed_amount = (cycles_consumed / 1_000_000) * 0.01
        log.info(f"[Billing] Charged ${billed_amount:.4f} for {cycles_consumed:,} cycles. Client: {client_id}")

    async def execute(self, client_project_id: str, schema: Dict[str, Any], entry: str, depth: int) -> SimulationResult:
        profile = await self._resolve_profile(client_project_id)
        target_tier = Tier.SYSTEM.value if profile.max_compute_time > 1.0 else Tier.STANDARD.value
        log.info(f"[{client_project_id}] Target Execution Tier mapped to: {target_tier}")
        sandbox_resolver = SandboxResolver(profile=profile)
        executor = WasmExecutor(resolvers={"SANDBOX": sandbox_resolver}) 
        
        context = TaskContext(
            task_type="execute_agent_schema",
            tier=target_tier, 
            payload={"schema": schema, "entry": entry, "depth": depth}
        )
        
        total_cycles = 0
        final_status = "UNKNOWN"
        reason = ""
        
        # 3. WASM 스트림 실행
        try:
            async for contract in executor.execute_stream(context):
                topos_id = contract.topos_id 
                if contract.state == CoherenceState.STREAMING:
                    current_cycles = contract.payload.get("data", {}).get("cycles") 
                    if current_cycles:
                        total_cycles = current_cycles
                    log.debug(f"[{topos_id}] Processing... Cycles: {total_cycles:,}")
                    continue
                elif contract.state == CoherenceState.FRAGMENTED:
                    final_status = contract.kind.upper()
                    reason = contract.payload.get("reason") or contract.payload.get("detail", "sandbox_constraint_triggered")
                    total_cycles = contract.payload.get("cycles", total_cycles)
                    break
                elif contract.state == CoherenceState.COHERENT:
                    final_status = contract.kind.upper()
                    total_cycles = contract.payload.get("cycles", total_cycles)
                    break
        except Exception as e:
            log.error(f"[{context.topos_id}] Host Crash during execution: {e}")
            final_status = "HOST_DIVERGENCE"
            reason = str(e)

        # 4. 정산 처리
        self._charge_account(client_project_id, total_cycles)
        return SimulationResult(
            status=final_status, 
            cycles_used=total_cycles, 
            tier_applied=target_tier,
            reason=reason
        )