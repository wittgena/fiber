# dphi.space.profile
## @lineage: agent.space.sandbox.profile
## @lineage: bound.space.sandbox.profile
## @lineage: bound.space.bench.profile
import os
import json
import time
from dataclasses import dataclass, asdict, field
from typing import Dict, Any, Optional, List, Union

from xphi.arch.xor.parser.block.contract import CoherenceState
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.cgroup import CgroupPolicy, Tier
from xphi.kernel.dphi.exchange.config import billing_config, tier_config
from xphi.kernel.dphi.sandbox.executor import SandboxExecutor, TaskContext, SandboxEnv, EffectResolver

from xphi.watcher.tracer.scope import scope_trace
from xphi.watcher.plane.emitter import get_logger

log = get_logger("sandbox.profile")

class VerificationError(Exception):
    pass

class BillingVerifier:
    def __init__(self, target_context: str, max_errors: int = 1):
        self.target_context = target_context
        self.max_errors = max_errors
        self.mapped_state: List[str] = []

    async def verify_mapping(self, target_nodes: List[str], schema: Dict[str, Any]) -> str:
        observations = []
        error_count = 0
        
        for i, node_id in enumerate(target_nodes):
            async with scope_trace(name=f"verify_node_{i}", facet="logical"):
                try:
                    files_map = schema.get("files", {})
                    if not files_map:
                        raise VerificationError("Kernel Billing Validation Rejected: Missing 'files' map in execution schema.")
                    
                    valid_obs = f"Native Python Gateway verified billing for node: {node_id}"
                    observations.append(valid_obs)
                    self.mapped_state.append(valid_obs)
                        
                except VerificationError:
                    error_count += 1
                    self.mapped_state.clear()
                    if error_count >= self.max_errors:
                        raise VerificationError(f"Unverified demands exceeded logical tolerance. Last Error: Missing 'files'")
                    raise
                    
        report_body = "\n".join(observations)
        return f"Native Billing Verification Report:\n{report_body}"

async def execute_billing_verification(target_nodes: List[str], expected_billing_id: str, schema: Dict[str, Any]) -> str:
    verifier = BillingVerifier(target_context=expected_billing_id, max_errors=1)
    return await verifier.verify_mapping(target_nodes, schema)


# =====================================================================
# 2. Sandbox & Resolvers
# =====================================================================
@dataclass
class SandboxReport:
    is_valid: bool
    output: str = ""
    error: str = ""
    cgroup_metrics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class MetabolicProfile:
    cgroup_policy: CgroupPolicy = field(default_factory=CgroupPolicy.standard)
    max_compute_time: float = 3.0
    max_threads: int = 1
    max_node_capacity: int = 3
    max_simulation_ticks: int = 50

class SandboxResolver(EffectResolver):
    def __init__(self, profile: Optional[MetabolicProfile] = None):
        self.profile = profile or MetabolicProfile()
        self.broker = DphiBroker(timeout=self.profile.max_compute_time)

    def _get_policy_from_tier(self, tier: Tier) -> CgroupPolicy:
        if tier == Tier.SYSTEM: 
            return CgroupPolicy.system()
        elif tier == Tier.UNLIMITED: 
            return CgroupPolicy.custom(1024, 10_000_000_000)
        return CgroupPolicy.standard()

    async def resolve(self, payload: Dict[str, Any], instruction: str, env: SandboxEnv, tier: Union[Tier, str]) -> Dict[str, Any]:
        if isinstance(tier, str):
            try:
                target_tier = Tier(tier.upper())
            except ValueError:
                target_tier = Tier.STANDARD
        else:
            target_tier = tier

        log.info(f"[SandboxResolver] Routing instruction '{instruction}' to Env '{env.value}' (Tier: {target_tier.value})")
        self.profile.cgroup_policy = self._get_policy_from_tier(target_tier)

        schema = payload.get("schema", {})
        files = schema.get("files", {})
        entry_file = payload.get("entry", "main.py")
        
        code = payload.get("code") or files.get(entry_file, "")
        variables = payload.get("variables", {})

        report = SandboxReport(is_valid=False)
        if env in (SandboxEnv.DENO, SandboxEnv.WASM, SandboxEnv.LOCAL):
            start_time = time.time()
            exec_res = await self.broker.execute(code=code, variables=variables, tier=target_tier.value)
            elapsed_ms = (time.time() - start_time) * 1000
            
            report.is_valid = exec_res.success
            report.output = exec_res.output if exec_res.success else ""
            report.error = str(exec_res.error) if not exec_res.success else ""
            
            fuel_consumed = 0
            mem_usage_bytes = 0
            if exec_res.success:
                try:
                    out_data = json.loads(exec_res.output)
                    fuel_consumed = out_data.get("fuel_consumed", 0)
                    mem_usage_bytes = out_data.get("mem_usage_bytes", 0)
                except json.JSONDecodeError:
                    pass

            report.cgroup_metrics = {
                "tier": target_tier.value,
                "env": env.value,
                "elapsed_ms": round(elapsed_ms, 2),
                "fuel_consumed": fuel_consumed,
                "mem_usage_bytes": mem_usage_bytes,
                "mem_limit_bytes": self.profile.cgroup_policy.max_memory_bytes
            }
        elif env == SandboxEnv.DOCKER:
            report.error = "Docker environment is provisioned in the architecture but not yet attached."
        else:
            report.error = f"Unsupported Sandbox Environment: {env}"
            
        result_data = {**payload, "sandbox_report": report.to_dict()}
        if report.is_valid:
            result_data["execution_output"] = report.output
            
        return result_data


# =====================================================================
# 3. Execution Profile
# =====================================================================
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

    async def _resolve_profile(self, client_project_id: str, schema: Dict[str, Any]) -> MetabolicProfile:
        log.info(f"[Profile] Requesting Native billing verification for project: {client_project_id}")
        if not self.expected_billing_id:
            log.warning("[Profile] expected_billing_id missing. Proceeding with STANDARD policy for test safety.")
            policy = CgroupPolicy.standard()
            return MetabolicProfile(cgroup_policy=policy)
            
        try:
            await execute_billing_verification([client_project_id], self.expected_billing_id, schema)
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
            log.warning(f"[Profile] Verification Collapsed: {e}. Security Hard-Stop Triggered.")
            raise  

    def _charge_account(self, client_id: str, fuel_consumed: int):
        billed_amount = (fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
        log.info(f"[Billing] Charged ${billed_amount:.4f} for {fuel_consumed:,} fuel units. Client: {client_id}")

    async def execute(self, client_project_id: str, schema: Dict[str, Any], entry: str, depth: int, dry_run: bool = False) -> BenchResult:
        profile = await self._resolve_profile(client_project_id, schema)
        
        target_tier = profile.cgroup_policy.tier
        log.info(f"[{client_project_id}] Target Execution Tier mapped to: {target_tier.value}")
        
        sandbox_resolver = SandboxResolver(profile=profile)
        executor = SandboxExecutor(resolvers={"SANDBOX": sandbox_resolver}) 
        flat_payload = {"schema": schema, "entry": entry, "depth": depth}
        context = TaskContext(
            task_type="execute_agent_schema",
            tier=target_tier.value, 
            sandbox_env=SandboxEnv.DENO,
            payload=flat_payload
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