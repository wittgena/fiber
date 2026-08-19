# phase.node.daemon.risk
"""@desc: Defines the autonomous risk daemon that monitors dynamic tension and executes safe routing based on cryptographic receipts intent"""
import time
import asyncio
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from eco.observer.intent.trajectory import TrajectoryOracleReceptor, ArbitrageIntent
from eco.observer.tension import (
    TensionPhase,
    RiskPolicy,
    TensionGradientObserver,
    get_source_hash
)

from arch.contract.registry.unified import contract
from kernel.daemon.base import AbstractDaemon
from watcher.plane.emitter import get_emitter

log = get_emitter("daemon.risk")

"""@phase.1: Core Risk Data Structures intent"""
@dataclass(frozen=True)
class RiskAssessment:
    """@desc: Immutable evaluation result determining whether the captured intent is safe to execute intent"""
    is_safe: bool
    executable_size: float
    net_yield: float
    reason: str


"""@phase.2: Trajectory Monitoring and Alert Subsystem intent"""
class DormantTrajectorySentinel:
    """@desc: Continuously monitors market states and wakes up the resource vault only when critical systemic tension is detected intent"""
    def __init__(self, observer: TensionGradientObserver):
        self.receptor = TrajectoryOracleReceptor()
        self.observer = observer
        self.alert_emitter = get_emitter("sentinel.awakening")

    async def run_dormant_loop_async(self, symbol: str, target_arns: List[str], base_interval: int = 3600):
        self.alert_emitter.info(f"[{symbol}] Sentinel engaged. Operating on Dynamic Tension Gradient.")
        current_interval = base_interval
        
        while True:
            try:
                eval_result = self.receptor.engine.execute_flow(symbol, target_arns, current_interval, int(time.time()))
                current_spread = eval_result.intent.expected_yield
                current_stress = eval_result.dynamics.accumulated_stress
                phase, z_score = self.observer.evaluate_tension(current_spread, current_stress)

                if phase == TensionPhase.NORMAL:
                    current_interval = base_interval  
                elif phase == TensionPhase.PRE_HEATING:
                    self.alert_emitter.info(f"[{symbol}] ⚠️ Pre-heating engaged. Z-Score: {z_score:.2f}. Preparing resources.")
                    current_interval = int(base_interval / 6)  
                elif phase == TensionPhase.RUPTURE:
                    self.alert_emitter.critical(f"[{symbol}] 🚨 RUPTURE DETECTED. Z-Score: {z_score:.2f}. Initiating Capture.")
                    if asyncio.iscoroutinefunction(self._trigger_awakening):
                        await self._trigger_awakening(symbol, target_arns, current_interval, z_score)
                    else:
                        self._trigger_awakening(symbol, target_arns, current_interval, z_score)
                    
                    await asyncio.sleep(base_interval * 24) 
                    current_interval = base_interval
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.alert_emitter.warning(f"Sentinel evaluation error: {e}")
                
            await asyncio.sleep(current_interval)

    def _trigger_awakening(self, symbol: str, target_arns: List[str], interval_sec: int, z_score: float):
        """@desc: Overridden dynamically by Vault injection to decouple monitoring from execution intent"""
        pass


"""@phase.3: Dynamic Risk and Execution Handlers intent"""
class DynamicRiskManager:
    """@desc: Calculates dynamic friction and sizing allocation based on tension Z-Scores to protect the vault capital intent"""
    def __init__(self, policy: RiskPolicy):
        self.policy = policy

    async def verify_execution_safety(self, intent: ArbitrageIntent, tvl: float, z_score: float) -> RiskAssessment:
        ## @step.1: Calculate dynamic friction based on current systemic stress intent
        dynamic_friction_rate = (self.policy.base_friction_bps / 10000.0) * (1.0 + (z_score / 10.0))
        effective_yield = intent.expected_yield - dynamic_friction_rate
        
        ## @step.2: Verify execution feasibility intent
        is_safe = intent.is_actionable and (effective_yield > 0)
        
        ## @step.3: Determine capital allocation multiplier intent
        confidence_multiplier = min(1.0, (z_score - self.policy.sigma_preheat_threshold) / 2.0)
        allocated_pct = self.policy.max_allocation_pct * confidence_multiplier
        executable_size = tvl * max(0.01, allocated_pct)

        log.info(
            f"[RiskEngine] Effective Yield: {effective_yield*100:.4f}% (Dynamic Friction: {dynamic_friction_rate*100:.4f}% deducted) | "
            f"Z-Score: {z_score:.2f} -> Allocation: {allocated_pct*100:.1f}% | Passed: {'✔️' if is_safe else '❌'}"
        )

        return RiskAssessment(
            is_safe=is_safe,
            executable_size=executable_size, 
            net_yield=effective_yield,
            reason="Utility post-friction is negative" if not is_safe else "Passed"
        )

class SmartExecutionRouter:
    """@desc: Actuator interface representing the physical state routing between venues intent"""
    async def execute_routing(self, source_arn: str, target_arn: str, size: float) -> bool:
        log.info(f"[Actuator] ⚡ Executing State Routing: {source_arn} -> {target_arn} | Volume: {size:,.2f}")
        await asyncio.sleep(0.2) 
        return True


"""@phase.4: Vault Management and Execution Orchestration intent"""
class ResourceVault:
    """@desc: Manages capital, coordinates risk assessment, and securely triggers state routing based on cryptographic proofs intent"""
    def __init__(
        self, 
        base_capacity: float,
        observer: TensionGradientObserver,
        logic_hash: str,
        router: SmartExecutionRouter,
        risk_manager: DynamicRiskManager
    ):
        self.tvl = base_capacity
        self.log = get_emitter("system.vault")
        
        self.logic_hash = logic_hash
        self.router = router
        self.risk = risk_manager
        
        self.sentinel = DormantTrajectorySentinel(observer)
        self.sentinel._trigger_awakening = self._capture_anomaly

    async def deploy_daemon(self, target_symbol: str, target_arns: List[str]):
        self.log.info(f"[Vault] Guardian node deployed for {target_symbol}.")
        if hasattr(self.sentinel, "run_dormant_loop_async"):
            await self.sentinel.run_dormant_loop_async(target_symbol, target_arns, base_interval=3600)

    async def _capture_anomaly(self, symbol: str, target_arns: List[str], interval_sec: int, z_score: float):
        self.log.critical(f"[Vault] Structural anomaly validated. Logic Hash: {self.logic_hash[:16]}...")
        
        ## @step.1: Retrieve the cryptographically sealed receipt (SealedTrajectoryReceipt) intent
        receipt = self.sentinel.receptor.fetch_and_seal(symbol, target_arns, interval_sec)
        
        ## @step.2: Extract the intent and cryptographic root safely via dataclass properties intent
        target_intent = receipt.observation.intent
        proof_hash = receipt.attestation.canonical_root

        ## @step.3: Request dynamic sizing and safety assessment from Risk Manager intent
        risk_report = await self.risk.verify_execution_safety(target_intent, self.tvl, z_score)
        if not risk_report.is_safe:
            self.log.warning(f"[Vault] Action aborted by Risk Manager: {risk_report.reason}")
            return
            
        execute_size = risk_report.executable_size
        net_yield = risk_report.net_yield

        ## @step.4: Execute state routing relying on the evaluated target intent intent
        if target_intent.is_actionable:
            source = target_intent.optimal_long_venue
            target = target_intent.optimal_short_venue
            
            success = await self.router.execute_routing(source, target, execute_size)
            if success:
                extracted_value = execute_size * net_yield
                self.tvl += extracted_value
                
                self.log.critical(
                    f"\n{'='*75}"
                    f"\n[ACTION COMPLETED] RESOURCE ROUTING VERIFIED"
                    f"\n  -> Attestation Root : {proof_hash[:16]}..."
                    f"\n  -> Applied Logic ID : {self.logic_hash}"
                    f"\n  -> Realized Utility : {net_yield * 100:.4f}% (Net of dynamic friction)"
                    f"\n  -> Routed Volume    : {execute_size:,.2f}"
                    f"\n  -> Updated Capacity : {self.tvl:,.2f}"
                    f"\n{'='*75}"
                )
        else:
            self.log.warning("[Vault] Discrepancy collapsed below actionable yield. Aborting.")
        
        self.log.info("[Vault] Execution cycle terminated. Returning to monitoring state.")


"""@phase.5: Top-Level Application Daemon intent"""
@contract.daemon("risk_vault")
class RiskVaultDaemon(AbstractDaemon):
    """@desc: Root lifecycle manager binding the internal vault components to the overarching DPHI node context intent"""
    def __init__(self, ctx):
        super().__init__("RiskVaultDaemon")
        self.ctx = ctx  
        
        ## @step.1: Instantiate private policies and observers intent
        self.policy = RiskPolicy(
            base_friction_bps=15.0, 
            max_allocation_pct=0.20,
            sigma_preheat_threshold=2.0, 
            sigma_rupture_threshold=3.5 
        )
        self.observer = TensionGradientObserver(self.policy)
        
        ## @step.2: Extract raw source code byte hash for integrity proofing intent
        self.logic_hash = get_source_hash()
        
        self.risk_engine = DynamicRiskManager(self.policy)
        self.router = SmartExecutionRouter()
        
        self.target_nodes = [
            "arn:bound:oracle:binance:funding:v1.0.0",
            "arn:bound:oracle:coinbase:funding:v1.0.0"
        ]
        self.target_symbol = "BTCUSDT"
        
        ## @step.3: Bind dependencies and logic hashes to the Vault assembly intent
        self.vault = ResourceVault(
            base_capacity=1_000_000.0,
            observer=self.observer,
            logic_hash=self.logic_hash,
            router=self.router,
            risk_manager=self.risk_engine
        )

    async def run(self):
        log.info(f"[{self.name}] Autonomous State Daemon Started. (Node ID: {self.ctx.node_id})")
        log.info(f"[{self.name}] Bound Logic Hash: {self.logic_hash[:16]}... (Proprietary execution locked)")
        
        vault_task = asyncio.create_task(self.vault.deploy_daemon(self.target_symbol, self.target_nodes))
        
        try:
            while self.running:
                await asyncio.sleep(1.0)
                if vault_task.done():
                    log.error(f"[{self.name}] Vault Task exited unexpectedly.")
                    break
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancellation signal received from Node Supervisor.")
        finally:
            if not vault_task.done():
                vault_task.cancel()
                log.info(f"[{self.name}] Sub-task (Vault Daemon) safely cancelled.")
            log.info(f"[{self.name}] Daemon evaporated cleanly.")