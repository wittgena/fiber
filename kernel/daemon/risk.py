# fiber.kernel.daemon.risk
import time
import asyncio
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from fiber.dphi.observer.intent.trajectory import (
    TrajectoryOracleReceptor, 
    ArbitrageIntent,
    TensionPhase,
    RiskPolicy,
    _get_module_source_hash
)

from xphi.arch.contract.registry.unified import contract
from xphi.kernel.daemon.base import AbstractDaemon
from xphi.kernel.dphi.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.risk")

# =========================================================================
# @phase.1: Core Risk Data Structures
# =========================================================================
@dataclass(frozen=True)
class RiskAssessment:
    is_safe: bool
    executable_size: float
    net_yield: float
    reason: str


# =========================================================================
# @phase.2: Trajectory Monitoring and Alert Subsystem
# =========================================================================
class DormantTrajectorySentinel:
    """
    WASM 커널 기반의 동역학 평가 엔진을 백그라운드에서 주기적으로 호출하여
    시장의 비선형적 발작(Spiking) 및 위상장 텐션을 모니터링하는 센티널.
    """
    def __init__(self, broker: DphiBroker):
        self.receptor = TrajectoryOracleReceptor(broker=broker)
        self.alert_emitter = get_emitter("sentinel.awakening")

    async def run_dormant_loop_async(self, symbol: str, target_arns: List[str], base_interval: int = 3600):
        self.alert_emitter.info(f"[{symbol}] Sentinel engaged. Operating on WASM Dynamics Kernel.")
        current_interval = base_interval
        
        while True:
            try:
                # 1. 비동기 동역학 엔진 호출
                eval_result = await self.receptor.engine.execute_flow_async(
                    symbol, target_arns, current_interval, int(time.time())
                )
                
                # 2. 통계적 Z-Score 대신 구조적 텐션(Tension) 및 발작 여부 추출
                phase_name = eval_result.dynamics.tension_phase
                tension = eval_result.dynamics.system_tension
                is_spiking = eval_result.dynamics.is_spiking

                # 3. Phase에 따른 주기 조정 및 볼트 트리거
                if phase_name == TensionPhase.NORMAL.name:
                    current_interval = base_interval  
                    
                elif phase_name == TensionPhase.PRE_HEATING.name:
                    self.alert_emitter.info(f"[{symbol}] ⚠️ Pre-heating engaged. Tension: {tension:.2f}. Preparing resources.")
                    current_interval = int(base_interval / 6)  
                    
                elif phase_name == TensionPhase.RUPTURE.name or is_spiking:
                    self.alert_emitter.critical(f"[{symbol}] 🚨 RUPTURE DETECTED. Tension: {tension:.2f} | Spiking: {is_spiking}. Initiating Capture.")
                    
                    if asyncio.iscoroutinefunction(self._trigger_awakening):
                        await self._trigger_awakening(symbol, target_arns, current_interval, tension)
                    else:
                        self._trigger_awakening(symbol, target_arns, current_interval, tension)
                    
                    # 캡처 완료 후 쿨다운
                    await asyncio.sleep(base_interval * 24) 
                    current_interval = base_interval
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.alert_emitter.warning(f"Sentinel evaluation error: {e}")
                
            await asyncio.sleep(current_interval)

    async def _trigger_awakening(self, symbol: str, target_arns: List[str], interval_sec: int, tension: float):
        """ResourceVault에 의해 오버라이드 됨"""
        pass


# =========================================================================
# @phase.3: Dynamic Risk and Execution Handlers
# =========================================================================
class DynamicRiskManager:
    def __init__(self, policy: RiskPolicy):
        self.policy = policy

    async def verify_execution_safety(self, intent: ArbitrageIntent, tvl: float, tension: float) -> RiskAssessment:
        # 시스템 텐션에 비례하여 동적 마찰 비용(Friction)을 증가시켜 보수적인 리스크 엣지 확보
        dynamic_friction_rate = (self.policy.base_friction_bps / 10000.0) * (1.0 + (tension / 10.0))
        effective_yield = intent.expected_yield - dynamic_friction_rate
        
        is_safe = intent.is_actionable and (effective_yield > 0)
        
        # 텐션이 예열(Preheat) 구간을 초과한 정도에 따라 투입 자본 비중(Allocation) 확대
        confidence_multiplier = min(1.0, (tension - self.policy.tension_preheat_threshold) / 5.0)
        allocated_pct = self.policy.max_allocation_pct * max(0.0, confidence_multiplier)
        executable_size = tvl * max(0.01, allocated_pct)

        log.info(
            f"[RiskEngine] Effective Yield: {effective_yield*100:.4f}% (Dynamic Friction: {dynamic_friction_rate*100:.4f}% deducted) | "
            f"Sys-Tension: {tension:.2f} -> Allocation: {allocated_pct*100:.1f}% | Passed: {'✔️' if is_safe else '❌'}"
        )

        return RiskAssessment(
            is_safe=is_safe,
            executable_size=executable_size, 
            net_yield=effective_yield,
            reason="Utility post-friction is negative" if not is_safe else "Passed"
        )

class SmartExecutionRouter:
    async def execute_routing(self, source_arn: str, target_arn: str, size: float) -> bool:
        log.info(f"[Actuator] ⚡ Executing State Routing: {source_arn} -> {target_arn} | Volume: {size:,.2f}")
        # 실제 체인 상호작용 또는 CEX API 라우팅 대기 모의
        await asyncio.sleep(0.2) 
        return True


# =========================================================================
# @phase.4: Vault Management and Execution Orchestration
# =========================================================================
class ResourceVault:
    def __init__(
        self, 
        base_capacity: float,
        logic_hash: str,
        router: SmartExecutionRouter,
        risk_manager: DynamicRiskManager,
        broker: DphiBroker
    ):
        self.tvl = base_capacity
        self.log = get_emitter("system.vault")
        
        self.logic_hash = logic_hash
        self.router = router
        self.risk = risk_manager
        
        # DphiBroker 의존성 주입
        self.sentinel = DormantTrajectorySentinel(broker)
        self.sentinel._trigger_awakening = self._capture_anomaly

    async def deploy_daemon(self, target_symbol: str, target_arns: List[str]):
        self.log.info(f"[Vault] Guardian node deployed for {target_symbol}.")
        if hasattr(self.sentinel, "run_dormant_loop_async"):
            await self.sentinel.run_dormant_loop_async(target_symbol, target_arns, base_interval=3600)

    async def _capture_anomaly(self, symbol: str, target_arns: List[str], interval_sec: int, tension: float):
        self.log.critical(f"[Vault] Structural anomaly validated. Logic Hash: {self.logic_hash[:16]}...")
        
        # 비동기 봉인(Seal) 호출
        receipt = await self.sentinel.receptor.fetch_and_seal_async(symbol, target_arns, interval_sec)
        
        target_intent = receipt.observation.intent
        proof_hash = receipt.attestation.canonical_root
        
        risk_report = await self.risk.verify_execution_safety(target_intent, self.tvl, tension)
        if not risk_report.is_safe:
            self.log.warning(f"[Vault] Action aborted by Risk Manager: {risk_report.reason}")
            return
            
        execute_size = risk_report.executable_size
        net_yield = risk_report.net_yield

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


# =========================================================================
# @phase.5: Top-Level Application Daemon
# =========================================================================
@contract.daemon("risk_vault")
class RiskVaultDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RiskVaultDaemon")
        self.ctx = ctx  
        
        # [NEW] 최상위 컨텍스트에서 DphiBroker 인스턴스 획득 (SYSTEM 티어 FFI 통신용)
        self.broker = DphiBroker.get_instance()
        
        # 정책을 동역학 텐션 모델에 맞게 재정의
        self.policy = RiskPolicy(
            base_friction_bps=15.0, 
            max_allocation_pct=0.20,
            tension_preheat_threshold=5.0,  
            tension_rupture_threshold=15.0  
        )
        
        self.logic_hash = _get_module_source_hash(__file__)
        self.risk_engine = DynamicRiskManager(self.policy)
        self.router = SmartExecutionRouter()
        
        self.target_nodes = [
            "arn:bound:oracle:binance:funding:v1.0.0",
            "arn:bound:oracle:coinbase:funding:v1.0.0"
        ]
        self.target_symbol = "BTCUSDT"
        
        # 리소스 볼트에 Broker 의존성 주입
        self.vault = ResourceVault(
            base_capacity=1_000_000.0,
            logic_hash=self.logic_hash,
            router=self.router,
            risk_manager=self.risk_engine,
            broker=self.broker
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