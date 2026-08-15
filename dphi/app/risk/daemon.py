# dphi.app.risk.daemon
import time
import asyncio
from typing import Dict, Any, List, Optional

# Core Infrastructure Imports
from kernel.daemon.base import AbstractDaemon
from arch.contract.registry.unified import contract
from watcher.plane.emitter import get_emitter

# External Modules
from bound.exchange.intent.trajectory import TrajectoryOracleReceptor

from bound.proof.observer import (
    TensionPhase,
    RiskPolicy,
    TensionGradientObserver,
    get_source_hash
)

log = get_emitter("risk.daemon")


# =========================================================================
# 1. Level 2: Sentinel (자율 감시 및 위상 제어기)
# =========================================================================
class DormantTrajectorySentinel:
    def __init__(self, observer: TensionGradientObserver):
        self.receptor = TrajectoryOracleReceptor()
        self.observer = observer  # 주입받은 비공개 로직 사용
        self.alert_emitter = get_emitter("sentinel.awakening")

    async def run_dormant_loop_async(self, symbol: str, target_arns: List[str], base_interval: int = 3600):
        self.alert_emitter.info(f"[{symbol}] Sentinel engaged. Operating on Dynamic Tension Gradient.")
        
        current_interval = base_interval
        
        while True:
            try:
                raw_state = self.receptor.engine.execute_flow(symbol, target_arns, current_interval, int(time.time()))
                current_spread = raw_state["spread_matrix"]["net_spread_yield"]
                current_stress = raw_state["integral"]
                
                # 비공개 로직(TensionGradientObserver)을 통한 위상 판별
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
        # Vault에 의해 동적으로 오버라이딩 됨
        pass


# =========================================================================
# 2. Level 3: Dynamic Risk & Execution Engines
# =========================================================================
class DynamicRiskManager:
    def __init__(self, policy: RiskPolicy):
        self.policy = policy

    async def verify_execution_safety(self, signal: Dict[str, Any], tvl: float, z_score: float) -> Dict[str, Any]:
        net_yield = signal.get("net_spread_yield", 0.0)
        is_actionable = signal.get("is_actionable", False)
        
        dynamic_friction_rate = (self.policy.base_friction_bps / 10000.0) * (1.0 + (z_score / 10.0))
        effective_yield = net_yield - dynamic_friction_rate
        
        is_safe = is_actionable and (effective_yield > 0)
        
        confidence_multiplier = min(1.0, (z_score - self.policy.sigma_preheat_threshold) / 2.0)
        allocated_pct = self.policy.max_allocation_pct * confidence_multiplier
        executable_size = tvl * max(0.01, allocated_pct)

        log.info(
            f"[RiskEngine] 실효 효용: {effective_yield*100:.4f}% (동적 마찰력 {dynamic_friction_rate*100:.4f}% 차감) | "
            f"Z-Score: {z_score:.2f} -> 자원 할당률: {allocated_pct*100:.1f}% | 통과: {'✔️' if is_safe else '❌'}"
        )

        return {
            "is_safe": is_safe,
            "executable_size": executable_size, 
            "net_yield": effective_yield,
            "reason": "Utility post-friction is negative" if not is_safe else "Passed"
        }

class SmartExecutionRouter:
    async def execute_routing(self, source_arn: str, target_arn: str, size: float) -> bool:
        log.info(f"[Actuator] ⚡ Executing State Routing: {source_arn} -> {target_arn} | Volume: {size:,.2f}")
        await asyncio.sleep(0.2) 
        return True


# =========================================================================
# 3. Level 4: Vault (자원 통제소 및 증명 조립기)
# =========================================================================
class ResourceVault:
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
        
        self.logic_hash = logic_hash  # 증명용 로직 해시 바인딩
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
        
        # 1. 상태 증명 수집 (추후 Receptor 내부에서 logic_hash를 Recipe에 포함시키도록 확장 가능)
        receipt = self.sentinel.receptor.fetch_and_seal(symbol, target_arns, interval_sec)
        payload = receipt["observation"]["payload"]
        signal = payload["spread_matrix"]["arbitrage_signal"]
        proof_hash = receipt["attestation"]["canonical_root"]

        # 2. 동적 리스크 및 비례 투입량 산출
        risk_report = await self.risk.verify_execution_safety(signal, self.tvl, z_score)
        if not risk_report["is_safe"]:
            self.log.warning(f"[Vault] Action aborted by Risk Manager: {risk_report.get('reason')}")
            return
            
        execute_size = risk_report["executable_size"]
        net_yield = risk_report["net_yield"]

        # 3. 라우팅 실행
        if signal.get("is_actionable", True):
            source = signal["optimal_long_venue"]
            target = signal["optimal_short_venue"]
            
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
# 4. Level 5: Application Daemon (최상위 노드 어댑터)
# =========================================================================
@contract.daemon("risk_vault")
class RiskVaultDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("RiskVaultDaemon")
        self.ctx = ctx  
        
        # 1. 비공개 정책 및 옵저버 인스턴스화
        self.policy = RiskPolicy(
            base_friction_bps=15.0, 
            max_allocation_pct=0.20,
            sigma_preheat_threshold=2.0, 
            sigma_rupture_threshold=3.5 
        )
        self.observer = TensionGradientObserver(self.policy)
        
        # 2. 무결성 증명을 위한 순수 소스코드 바이트 해시 추출
        self.logic_hash = get_source_hash()
        
        self.risk_engine = DynamicRiskManager(self.policy)
        self.router = SmartExecutionRouter()
        
        self.target_nodes = [
            "arn:bound:oracle:binance:funding:v1.0.0",
            "arn:bound:oracle:coinbase:funding:v1.0.0"
        ]
        self.target_symbol = "BTCUSDT"
        
        # 3. Vault 조립 시 의존성 및 해시 바인딩
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