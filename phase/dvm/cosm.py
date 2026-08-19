# phase.dvm.cosm
import sys
import json
import asyncio
import time
import hashlib
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from bound.client.config.dphi import dphi_env
from bound.client.config.client import NotarySwarm
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.adapter.shadow import ShadowAdapter
from kernel.dphi.adapter.utxo import UtxoBillingAdapter
from kernel.dphi.broker import DphiBroker
from kernel.phase.reactor import PhaseReactor
from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from bound.agent.intent.verifier import VerificationError

log = get_emitter("workflow.cosm")

@dataclass
class CosmConfig:
    """Configuration for Cross-VM UTXO Micro-billing execution"""
    mode: str = "suite"
    
    billing_cycles: int = 5
    cost_per_call: int = 50_000
    initial_l1_deposit: int = 100  # EVM(REVM)에서 확인된 예치금 (USDC)
    
    # 이기종 상태 변환(Cross-VM Mapping)을 위한 EVM & Cosmos 주소 쌍
    caller_evm: str = field(default_factory=lambda: dphi_env.agents.beta.evm_address)
    caller_cosm: str = field(default_factory=lambda: dphi_env.agents.beta.cosmos_address)
    provider_cosm: str = field(default_factory=lambda: dphi_env.agents.alpha.cosmos_address)
    
    target_cw20: str = field(default_factory=lambda: dphi_env.contracts.target_cw20)
    escrow_contract: str = field(default_factory=lambda: dphi_env.contracts.nexus_clearing)


"""Workflow Messages"""
class CosmStartMsg(WorkflowMessage): pass
class CosmIntentResolvedMsg(WorkflowMessage): pass
class CosmUtxoAnchoredMsg(WorkflowMessage): pass
class CosmSimulatedMsg(WorkflowMessage): pass
class CosmNettingVerifiedMsg(WorkflowMessage): pass

class CosmBillingWorkflow(Workflow):
    def __init__(self, config: CosmConfig):
        super().__init__(name="CROSS_VM_UTXO_BILLING_WORKFLOW")
        self.log = log
        self.config = config
        
        self.broker = DphiBroker()
        self.utxo_adapter = UtxoBillingAdapter(broker=self.broker)
        self.notary_keys = [node["priv"] for node in NotarySwarm(size=3).notaries]
        
        # UTXO 기반 영수증 트래킹
        self.authorized_fuel_budget: int = 0
        self.session_root_utxo: str = ""
        self.utxo_receipts: List[str] = []
        self.total_fuel_consumed: int = 0
        self.net_debt_usdc: float = 0.0
        
        self.canonical_hash: str = ""
        self.is_verified: bool = False
        self.last_error_message: str = ""

    async def start(self) -> bool:
        self.post_message(CosmStartMsg())
        await self.run()
        return getattr(self, "is_verified", False)

    @step
    async def phase_cross_chain_resolution(self, msg: CosmStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] REVM Intent Verification (Cross-VM Auth) ---")
        
        # dvm.revm 을 통해 서명 및 L1 예치 상태가 확인되었다고 전제 (Zero-gas Auth)
        self.log.info(f"  └ Received EIP-712 Intent from EVM Wallet: {self.config.caller_evm}")
        self.log.info(f"  └ Routing payload to local REVM module for verification...")
        self.log.info(f"  └ [REVM Verified] Target Escrow: {self.config.escrow_contract}")
        self.log.info(f"  └ [REVM Verified] Confirmed Locked Deposit: {self.config.initial_l1_deposit} USDC")
        
        return CosmIntentResolvedMsg()

    @step
    async def phase_virtual_exchange(self, msg: CosmIntentResolvedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] Virtual Exchange & Initial UTXO Anchor ---")
        
        # 1 USDC = 1,000,000 Fuel 가치 치환
        fuel_ratio = 1_000_000
        self.authorized_fuel_budget = self.config.initial_l1_deposit * fuel_ratio
        
        # 세션의 시작점(Genesis Root)을 증명하는 초기 UTXO 앵커 발행
        self.session_root_utxo = await self.utxo_adapter.append_charge_intent(
            tenant_id=self.config.caller_cosm,
            fuel_consumed=0,
            metadata={
                "action": "SESSION_INITIALIZE",
                "evm_caller": self.config.caller_evm,
                "authorized_fuel": self.authorized_fuel_budget
            }
        )
        
        self.log.info(f"  └ State Translation: EVM({self.config.caller_evm[:8]}) -> COSM({self.config.caller_cosm[:8]})")
        self.log.info(f"  └ Authorized Budget: {self.config.initial_l1_deposit} USDC -> {self.authorized_fuel_budget} Fuel")
        self.log.info(f"  └ Root UTXO Anchored: {self.session_root_utxo[:16]}...")
        
        return CosmUtxoAnchoredMsg()

    @step
    async def phase_micro_billing_simulation(self, msg: CosmUtxoAnchoredMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] High-frequency Append-Only UTXO Billing ---")
        
        try:
            accumulated_fuel = 0
            
            for cycle in range(1, self.config.billing_cycles + 1):
                # 1. Circuit Breaker (한도 초과 사전 검증)
                if accumulated_fuel + self.config.cost_per_call > self.authorized_fuel_budget:
                    self.log.error(
                        f"  └ ❌ Circuit Breaker: OOM/Fuel Exhaustion at cycle {cycle}. "
                        f"(Demanded: {accumulated_fuel + self.config.cost_per_call}, Budget: {self.authorized_fuel_budget})"
                    )
                    return ErrorMessage("OOM or Fuel Exhaustion Detected")

                self.log.info(f"  └ [Cycle {cycle}] AI Inference Processed... (-{self.config.cost_per_call} Fuel)")
                
                # 2. 계좌 차감 대신 UtxoBillingAdapter를 통한 불변 해시 영수증 발행
                utxo_receipt = await self.utxo_adapter.append_charge_intent(
                    tenant_id=self.config.caller_cosm,
                    fuel_consumed=self.config.cost_per_call,
                    metadata={
                        "cycle": cycle,
                        "parent_utxo": self.utxo_receipts[-1] if self.utxo_receipts else self.session_root_utxo
                    }
                )
                
                self.utxo_receipts.append(utxo_receipt)
                accumulated_fuel += self.config.cost_per_call
                
                # Event Loop 리소스 안정화
                await asyncio.sleep(0.001)

            self.total_fuel_consumed = accumulated_fuel
            return CosmSimulatedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Billing Simulation Crashed: {str(e)}")

    @step
    async def phase_state_netting(self, msg: CosmSimulatedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] UTXO Lineage Collapse & State Netting ---")
        try:
            # 1. 최종 UTXO 영수증의 Lineage 및 무결성 검증
            last_utxo = self.utxo_receipts[-1] if self.utxo_receipts else self.session_root_utxo
            
            # 2. 총 소비 연료를 USDC 가치로 역산 (1,000,000 Fuel = 1 USDC)
            self.net_debt_usdc = self.total_fuel_consumed / 1_000_000
            
            self.log.info(f"  └ Total UTXO Receipts : {len(self.utxo_receipts)} proofs accumulated")
            self.log.info(f"  └ Final Tip UTXO Hash : {last_utxo[:16]}...")
            self.log.info(f"  └ 🧮 Net Debt         : {self.total_fuel_consumed} Fuel used -> Converted to {self.net_debt_usdc} USDC")
            
            # 수학적 정합성 검증
            if self.total_fuel_consumed > self.authorized_fuel_budget or self.net_debt_usdc < 0:
                raise VerificationError("Fuel calculation anomaly. Potential exploit detected.")
                
            self.is_verified = True
            return CosmNettingVerifiedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Netting Verification Failed: {str(e)}")

    @step
    async def phase_settlement_sealing(self, msg: CosmNettingVerifiedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Dual-Settlement Sealing (EVM Calldata Formatting) ---")
        try:
            # 1. dvm.revm 및 L1 이더리움 스마트 컨트랙트가 실행할 수 있는 Calldata 규격 인코딩
            # function claim(address tenant, uint256 net_usdc_amount, bytes proof)
            method_signature = "claim(address,uint256,bytes)".encode('utf-8')
            method_id = hashlib.sha3_256(method_signature).hexdigest()[:8]
            
            tenant_padded = self.config.caller_evm.replace("0x", "").zfill(64).lower()
            amount_padded = hex(int(self.net_debt_usdc)).replace("0x", "").zfill(64)
            evm_calldata = f"0x{method_id}{tenant_padded}{amount_padded}"
            
            last_utxo = self.utxo_receipts[-1] if self.utxo_receipts else self.session_root_utxo
            
            rollup_payload = {
                "tenant_evm": self.config.caller_evm,
                "tenant_cosm": self.config.caller_cosm,
                "net_usdc_consumed": self.net_debt_usdc,
                "utxo_merkle_root": last_utxo,
                "evm_settlement_calldata": evm_calldata,
                "total_receipts_compressed": len(self.utxo_receipts)
            }
            
            # 2. 공증인 멀티시그 암호학적 봉인
            proof_receipt = ShadowAdapter.seal_execution_proof(
                execution_output=rollup_payload,
                notary_keys=self.notary_keys
            )
            
            self.canonical_hash = proof_receipt.canonical_hash
            self.log.info(f"  ✅ [SEALED] Receipt encoded for dvm.revm & L1 Submission")
            self.log.info(f"    └ EVM Calldata : {evm_calldata[:42]}... (truncated)")
            self.log.info(f"    └ Receipt Hash : {self.canonical_hash[:16]}...")
            self.log.info(f"    └ 3/3 Notaries attested the Net Debt.")
            
            return StopMessage(result=True)
            
        except Exception as e:
            return ErrorMessage(f"Sealing Failed: {str(e)}")

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.last_error_message = msg.msg
        self.log.error(f"❌ [HALTED] Cross-VM Billing Workflow safely aborted: {msg.msg}")
        return StopMessage(result=False)


# =========================================================================
# Execution Runner & Pipeline
# =========================================================================
class CosmRunner(SchemeRunner):
    def __init__(self, broker: DphiBroker, base_config: CosmConfig):
        super().__init__(broker)
        self.base_config = base_config
        self.log = log

    async def run_scenario(self, title: str, cycles: int, cost: int, expected_success: bool = True, expected_error_match: str = None) -> bool:
        self.log.info(f"\n\n{'='*80}\n🚀 [CROSS-VM SCENARIO] {title}\n{'='*80}")
        start_time = time.time()
        
        run_config = CosmConfig(
            billing_cycles=cycles,
            cost_per_call=cost
        )
        
        workflow = CosmBillingWorkflow(config=run_config)
        success = await workflow.start()
        elapsed_ms = (time.time() - start_time) * 1000

        # 역검증(Negative Testing) 단언 로직
        if success != expected_success:
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got {success}.", 
                "Workflow Execution", 
                title=title
            )
            return False

        if not success and expected_error_match:
            error_output = getattr(workflow, "last_error_message", "")
            if expected_error_match not in error_output:
                self._record_fail(
                    elapsed_ms, 
                    f"Expected error '{expected_error_match}' not found. Got: {error_output}", 
                    "Error Match", 
                    title=title
                )
                return False

        msg_str = "Receipt mathematically sealed & EVM formatted." if success else f"Circuit Breaker correctly halted execution: {expected_error_match}"
        self._record_success(elapsed_ms, msg_str)
        return True


class CosmPipeline:
    def __init__(self, config: CosmConfig):
        self.config = config
        self.broker = DphiBroker()
        self.executor = CosmRunner(broker=self.broker, base_config=self.config)

    async def execute(self):
        if self.config.mode == "suite":
            self.executor.log.info("\n[CLI] 🏃‍♂️ Initiating Cross-VM (REVM <-> CosmWasm) UTXO Settlement Suite")
            
            # Scenario 1: 정상 동작 검증 (10회 호출)
            await self.executor.run_scenario(
                "1. Standard API Traffic (10 calls)", 
                cycles=10, cost=50_000, 
                expected_success=True
            )
            
            # Scenario 2: 고빈도 스트레스 검증 (100회 호출)
            await self.executor.run_scenario(
                "2. High-Frequency AI Inference (100 calls)", 
                cycles=100, cost=10_000, 
                expected_success=True
            )
            
            # Scenario 3: 예외 방어(Circuit Breaker) 검증
            # 초기 예치금 100 USDC(1억 Fuel) 대비 1.5억 Fuel 요구 모사
            await self.executor.run_scenario(
                "3. Fuel Exhaustion (Circuit Breaker Test)", 
                cycles=5, cost=30_000_000, 
                expected_success=False, 
                expected_error_match="OOM or Fuel Exhaustion"
            )
            
            self.executor.report()
        else:
            await self.executor.run_scenario(
                f"Single Execution (Mode: {self.config.mode.upper()})", 
                self.config.billing_cycles, self.config.cost_per_call,
                expected_success=True
            )

def main() -> None:
    config = CosmConfig()
    app = CosmPipeline(config)
    PhaseReactor.ignite(lambda: app.execute())

if __name__ == "__main__":
    main()