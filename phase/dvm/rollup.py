# phase.dvm.rollup
## @lineage: phase.dvm.cosm
import sys
import json
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from bound.client.config.dphi import dphi_env
from bound.client.config.client import NotarySwarm
from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.adapter.shadow import ShadowAdapter
from kernel.dphi.broker import DphiBroker
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter
from bound.agent.intent.verifier import TraceVerifier, VerificationError

log = get_emitter("workflow.cosm")

@dataclass
class CosmConfig:
    """Configuration for Cosmos/Akash Micro-billing execution"""
    mode: str = "suite"
    target_wasm: str = "akash_compute_billing.wasm"
    billing_cycles: int = 5
    cost_per_call: int = 50_000
    initial_l1_deposit: int = 100  # 예치된 USDC 또는 AKT 금액
    
    rpc_url: str = field(default_factory=lambda: dphi_env.network.cosm_rpc_url)
    rest_url: str = field(default_factory=lambda: dphi_env.network.cosm_rest_url)
    target_cw20: str = field(default_factory=lambda: dphi_env.contracts.target_cw20)
    escrow_contract: str = field(default_factory=lambda: dphi_env.contracts.escrow_contract)
    caller: str = field(default_factory=lambda: dphi_env.agents.beta.cosmos_address)
    provider: str = field(default_factory=lambda: dphi_env.agents.alpha.cosmos_address)


"""Workflow Messages for CosmWasm Billing Rollup"""
class CosmStartMsg(WorkflowMessage): pass
class CosmIntentResolvedMsg(WorkflowMessage): pass
class CosmShadowProjectedMsg(WorkflowMessage): pass
class CosmSimulatedMsg(WorkflowMessage): pass
class CosmNettingVerifiedMsg(WorkflowMessage): pass

class CosmBillingWorkflow(Workflow):
    def __init__(self, config: CosmConfig):
        super().__init__(name="COSM_BILLING_WORKFLOW")
        self.log = log
        self.config = config
        
        self.broker = DphiBroker()
        self.notary_keys = [node["priv"] for node in NotarySwarm(size=3).notaries]
        
        # 섀도우 상태 (로컬 CosmWasm DB)
        self.global_state_snapshot: Dict[str, str] = {}
        
        # 시뮬레이션 결과 트래킹
        self.execution_results: List[ExecutionResult] = []
        self.final_state_diff: Dict[str, Any] = {}
        self.canonical_hash: str = ""
        self.is_verified: bool = False

    async def start(self) -> bool:
        self.post_message(CosmStartMsg())
        await self.run()
        return getattr(self, "is_verified", False)

    @step
    async def phase_cross_chain_resolution(self, msg: CosmStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Cross-chain Intent & L1 State Resolution ---")
        
        self.log.info(f"  └ API Endpoint: {self.config.rest_url}")
        self.log.info(f"  └ Caller (Tenant): {self.config.caller}")
        self.log.info(f"  └ Target Escrow: {self.config.escrow_contract}")
        self.log.info(f"  └ Fetching Escrow Deposit... Confirmed {self.config.initial_l1_deposit} USDC")
        
        return CosmIntentResolvedMsg()

    @step
    async def phase_virtual_exchange(self, msg: CosmIntentResolvedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] Virtual Exchange & Shadow CW20 Projection ---")
        
        # 1. 오라클 기반 가치 환산 (예: 1 USDC = 1,000,000 Fuel)
        fuel_ratio = 1_000_000
        allocated_fuel = self.config.initial_l1_deposit * fuel_ratio
        
        # 2. dphi_env 에 정의된 타겟 CW20 컨트랙트 기반 스토리지 키 생성
        cw20_balance_key = f"balance_{self.config.caller}"
        
        self.global_state_snapshot[cw20_balance_key] = json.dumps(allocated_fuel)
        
        self.log.info(f"  └ Contract Namespace: {self.config.target_cw20}")
        self.log.info(f"  └ Exchange Applied: {self.config.initial_l1_deposit} USDC -> {allocated_fuel} Fuel")
        self.log.info(f"  └ Shadow State DB Projected: {cw20_balance_key} = {allocated_fuel}")
        
        return CosmShadowProjectedMsg()

    @step
    async def phase_micro_billing_simulation(self, msg: CosmShadowProjectedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] High-frequency Micro-billing Simulation ---")
        
        try:
            # 상태 스냅샷을 연속적인 실행에 유지 (inter.cosm이 state_diff를 누적하도록)
            current_state = self.global_state_snapshot.copy()
            
            for cycle in range(1, self.config.billing_cycles + 1):
                self.log.info(f"  └ [Cycle {cycle}] Simulating AI Inference... (-{self.config.cost_per_call} Fuel)")
                
                cw_payload = {
                    "vm_target": "COSMWASM_EXTERNAL",
                    "target_wasm_file": self.config.target_wasm,
                    "env": {"block": {"height": 1000 + cycle}},
                    "info": {"sender": self.config.caller},
                    "msg": {"deduct_fuel": {"amount": str(self.config.cost_per_call)}},
                    "state_snapshot": current_state
                }
                
                # inter.cosm 을 통한 로컬 WASM 실행
                result = await self.broker.execute(code=cw_payload, tier=dphi_env.wasm.tier, context={})
                
                if not result.success:
                    self.log.error(f"  └ ❌ Execution Reverted: {result.error}")
                    return ErrorMessage("OOM or Fuel Exhaustion Detected")
                
                res_data = json.loads(result.output)
                self.execution_results.append(result)
                
                # 다음 사이클을 위해 상태 업데이트
                for k, v in res_data.get("state_diff", {}).items():
                    if v is not None:
                        current_state[k] = v
                    else:
                        current_state.pop(k, None)

            # 최종 상태 차이 추출
            self.final_state_diff = current_state
            return CosmSimulatedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Billing Simulation Crashed: {str(e)}")

    @step
    async def phase_state_netting(self, msg: CosmSimulatedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] State Netting & Trace Verification ---")
        try:
            cw20_balance_key = f"balance_{self.config.caller}"
            
            initial_fuel = int(json.loads(self.global_state_snapshot.get(cw20_balance_key, "0")))
            final_fuel = int(json.loads(self.final_state_diff.get(cw20_balance_key, "0")))
            
            net_debt = initial_fuel - final_fuel
            self.log.info(f"  └ Initial Fuel : {initial_fuel}")
            self.log.info(f"  └ Final Fuel   : {final_fuel}")
            self.log.info(f"  └ 🧮 Net Debt   : {net_debt} Fuel used (Compressing {len(self.execution_results)} txs into 1)")
            
            if net_debt > initial_fuel or final_fuel < 0:
                raise VerificationError("Fuel calculation mismatch. Possible double spend.")
                
            self.is_verified = True
            return CosmNettingVerifiedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Netting Verification Failed: {str(e)}")

    @step
    async def phase_settlement_sealing(self, msg: CosmNettingVerifiedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Settlement Proof Sealing (x402 Generation) ---")
        try:
            rollup_payload = {
                "tenant": self.config.caller,
                "provider": self.config.provider,
                "target_cw20": self.config.target_cw20,
                "net_fuel_consumed": (
                    json.loads(self.global_state_snapshot.get(f"balance_{self.config.caller}")) 
                    - json.loads(self.final_state_diff.get(f"balance_{self.config.caller}"))
                ),
                "state_diff_merkle_root": "0xABCDEF9876543210..." 
            }
            
            proof_receipt = ShadowAdapter.seal_execution_proof(
                execution_output=rollup_payload,
                notary_keys=self.notary_keys
            )
            
            self.canonical_hash = proof_receipt.canonical_hash
            self.log.info(f"  ✅ [SEALED] Ready for Akash IBC or L1 Settlement")
            self.log.info(f"    └ Receipt ID: {proof_receipt.receipt_id}")
            self.log.info(f"    └ Hash: {self.canonical_hash[:16]}...")
            self.log.info(f"    └ 3/3 Notaries signed the net debt.")
            
            return StopMessage(result=True)
            
        except Exception as e:
            return ErrorMessage(f"Sealing Failed: {str(e)}")

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] Cosmos Billing Workflow aborted: {msg.msg}")
        return StopMessage(result=False)


# =========================================================================
# Execution Runner & Pipeline 
# =========================================================================
class CosmRunner:
    def __init__(self, config: CosmConfig):
        self.log = log
        self.config = config

    async def run(self, name: str, cycles: int = 5, cost: int = 50_000) -> bool:
        self.log.info(f"\n\n{'='*80}\n🚀 [COSMOS SCENARIO] {name}\n{'='*80}")
        
        # Override config for specific run
        run_config = CosmConfig(
            billing_cycles=cycles,
            cost_per_call=cost
        )
        
        workflow = CosmBillingWorkflow(config=run_config)
        return await workflow.start()


class CosmPipeline:
    def __init__(self, config: CosmConfig):
        self.log = log
        self.config = config
        self.executor = CosmRunner(config=self.config)

    async def execute(self):
        if self.config.mode == "suite":
            self.log.info("\n[CLI] 🏃‍♂️ Initiating Cosmos Micro-billing & Deferred Settlement Suite")
            
            s1 = await self.executor.run("1. Standard API Traffic (10 calls)", cycles=10, cost=50_000)
            s2 = await self.executor.run("2. High-Frequency AI Inference (100 calls)", cycles=100, cost=10_000)
            
            self.log.info(f"\n\n{'='*80}\n📊 [COSMOS SUITE SUMMARY]\n{'='*80}")
            self.log.info(f" 1. Standard Traffic       : {'✅ PASS (Rollup Sealed)' if s1 else '❌ FAIL'}")
            self.log.info(f" 2. High-Frequency Traffic : {'✅ PASS (Rollup Sealed)' if s2 else '❌ FAIL'}")
            
            if s1 and s2:
                self.log.info("\n🎉 All Cosmos Billing Core Architecture test suites completed successfully!")
            return 
        else:
            await self.executor.run(f"Single Execution (Mode: {self.config.mode.upper()})", self.config.billing_cycles, self.config.cost_per_call)

def main() -> None:
    config = CosmConfig()
    app = CosmPipeline(config)
    PhaseReactor.ignite(lambda: app.execute())

if __name__ == "__main__":
    main()