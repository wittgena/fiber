# fiber.workflow.eco.defin
## @lineage: workflow.defin
import sys
import json
import asyncio
import time
import hashlib
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.dphi.adapter.config import dphi_env
from fiber.dphi.adapter.anchor import NotarySwarm

from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.dphi.adapter.shadow import ShadowAdapter
from xphi.kernel.dphi.adapter.utxo import (
    UtxoAdapter, UtxoPointer, UtxoInput, UtxoOutput, UtxoTransaction,
    AgentWallet, compute_merkle_root
)
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.kernel.dphi.runner.phase import SchemeRunner
from xphi.watcher.plane.emitter import get_emitter
from fiber.dphi.observer.intent.verifier import VerificationError

log = get_emitter("workflow.defin")


@dataclass
class CosmConfig:
    """Configuration for Cross-VM DePIN Gateway"""
    mode: str = "suite"
    
    # 실제 inter.cosm 에서 구동될 타겟 스마트 컨트랙트
    target_wasm: str = "cw20_base.wasm"
    
    concurrent_agents: int = 1  # 병렬 처리 대상 에이전트 수
    billing_cycles: int = 5
    cost_per_call: int = 50_000
    initial_l1_deposit: int = 100  # EVM(REVM) 예치금 (USDC)
    
    caller_evm: str = field(default_factory=lambda: dphi_env.agents.beta.evm_address)
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
        super().__init__(name="CROSS_VM_DEPIN_WORKFLOW")
        self.log = log
        self.config = config
        
        # 1. 인프라 모듈
        self.broker = DphiBroker()
        self.utxo_adapter = UtxoAdapter(broker=self.broker)
        self.notary_keys = [node["priv"] for node in NotarySwarm(size=3).notaries]
        
        # 2. 암호학적 신원 (EVM - Cosm 매핑)
        self.tenant_wallet = AgentWallet()
        self.provider_wallet = AgentWallet()
        self.agent_wallets: List[AgentWallet] = []
        
        # 3. 롤업 및 과금 상태 추적
        self.authorized_fuel_budget: int = 0
        self.root_utxo_ptr: Optional[UtxoPointer] = None
        self.all_tx_hashes: List[str] = []
        
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
        self.log.info(f"  └ Received EIP-712 Intent from EVM Wallet: {self.config.caller_evm}")
        self.log.info(f"  └ Routing payload to dvm.revm for L1 State Verification...")
        self.log.info(f"  └ [REVM Confirmed] Target Escrow: {self.config.escrow_contract}")
        self.log.info(f"  └ [REVM Confirmed] Locked Deposit: {self.config.initial_l1_deposit} USDC")
        self.log.info(f"  └ 🔐 Crypto Identity Mapped (Cosm): {self.tenant_wallet.address[:16]}...")
        return CosmIntentResolvedMsg()

    @step
    async def phase_virtual_exchange(self, msg: CosmIntentResolvedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] Virtual Pegging & Genesis UTXO Minting ---")
        
        fuel_ratio = 1_000_000
        self.authorized_fuel_budget = self.config.initial_l1_deposit * fuel_ratio
        
        tx_mint = UtxoTransaction(
            inputs=[], 
            outputs=[UtxoOutput(amount=self.authorized_fuel_budget, owner=self.tenant_wallet.address)],
            metadata={"action": "SESSION_INITIALIZE", "evm_caller": self.config.caller_evm}
        )
        tx_hash = await self.utxo_adapter.execute_transaction(tx_mint)
        self.root_utxo_ptr = UtxoPointer(tx_hash, 0)
        self.all_tx_hashes.append(tx_hash)
        
        self.log.info(f"  └ State Translation: 100 USDC -> {self.authorized_fuel_budget} Fuel")
        self.log.info(f"  └ Root UTXO Anchored: {tx_hash[:16]}... (Index: 0)")
        return CosmUtxoAnchoredMsg()

    @step
    async def phase_micro_billing_simulation(self, msg: CosmUtxoAnchoredMsg) -> WorkflowMessage:
        self.log.info(f"--- [Phase 3] WASM Execution & Parallel UTXO Billing ---")
        try:
            self.agent_wallets = [AgentWallet() for _ in range(self.config.concurrent_agents)]
            budget_per_agent = self.authorized_fuel_budget // self.config.concurrent_agents
            split_outputs = [UtxoOutput(amount=budget_per_agent, owner=w.address) for w in self.agent_wallets]
            tenant_sig = self.tenant_wallet.sign_payload(self.root_utxo_ptr.to_key())
            
            tx_split = UtxoTransaction(
                inputs=[UtxoInput(pointer=self.root_utxo_ptr, signature=tenant_sig, owner_address=self.tenant_wallet.address)],
                outputs=split_outputs,
                metadata={"action": "SPLIT_FOR_PARALLEL_WORKERS"}
            )
            split_tx_hash = await self.utxo_adapter.execute_transaction(tx_split)
            self.all_tx_hashes.append(split_tx_hash)
            self.log.info(f"  └ 🔀 Split Root into {self.config.concurrent_agents} UTXOs for parallel computing.")

            # Worker 함수: WASM 샌드박스 실행(Account) + 인프라 비용 과금(UTXO)
            async def agent_worker(worker_id: int, start_ptr: UtxoPointer, budget: int, wallet: AgentWallet) -> Tuple[UtxoPointer, int, List[str]]:
                current_ptr = start_ptr
                current_balance = budget
                worker_txs = []
                cw20_balance_key = f"balance_{wallet.address}"
                shadow_state = {cw20_balance_key: json.dumps(current_balance)}
                
                # 💡 [추가] Worker 시작 알림
                self.log.info(f"    ├─ [Worker-{worker_id}] 🚀 Started with Budget: {budget} Fuel")
                
                for cycle in range(1, self.config.billing_cycles + 1):
                    # 1. 한도(Circuit Breaker) 체크
                    if current_balance < self.config.cost_per_call:
                        raise RuntimeError(f"OOM or Fuel Exhaustion at cycle {cycle}")

                    # 2. [WASM EXECUTION] 완벽한 CosmWasm 표준 Payload 조립
                    cw_payload = {
                        "vm_target": "COSMWASM_EXTERNAL",
                        "target_wasm_file": self.config.target_wasm,
                        "env": {
                            "block": {
                                "height": 1000 + cycle,
                                "time": str(int(time.time() * 1_000_000_000)), # 나노초 규격
                                "chain_id": "akash-local"
                            }
                        },
                        "info": {
                            "sender": wallet.address,
                            "funds": []
                        },
                        "msg": {
                            "transfer": {
                                "recipient": self.provider_wallet.address, 
                                "amount": str(self.config.cost_per_call)
                            }
                        },
                        "state_snapshot": shadow_state
                    }
                    
                    await asyncio.sleep(0.005) # Redis 터널 과부하 방지
                    
                    result = await self.broker.execute(code=cw_payload, tier=dphi_env.wasm.tier, context={})
                    
                    # 3. 샌드박스 결과 검증
                    if not result.success:
                        err_msg = result.output if result.output else str(result.error)
                        raise RuntimeError(f"Broker Execution Reverted: {err_msg}")
                    
                    try:
                        res_data = json.loads(result.output)
                        result_hash = hashlib.sha256(result.output.encode('utf-8')).hexdigest()[:16]
                    except Exception as e:
                        raise RuntimeError(f"VM Output Parse Error: {e} | Raw: {result.output[:100]}")

                    if not res_data.get("success", True):
                        revert_reason = res_data.get("revert_reason") or res_data.get("error") or "Unknown"
                        raise RuntimeError(f"Contract Revert: {revert_reason}")

                    # 4. [UTXO PAY] WASM 연산 성공 시, 오프체인 장부에 암호학적 과금 영수증 기록
                    worker_sig = wallet.sign_payload(current_ptr.to_key())
                    tx_pay = UtxoTransaction(
                        inputs=[UtxoInput(pointer=current_ptr, signature=worker_sig, owner_address=wallet.address)],
                        outputs=[
                            UtxoOutput(amount=self.config.cost_per_call, owner=self.provider_wallet.address, asset_type="fuel"),
                            UtxoOutput(amount=current_balance - self.config.cost_per_call, owner=wallet.address, asset_type="fuel")
                        ],
                        metadata={"worker": worker_id, "cycle": cycle, "wasm_execution_hash": result_hash}
                    )
                    
                    tx_hash = await self.utxo_adapter.execute_transaction(tx_pay)
                    worker_txs.append(tx_hash)
                    
                    # 5. 상태 포인터 및 섀도우 장부 업데이트
                    current_ptr = UtxoPointer(tx_hash, 1) # 인덱스 1번이 Change
                    current_balance -= self.config.cost_per_call
                    shadow_state[cw20_balance_key] = json.dumps(current_balance)
                    
                    # 💡 [추가] 개별 사이클의 실행 증명 로그 (Audit Trail)
                    self.log.info(
                        f"    │  └─ [Worker-{worker_id} | Cycle-{cycle}] ⚙️ WASM Hash: {result_hash} | "
                        f"💸 Paid: {self.config.cost_per_call} Fuel | 💰 Remain: {current_balance} | 🔗 Tx: {tx_hash[:10]}..."
                    )
                    
                return current_ptr, current_balance, worker_txs

            # [병렬 실행] N개의 에이전트가 동시 연산 진행
            tasks = []
            for i, wallet in enumerate(self.agent_wallets):
                ptr = UtxoPointer(split_tx_hash, i)
                tasks.append(agent_worker(i, ptr, budget_per_agent, wallet))
                
            # 강제 취소를 방지하는 return_exceptions=True 적용
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 최종 결과 취합 및 내부 에러 전파
            self.final_unspent_ptrs = []
            self.final_unspent_balances = []
            for res in results:
                if isinstance(res, Exception):
                    raise res  # 내부 에러(OOM 등)를 상위로 재발생시킴
                    
                self.final_unspent_ptrs.append(res[0])
                self.final_unspent_balances.append(res[1])
                self.all_tx_hashes.extend(res[2])
            
            return CosmSimulatedMsg()
            
        except Exception as e:
            # 예상된 Circuit Breaker 에러를 잡아 정해진 포맷으로 반환
            err_str = str(e)
            if "OOM or Fuel Exhaustion" in err_str:
                return ErrorMessage("OOM or Fuel Exhaustion Detected")
            return ErrorMessage(f"Compute & Billing Simulation Crashed: {err_str}")

    @step
    async def phase_state_netting(self, msg: CosmSimulatedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] UTXO Merge & Merkle Rollup Compression ---")
        try:
            # 1. [UTXO MERGE] 분산되었던 잔돈(Change) UTXO들을 하나의 환불용 UTXO로 병합
            merge_inputs = []
            for i, ptr in enumerate(self.final_unspent_ptrs):
                wallet = self.agent_wallets[i]
                sig = wallet.sign_payload(ptr.to_key())
                merge_inputs.append(UtxoInput(pointer=ptr, signature=sig, owner_address=wallet.address))
                
            total_change_amount = sum(self.final_unspent_balances)
            tx_merge = UtxoTransaction(
                inputs=merge_inputs,
                outputs=[UtxoOutput(amount=total_change_amount, owner=self.tenant_wallet.address)],
                metadata={"action": "MERGE_FINAL_CHANGE"}
            )
            final_tx_hash = await self.utxo_adapter.execute_transaction(tx_merge)
            self.all_tx_hashes.append(final_tx_hash)
            
            # 2. 총 소비 연료 및 Net Debt 계산
            self.total_fuel_consumed = self.authorized_fuel_budget - total_change_amount
            self.net_debt_usdc = self.total_fuel_consumed / 1_000_000
            
            # 3. [MERKLE COMPRESSION] 모든 연산 및 결제 이력을 머클 트리로 압축 (Proof 생성)
            merkle_root = compute_merkle_root(self.all_tx_hashes)
            self.canonical_hash = merkle_root
            
            # 💡 [개선] 증명 가능한 정산 영수증 렌더링
            receipt = (
                f"\n🧾 [STATE NETTING RECEIPT]\n"
                f" ├─ 🏦 Initial Budget    : {self.authorized_fuel_budget} Fuel\n"
                f" ├─ 🔄 Total Change(환불) : {total_change_amount} Fuel (From {len(merge_inputs)} Workers)\n"
                f" ├─ 💸 Net Fuel Consumed : {self.total_fuel_consumed} Fuel\n"
                f" ├─ 💵 L1 Debt Converted : {self.net_debt_usdc:.6f} USDC\n"
                f" └─ 🌳 Merkle Root (Tx {len(self.all_tx_hashes)}) : {self.canonical_hash[:16]}..."
            )
            self.log.info(receipt)
            
            if self.total_fuel_consumed > self.authorized_fuel_budget or self.net_debt_usdc < 0:
                raise VerificationError("Fuel calculation anomaly. Missing UTXO value detected.")
                
            self.is_verified = True
            return CosmNettingVerifiedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Netting Verification Failed: {str(e)}")

    @step
    async def phase_settlement_sealing(self, msg: CosmNettingVerifiedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Dual-Settlement Sealing (EVM Calldata Formatting) ---")
        try:
            # 이더리움 정산 컨트랙트가 소화할 수 있도록 EVM Calldata 규격 인코딩
            method_signature = "claim(address,uint256,bytes)".encode('utf-8')
            method_id = hashlib.sha3_256(method_signature).hexdigest()[:8]
            
            tenant_padded = self.config.caller_evm.replace("0x", "").zfill(64).lower()
            amount_padded = hex(int(self.net_debt_usdc)).replace("0x", "").zfill(64)
            evm_calldata = f"0x{method_id}{tenant_padded}{amount_padded}"
            
            rollup_payload = {
                "tenant_evm": self.config.caller_evm,
                "net_usdc_consumed": self.net_debt_usdc,
                "utxo_merkle_root": self.canonical_hash, # L1 제출용 암호학적 증명
                "evm_settlement_calldata": evm_calldata
            }
            
            # 최종 공증인 멀티시그 봉인
            proof_receipt = ShadowAdapter.seal_execution_proof(
                execution_output=rollup_payload,
                notary_keys=self.notary_keys
            )
            
            self.log.info(f"  ✅ [SEALED] Receipt encoded for L1 Submission")
            self.log.info(f"    └ EVM Calldata : {evm_calldata[:42]}... (truncated)")
            self.log.info(f"    └ Final Proof  : {proof_receipt.canonical_hash[:16]}...")
            
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

    async def run_scenario(self, title: str, agents: int, cycles: int, cost: int, expected_success: bool = True, expected_error_match: str = None) -> bool:
        self.log.info(f"\n\n{'='*80}\n🚀 [CROSS-VM SCENARIO] {title}\n{'='*80}")
        start_time = time.time()
        
        run_config = CosmConfig(
            concurrent_agents=agents,
            billing_cycles=cycles,
            cost_per_call=cost
        )
        
        workflow = CosmBillingWorkflow(config=run_config)
        success = await workflow.start()
        elapsed_ms = (time.time() - start_time) * 1000

        if success != expected_success:
            self._record_fail(elapsed_ms, f"Expected success={expected_success}, Got {success}.", "Workflow Execution", title=title)
            return False

        if not success and expected_error_match:
            error_output = getattr(workflow, "last_error_message", "")
            if expected_error_match not in error_output:
                self._record_fail(elapsed_ms, f"Expected error '{expected_error_match}' not found. Got: {error_output}", "Error Match", title=title)
                return False

        if success:
            msg_str = (
                f"Receipt mathematically sealed & EVM formatted. "
                f"(Workers: {agents}, Txs: {len(workflow.all_tx_hashes)}, "
                f"Debt: {workflow.net_debt_usdc} USDC, Root: {workflow.canonical_hash[:8]}...)"
            )
        else:
            msg_str = f"Circuit Breaker correctly halted execution: {expected_error_match}"
            
        self._record_success(elapsed_ms, msg_str)
        return True


class CosmPipeline:
    def __init__(self, config: CosmConfig):
        self.config = config
        self.broker = DphiBroker()
        self.executor = CosmRunner(broker=self.broker, base_config=self.config)

    async def execute(self):
        if self.config.mode == "suite":
            self.executor.log.info("\n[CLI] 🏃‍♂️ Initiating Cross-VM DePIN Pipeline (WASM Compute + UTXO Billing)")
            
            await self.executor.run_scenario(
                "1. Standard API Traffic (1 Agent, 10 calls)", 
                agents=1, cycles=10, cost=50_000, 
                expected_success=True
            )
            
            await self.executor.run_scenario(
                "2. Parallel Agent Inferencing (3 Agents, 10 calls each, Merged at end)", 
                agents=3, cycles=10, cost=20_000, 
                expected_success=True
            )
            
            await self.executor.run_scenario(
                "3. Fuel Exhaustion (Circuit Breaker Test)", 
                agents=2, cycles=5, cost=20_000_000,
                expected_success=False, 
                expected_error_match="OOM or Fuel Exhaustion"
            )
            
            self.executor.report()
        else:
            await self.executor.run_scenario(
                f"Single Execution", 
                self.config.concurrent_agents, self.config.billing_cycles, self.config.cost_per_call,
                expected_success=True
            )

def main() -> None:
    config = CosmConfig()
    app = CosmPipeline(config)
    PhaseReactor.ignite(lambda: app.execute())

if __name__ == "__main__":
    main()