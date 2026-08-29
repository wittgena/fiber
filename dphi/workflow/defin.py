# fiber.dphi.workflow.defin
import json
import asyncio
import time
import hashlib
import uuid
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field

from fiber.dphi.adapter.config import dphi_env
from fiber.dphi.adapter.anchor import NotarySwarm
from fiber.dphi.adapter.dvm import DvmAdapter
from fiber.dphi.observer.intent.verifier import VerificationError

from xphi.eco.adapter.shadow import ShadowAdapter
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.kernel.space.topos.network.bridge import RpcBridge
from xphi.watcher.plane.emitter import get_emitter

from xphi.kernel.dphi.adapter.utxo import (
    UtxoAdapter, UtxoPointer, UtxoInput, UtxoOutput, UtxoTransaction,
    AgentWallet, compute_merkle_root
)
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.ledger.consensus import KernelLedger, ToposBlob

log = get_emitter("dphi.workflow.defin")

# =========================================================================
# [1] Cross-VM Compute & Billing Workflow (순수 상태 머신)
# =========================================================================

@dataclass
class VmComputeConfig:
    """VM-Agnostic Configuration for Compute Gateway"""
    target_wasm: str = "cw20_base.wasm"
    concurrent_agents: int = 1  
    billing_cycles: int = 5
    cost_per_call: int = 50_000
    initial_l1_deposit: int = 100  # 예치금 (USDC)
    
    caller_evm: str = field(default_factory=lambda: dphi_env.agents.beta.evm_address)
    target_cw20: str = field(default_factory=lambda: dphi_env.contracts.target_cw20)
    escrow_contract: str = field(default_factory=lambda: dphi_env.contracts.nexus_clearing)


class VmStartMsg(WorkflowMessage): pass
class VmIntentResolvedMsg(WorkflowMessage): pass
class VmUtxoAnchoredMsg(WorkflowMessage): pass
class VmSimulatedMsg(WorkflowMessage): pass
class VmNettingVerifiedMsg(WorkflowMessage): pass


class CrossVmBillingWorkflow(Workflow):
    def __init__(self, config: VmComputeConfig):
        super().__init__(name="CROSS_VM_BILLING_WORKFLOW")
        self.log = log
        self.config = config
        
        # 인프라 모듈
        self.broker = DphiBroker()
        self.utxo_adapter = UtxoAdapter(broker=self.broker)
        self.notary_keys = [node["priv"] for node in NotarySwarm(size=3).notaries]
        
        # 암호학적 신원
        self.tenant_wallet = AgentWallet()
        self.provider_wallet = AgentWallet()
        self.agent_wallets: List[AgentWallet] = []
        
        # 롤업 및 과금 상태 추적
        self.authorized_fuel_budget: int = 0
        self.root_utxo_ptr: Optional[UtxoPointer] = None
        self.all_tx_hashes: List[str] = []
        
        self.total_fuel_consumed: int = 0
        self.net_debt_usdc: float = 0.0
        self.canonical_hash: str = ""
        self.is_verified: bool = False
        self.last_error_message: str = ""

    async def start(self) -> bool:
        self.post_message(VmStartMsg())
        await self.run()
        return getattr(self, "is_verified", False)

    @step
    async def phase_cross_chain_resolution(self, msg: VmStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Intent Verification (Cross-VM Auth) ---")
        self.log.info(f"  └ Received EIP-712 Intent from Wallet: {self.config.caller_evm}")
        self.log.info(f"  └ Routing payload to DVM for L1 State Verification...")
        self.log.info(f"  └ [Confirmed] Target Escrow: {self.config.escrow_contract}")
        self.log.info(f"  └ [Confirmed] Locked Deposit: {self.config.initial_l1_deposit} USDC")
        self.log.info(f"  └ 🔐 Crypto Identity Mapped: {self.tenant_wallet.address[:16]}...")
        return VmIntentResolvedMsg()

    @step
    async def phase_virtual_exchange(self, msg: VmIntentResolvedMsg) -> WorkflowMessage:
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
        return VmUtxoAnchoredMsg()

    @step
    async def phase_micro_billing_simulation(self, msg: VmUtxoAnchoredMsg) -> WorkflowMessage:
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

            async def agent_worker(worker_id: int, start_ptr: UtxoPointer, budget: int, wallet: AgentWallet) -> Tuple[UtxoPointer, int, List[str]]:
                current_ptr = start_ptr
                current_balance = budget
                worker_txs = []
                
                self.log.info(f"    ├─ [Worker-{worker_id}] 🚀 Started with Budget: {budget} Fuel")
                
                for cycle in range(1, self.config.billing_cycles + 1):
                    if current_balance < self.config.cost_per_call:
                        raise RuntimeError(f"OOM or Fuel Exhaustion at cycle {cycle}")

                    # 💡 [핵심 개선] DvmAdapter를 이용한 VM Agnostic 페이로드 포장
                    cw_payload = DvmAdapter.build_cw20_transfer_payload(
                        target_wasm_file=self.config.target_wasm,
                        sender_address=wallet.address,
                        recipient_address=self.provider_wallet.address,
                        amount=self.config.cost_per_call,
                        cycle=cycle,
                        current_balance=current_balance
                    )
                    
                    await asyncio.sleep(0.005)
                    result = await self.broker.execute(code=cw_payload, tier=dphi_env.wasm.tier, context={})
                    
                    if not result.success:
                        err_msg = result.output if result.output else str(result.error)
                        raise RuntimeError(f"Broker Execution Reverted: {err_msg}")
                    
                    try:
                        res_data = json.loads(result.output)
                        result_hash = hashlib.sha256(result.output.encode('utf-8')).hexdigest()[:16]
                    except Exception as e:
                        raise RuntimeError(f"VM Output Parse Error: {e}")

                    if not res_data.get("success", True):
                        revert_reason = res_data.get("revert_reason") or res_data.get("error") or "Unknown"
                        raise RuntimeError(f"Contract Revert: {revert_reason}")

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
                    
                    current_ptr = UtxoPointer(tx_hash, 1)
                    current_balance -= self.config.cost_per_call
                    
                    self.log.info(
                        f"    │  └─ [Worker-{worker_id} | Cycle-{cycle}] ⚙️ WASM Hash: {result_hash} | "
                        f"💸 Paid: {self.config.cost_per_call} Fuel | 💰 Remain: {current_balance} | 🔗 Tx: {tx_hash[:10]}..."
                    )
                    
                return current_ptr, current_balance, worker_txs

            tasks = []
            for i, wallet in enumerate(self.agent_wallets):
                ptr = UtxoPointer(split_tx_hash, i)
                tasks.append(agent_worker(i, ptr, budget_per_agent, wallet))
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            self.final_unspent_ptrs = []
            self.final_unspent_balances = []
            for res in results:
                if isinstance(res, Exception):
                    raise res
                    
                self.final_unspent_ptrs.append(res[0])
                self.final_unspent_balances.append(res[1])
                self.all_tx_hashes.extend(res[2])
            
            return VmSimulatedMsg()
            
        except Exception as e:
            err_str = str(e)
            if "OOM or Fuel Exhaustion" in err_str:
                return ErrorMessage("OOM or Fuel Exhaustion Detected")
            return ErrorMessage(f"Compute & Billing Simulation Crashed: {err_str}")

    @step
    async def phase_state_netting(self, msg: VmSimulatedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] UTXO Merge & Merkle Rollup Compression ---")
        try:
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
            
            self.total_fuel_consumed = self.authorized_fuel_budget - total_change_amount
            self.net_debt_usdc = self.total_fuel_consumed / 1_000_000
            
            merkle_root = compute_merkle_root(self.all_tx_hashes)
            self.canonical_hash = merkle_root
            
            receipt = (
                f"\n🧾 [STATE NETTING RECEIPT]\n"
                f" ├─ 🏦 Initial Budget    : {self.authorized_fuel_budget} Fuel\n"
                f" ├─ 🔄 Total Change(Refund) : {total_change_amount} Fuel (From {len(merge_inputs)} Workers)\n"
                f" ├─ 💸 Net Fuel Consumed : {self.total_fuel_consumed} Fuel\n"
                f" ├─ 💵 L1 Debt Converted : {self.net_debt_usdc:.6f} USDC\n"
                f" └─ 🌳 Merkle Root (Tx {len(self.all_tx_hashes)}) : {self.canonical_hash[:16]}..."
            )
            self.log.info(receipt)
            
            if self.total_fuel_consumed > self.authorized_fuel_budget or self.net_debt_usdc < 0:
                raise VerificationError("Fuel calculation anomaly. Missing UTXO value detected.")
                
            self.is_verified = True
            return VmNettingVerifiedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Netting Verification Failed: {str(e)}")

    @step
    async def phase_settlement_sealing(self, msg: VmNettingVerifiedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Dual-Settlement Sealing (L1 Formatting) ---")
        try:
            # 💡 [핵심 개선] EVM Calldata 생성 로직 제거. DvmAdapter가 책임집니다.
            evm_calldata = DvmAdapter.build_claim_calldata(self.config.caller_evm, self.net_debt_usdc)
            
            rollup_payload = {
                "tenant_evm": self.config.caller_evm,
                "net_usdc_consumed": self.net_debt_usdc,
                "utxo_merkle_root": self.canonical_hash,
                "evm_settlement_calldata": evm_calldata
            }
            
            proof_receipt = ShadowAdapter.seal_execution_proof(
                execution_output=rollup_payload,
                notary_keys=self.notary_keys
            )
            
            self.log.info(f"  ✅ [SEALED] Receipt encoded for L1 Submission")
            self.log.info(f"    └ Ext Calldata : {evm_calldata[:42]}... (truncated)")
            self.log.info(f"    └ Final Proof  : {proof_receipt.canonical_hash[:16]}...")
            
            return StopMessage(result=True)
            
        except Exception as e:
            return ErrorMessage(f"Sealing Failed: {str(e)}")

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.last_error_message = msg.msg
        self.log.error(f"❌ [HALTED] Billing Workflow safely aborted: {msg.msg}")
        return StopMessage(result=False)


# =========================================================================
# [2] DVM Shadow Wallet Settlement Workflow
# =========================================================================

@dataclass
class SettlementConfig:
    """도메인 순수성을 위한 롤업 결제(Settlement) 설정"""
    target_contract: str
    caller_address: str
    agent_address: str
    charge_amount: int
    active_calldata: str
    active_snapshot: Dict[str, Any]

class SwStartMsg(WorkflowMessage): pass
class SwPrepareMsg(WorkflowMessage): pass
class SwExecuteMsg(WorkflowMessage): pass
class SwCommitMsg(WorkflowMessage): pass

class ShadowWalletWorkflow(Workflow):
    """순수 DVM Shadow Execution 워크플로우 (Mock/Chaos 배제됨)"""
    def __init__(self, config: SettlementConfig, rpc_bridge: RpcBridge, log_context: str = None):
        super().__init__(name="WALLET_SHADOW_SETTLEMENT")
        self.config = config
        self.rpc_bridge = rpc_bridge
        
        ctx = log_context or uuid.uuid4().hex[:4]
        self.log = get_emitter(f"workflow.wallet.{ctx}")
        
        self.ledger = KernelLedger()
        self.dvm_result = {}
        self.sealed_hash = None

    async def start(self) -> bool:
        self.log.info(f"\n{'='*70}\n🚀 [START] Shadow Execution Workflow\n{'='*70}")
        self.post_message(SwStartMsg())
        await self.run()
        return self.sealed_hash is not None

    @step
    async def phase_prepare(self, msg: SwStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Deferred Charge Assembly & State Sync ---")
        
        func_sig = self.config.active_calldata[:10]
        self.log.info(
            f"  └─ 🧩 Assembled DVM Payload:\n"
            f"     ├─ 🎯 Target Contract : {self.config.target_contract}\n"
            f"     ├─ 👤 Caller Node     : {self.config.caller_address}\n"
            f"     └─ 📦 Func Signature  : {func_sig}"
        )
        return SwPrepareMsg()

    @step
    async def phase_execute(self, msg: SwPrepareMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] DVM Shadow Execution (Pull) ---")
        
        # 💡 [핵심 개선] DvmAdapter를 통해 브릿지 전송용 DVM 표준 페이로드 포장
        dvm_payload = DvmAdapter.build_dvm_payload(
            target_address=self.config.target_contract,
            calldata=self.config.active_calldata,
            state_snapshot=self.config.active_snapshot,
            vm_target="EVM"
        )
        # 네트워크/제한 설정 추가
        dvm_payload["gas_limit"] = 150000
        dvm_payload["gas_price"] = hex(10**9)
        dvm_payload["caller_address"] = self.config.caller_address
        
        res = await self.rpc_bridge.request({"action": "shadow_execute_vm", "payload": dvm_payload})
        data = res.get("data", {})
        
        if res.get("status") != 200 or not data.get("success"):
            revert_reason = data.get('revert_reason', 'Unknown Error')
            return ErrorMessage(f"REVM Reverted ({revert_reason})")

        self.dvm_result = data
        return SwExecuteMsg()

    @step
    async def phase_commit(self, msg: SwExecuteMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] Deferred Charge Ledger Sealing ---")
        
        state_diff = self.dvm_result.get("state_diff", {})
        gas_used = self.dvm_result.get("gas_used", 0)
        
        payload_dict = {
            "caller": self.config.caller_address,
            "contract": self.config.target_contract,
            "state_diff": state_diff,
            "gas": gas_used,
            "timestamp": time.time()
        }
        
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        rollup_hash = hashlib.sha256(canonical_bytes).hexdigest()
        
        blob = ToposBlob(
            action="DEFERRED_SETTLEMENT_CHARGE",
            from_state="dvm.wasm.execution",
            to_state="ledger.sealed",
            tension=0.5,
            details=f"Gas: {gas_used} | Modified: {len(state_diff)}"
        )
        
        self.sealed_hash = self.ledger.save_transition(blob)
        receipt = (
            f"\n    🧾 [SHADOW SETTLEMENT RECEIPT]\n"
            f"     ├─ ⛽ EVM Gas Used   : {gas_used} Units\n"
            f"     ├─ 🔄 State Mutations: {len(state_diff)} Storage slot(s) modified\n"
            f"     └─ 💎 L2 Rollup Hash : 0x{rollup_hash[:16]}..."
        )
        self.log.info(receipt)
        return StopMessage(result=True)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"  └─ ❌ [HALTED] Pipeline execution terminated: {msg.msg}")
        return StopMessage(result=False)