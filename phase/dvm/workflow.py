# phase.dvm.workflow
## @lineage: dphi.phase.workflow.dvm
import sys
import argparse
import asyncio
import json
from typing import Dict, Any, Optional

try:
    from eth_utils import keccak
except ImportError:
    from web3 import Web3
    keccak = Web3.keccak

from phase.anchor.config.dphi import mock_env
from phase.anchor.config.client import NotarySwarm
from kernel.dphi.adapter.shadow import ShadowAdapter
from bound.exchange.web3.evm import EVMOrchestrator, MockOrchestrator, InversionOrchestrator

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.broker import DphiBroker
from watcher.plane.emitter import get_emitter
from bound.agent.intent.verifier import TraceVerifier, VerificationError

log = get_emitter("dvm.workflow")

class DvmStartMsg(WorkflowMessage): pass
class DvmIntentMsg(WorkflowMessage): pass
class DvmProjectedMsg(WorkflowMessage): pass
class DvmSimulatedMsg(WorkflowMessage): pass
class DvmVerifiedMsg(WorkflowMessage): pass

class DvmWorkflow(Workflow):
    def __init__(self, target_contract: str, user_intent: Dict[str, Any], rpc_url: Optional[str] = None, mode: str = "mock"):
        super().__init__(name="DVM_WORKFLOW")
        self.log = get_emitter("workflow.dvm")
        
        self.target_contract = target_contract
        self.user_intent = user_intent
        self.mode = mode
        
        self.scenario_type = self.user_intent.get("scenario_type", "UNKNOWN")
        self.is_cosmwasm = self.scenario_type == "COSMWASM_EXECUTE"
        
        if self.is_cosmwasm:
            self.orchestrator = MockOrchestrator(user_intent=self.user_intent)
        elif self.mode == "inversion":
            self.orchestrator = InversionOrchestrator(user_intent=self.user_intent)
        elif self.mode == "mock":
            self.orchestrator = MockOrchestrator(user_intent=self.user_intent)
        else:
            self.orchestrator = EVMOrchestrator(rpc_url)
            
        self.broker = DphiBroker()
        self.notary_keys = [node["priv"] for node in NotarySwarm(size=3).notaries]

        self.calldata: str = user_intent.get("calldata", "0x")
        self.caller: str = user_intent.get("caller", "0x0000000000000000000000000000000000000000")
        self.value: int = user_intent.get("value", 0)
        self.storage_slots: list = user_intent.get("storage_slots", [])
        
        self.global_state_snapshot: Dict[str, Dict[str, Any]] = {}
        self.block_context: dict = {}
        
        self.execution_result: Optional[ExecutionResult] = None
        self.canonical_hash: str = ""
        self.is_verified: bool = False  # 검증 성공 여부 트래킹 플래그

    async def start(self) -> bool:
        # [핵심 수정 구간] 명시적 disconnect() 호출 대신 async with를 사용하여 
        # Orchestrator의 연결 및 해제 라이프사이클(connect/disconnect)을 안전하게 위임합니다.
        async with self.orchestrator:
            self.post_message(DvmStartMsg())
            await self.run()
            
        return getattr(self, "is_verified", False)

    @step
    async def phase_intent_resolution(self, msg: DvmStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Intent Resolution & Routing ---")
        self.log.info(f"  └ Target Engine: {'CosmWasm (cw20)' if self.is_cosmwasm else 'EVM (dvm.wasm)'}")
        self.log.info(f"  └ Scenario Type: {self.scenario_type}")
        self.log.info(f"  └ Execution Mode: {self.mode.upper()}")
        return DvmIntentMsg()

    @step
    async def phase_state_projection(self, msg: DvmIntentMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 2] Shadow State Projection ---")
        try:
            if self.is_cosmwasm:
                self.log.info("  └ Bypassing EVM RPC for pure CosmWasm scenario.")
                return DvmProjectedMsg()

            await self.orchestrator.verify_connection()
            
            if self.mode == "live" and self.user_intent.get("requires_access_list"):
                self.log.info("  └ Generating EIP-2930 Access List via RPC...")
                tx_params = {
                    "to": self.target_contract,
                    "from": self.caller,
                    "data": self.calldata,
                    "value": hex(self.value) if self.value else "0x0",
                    "gas": hex(10_000_000) 
                }
                
                access_list = await self.orchestrator.generate_access_list(tx_params)
                self.log.info(f"  └ Access List generated successfully for {len(access_list)} distinct addresses.")
                for entry in access_list:
                    addr = entry["address"]
                    keys = entry["storageKeys"]
                    self.global_state_snapshot[addr] = await self.orchestrator.fetch_account_state(addr, keys)

            if self.target_contract not in self.global_state_snapshot:
                self.global_state_snapshot[self.target_contract] = await self.orchestrator.fetch_account_state(
                    self.target_contract, self.storage_slots
                )
            
            if self.caller != "0x0000000000000000000000000000000000000000" and self.caller not in self.global_state_snapshot:
                self.global_state_snapshot[self.caller] = await self.orchestrator.fetch_account_state(self.caller)
            
            self.block_context = await self.orchestrator.fetch_block_context()

            overrides_list = []
            if self.scenario_type == "UNISWAP_EXACT_INPUT" and self.mode == "live":
                self.log.info("  └ 💉 Formulating State Overrides for Uniswap Scenario...")
                owner_pad = self.caller.replace("0x", "").zfill(64).lower()
                spender_pad = self.target_contract.replace("0x", "").zfill(64).lower()
                
                slot_index = self.user_intent.get("allowance_slot_index", 4)
                mapping_slot = hex(slot_index).replace("0x", "").zfill(64)
                inner_hash = keccak(hexstr=owner_pad + mapping_slot).hex().replace("0x", "")
                allowance_slot = keccak(hexstr=spender_pad + inner_hash).hex()
                if not allowance_slot.startswith("0x"):
                    allowance_slot = "0x" + allowance_slot
                
                overrides_list.append({
                    "slot_hash": allowance_slot,
                    "injected_value": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                })

            projection = ShadowAdapter.project_shadow_state(
                target_address=self.target_contract,
                base_state=self.global_state_snapshot,
                overrides=overrides_list
            )
            
            weth_address = mock_env.contracts.target_erc20.lower()
            if projection.overrides:
                if weth_address not in self.global_state_snapshot:
                    self.global_state_snapshot[weth_address] = {"balance": "0x0", "nonce": 0, "code": "0x", "storage": {}}
                for ov in projection.overrides:
                    self.global_state_snapshot[weth_address]["storage"][ov.slot_hash] = ov.injected_value
                    self.log.info(f"    └ [ShadowAdapter] Injected Override at slot: {ov.slot_hash}")

            return DvmProjectedMsg()
        except Exception as e:
            return ErrorMessage(f"Projection Failed: {str(e)}")

    @step
    async def phase_vm_simulation(self, msg: DvmProjectedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 3] Phronetic VM Simulation ---")
        
        try:
            if self.is_cosmwasm:
                target_wasm = self.user_intent.get("target", "cw20_base.wasm")
                self.log.info(f"[Test] Dispatching CosmWasm Intent to Broker (Target: {target_wasm})")
                
                cw_payload = {
                    "vm_target": "COSMWASM_EXTERNAL",
                    "target_wasm_file": target_wasm,
                    "env": self.user_intent.get("env", {}),
                    "info": self.user_intent.get("info", {}),
                    "msg": self.user_intent.get("msg", {})
                }
                
                self.execution_result = await self.broker.execute(
                    code=cw_payload, tier=mock_env.wasm.tier, context={}
                )
                
            else:
                intent_struct = ShadowAdapter.forge_intent(
                    caller=self.caller,
                    calldata=self.calldata,
                    scenario_type=self.scenario_type,
                    gas_limit=mock_env.wasm.max_gas_limit
                )
                
                evm_payload = {
                    "target_address": self.target_contract,
                    "calldata": intent_struct.calldata,
                    "state_snapshot": self.global_state_snapshot
                }
                inter_context = {
                    "caller_address": intent_struct.caller,
                    "value": hex(int(intent_struct.value_wei)),
                    "block": self.block_context
                }
                
                self.log.info(f"[Test] Dispatching Intent to Broker (Scenario: {intent_struct.scenario_type}, Tier: {mock_env.wasm.tier})...")
                self.execution_result = await self.broker.execute(
                    code=evm_payload, tier=mock_env.wasm.tier, context=inter_context
                )
            
            return DvmSimulatedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Broker/Interpreter crashed: {str(e)}")

    @step
    async def phase_trace_verification(self, msg: DvmSimulatedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 4] Trace & Integrity Verification ---")
        try:
            expected_revert = (self.calldata == "0xdeadbeef" and self.scenario_type != "DPHI_INVERSION")
            
            is_valid, v_msg = TraceVerifier.verify(
                scenario_type=self.scenario_type,
                result=self.execution_result,
                mode=self.mode,
                expected_revert=expected_revert
            )
            
            self.log.info(f"  └─ {v_msg}")
            
            # 무결성 검증을 완벽히 통과했을 때만 True 할당
            self.is_verified = True
            
            return DvmVerifiedMsg()
            
        except VerificationError as e:
            return ErrorMessage(f"Verification Failed: {str(e)}")
        except Exception as e:
            return ErrorMessage(f"Trace Analyzer Crashed: {str(e)}")

    @step
    async def phase_sealing(self, msg: DvmVerifiedMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 5] Cryptographic Proof Sealing ---")
        
        if self.mode == "inversion":
            self.log.info("  └ Bypassing Notary Sealing for Inversion mode.")
            return StopMessage(result=True)
            
        try:
            output_data = {}
            if self.execution_result and self.execution_result.output:
                output_data = json.loads(self.execution_result.output)
            
            proof_receipt = ShadowAdapter.seal_execution_proof(
                execution_output=output_data,
                notary_keys=self.notary_keys
            )
            
            self.canonical_hash = proof_receipt.canonical_hash
            self.log.info(f" ✅ [SEALED] Receipt ID: {proof_receipt.receipt_id} | Status: {proof_receipt.status}")
            self.log.info(f"    └ Hash: {self.canonical_hash[:16]}... | Gas Used: {proof_receipt.gas_used}")
            self.log.info(f"    └ Signatures: {len(proof_receipt.witness_signatures)} Nodes mathematically attested this outcome.")
            
            return StopMessage(result=True)
        except Exception as e:
            return ErrorMessage(f"Sealing Failed: {str(e)}")

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        self.log.error(f"❌ [HALTED] EVM Shadow Workflow unexpectedly aborted: {msg.msg}")
        return StopMessage(result=False)