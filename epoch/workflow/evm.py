# epoch.workflow.evm
## @lineage: entry.workflow.evm
import sys
import argparse
import asyncio
import json
from typing import Dict, Any, Optional

from web3 import AsyncWeb3
from phase.dphi.config.dphi import mock_env
from phase.dphi.builder.phase import NotarySwarm
from phase.dphi.adapter.shadow import ShadowAdapter
from phase.dphi.adapter.evm import EVMOrchestrator, MockOrchestrator, InversionOrchestrator

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.broker import DphiBroker
from watcher.plane.emitter import get_emitter

log = get_emitter("dvm.tester")

class EvmStartMsg(WorkflowMessage): pass
class EvmPreparedMsg(WorkflowMessage): pass
class EvmExecutedMsg(WorkflowMessage): pass

class EvmWorkflow(Workflow):
    def __init__(self, target_contract: str, user_intent: Dict[str, Any], rpc_url: Optional[str] = None, mode: str = "mock"):
        super().__init__(name="EVM_WORKFLOW")
        self.log = get_emitter("workflow.evm")
        
        self.target_contract = target_contract
        self.user_intent = user_intent
        self.mode = mode
        
        if self.mode == "inversion":
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

    async def start(self) -> bool:
        self.post_message(EvmStartMsg())
        await self.run()
        await self.orchestrator.disconnect()
        return bool(self.canonical_hash) or (self.execution_result is not None and self.execution_result.success)

    @step
    async def phase_projection(self, msg: EvmStartMsg) -> WorkflowMessage:
        self.log.info("--- [Phase 1] Shadow State Projection ---")
        try:
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
            weth_address = mock_env.contracts.target_erc20.lower()
            if self.user_intent.get("scenario_type") == "UNISWAP_EXACT_INPUT" and self.mode == "live":
                self.log.info("  └ 💉 Formulating State Overrides for Uniswap Scenario...")
                owner_pad = self.caller.replace("0x", "").zfill(64).lower()
                spender_pad = self.target_contract.replace("0x", "").zfill(64).lower()
                
                slot_index = self.user_intent.get("allowance_slot_index", 4)
                mapping_slot = hex(slot_index).replace("0x", "").zfill(64)
                
                w3 = AsyncWeb3()
                inner_hash = w3.keccak(hexstr=owner_pad + mapping_slot).hex().replace("0x", "")
                allowance_slot = w3.keccak(hexstr=spender_pad + inner_hash).hex()
                
                overrides_list.append({
                    "slot_hash": allowance_slot,
                    "injected_value": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                })

            projection = ShadowAdapter.project_shadow_state(
                target_address=self.target_contract,
                base_state=self.global_state_snapshot,
                overrides=overrides_list
            )
            
            if projection.overrides:
                if weth_address not in self.global_state_snapshot:
                    self.global_state_snapshot[weth_address] = {"balance": "0x0", "nonce": 0, "code": "0x", "storage": {}}
                for ov in projection.overrides:
                    self.global_state_snapshot[weth_address]["storage"][ov.slot_hash] = ov.injected_value
                    self.log.info(f"    └ [ShadowAdapter] Injected Override at slot: {ov.slot_hash}")

            return EvmPreparedMsg()
        except Exception as e:
            return ErrorMessage(f"Projection Failed: {str(e)}")

    @step
    async def phase_simulation(self, msg: EvmPreparedMsg) -> WorkflowMessage:
        if self.mode == "inversion":
            self.log.info("--- [Phase 2] dvm.wasm -> Host -> dphi.wasm Inversion ---")
        else:
            self.log.info("--- [Phase 2] Phronetic Simulation via WasmBroker ---")
        
        intent_struct = ShadowAdapter.forge_intent(
            caller=self.caller,
            calldata=self.calldata,
            scenario_type=self.user_intent.get("scenario_type", "UNKNOWN"),
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
        
        try:
            self.log.info(f"[Test] Dispatching Intent to Broker (Scenario: {intent_struct.scenario_type}, Tier: {mock_env.wasm.tier})...")
            result: ExecutionResult = await self.broker.execute(
                code=evm_payload, tier=mock_env.wasm.tier, context=inter_context
            )
            
            self.execution_result = result
            
            if not result.success:
                try:
                    parsed_out = json.loads(result.output)
                    revert_msg = parsed_out.get('revert_reason', str(result.error))
                    output_data = parsed_out.get('output', '')
                    
                    if intent_struct.scenario_type == "ERC4337_HANDLE_OPS" and "41413930" in output_data:
                        self.log.info("  └─ [ASSERT SUCCESS] Expected EntryPoint Revert (AA90) securely bounded by sandbox.")
                        return EvmExecutedMsg()
                    
                    self.log.error(f"🚨 [RAW ERROR] Reverted. Gas Used: {parsed_out.get('gas_used')} | Reason: {revert_msg}")
                    return EvmExecutedMsg() 
                except Exception:
                    return ErrorMessage(f"EVM Execution Failed: {result.error}")

            parsed_out = json.loads(result.output)
            self.log.info(f"  └─ [PASS] Execution Successful via dvm.wasm. (Gas: {parsed_out.get('gas_used')})")
            
            if self.mode == "inversion":
                output_hex = parsed_out.get('output', '')
                self.log.info(f"  └─ 🌌 Phase Residue Returned to EVM: {output_hex}")

            return EvmExecutedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Broker/Interpreter crashed: {str(e)}")

    @step
    async def phase_sealing(self, msg: EvmExecutedMsg) -> WorkflowMessage:
        if self.mode == "inversion":
            self.log.info("--- [Phase 3] Inversion Verification Completed ---")
            if self.execution_result and self.execution_result.success:
                self.log.info("🎉 EVM <-> Host <-> DPHI Core communication cycle verified.")
                return StopMessage(result=True)
            else:
                return ErrorMessage("Inversion cycle failed to produce successful trace.")
                
        self.log.info("--- [Phase 3] Cryptographic Proof Sealing ---")
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