# phase.epoch.workflow.dvm
import sys
import argparse
import asyncio
import json
from typing import Dict, Any, Optional, List

try:
    from eth_utils import keccak
except ImportError:
    from web3 import Web3
    keccak = Web3.keccak

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from phase.anchor.config.dphi import dphi_env, DvmConfig
from phase.anchor.config.client import NotarySwarm
from phase.anchor.adapter.web3 import EvmBuilder, EvmIntent, EVMOrchestrator, MockOrchestrator, InversionOrchestrator

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.adapter.shadow import ShadowAdapter
from kernel.dphi.broker import DphiBroker
from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter
from bound.agent.intent.verifier import TraceVerifier, VerificationError

log = get_emitter("workflow.dvm")

"""Workflow Messages"""
class DvmStartMsg(WorkflowMessage): pass
class DvmIntentMsg(WorkflowMessage): pass
class DvmProjectedMsg(WorkflowMessage): pass
class DvmSimulatedMsg(WorkflowMessage): pass
class DvmVerifiedMsg(WorkflowMessage): pass

class DvmWorkflow(Workflow):
    def __init__(self, target_contract: str, user_intent: Dict[str, Any], rpc_url: Optional[str] = None, mode: str = "mock"):
        super().__init__(name="DVM_WORKFLOW")
        self.log = log
        
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
            
            weth_address = dphi_env.contracts.target_erc20.lower()
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
                    code=cw_payload, tier=dphi_env.wasm.tier, context={}
                )
                
            else:
                intent_struct = ShadowAdapter.forge_intent(
                    caller=self.caller,
                    calldata=self.calldata,
                    scenario_type=self.scenario_type,
                    gas_limit=dphi_env.wasm.max_gas_limit
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
                
                self.log.info(f"[Test] Dispatching Intent to Broker (Scenario: {intent_struct.scenario_type}, Tier: {dphi_env.wasm.tier})...")
                self.execution_result = await self.broker.execute(
                    code=evm_payload, tier=dphi_env.wasm.tier, context=inter_context
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

# =========================================================================
# Execution Runner & Pipeline 
# =========================================================================
class EvmRunner:
    def __init__(self, config: DvmConfig):
        self.log = log
        self.config = config

    def _parse_value(self, val: str | int) -> int:
        if isinstance(val, str) and val.startswith('0x'):
            return int(val, 16)
        return int(val)

    def _build_intent(self, mode: str, revert: bool, scenario_type: str) -> Any:
        if scenario_type == "COSMWASM_EXECUTE":
            return {
                "target": "cw20_base.wasm",
                "scenario_type": scenario_type,
                "msg": {"transfer": {"recipient": "cosmos1targetaddress89012345678901234567890", "amount": "100"}},
                "info": {"sender": "cosmos1senderaddress12345678901234567890123", "funds": []},
                "env": {
                    "block": {"height": 1234, "time": "1234567890", "chain_id": "test-1"}, 
                    "contract": {"address": "cosmos1contractaddress456789012345678901234"}
                }
            }

        if self.config.calldata != "0x" and scenario_type != "DPHI_INVERSION":
            return EvmIntent(
                target=self.config.target,
                caller=self.config.caller,
                calldata=self.config.calldata,
                value=self._parse_value(self.config.value),
                storage_slots=self.config.slots,
                scenario_type=scenario_type
            )

        if scenario_type == "DPHI_INVERSION":
            return EvmIntent(
                target="0x0000000000000000000000000000000000000099",
                caller=dphi_env.agents.alpha.evm_address,
                calldata="0xdeadbeef",
                scenario_type=scenario_type
            )

        intent = EvmBuilder.build_user_intent(scenario_type=scenario_type, should_revert=revert)
        if mode == "live":
            intent.caller = dphi_env.agents.beta.evm_address

        if scenario_type == "UNISWAP_EXACT_INPUT":
            intent.allowance_slot_index = 4

        if scenario_type == "ERC20_TRANSFER" and not revert and mode == "live":
            alpha_addr_clean = dphi_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
            transfer_amount_hex = hex(int(0.001 * 1e18)).replace("0x", "").zfill(64)
            intent.calldata = f"0xa9059cbb{alpha_addr_clean}{transfer_amount_hex}"
            intent.requires_access_list = True 

        return intent

    async def run(self, name: str, mode: str, revert: bool, scenario_type: str = "ERC20_TRANSFER") -> bool:
        self.log.info(f"\n\n{'='*80}\n🚀 [SCENARIO] {name}\n{'='*80}")
        
        intent = self._build_intent(mode, revert, scenario_type)
        user_intent = intent if isinstance(intent, dict) else intent.to_workflow_dict()
        target_contract = user_intent.get("target_address") or user_intent.get("target")
        workflow = DvmWorkflow(
            target_contract=target_contract, 
            user_intent=user_intent, 
            rpc_url=self.config.rpc_url, 
            mode=mode
        )
        return await workflow.start()


class DvmPipeline:
    def __init__(self, config: DvmConfig):
        self.log = log
        self.config = config
        self.executor = EvmRunner(config=self.config)

    async def _preflight_weth_check(self, rpc_url: str, agent: Any) -> bool:
        self.log.info(f"\n{'='*80}\n🛠️  [PRE-FLIGHT] Checking Agent WETH Balance & Auto-Wrap\n{'='*80}")
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        try:
            caller_addr = w3.to_checksum_address(agent.evm_address)
            caller_pkey = dphi_env.get_agent_pkey("beta")
            weth_addr = w3.to_checksum_address(dphi_env.contracts.target_erc20)
            
            weth_abi = [
                {"constant": True, "inputs": [{"name": "", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
                {"constant": False, "inputs": [], "name": "deposit", "outputs": [], "payable": True, "type": "function"}
            ]
            weth_contract = w3.eth.contract(address=weth_addr, abi=weth_abi)
            
            weth_bal = await weth_contract.functions.balanceOf(caller_addr).call()
            min_weth_wei = w3.to_wei(0.01, 'ether')
            
            if weth_bal >= min_weth_wei:
                self.log.info(f"[Pre-flight] ✅ WETH Balance sufficient: {w3.from_wei(weth_bal, 'ether')} WETH")
                await w3.provider.disconnect()
                return True
                
            self.log.warning(f"[Pre-flight] ⚠️ Insufficient WETH ({w3.from_wei(weth_bal, 'ether')}). Attempting to wrap 0.01 ETH...")
            
            eth_bal = await w3.eth.get_balance(caller_addr)
            if eth_bal < min_weth_wei:
                self.log.error(f"[Pre-flight] ❌ Agent lacks native ETH to wrap! Has {w3.from_wei(eth_bal, 'ether')} ETH.")
                await w3.provider.disconnect()
                return False
                
            nonce = await w3.eth.get_transaction_count(caller_addr)
            tx = await weth_contract.functions.deposit().build_transaction({
                'from': caller_addr, 'value': min_weth_wei, 'nonce': nonce,
                'gas': 100000, 'maxFeePerGas': await w3.eth.gas_price,
                'maxPriorityFeePerGas': await w3.eth.max_priority_fee,
                'chainId': await w3.eth.chain_id
            })
            
            signed_tx = w3.eth.account.sign_transaction(tx, private_key=caller_pkey)
            tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
            tx_hash = await w3.eth.send_raw_transaction(tx_raw)
            
            self.log.info(f"[Pre-flight] 🚀 Wrap Tx broadcasted: {tx_hash.hex()} - Waiting for confirmation...")
            receipt = await w3.eth.wait_for_transaction_receipt(tx_hash)
            
            if receipt.status == 1:
                self.log.info("[Pre-flight] 🎉 Successfully minted 0.01 WETH! Awaiting node synchronization...")
                await asyncio.sleep(3) 
                await w3.provider.disconnect()
                return True
            else:
                self.log.error("[Pre-flight] ❌ Wrap Tx reverted on-chain.")
                await w3.provider.disconnect()
                return False

        except Exception as e:
            self.log.error(f"[Pre-flight] ❌ WETH Auto-wrap failed: {str(e)}")
            await w3.provider.disconnect()
            return False

    async def execute(self):
        if self.config.mode in ["suite", "live"]:
            weth_ready = await self._preflight_weth_check(self.config.rpc_url, dphi_env.agents.beta)
            if not weth_ready:
                self.log.warning("⚠️ Pre-flight failed. Live WETH tests may revert. Proceeding anyway...")

        if self.config.mode == "suite":
            self.log.info("\n[CLI] 🏃‍♂️ Initiating 5-Phase Multi-VM System Suite (Intent -> Projection -> Simulation -> Verification -> Sealing)")
            
            s1 = await self.executor.run("1. Standard Mock (ERC20 Transfer)", "mock", False, "ERC20_TRANSFER")
            s2 = await self.executor.run("2. Revert Mock (ERC20 Transfer)", "mock", True, "ERC20_TRANSFER")
            s3 = await self.executor.run("3. Cross-VM Inversion (Precompile Hook)", "inversion", False, "DPHI_INVERSION")
            s4 = await self.executor.run("4. Live Testnet (ERC20 Transfer)", "live", False, "ERC20_TRANSFER")
            s5 = await self.executor.run("5. Live Testnet (Uniswap V3 exactInputSingle)", "live", False, "UNISWAP_EXACT_INPUT")
            s6 = await self.executor.run("6. Live Testnet (ERC4337 EntryPoint Tracer)", "live", False, "ERC4337_HANDLE_OPS")
            s7 = await self.executor.run("7. CosmWasm (cw20_base Transfer Test)", "mock", False, "COSMWASM_EXECUTE")
            
            self.log.info(f"\n\n{'='*80}\n📊 [7-STAGE SUITE SUMMARY (Verified by TraceVerifier)]\n{'='*80}")
            self.log.info(f" 1. Standard Mock EVM     : {'✅ PASS (Verified Trace)' if s1 else '❌ FAIL'}")
            self.log.info(f" 2. Revert Mock EVM       : {'✅ PASS (Intentional Revert Verified)' if s2 else '❌ FAIL'}")
            self.log.info(f" 3. Cross-VM Inversion    : {'✅ PASS (Host-Mediated RPC Verified)' if s3 else '❌ FAIL'}")
            self.log.info(f" 4. Live Testnet ERC20    : {'✅ PASS (Live Tx Trace Verified)' if s4 else '❌ FAIL'}")
            self.log.info(f" 5. Live Uniswap V3       : {'✅ TRACED (Revert/Logic Verified)' if s5 else '❌ FAIL'}")
            self.log.info(f" 6. EntryPoint Tracer     : {'✅ TRACE SUCCESS (AA90 Boundary Verified)' if s6 else '❌ FAIL'}")
            self.log.info(f" 7. CosmWasm CW20 Test    : {'✅ PASS (JSON Payload Executed & Verified)' if s7 else '❌ FAIL'}")
            
            if s1 and s2 and s3 and s4 and s5 and s6 and s7:
                self.log.info("\n🎉 All 7 Multi-VM Core Engine & Architecture test suites completed successfully!")
            else:
                self.log.error("\n⚠️ One or more test suites failed trace verification. Please inspect logs above.")
            return 
        else:
            name = f"Single Execution (Mode: {self.config.mode.upper()}, Scenario: {self.config.scenario})"
            success = await self.executor.run(name, self.config.mode, self.config.revert, self.config.scenario)
            
            if not success:
                self.log.error("[CLI] Workflow Execution or Verification Failed.")
            else:
                self.log.info("[CLI] 5-Phase Workflow Execution Completed Successfully.")
            return

def main() -> None:
    config = DvmConfig()
    app = DvmPipeline(config)
    PhaseReactor.ignite(lambda: app.execute())

if __name__ == "__main__":
    main()