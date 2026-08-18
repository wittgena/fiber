# phase.dvm.entry
import sys
import asyncio
from typing import Dict, Any, Optional, List

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from phase.anchor.config.dphi import dphi_env, DvmConfig
from bound.exchange.web3.adapter import EvmBuilder, EvmIntent
from phase.dvm.workflow import DvmWorkflow 

from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

log = get_emitter("phase.dvm")

class EvmRunner:
    def __init__(self, config: DvmConfig):
        self.log = log
        self.config = config

    def _parse_value(self, val: str | int) -> int:
        if isinstance(val, str) and val.startswith('0x'):
            return int(val, 16)
        return int(val)

    def _build_intent(self, mode: str, revert: bool, scenario_type: str) -> Any:
        # =================================================================
        # [정렬 1] CosmWasm 테스트용 인텐트 반환 구조 (EVM 아님)
        # =================================================================
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

        # 기존 EVM 로직
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
        
        # [정렬 2] CosmWasm용 dict 분기 및 안전한 target_contract 추출
        user_intent = intent if isinstance(intent, dict) else intent.to_workflow_dict()
        
        # EvmIntent 변환 시 target이 target_address로 치환될 수 있으므로 둘 다 확인하여 안전하게 확보
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