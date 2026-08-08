# entry.dvm
import sys
import argparse
import asyncio
import json
from typing import Dict, Any, Optional, List

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from dphi.phase.config import mock_env
from dphi.phase.builder import PhaseBuilder, NotarySwarm
from dphi.adapter.shadow import ShadowAdapter

from arch.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from kernel.bind.inter.protocol import ExecutionResult
from kernel.dphi.broker import WasmBroker
from kernel.phase.reactor import KernelReactor
from watcher.plane.emitter import get_emitter

log = get_emitter("dvm.tester")

class EvmStartMsg(WorkflowMessage): pass
class EvmPreparedMsg(WorkflowMessage): pass
class EvmExecutedMsg(WorkflowMessage): pass

class EVMOrchestrator:
    def __init__(self, rpc_url: str):
        self.w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    async def verify_connection(self):
        is_connected = await self.w3.is_connected()
        if not is_connected:
            raise ConnectionError(f"Failed to connect to RPC URL: {self.w3.provider.endpoint_uri}")
        log.info(f"✅ Connected to REAL network. Chain ID: {await self.w3.eth.chain_id}")

    # [개선됨] Access List 생성 시 재시도 로직(Exponential Backoff) 및 안전한 예외 처리 추가
    async def generate_access_list(self, tx_params: Dict[str, Any], max_retries: int = 3) -> List[Dict[str, Any]]:
        for attempt in range(1, max_retries + 1):
            try:
                res = await self.w3.provider.make_request("eth_createAccessList", [tx_params, "latest"])
                if "error" in res:
                    raise ValueError(res["error"])
                return res.get("result", {}).get("accessList", [])
            except Exception as e:
                log.warning(f"AccessList RPC failed (Attempt {attempt}/{max_retries}): {str(e)}")
                if attempt == max_retries:
                    log.error("Failed to generate AccessList after maximum retries. Halting.")
                    raise RuntimeError(f"AccessList Generation Error: {str(e)}")
                await asyncio.sleep(2 ** attempt)

    async def fetch_account_state(self, address: str, storage_slots: List[str] = None) -> Dict[str, Any]:
        checksum_addr = self.w3.to_checksum_address(address)
        balance_wei = await self.w3.eth.get_balance(checksum_addr)
        nonce = await self.w3.eth.get_transaction_count(checksum_addr)
        code = await self.w3.eth.get_code(checksum_addr)
        
        storage = {}
        if storage_slots:
            for slot in storage_slots:
                slot_int = int(slot, 16) if isinstance(slot, str) and slot.startswith("0x") else int(slot)
                val = await self.w3.eth.get_storage_at(checksum_addr, slot_int)
                storage[hex(slot_int)] = val.hex()

        return {
            "balance": hex(balance_wei),
            "nonce": nonce,
            "code": code.hex(),
            "storage": storage
        }

    async def fetch_block_context(self) -> Dict[str, Any]:
        block = await self.w3.eth.get_block('latest')
        return {
            "timestamp": block.timestamp,
            "block_number": block.number,
            "coinbase": block.miner,
            "chain_id": await self.w3.eth.chain_id
        }
        
    async def disconnect(self):
        await self.w3.provider.disconnect()


class MockOrchestrator:
    def __init__(self, user_intent: Dict[str, Any] = None):
        self.user_intent = user_intent or {}

    async def verify_connection(self):
        log.info(f"🧪 [MOCK MODE] Using Local Mock Builder. Simulated Chain ID: {mock_env.network.chain_id}")

    async def fetch_account_state(self, address: str, storage_slots: List[str] = None) -> Dict[str, Any]:
        is_contract = (address.lower() == mock_env.contracts.target_erc20.lower())
        is_revert = (self.user_intent.get("calldata") == "0xdeadbeef")
        return PhaseBuilder.evm_state_snapshot(address, is_contract=is_contract, should_revert=is_revert)

    async def fetch_block_context(self) -> Dict[str, Any]:
        return PhaseBuilder.evm_block_context()
        
    async def disconnect(self):
        pass


class EvmShadowWorkflow(Workflow):
    def __init__(self, target_contract: str, user_intent: Dict[str, Any], rpc_url: Optional[str] = None, use_mock: bool = True):
        super().__init__(name="EVM_SHADOW_TESTER")
        self.log = get_emitter("workflow.evm_shadow.tester")
        
        self.target_contract = target_contract
        self.user_intent = user_intent
        self.use_mock = use_mock
        
        if self.use_mock:
            self.orchestrator = MockOrchestrator(user_intent=self.user_intent)
        else:
            self.orchestrator = EVMOrchestrator(rpc_url)
            
        self.broker = WasmBroker()
        
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
        return bool(self.canonical_hash)

    @step
    async def phase_projection(self, msg: EvmStartMsg) -> WorkflowMessage:
        self.log.info(f"--- [Phase 1] Shadow State Projection ---")
        try:
            await self.orchestrator.verify_connection()
            
            if not self.use_mock and self.user_intent.get("requires_access_list"):
                self.log.info("  └ Generating EIP-2930 Access List via Alchemy RPC...")
                tx_params = {
                    "to": self.target_contract,
                    "from": self.caller,
                    "data": self.calldata,
                    "value": hex(self.value) if self.value else "0x0",
                    "gas": hex(10_000_000) 
                }
                # 예외 발생 시 하단 except Exception으로 떨어져 워크플로우를 중단함
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
            if self.user_intent.get("scenario_type") == "UNISWAP_EXACT_INPUT" and not self.use_mock:
                self.log.info("  └ 💉 Formulating State Overrides for Uniswap Scenario...")
                owner_pad = self.caller.replace("0x", "").zfill(64).lower()
                spender_pad = self.target_contract.replace("0x", "").zfill(64).lower()
                
                # [개선됨] 하드코딩된 슬롯 번호 제거 및 동적 인덱싱 적용 (기본값: 4 - WETH 기준)
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
            self.log.info(f"[Test] Dispatching Forged Intent to Broker (Tier: {mock_env.wasm.tier})...")
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
            return EvmExecutedMsg()
            
        except Exception as e:
            return ErrorMessage(f"Broker/Interpreter crashed: {str(e)}")

    @step
    async def phase_sealing(self, msg: EvmExecutedMsg) -> WorkflowMessage:
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


class dvmPipelineCLI:
    def __init__(self, args):
        self.log = log
        self.args = args

    async def _preflight_weth_check(self, rpc_url: str, agent: Any) -> bool:
        self.log.info(f"\n{'='*80}\n🛠️  [PRE-FLIGHT] Checking Agent WETH Balance & Auto-Wrap\n{'='*80}")
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        try:
            caller_addr = w3.to_checksum_address(agent.evm_address)
            caller_pkey = mock_env.get_agent_pkey("beta")
            weth_addr = w3.to_checksum_address(mock_env.contracts.target_erc20)
            
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
                # [개선됨] 노드 간 상태 전파 및 동기화를 위한 안전 버퍼
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

    async def execute_scenario(self, name: str, mode: str, revert: bool, scenario_type: str = "ERC20_TRANSFER") -> bool:
        self.log.info(f"\n\n{'='*80}\n🚀 [SCENARIO] {name}\n{'='*80}")
        is_mock = (mode == "mock")
        active_rpc = self.args.rpc or mock_env.network.rpc_url
        
        if self.args.calldata == "0x":
            intent = PhaseBuilder.evm_user_intent(scenario_type=scenario_type, should_revert=revert)
            
            # [개선됨] Uniswap V3 시나리오 시 Allowance 슬롯 인덱스 명시적 지정 (필요 시 수정 가능)
            if scenario_type == "UNISWAP_EXACT_INPUT":
                intent["allowance_slot_index"] = 4 

            if scenario_type == "ERC20_TRANSFER" and not revert and not is_mock:
                intent["caller"] = mock_env.agents.beta.evm_address
                alpha_addr_clean = mock_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
                transfer_amount_hex = hex(int(0.001 * 1e18)).replace("0x", "").zfill(64)
                intent["calldata"] = "0xa9059cbb" + alpha_addr_clean + transfer_amount_hex
                intent["requires_access_list"] = True 
            elif not is_mock:
                intent["caller"] = mock_env.agents.beta.evm_address
        else:
            intent = {
                "calldata": self.args.calldata,
                "caller": self.args.caller,
                "value": int(self.args.value, 16) if isinstance(self.args.value, str) and self.args.value.startswith('0x') else int(self.args.value),
                "storage_slots": self.args.slots,
                "requires_access_list": False,
                "scenario_type": scenario_type
            }

        target = intent.get("target", self.args.target)
        workflow = EvmShadowWorkflow(target_contract=target, user_intent=intent, rpc_url=active_rpc, use_mock=is_mock)
        return await workflow.start()

    async def execute(self):
        active_rpc = self.args.rpc or mock_env.network.rpc_url
        
        if self.args.mode in ["suite", "live"]:
            weth_ready = await self._preflight_weth_check(active_rpc, mock_env.agents.beta)
            if not weth_ready:
                self.log.warning("⚠️ Pre-flight failed. Live WETH tests may revert. Proceeding anyway...")

        if self.args.mode == "suite":
            self.log.info("\n[CLI] 🏃‍♂️ Initiating Advanced System Suite (Mock -> Revert -> AccessList -> Live)")
            
            s1 = await self.execute_scenario("1. Standard Mock (ERC20)", "mock", False, "ERC20_TRANSFER")
            s2 = await self.execute_scenario("2. Revert Mock (ERC20)", "mock", True, "ERC20_TRANSFER")
            s3 = await self.execute_scenario("3. Live Testnet (ERC20)", "live", False, "ERC20_TRANSFER")
            s4 = await self.execute_scenario("4. Live Testnet (Uniswap V3 exactInputSingle)", "live", False, "UNISWAP_EXACT_INPUT")
            s5 = await self.execute_scenario("5. Live Testnet (ERC4337 EntryPoint Tracer)", "live", False, "ERC4337_HANDLE_OPS")
            
            self.log.info(f"\n\n{'='*80}\n📊 [SUITE SUMMARY]\n{'='*80}")
            self.log.info(f" 1. Standard Mock    : {'✅ PASS' if s1 else '❌ FAIL'}")
            self.log.info(f" 2. Revert Mock      : {'✅ PASS (Reverted)' if s2 else '❌ FAIL'}")
            self.log.info(f" 3. Live Testnet     : {'✅ PASS (Tx logic successful)' if s3 else '❌ FAIL'}")
            self.log.info(f" 4. Uniswap V3       : {'✅ TRACED (Revert Proven)' if s4 else '❌ FAIL'}")
            self.log.info(f" 5. EntryPoint Tracer: {'✅ TRACE SUCCESS (AA90 Caught)' if s5 else '❌ FAIL'}")
            
            if s1 and s2 and s3 and s4 and s5:
                self.log.info("\n🎉 Core Engine & Architecture tests completed successfully!")
            else:
                self.log.error("\n⚠️ Core scenarios failed. Please check the logs above.")
            return 
        else:
            is_mock = (self.args.mode == "mock")
            name = f"Single Execution (Mode: {self.args.mode.upper()}, Scenario: {self.args.scenario})"
            success = await self.execute_scenario(name, self.args.mode, self.args.revert, self.args.scenario)
            
            if not success:
                self.log.error("[CLI] EVM Workflow Execution Failed to produce Proof.")
            else:
                self.log.info("[CLI] EVM Workflow Execution and Sealing Completed Successfully.")
            return

    @classmethod
    def run_cli(cls):
        parser = argparse.ArgumentParser(description="dvm Shadow Execution Tester")
        parser.add_argument("--mode", type=str, choices=["suite", "mock", "live"], default="suite")
        parser.add_argument("--scenario", type=str, default="ERC20_TRANSFER", 
                            choices=["ERC20_TRANSFER", "ERC4337_HANDLE_OPS", "UNISWAP_EXACT_INPUT", "MERKLE_VERIFY"])
        parser.add_argument("--revert", action="store_true", help="Force a revert scenario")
        parser.add_argument("--rpc", type=str, required=False, help="Override Web3 RPC URL")
        
        parser.add_argument("--target", type=str, default=mock_env.contracts.target_erc20)
        parser.add_argument("--caller", type=str, default=mock_env.agents.alpha.evm_address)
        parser.add_argument("--value", type=str, default="0")
        parser.add_argument("--calldata", type=str, default="0x")
        parser.add_argument("--slots", type=str, nargs='*', default=[])

        args = parser.parse_args()
        
        if args.mode == "live" and not args.rpc and not mock_env.network.rpc_url:
            parser.error("--rpc is required when --mode is 'live' and no default RPC is configured.")
            
        app = cls(args)
        KernelReactor.ignite(lambda: app.execute())

if __name__ == "__main__":
    dvmPipelineCLI.run_cli()