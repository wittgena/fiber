# fiber.dphi.client.ext.evm
## @lineage: dphi.client.ext.evm
## @lineage: phase.client.ext.evm
## @lineage: bound.client.ext.evm
## @lineage: ator.client.ext.evm
## @lineage: bound.eco.web3
import os
import time
import random
import asyncio
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from fiber.dphi.eco.config import dphi_env
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("web3.adapter")

@dataclass
class EvmIntent:
    """@desc: Standardized carrier for EVM execution intent without external dependencies intent"""
    target: str
    caller: str
    calldata: str
    scenario_type: str
    value: int = 0
    storage_slots: List[str] = field(default_factory=list)
    requires_access_list: bool = False
    allowance_slot_index: Optional[int] = None
    
    def to_workflow_dict(self) -> dict:
        """@desc: Translates internal intent into canonical workflow routing dictionaries intent"""
        data = asdict(self)
        data["target_address"] = data["target"]
        return {k: v for k, v in data.items() if v is not None}

class EvmBuilder:
    """@desc: Deterministic builder exclusively responsible for EVM intent state and block context assemblies intent"""
    
    @staticmethod
    def build_user_intent(
        scenario_type: str = "ERC20_TRANSFER",
        should_revert: bool = False
    ) -> EvmIntent:
        """@desc: Constructs deterministic scenario execution vectors bounded by strict system parameters intent"""
        caller = dphi_env.agents.alpha.evm_address
        value = 0
        requires_access_list = False
        
        if scenario_type == "ERC20_TRANSFER":
            target = dphi_env.contracts.target_erc20
            calldata = "0xa9059cbb" + "000000000000000000000000" + dphi_env.agents.beta.evm_address[2:] + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            storage_slots = ["0x0", "0x1", "0x2"]
            requires_access_list = True
            
        elif scenario_type == "ERC4337_HANDLE_OPS":
            target = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
            calldata = "0x1fad948c" + "0000000000000000000000000000000000000000000000000000000000000040" + "0000000000000000000000001111111111111111111111111111111111111111" + ("00" * 32)
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "UNISWAP_EXACT_INPUT":
            target = "0x3bFA4769FB09eefC5a80d6E87c3B9C650f7Ae48E"
            token_in = dphi_env.contracts.target_erc20.replace("0x", "").zfill(64).lower() 
            token_out = "1c7D4B196Cb0C7B01d743Fbc6116a902379C7238".zfill(64).lower()       
            fee = hex(3000).replace("0x", "").zfill(64)                                     
            recipient = dphi_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
            deadline = hex(int(time.time()) + 1800).replace("0x", "").zfill(64)             
            amount_in = hex(int(0.001 * 1e18)).replace("0x", "").zfill(64)                  
            amount_out_min = "0000000000000000000000000000000000000000000000000000000000000000"
            sqrt_price_limit = "0000000000000000000000000000000000000000000000000000000000000000"
            params_struct = token_in + token_out + fee + recipient + deadline + amount_in + amount_out_min + sqrt_price_limit
            calldata = "0x414bf389" + "0000000000000000000000000000000000000000000000000000000000000020" + params_struct
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "MERKLE_VERIFY":
            target = "0x6EDCE65403992e310A62460808c4b910D972f10f"
            calldata = "0xdeadbeef" + ("aa" * 32)
            storage_slots = []
            requires_access_list = False 
        else:
            raise ValueError(f"Unknown EVM scenario_type: {scenario_type}")

        if should_revert:
            calldata = "0xdeadbeef"
            
        return EvmIntent(
            target=target, caller=caller, calldata=calldata, value=value,
            storage_slots=storage_slots, requires_access_list=requires_access_list, scenario_type=scenario_type
        )

    @staticmethod
    def build_state_snapshot(
        address: str, is_contract: bool = False, balance_wei: int = int(10 * 1e18), should_revert: bool = False
    ) -> Dict[str, Any]:
        """@desc: Generates a mathematically deterministic initial state geometry intent"""
        if is_contract:
            mock_code = "0xfd" if should_revert else "0x608060405234801561001057600080fd5b506101"
        else:
            mock_code = "0x"
        padded_alpha_address = "0x000000000000000000000000" + dphi_env.agents.alpha.evm_address[2:]
        return {
            "balance": hex(balance_wei),
            "nonce": random.randint(1, 100) if not is_contract else 1,
            "code": mock_code,
            "storage": {
                "0x0": padded_alpha_address,
                "0x1": hex(int(1000 * 1e18)),
                "0x2": "0x0000000000000000000000000000000000000000000000000000000000000001"
            }
        }

    @staticmethod
    def build_block_context() -> Dict[str, Any]:
        return {
            "timestamp": int(time.time()),
            "block_number": random.randint(19_000_000, 20_000_000),
            "coinbase": "0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5",
            "chain_id": dphi_env.network.chain_id
        }
    
class EVMOrchestrator:
    """@desc: Subjugates external Web3 RPC providers directly commanding physical lifecycles without non-deterministic delays intent"""
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self.w3: Optional[AsyncWeb3] = None
        self._is_active: bool = False

    async def connect(self):
        """@desc: Binds the remote network into the controlled orchestration scope intent"""
        if self._is_active:
            return
            
        self.w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        if dphi_env.network.use_poa_middleware:
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            
        self._is_active = True
        log.trace("[EVMOrchestrator] Network scope bound and activated")

    async def verify_connection(self):
        """@desc: Validates the physical network binding before allowing projection phases intent"""
        if not self._is_active or not self.w3:
            raise ConnectionError("Orchestrator is not in an active state. Call connect() or use 'async with' first.")
            
        is_connected = await self.w3.is_connected()
        if not is_connected:
            raise ConnectionError(f"Failed to connect to RPC URL {self.w3.provider.endpoint_uri}")
        log.info(f"✅ Connected to REAL network. Chain ID: {await self.w3.eth.chain_id}")

    async def generate_access_list(self, tx_params: Dict[str, Any], max_retries: int = 3) -> List[Dict[str, Any]]:
        for attempt in range(1, max_retries + 1):
            try:
                res = await self.w3.provider.make_request("eth_createAccessList", [tx_params, "latest"])
                if "error" in res:
                    raise ValueError(res["error"])
                return res.get("result", {}).get("accessList", [])
            except Exception as e:
                log.warning(f"AccessList RPC failed (Attempt {attempt}/{max_retries}) {str(e)}")
                if attempt == max_retries:
                    log.error("Failed to generate AccessList after maximum retries. Halting.")
                    raise RuntimeError(f"AccessList Generation Error: {str(e)}")

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
        """@desc: Forcibly amputates external TCP/SSL connectors bypassing graceful GC delays to obliterate orphan tasks intent"""
        if not self._is_active or not self.w3:
            return

        try:
            provider = self.w3.provider
            log.trace("[EVMOrchestrator] Executing absolute structural teardown of external network bindings...")

            session = getattr(provider, '_session', None) or getattr(provider, 'session', None)
            if session:
                connector = getattr(session, 'connector', None)
                if connector:
                    connector.force_close()
                session.detach()

            if hasattr(provider, 'cache_async_session'):
                provider.cache_async_session = lambda *args, **kwargs: None

            log.trace("[EVMOrchestrator] RPC external bindings forcibly amputated. Zero tasks leaked.")

        except Exception as e:
            log.warning(f"[EVMOrchestrator] Teardown executed with anomalies: {e}")
        finally:
            self._is_active = False
            self.w3 = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class MockOrchestrator:
    """@desc: Autonomous local mock builder fully integrated into the orchestrated lifecycle intent"""
    def __init__(self, user_intent: Dict[str, Any] = None):
        self.user_intent = user_intent or {}
        
    async def connect(self):
        pass

    async def verify_connection(self):
        log.info(f"🧪 [MOCK MODE] Using Local Mock Builder Simulated Chain ID: {dphi_env.network.chain_id}")

    async def fetch_account_state(self, address: str, storage_slots: List[str] = None) -> Dict[str, Any]:
        if not address or not address.startswith("0x"):
            log.warning(f"MockOrchestrator received non-EVM address {address}. Returning empty state.")
            return {}

        is_contract = (address.lower() == dphi_env.contracts.target_erc20.lower())
        is_revert = (self.user_intent.get("calldata") == "0xdeadbeef" and self.user_intent.get("scenario_type") != "DPHI_INVERSION")
        return EvmBuilder.build_state_snapshot(address, is_contract=is_contract, should_revert=is_revert)

    async def fetch_block_context(self) -> Dict[str, Any]:
        return EvmBuilder.build_block_context()
        
    async def disconnect(self):
        log.trace("[MockOrchestrator] Mock context dismantled instantaneously")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class InversionOrchestrator:
    """@desc: Special mock boundary to trigger cross-VM precompile callbacks intent"""
    def __init__(self, user_intent: Dict[str, Any] = None):
        self.user_intent = user_intent or {}
        
    async def connect(self):
        pass

    async def verify_connection(self):
        log.info("🌌 [INVERSION MODE] Cross-VM Precompile Hook Testing initialized")

    async def fetch_account_state(self, address: str, storage_slots: List[str] = None) -> Dict[str, Any]:
        return {
            "balance": "0x1000000000000000000",
            "nonce": 0,
            "code": "0x",
            "storage": {}
        }

    async def fetch_block_context(self) -> Dict[str, Any]:
        return EvmBuilder.build_block_context()

    async def disconnect(self):
        log.trace("[InversionOrchestrator] Inversion hook context obliterated")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


# ============================================================================
# Phase 3: Action Adapters (Wallet & Interactions)
# ============================================================================

class Web3Adapter:
    """순수 AsyncWeb3 기반 RPC 통신 어댑터"""
    def __init__(self, rpc_url: str = None):
        self.rpc_url = rpc_url or dphi_env.network.rpc_url
        self.w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        
        # PoA 네트워크(Sepolia 등) 호환성을 위한 미들웨어 주입
        if dphi_env.network.use_poa_middleware:
            self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            
        self.weth_address = self.w3.to_checksum_address(dphi_env.contracts.target_erc20)
        self.weth_abi = [
            {"constant": True, "inputs": [{"name": "", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
            {"constant": False, "inputs": [], "name": "deposit", "outputs": [], "payable": True, "type": "function"}
        ]
        self.contract = self.w3.eth.contract(address=self.weth_address, abi=self.weth_abi)

    async def close(self):
        await self.w3.provider.disconnect()

    async def get_balances(self, address: str) -> Dict[str, str]:
        checksum_addr = self.w3.to_checksum_address(address)
        eth_bal = await self.w3.eth.get_balance(checksum_addr)
        weth_bal = await self.contract.functions.balanceOf(checksum_addr).call()
        
        return {
            "eth_wei": str(eth_bal),
            "weth_wei": str(weth_bal)
        }

    async def wrap_weth(self, caller_address: str, amount_wei: int, private_key: str) -> str:
        checksum_addr = self.w3.to_checksum_address(caller_address)
        
        # 1. Nonce 충돌 방지를 위한 'pending' 상태 조회
        nonce = await self.w3.eth.get_transaction_count(checksum_addr, 'pending')
        
        gas_price = await self.w3.eth.gas_price
        max_priority_fee = await self.w3.eth.max_priority_fee
        chain_id = await self.w3.eth.chain_id  # Web3Adapter는 초기화 시 chain_id 캐시가 없으므로 직접 호출
        
        # 2. Double Estimation 방지를 위한 선행 가스 추정
        estimated_gas = await self.contract.functions.deposit().estimate_gas({
            'from': checksum_addr,
            'value': amount_wei
        })
        
        # 3. 계산된 가스 포함하여 트랜잭션 빌드 (내부 중복 RPC 차단)
        tx_params = {
            'from': checksum_addr, 
            'value': amount_wei, 
            'nonce': nonce,
            'maxFeePerGas': gas_price,
            'maxPriorityFeePerGas': max_priority_fee,
            'chainId': chain_id,
            'gas': int(estimated_gas * 1.2)
        }
        tx = await self.contract.functions.deposit().build_transaction(tx_params)

        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
        tx_hash = await self.w3.eth.send_raw_transaction(tx_raw)
        
        log.info(f"[Web3] Wrap Tx broadcasted: {tx_hash.hex()}. Waiting for confirmation...")
        
        # 4. 무한 대기(Hang) 방지를 위한 타임아웃
        try:
            receipt = await asyncio.wait_for(
                self.w3.eth.wait_for_transaction_receipt(tx_hash),
                timeout=180.0
            )
        except asyncio.TimeoutError:
            raise RuntimeError(f"Wrap transaction receipt polling timed out. TxHash: {tx_hash.hex()}")
            
        if receipt.status == 1:
            log.info(f"[Web3] Successfully wrapped {amount_wei} Wei into WETH.")
            return tx_hash.hex()
        else:
            raise RuntimeError(f"Wrap Transaction Reverted. Hash: {tx_hash.hex()}")