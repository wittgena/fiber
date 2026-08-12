# ext.web3.evm
import time
import random
import asyncio
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from dphi.adapter.config.dphi import mock_env
from watcher.plane.emitter import get_emitter

log = get_emitter("web3.evm")

@dataclass
class EvmIntent:
    target: str
    caller: str
    calldata: str
    scenario_type: str
    value: int = 0
    storage_slots: List[str] = field(default_factory=list)
    requires_access_list: bool = False
    allowance_slot_index: Optional[int] = None
    
    def to_workflow_dict(self) -> dict:
        data = asdict(self)
        data["target_address"] = data["target"]
        return {k: v for k, v in data.items() if v is not None}

class EvmBuilder:
    """EVM 관련 인텐트, 상태, 블록 컨텍스트 구성을 전담하는 빌더 클래스"""
    
    @staticmethod
    def build_user_intent(
        scenario_type: str = "ERC20_TRANSFER",
        should_revert: bool = False
    ) -> EvmIntent:
        caller = mock_env.agents.alpha.evm_address
        value = 0
        requires_access_list = False
        
        if scenario_type == "ERC20_TRANSFER":
            target = mock_env.contracts.target_erc20
            calldata = "0xa9059cbb" + "000000000000000000000000" + mock_env.agents.beta.evm_address[2:] + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            storage_slots = ["0x0", "0x1", "0x2"]
            requires_access_list = True
            
        elif scenario_type == "ERC4337_HANDLE_OPS":
            target = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
            calldata = "0x1fad948c" + "0000000000000000000000000000000000000000000000000000000000000040" + "0000000000000000000000001111111111111111111111111111111111111111" + ("00" * 32)
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "UNISWAP_EXACT_INPUT":
            target = "0x3bFA4769FB09eefC5a80d6E87c3B9C650f7Ae48E"
            token_in = mock_env.contracts.target_erc20.replace("0x", "").zfill(64).lower() 
            token_out = "1c7D4B196Cb0C7B01d743Fbc6116a902379C7238".zfill(64).lower()       
            fee = hex(3000).replace("0x", "").zfill(64)                                     
            recipient = mock_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
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
        if is_contract:
            mock_code = "0xfd" if should_revert else "0x608060405234801561001057600080fd5b506101"
        else:
            mock_code = "0x"
        padded_alpha_address = "0x000000000000000000000000" + mock_env.agents.alpha.evm_address[2:]
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
            "chain_id": mock_env.network.chain_id
        }


class EVMOrchestrator:
    def __init__(self, rpc_url: str):
        self.w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    async def verify_connection(self):
        is_connected = await self.w3.is_connected()
        if not is_connected:
            raise ConnectionError(f"Failed to connect to RPC URL: {self.w3.provider.endpoint_uri}")
        log.info(f"✅ Connected to REAL network. Chain ID: {await self.w3.eth.chain_id}")

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
        # [방어적 프로그래밍] address가 정상적인 EVM hex 포맷인지 확인
        if not address or not address.startswith("0x"):
            log.warning(f"MockOrchestrator received non-EVM address: {address}. Returning empty state.")
            return {}

        is_contract = (address.lower() == mock_env.contracts.target_erc20.lower())
        is_revert = (self.user_intent.get("calldata") == "0xdeadbeef" and self.user_intent.get("scenario_type") != "DPHI_INVERSION")
        return EvmBuilder.build_state_snapshot(address, is_contract=is_contract, should_revert=is_revert)

    async def fetch_block_context(self) -> Dict[str, Any]:
        return EvmBuilder.build_block_context()
        
    async def disconnect(self):
        pass


class InversionOrchestrator:
    """dvm.wasm 내부에서 dphi.wasm을 역호출하도록 유도하는 특수 Mock 환경 구성"""
    def __init__(self, user_intent: Dict[str, Any] = None):
        self.user_intent = user_intent or {}

    async def verify_connection(self):
        log.info("🌌 [INVERSION MODE] Cross-VM Precompile Hook Testing initialized.")

    async def fetch_account_state(self, address: str, storage_slots: List[str] = None) -> Dict[str, Any]:
        return {
            "balance": "0x1000000000000000000",  # 1 ETH
            "nonce": 0,
            "code": "0x",
            "storage": {}
        }

    async def fetch_block_context(self) -> Dict[str, Any]:
        return EvmBuilder.build_block_context()

    async def disconnect(self):
        pass