# phase.dphi.adapter.evm
## @lineage: dphi.adapter.evm
import asyncio
from typing import Dict, Any, List

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from phase.dphi.config.dphi import mock_env
from phase.dphi.builder.phase import PhaseBuilder
from watcher.plane.emitter import get_emitter

log = get_emitter("dphi.adapter.evm")

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
        is_contract = (address.lower() == mock_env.contracts.target_erc20.lower())
        is_revert = (self.user_intent.get("calldata") == "0xdeadbeef" and self.user_intent.get("scenario_type") != "DPHI_INVERSION")
        return PhaseBuilder.evm_state_snapshot(address, is_contract=is_contract, should_revert=is_revert)

    async def fetch_block_context(self) -> Dict[str, Any]:
        return PhaseBuilder.evm_block_context()
        
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
        return PhaseBuilder.evm_block_context()

    async def disconnect(self):
        pass