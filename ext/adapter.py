# ext.adapter
## @lineage: receptor.ext.adapter
## @lineage: receptor.ext.wallet
import os
import asyncio
from typing import Dict, Any, Tuple, Optional

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from arch.xor.secret.manager import get_secret_str
from phase.epoch.config.dphi import mock_env
from watcher.plane.emitter import get_emitter

log = get_emitter("ext.adapter")

def inject_and_clear_secrets(secrets: dict[str, str], action_fn: callable):
    """지정된 함수의 실행 동안만 환경변수에 Secret을 주입하고 즉시 삭제합니다."""
    os.environ.update(secrets)
    try:
        return action_fn()
    finally:
        for k in secrets.keys():
            os.environ.pop(k, None)

class WalletAdapter:
    """Edge Server 전용: CDP(Coinbase Developer Platform) 연동 지갑 어댑터"""
    def __init__(
        self, 
        network_id: str = "base-sepolia", 
        simulate: bool = False,
        api_name: Optional[str] = None,
        api_pkey: Optional[str] = None
    ):
        self.network_id = network_id
        self.simulate = simulate
        self.wallet = None
        self._api_name = api_name
        self._api_pkey = api_pkey
        
        if not self.simulate:
            self._initialize_secure_wallet()

    def _initialize_secure_wallet(self):
        try:
            from coinbase_agentkit import CdpWalletProvider
        except ImportError as e:
            log.error("[Wallet] coinbase_agentkit not installed.")
            raise RuntimeError("Missing required SDK for secure wallet") from e

        api_name = self._api_name or get_secret_str("CDP_API_KEY_NAME")
        api_pkey = self._api_pkey or get_secret_str("CDP_API_KEY_PRIVATE_KEY")
        
        if not api_name or not api_pkey:
            log.error("[Wallet] CDP API Keys missing.")
            raise ValueError("Incomplete credentials for CDP Wallet initialization")

        api_pkey = api_pkey.replace('\\n', '\n')
        injected_secrets = {
            "CDP_API_KEY_NAME": api_name,
            "CDP_API_KEY_PRIVATE_KEY": api_pkey
        }
        
        try:
            self.wallet = inject_and_clear_secrets(
                injected_secrets, 
                lambda: CdpWalletProvider.create_wallet(network_id=self.network_id)
            )
            log.info(f"[Wallet] CDP Wallet created successfully on {self.network_id}")
        except Exception as e:
            log.error(f"[Wallet] Failed to initialize CDP Wallet: {e}")
            raise

    def transfer(self, to_address: str, amount: str, asset: str = "usdc") -> str:
        if self.simulate or not self.wallet:
            log.warning(f"[Wallet] Simulated transfer: {amount} {asset} to {to_address}")
            return f"0x_simulated_tx_{os.urandom(8).hex()}"

        try:
            log.info(f"[Wallet] Transferring {amount} {asset} to {to_address}...")
            invocation = self.wallet.transfer(amount=amount, asset_id=asset, destination=to_address)
            tx_hash = invocation.transaction.transaction_hash if hasattr(invocation, 'transaction') else "0x_unknown_tx"
            log.info(f"[Wallet] Transfer success. TxHash: {tx_hash}")
            return tx_hash
        except Exception as e:
            log.error(f"[Wallet] Transfer failed: {e}")
            raise RuntimeError(f"Wallet transfer failed: {str(e)}")

class Web3Adapter:
    def __init__(self, rpc_url: str = None):
        self.rpc_url = rpc_url or mock_env.network.rpc_url
        self.w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.weth_address = self.w3.to_checksum_address(mock_env.contracts.target_erc20)
        
        # WETH 스마트 컨트랙트 ABI (Balance 조회 및 Deposit 전용)
        self.weth_abi = [
            {"constant": True, "inputs": [{"name": "", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
            {"constant": False, "inputs": [], "name": "deposit", "outputs": [], "payable": True, "type": "function"}
        ]
        self.contract = self.w3.eth.contract(address=self.weth_address, abi=self.weth_abi)

    async def close(self):
        """Web3 세션을 안전하게 종료합니다."""
        await self.w3.provider.disconnect()

    async def get_balances(self, address: str) -> Dict[str, str]:
        """지정된 주소의 ETH(Native) 및 WETH 잔고를 Wei 단위 문자열로 반환합니다."""
        checksum_addr = self.w3.to_checksum_address(address)
        eth_bal = await self.w3.eth.get_balance(checksum_addr)
        weth_bal = await self.contract.functions.balanceOf(checksum_addr).call()
        
        return {
            "eth_wei": str(eth_bal),
            "weth_wei": str(weth_bal)
        }

    async def wrap_weth(self, caller_address: str, amount_wei: int, private_key: str) -> str:
        """ETH를 WETH로 Wrap하는 트랜잭션을 전송하고 TxHash를 반환합니다."""
        checksum_addr = self.w3.to_checksum_address(caller_address)
        nonce = await self.w3.eth.get_transaction_count(checksum_addr)
        
        tx = await self.contract.functions.deposit().build_transaction({
            'from': checksum_addr, 
            'value': amount_wei, 
            'nonce': nonce,
            'gas': 100000, 
            'maxFeePerGas': await self.w3.eth.gas_price,
            'maxPriorityFeePerGas': await self.w3.eth.max_priority_fee,
            'chainId': await self.w3.eth.chain_id
        })
        
        signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
        tx_hash = await self.w3.eth.send_raw_transaction(tx_raw)
        
        log.info(f"[Web3] Wrap Tx broadcasted: {tx_hash.hex()}. Waiting for confirmation...")
        
        receipt = await self.w3.eth.wait_for_transaction_receipt(tx_hash)
        if receipt.status == 1:
            log.info(f"[Web3] Successfully wrapped {amount_wei} Wei into WETH.")
            return tx_hash.hex()
        else:
            raise RuntimeError(f"Wrap Transaction Reverted. Hash: {tx_hash.hex()}")