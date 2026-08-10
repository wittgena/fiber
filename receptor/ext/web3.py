# receptor.ext.web3
import os
import asyncio
from typing import Dict, Any, Tuple

from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from phase.epoch.config.dphi import mock_env
from watcher.plane.emitter import get_emitter

log = get_emitter("ext.web3")

class Web3Adapter:
    """Edge 서버 전용 Web3 어댑터. EVM 체인과의 온체인 통신을 담당합니다."""
    
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