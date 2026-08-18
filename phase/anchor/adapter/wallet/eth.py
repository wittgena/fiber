# phase.anchor.adapter.wallet.eth
## @lineage: bound.client.wallet.eth
## @lineage: bound.exchange.wallet.eth
import os
import asyncio
import time
from typing import Optional

from phase.anchor.adapter.web3 import Web3Adapter
from phase.anchor.config.dphi import dphi_env
from watcher.plane.emitter import get_emitter

log = get_emitter("wallet.eth")

class EthWalletAdapter:
    def __init__(
        self,
        web3_adapter: Web3Adapter,
        agent_alias: str = "alpha",
        simulate: bool = False
    ):
        self.w3 = web3_adapter.w3
        self.simulate = simulate
        self.agent_alias = agent_alias
        
        self.private_key = dphi_env.get_agent_pkey(agent_alias)
        self.account = self.w3.eth.account.from_key(self.private_key)
        self.wallet_address = self.account.address
        self.network_id = str(dphi_env.network.chain_id)
        self.erc20_abi = [
            {"constant": False, "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"constant": False, "inputs": [{"name": "_from", "type": "address"}, {"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transferFrom", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"constant": False, "inputs": [{"name": "_spender", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "approve", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        
        log.info(f"[Wallet] Native Wallet initialized for {agent_alias}: {self.wallet_address} (Simulate: {simulate})")

    async def approve(self, spender_address: str, amount_str: str, asset: str = "usdc") -> str:
        """자신의 지갑에서 다른 지갑(spender)이 자금을 Pull 할 수 있도록 한도를 허용(Approve)"""
        if self.simulate:
            log.warning(f"[Wallet] Simulated approve: {amount_str} {asset} for {spender_address}")
            return f"0x_simulated_approve_{os.urandom(8).hex()}"

        try:
            asset_contract_addr = getattr(dphi_env.contracts, f"target_{asset.lower()}", dphi_env.contracts.target_erc20)
            contract_address = self.w3.to_checksum_address(asset_contract_addr)
            contract = self.w3.eth.contract(address=contract_address, abi=self.erc20_abi)
            
            decimals = await contract.functions.decimals().call()
            amount_wei = int(float(amount_str) * (10 ** decimals))
            checksum_spender_addr = self.w3.to_checksum_address(spender_address)
            
            nonce = await self.w3.eth.get_transaction_count(self.wallet_address, 'pending')
            gas_price = await self.w3.eth.gas_price
            max_priority_fee = await self.w3.eth.max_priority_fee
            chain_id = int(self.network_id)

            log.info(f"[Wallet] Approving {amount_str} {asset.upper()} to spender {spender_address}...")

            estimated_gas = await contract.functions.approve(checksum_spender_addr, amount_wei).estimate_gas({
                'from': self.wallet_address
            })

            tx_params = {
                'from': self.wallet_address,
                'nonce': nonce,
                'maxFeePerGas': gas_price,
                'maxPriorityFeePerGas': max_priority_fee,
                'chainId': chain_id,
                'gas': int(estimated_gas * 1.2)
            }
            tx = await contract.functions.approve(checksum_spender_addr, amount_wei).build_transaction(tx_params)
            
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
            tx_hash = await self.w3.eth.send_raw_transaction(tx_raw)

            log.info(f"[Wallet] Broadcasted approve TxHash: {tx_hash.hex()}. Waiting for confirmation...")
            
            try:
                receipt = await asyncio.wait_for(
                    self.w3.eth.wait_for_transaction_receipt(tx_hash),
                    timeout=180.0
                )
            except asyncio.TimeoutError:
                raise RuntimeError(f"Approve receipt polling timed out. TxHash: {tx_hash.hex()}")
                
            if receipt.status == 1:
                log.info(f"[Wallet] Approve success. TxHash: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                raise RuntimeError(f"Approve Transaction Reverted. Hash: {tx_hash.hex()}")
        except Exception as e:
            log.error(f"[Wallet] Native approve failed: {e}")
            raise RuntimeError(f"Wallet approve failed: {str(e)}")

    async def transfer(self, to_address: str, amount_str: str, asset: str = "usdc") -> str:
        """자신의 지갑에서 다른 지갑(to_address)으로 자금을 전송 (Push)"""
        if self.simulate:
            log.warning(f"[Wallet] Simulated transfer: {amount_str} {asset} to {to_address}")
            return f"0x_simulated_tx_{os.urandom(8).hex()}"

        try:
            asset_contract_addr = getattr(dphi_env.contracts, f"target_{asset.lower()}", dphi_env.contracts.target_erc20)
            contract_address = self.w3.to_checksum_address(asset_contract_addr)
            contract = self.w3.eth.contract(address=contract_address, abi=self.erc20_abi)
            
            decimals = await contract.functions.decimals().call()
            amount_wei = int(float(amount_str) * (10 ** decimals))
            checksum_to_addr = self.w3.to_checksum_address(to_address)
            
            nonce = await self.w3.eth.get_transaction_count(self.wallet_address, 'pending')

            gas_price = await self.w3.eth.gas_price
            max_priority_fee = await self.w3.eth.max_priority_fee
            chain_id = int(self.network_id)

            log.info(f"[Wallet] Transferring {amount_str} {asset.upper()} ({amount_wei} raw) to {to_address}...")

            estimated_gas = await contract.functions.transfer(checksum_to_addr, amount_wei).estimate_gas({
                'from': self.wallet_address
            })

            tx_params = {
                'from': self.wallet_address,
                'nonce': nonce,
                'maxFeePerGas': gas_price,
                'maxPriorityFeePerGas': max_priority_fee,
                'chainId': chain_id,
                'gas': int(estimated_gas * 1.2)
            }
            tx = await contract.functions.transfer(checksum_to_addr, amount_wei).build_transaction(tx_params)
            
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
            tx_hash = await self.w3.eth.send_raw_transaction(tx_raw)

            log.info(f"[Wallet] Broadcasted TxHash: {tx_hash.hex()}. Waiting for confirmation...")
            
            try:
                receipt = await asyncio.wait_for(
                    self.w3.eth.wait_for_transaction_receipt(tx_hash),
                    timeout=180.0
                )
            except asyncio.TimeoutError:
                raise RuntimeError(f"Transaction receipt polling timed out. TxHash: {tx_hash.hex()}")
                
            if receipt.status == 1:
                log.info(f"[Wallet] Transfer success. TxHash: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                raise RuntimeError(f"Transaction Reverted. Hash: {tx_hash.hex()}")
        except Exception as e:
            log.error(f"[Wallet] Native Transfer failed: {e}")
            raise RuntimeError(f"Wallet transfer failed: {str(e)}")

    async def transfer_from(self, from_address: str, amount_str: str, asset: str = "usdc", to_address: Optional[str] = None) -> str:
        """사전에 approve된 외부 지갑(from)에서 지정된 지갑(to)으로 자금을 강제 징수 (Pull)"""
        target_to_address = to_address if to_address else self.wallet_address
        if self.simulate:
            log.warning(f"[Wallet] Simulated transfer_from: Pulling {amount_str} {asset} from {from_address} to {target_to_address}")
            return f"0x_simulated_transferfrom_{os.urandom(8).hex()}"

        try:
            asset_contract_addr = getattr(dphi_env.contracts, f"target_{asset.lower()}", dphi_env.contracts.target_erc20)
            contract_address = self.w3.to_checksum_address(asset_contract_addr)
            contract = self.w3.eth.contract(address=contract_address, abi=self.erc20_abi)
            
            decimals = await contract.functions.decimals().call()
            amount_wei = int(float(amount_str) * (10 ** decimals))
            
            checksum_from_addr = self.w3.to_checksum_address(from_address)
            checksum_to_addr = self.w3.to_checksum_address(target_to_address)
            
            nonce = await self.w3.eth.get_transaction_count(self.wallet_address, 'pending')
            gas_price = await self.w3.eth.gas_price
            max_priority_fee = await self.w3.eth.max_priority_fee
            chain_id = int(self.network_id)

            log.info(f"[Wallet] Executing transferFrom: {amount_str} {asset.upper()} from {from_address} to {target_to_address}...")

            estimated_gas = await contract.functions.transferFrom(checksum_from_addr, checksum_to_addr, amount_wei).estimate_gas({
                'from': self.wallet_address
            })

            tx_params = {
                'from': self.wallet_address,
                'nonce': nonce,
                'maxFeePerGas': gas_price,
                'maxPriorityFeePerGas': max_priority_fee,
                'chainId': chain_id,
                'gas': int(estimated_gas * 1.2)
            }
            tx = await contract.functions.transferFrom(checksum_from_addr, checksum_to_addr, amount_wei).build_transaction(tx_params)
            
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key=self.private_key)
            tx_raw = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
            tx_hash = await self.w3.eth.send_raw_transaction(tx_raw)

            log.info(f"[Wallet] Broadcasted transferFrom TxHash: {tx_hash.hex()}. Waiting for confirmation...")
            
            try:
                receipt = await asyncio.wait_for(
                    self.w3.eth.wait_for_transaction_receipt(tx_hash),
                    timeout=180.0
                )
            except asyncio.TimeoutError:
                raise RuntimeError(f"transferFrom receipt polling timed out. TxHash: {tx_hash.hex()}")
                
            if receipt.status == 1:
                log.info(f"[Wallet] transferFrom success. TxHash: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                raise RuntimeError(f"transferFrom Transaction Reverted. Hash: {tx_hash.hex()}")
        except Exception as e:
            log.error(f"[Wallet] Native transferFrom failed: {e}")
            raise RuntimeError(f"Wallet transferFrom failed: {str(e)}")