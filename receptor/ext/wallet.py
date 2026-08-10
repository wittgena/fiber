# receptor.ext.wallet
import os
from typing import Optional

from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter

log = get_emitter("ext.wallet")

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
        """
        [신규 추가] EcoAdapter.process_x402_settlement 에서 호출하는 실제 송금 메서드
        """
        if self.simulate or not self.wallet:
            log.warning(f"[Wallet] Simulated transfer: {amount} {asset} to {to_address}")
            return f"0x_simulated_tx_{os.urandom(8).hex()}"

        try:
            log.info(f"[Wallet] Transferring {amount} {asset} to {to_address}...")
            # CDP AgentKit의 실제 송금 API 호출 (버전에 따라 메서드명이 다를 수 있음)
            invocation = self.wallet.transfer(amount=amount, asset_id=asset, destination=to_address)
            
            # 트랜잭션 해시 반환
            tx_hash = invocation.transaction.transaction_hash if hasattr(invocation, 'transaction') else "0x_unknown_tx"
            log.info(f"[Wallet] Transfer success. TxHash: {tx_hash}")
            return tx_hash
        except Exception as e:
            log.error(f"[Wallet] Transfer failed: {e}")
            raise RuntimeError(f"Wallet transfer failed: {str(e)}")