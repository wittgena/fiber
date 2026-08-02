# dphi.adapter.wallet
## @lineage: agent.dphi.adapter.wallet
import os
import time
import contextlib
from typing import Any

from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.wallet")

@contextlib.contextmanager
def ephemeral_secret_injection(secrets: dict[str, str]):
    """
    @desc: 서드파티 SDK 초기화를 위해 아주 짧은 시간 동안만 os.environ에 
           Secret을 주입하고, 작업이 끝나면 즉시 삭제하여 메모리/환경 변수를 보호합니다.
    """
    # 기존 환경 변수 백업
    original_env = {k: os.environ.get(k) for k in secrets.keys()}
    
    # Secret 임시 주입
    os.environ.update(secrets)
    try:
        yield
    finally:
        # 안전한 롤백 및 Secret 폐기
        for k, original_v in original_env.items():
            if original_v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = original_v

class WalletAdapter:
    """
    @desc: Coinbase AgentKit wrapper for managing on-chain wallets.
           Zero-Trust: Reads keys via SecretManager, isolating from raw OS env.
    """
    def __init__(self, network_id: str = "base-sepolia", simulate: bool = False):
        self.network_id = network_id
        self.simulate = simulate
        self.wallet = None
        
        if not self.simulate:
            self._initialize_secure_wallet()

    def _initialize_secure_wallet(self):
        try:
            # 1. 외부 라이브러리 로드
            from coinbase_agentkit import CdpWalletProvider
            
            # 2. SecretManager를 통해 안전하게 Vault/OIDC/KMS에서 키를 패치
            # (만약 없으면 None 반환)
            api_name = get_secret_str("CDP_API_KEY_NAME")
            api_pkey = get_secret_str("CDP_API_KEY_PRIVATE_KEY")
            
            if not api_name or not api_pkey:
                log.warning("[Wallet] CDP API Keys missing in SecretManager. Forcing simulate=True.")
                self.simulate = True
                return

            # 3. 임시 주입 패턴(Ephemeral Injection)으로 SDK 초기화
            # 이 블록 안에서만 CdpWalletProvider가 환경 변수에 접근할 수 있음
            injected_secrets = {
                "CDP_API_KEY_NAME": api_name,
                "CDP_API_KEY_PRIVATE_KEY": api_pkey
            }
            
            with ephemeral_secret_injection(injected_secrets):
                self.wallet = CdpWalletProvider.create_wallet(network_id=self.network_id)
                log.info(f"[Wallet] CDP Wallet created successfully on {self.network_id} (Secured via SecretManager)")
                
        except ImportError:
            log.warning("[Wallet] coinbase_agentkit not installed. Forcing simulate=True.")
            self.simulate = True
        except Exception as e:
            log.error(f"[Wallet] Failed to initialize CDP Wallet: {e}")
            self.simulate = True

    # ... (fund_wallet, transfer 등 기존 메서드는 동일하게 유지) ...
    def fund_wallet(self, asset: str = "usdc", amount: str = "0.1") -> bool:
        if self.simulate:
            log.info(f"[Wallet-Sim] Simulated funding {amount} {asset}.")
            return True
            
        log.info(f"[Wallet] Requesting faucet for {amount} {asset}...")
        try:
            self.wallet.fund(asset=asset, amount=amount)
            return True
        except Exception as e:
            log.error(f"[Wallet] Faucet funding failed: {e}")
            return False

    def transfer(self, to_address: str, amount: str, asset: str = "usdc") -> str:
        if self.simulate:
            mock_hash = f"0xsim_{int(time.time()*1000)}"
            log.info(f"[Wallet-Sim] Transferred {amount} {asset} to {to_address}. Tx: {mock_hash}")
            return mock_hash

        log.info(f"[Wallet] Transferring {amount} {asset} to {to_address}...")
        try:
            receipt = self.wallet.transfer(to_address=to_address, amount=amount, asset=asset)
            tx_hash = getattr(receipt, "transaction_hash", getattr(receipt, "hash", str(receipt)))
            log.info(f"[Wallet] Transfer success. Tx: {tx_hash}")
            return tx_hash
        except Exception as e:
            log.error(f"[Wallet] Transfer failed: {e}")
            raise