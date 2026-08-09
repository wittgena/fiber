# dphi.adapter.eco
## @lineage: phase.dphi.adapter.eco
## @lineage: receptor.adapter.eco
## @lineage: receptor.surface.adapter.eco
## @lineage: surface.adapter.eco
## @lineage: kernel.dphi.adapter.eco
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.xor.surge.model import DynamicSurgeModel
from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter
from kernel.dphi.exchange.config import billing_config
from kernel.dphi.cgroup import Tier

log = get_emitter("dphi.adapter")

class Ap2MandateConstraints(DynamicSurgeModel):
    max_spend_usdc: str
    expiration_ts: int

class Ap2MandatePayload(DynamicSurgeModel):
    protocol: str
    requester_id: str
    target_action: str
    constraints: Ap2MandateConstraints
    issued_at: int
    metadata: Optional[Dict[str, Any]] = None

class Ap2Authorization(DynamicSurgeModel):
    signer_pub: str
    signature: str

class Ap2MandateResult(DynamicSurgeModel):
    mandate: Ap2MandatePayload
    authorization: Ap2Authorization

class X402Invoice(DynamicSurgeModel):
    status: str
    x_payment_protocol: str
    pay_to: str
    amount_usdc: str
    resource_id: str
    timestamp: int

class X402SettlementReceipt(DynamicSurgeModel):
    receipt_id: str
    tx_hash: str
    network: str
    paid_amount_usdc: str
    payer_wallet: str
    settled_at: int

class SettlementPayload(DynamicSurgeModel):
    batch_id: str
    state_root: str
    attestations: List[str] 
    gas_used: int
    timestamp: int

class EcoAdapter:
    @classmethod
    def build_ap2_mandate(
        cls, 
        requester_id: str, 
        target_action: str, 
        max_spend_usdc: str, 
        signer_key: ed25519.Ed25519PrivateKey,
        validity_ms: int = 3600000,
        **kwargs
    ) -> Ap2MandateResult:
        """@desc: Creates a Verifiable Credential-like Mandate for agent authorization."""
        expiration_ts = int(time.time() * 1000) + validity_ms
        constraints = Ap2MandateConstraints(
            max_spend_usdc=max_spend_usdc,
            expiration_ts=expiration_ts
        )
        
        payload = Ap2MandatePayload(
            protocol="AP2-v1.0",
            requester_id=requester_id,
            target_action=target_action,
            constraints=constraints,
            issued_at=int(time.time() * 1000),
            metadata=kwargs if kwargs else None
        )
        mandate_dict = payload.model_dump(exclude_none=True)
        canonical_bytes = json.dumps(mandate_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')
        signature = signer_key.sign(hashlib.sha256(canonical_bytes).digest()).hex()
        pubhex = signer_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

        auth = Ap2Authorization(signer_pub=pubhex, signature=signature)
        return Ap2MandateResult(mandate=payload, authorization=auth)

    @classmethod
    def build_x402_invoice(cls, payee_address: str, amount_usdc: str, resource_id: str) -> X402Invoice:
        """@desc: Generates an HTTP 402 Payment Required invoice for M2M interactions."""
        return X402Invoice(
            status="HTTP_402_PAYMENT_REQUIRED",
            x_payment_protocol="x402/base-sepolia",
            pay_to=payee_address,
            amount_usdc=amount_usdc,
            resource_id=resource_id,
            timestamp=int(time.time() * 1000)
        )

    @classmethod
    def process_x402_settlement(
        cls, 
        invoice: X402Invoice, 
        agent_wallet_address: str,
        wallet_adapter: Any = None
    ) -> X402SettlementReceipt:
        """@desc: Settles the strictly typed X402Invoice via injected WalletAdapter"""
        tx_hash = ""
        if wallet_adapter and not wallet_adapter.simulate:
            tx_hash = wallet_adapter.transfer(
                to_address=invoice.pay_to,
                amount=invoice.amount_usdc,
                asset="usdc"
            )
        else:
            mock_tx_seed = f"{invoice.pay_to}_{invoice.amount_usdc}_{time.time()}".encode('utf-8')
            tx_hash = f"0x{hashlib.sha256(mock_tx_seed).hexdigest()}"

        return X402SettlementReceipt(
            receipt_id=f"rcpt_{tx_hash[2:14]}",
            tx_hash=tx_hash,
            network=invoice.x_payment_protocol,
            paid_amount_usdc=invoice.amount_usdc,
            payer_wallet=agent_wallet_address,
            settled_at=int(time.time() * 1000)
        )

    @classmethod
    def embed_economy_state(
        cls, 
        base_cached_states: Dict[str, Any], 
        mandate: Optional[Ap2MandateResult] = None, 
        receipt: Optional[X402SettlementReceipt] = None
    ) -> Dict[str, Any]:
        updated_state = dict(base_cached_states) if base_cached_states else {}
        if mandate is not None:
            updated_state["ap2_mandate"] = mandate.model_dump(exclude_none=True)
            
        if receipt is not None:
            updated_state["x402_settlement_receipt"] = receipt.model_dump(exclude_none=True)
            
        return updated_state

def inject_and_clear_secrets(secrets: dict[str, str], action_fn: callable):
    os.environ.update(secrets)
    try:
        return action_fn()
    finally:
        for k in secrets.keys():
            os.environ.pop(k, None)

class WalletAdapter:
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