# dphi.adapter.eco
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from arch.xor.surge.model import DynamicSurgeModel

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
        wallet_adapter: Any = None  # 타입 힌팅을 Any로 두어 순환 참조 방지
    ) -> X402SettlementReceipt:
        """@desc: Settles the strictly typed X402Invoice via injected WalletAdapter"""
        tx_hash = ""
        
        if wallet_adapter and not getattr(wallet_adapter, "simulate", True):
            # 분리된 WalletAdapter의 transfer 메서드를 호출
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