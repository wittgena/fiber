# phase.anchor.adapter.eco
## @lineage: bound.exchange.eco
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature
from arch.xor.surge.model import DynamicSurgeModel

# --- Mandate Models ---
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


# --- Settlement & Invoice Models ---
class X402Invoice(DynamicSurgeModel):
    status: str
    x_payment_protocol: str
    pay_to: str
    amount_usdc: str
    resource_id: str
    timestamp: int

class X402SettlementReceipt(DynamicSurgeModel):
    receipt_id: str
    receipt_type: str  # 🌟 추가: 'DEFERRED_ALLOWANCE' (예치), 'INSTANT_PUSH', 'DEFERRED_PULL'
    tx_hash: str       # 오프체인이면 'off-chain-mandate', 온체인이면 실제 해시
    network: str
    amount_usdc: str   # 🌟 변경: paid_amount_usdc -> amount_usdc (예산과 지불을 포괄)
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
        """에이전트가 오프체인에서 DPHI에게 과금을 허용하는 '백지수표(Mandate)' 생성"""
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
    def verify_mandate_signature(cls, mandate_result: Ap2MandateResult) -> bool:
        """DPHI 엣지에서 에이전트가 제출한 Mandate의 서명을 수학적으로 검증"""
        try:
            pub_bytes = bytes.fromhex(mandate_result.authorization.signer_pub)
            public_key = ed25519.Ed25519PublicKey.from_public_bytes(pub_bytes)
            
            mandate_dict = mandate_result.mandate.model_dump(exclude_none=True)
            canonical_bytes = json.dumps(mandate_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')
            signature_bytes = bytes.fromhex(mandate_result.authorization.signature)
            
            public_key.verify(signature_bytes, hashlib.sha256(canonical_bytes).digest())
            return True
        except (InvalidSignature, ValueError):
            return False

    @classmethod
    def issue_deferred_receipt(cls, mandate: Ap2MandateResult) -> X402SettlementReceipt:
        """
        🌟 [지연 정산 - 1단계] 
        트랜잭션 발생 없이 서명된 Mandate를 기반으로 롤업용 '역량 토큰(Capability Receipt)' 발급
        """
        amount = mandate.mandate.constraints.max_spend_usdc
        payer = mandate.mandate.requester_id
        ts = int(time.time() * 1000)
        
        # 오프체인 발급이므로 트랜잭션 해시 대신 Mandate의 서명 해시를 사용
        pseudo_hash = f"0x_offchain_{hashlib.sha256(mandate.authorization.signature.encode()).hexdigest()[:16]}"
        
        return X402SettlementReceipt(
            receipt_id=f"cap_{pseudo_hash[12:24]}",
            receipt_type="DEFERRED_ALLOWANCE",
            tx_hash=pseudo_hash,
            network="x402/dvm-rollup-offchain",
            amount_usdc=amount,
            payer_wallet=payer,
            settled_at=ts
        )

    @classmethod
    async def process_deferred_pull(
        cls, 
        agent_wallet_address: str, 
        accrued_amount_usdc: str,
        clearing_wallet_adapter: Any
    ) -> X402SettlementReceipt:
        """
        🌟 [지연 정산 - 2단계] 
        DPHI 백그라운드 워커가 L1/L2 메인넷에서 에이전트의 빚을 징수(transferFrom) 후 최종 영수증 발급
        """
        if clearing_wallet_adapter and not getattr(clearing_wallet_adapter, "simulate", True):
            # clearing_wallet_adapter는 DPHI 마스터 키를 가지고 있으며 transfer_from 메서드를 호출함
            tx_hash = await clearing_wallet_adapter.transfer_from(
                from_address=agent_wallet_address,
                amount_str=accrued_amount_usdc,
                asset="usdc"
            )
        else:
            mock_tx_seed = f"pull_{agent_wallet_address}_{accrued_amount_usdc}_{time.time()}".encode('utf-8')
            tx_hash = f"0x{hashlib.sha256(mock_tx_seed).hexdigest()}"

        return X402SettlementReceipt(
            receipt_id=f"rcpt_pull_{tx_hash[2:14]}",
            receipt_type="ONCHAIN_PULL",
            tx_hash=tx_hash,
            network="x402/base-sepolia",
            amount_usdc=accrued_amount_usdc,
            payer_wallet=agent_wallet_address,
            settled_at=int(time.time() * 1000)
        )

    # -----------------------------------------------------------------
    # 기존 레거시 (즉시 능동 결제) - 호환성을 위해 유지
    # -----------------------------------------------------------------
    @classmethod
    def build_x402_invoice(cls, payee_address: str, amount_usdc: str, resource_id: str) -> X402Invoice:
        return X402Invoice(
            status="HTTP_402_PAYMENT_REQUIRED",
            x_payment_protocol="x402/base-sepolia",
            pay_to=payee_address,
            amount_usdc=amount_usdc,
            resource_id=resource_id,
            timestamp=int(time.time() * 1000)
        )

    @classmethod
    async def process_instant_settlement(
        cls, 
        invoice: X402Invoice, 
        agent_wallet_address: str,
        wallet_adapter: Any = None
    ) -> X402SettlementReceipt:
        tx_hash = ""
        if wallet_adapter and not getattr(wallet_adapter, "simulate", True):
            tx_hash = await wallet_adapter.transfer(
                to_address=invoice.pay_to,
                amount_str=invoice.amount_usdc,
                asset="usdc"
            )
        else:
            mock_tx_seed = f"push_{invoice.pay_to}_{invoice.amount_usdc}_{time.time()}".encode('utf-8')
            tx_hash = f"0x{hashlib.sha256(mock_tx_seed).hexdigest()}"

        return X402SettlementReceipt(
            receipt_id=f"rcpt_push_{tx_hash[2:14]}",
            receipt_type="INSTANT_PUSH",
            tx_hash=tx_hash,
            network=invoice.x_payment_protocol,
            amount_usdc=invoice.amount_usdc,
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