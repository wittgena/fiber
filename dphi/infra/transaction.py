# fiber.dphi.infra.transaction
## @lineage: fiber.dphi.eco.transaction
import os
import time
import json
import hashlib
from typing import Any, Dict, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature

from xphi.xor.space.sandbox.config import fuel_config
from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.kernel.dphi.cgroup import Tier
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.transaction")

# =====================================================================
# 1. Data Models (DTOs)
# =====================================================================

# --- AP2 Protocol Models ---
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

# --- X402 Invoice & Receipt Models ---
class X402Invoice(DynamicSurgeModel):
    status: str
    x_payment_protocol: str
    pay_to: str
    amount_usdc: str
    resource_id: str
    timestamp: int

class X402SettlementReceipt(DynamicSurgeModel):
    receipt_id: str
    receipt_type: str
    tx_hash: str
    network: str
    amount_usdc: str
    payer_wallet: str
    settled_at: int

# --- Internal Transaction Models ---
class TransactionReceipt(DynamicSurgeModel):
    job_id: str
    topos_id: str
    parity_hash: str
    fuel_consumed: int
    settlement_status: str

class SettlementPayload(DynamicSurgeModel):
    batch_id: str
    state_root: str
    attestations: List[str] 
    gas_used: int
    timestamp: int


# =====================================================================
# 2. Adapters
# =====================================================================
class EcoAdapter:
    """
    AP2(Agentic Payment Protocol) 및 X402(HTTP 402 기반) 프로토콜 규격에 따른
    서명, 검증, 인보이스 발급 등 외부(External) 트랜잭션 프로토콜 처리를 담당합니다.
    """
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
        amount = mandate.mandate.constraints.max_spend_usdc
        payer = mandate.mandate.requester_id
        ts = int(time.time() * 1000)
        
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
        if clearing_wallet_adapter and not getattr(clearing_wallet_adapter, "simulate", True):
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
    # 기존 레거시 (즉시 능동 결제)
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


class ExchangeAdapter:
    """
    커널 내부의 컴퓨팅 리소스(Fuel)를 기반으로 달러(USDC) 환산 및 원장 정산(Settlement)을
    담당하는 내부(Internal) 트랜잭션 어댑터입니다.
    """
    
    def __init__(self, clearing_house_pub_key: str):
        self.clearing_house_pub = clearing_house_pub_key

    def _quantize_fuel_cost(self, fuel_consumed: int, tier: Tier) -> float:
        base_cost = (fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
        multiplier = 1.5 if tier == Tier.SYSTEM else 1.0
        return base_cost * multiplier

    def finalize_settlement(
        self, 
        entangled_state: dict, 
        cost_metrics: dict, 
        signatures: Optional[List[str]] = None,
        tier: Tier = Tier.SYSTEM
    ) -> TransactionReceipt:
        parity = entangled_state.get("parity", {})
        parity_topos_id = parity.get("topos_id", f"unknown_batch_{int(time.time())}")
        parity_phase_id = parity.get("phase_id", 0) 
        
        fuel_consumed = cost_metrics.get("fuel_consumed", 0)
        estimated_usd = self._quantize_fuel_cost(fuel_consumed, tier)
        log.info(f"[ExchangeAdapter] Settlement Finalized. Topos: {parity_topos_id}, Parity: {parity_phase_id}, Cost: ${estimated_usd:.6f}")
        
        return TransactionReceipt(
            job_id=str(parity_topos_id),
            topos_id=str(parity_topos_id),
            parity_hash=str(entangled_state.get("state_hash", parity_phase_id)), 
            fuel_consumed=fuel_consumed,
            settlement_status="COMMITTED_TO_NEXUS"
        )
        
    def generate_settlement_payload(
        self, 
        receipt: TransactionReceipt,
        attestations: Optional[List[str]] = None
    ) -> SettlementPayload:
        """
        내부 영수증(TransactionReceipt)을 공증인(Notary) 증명이 포함된 외부 전송용 페이로드로 패키징합니다.
        """
        return SettlementPayload(
            batch_id=receipt.job_id,
            state_root=receipt.parity_hash,
            attestations=attestations or [],
            gas_used=receipt.fuel_consumed,
            timestamp=int(time.time())
        )