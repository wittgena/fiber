# fiber.dphi.rpc.handler
## @lineage: fiber.receptor.rpc.handler
import json
import time
import uuid
import logging
from typing import Dict, Any

from pydantic import ValidationError

from fiber.dphi.adapter.anchor import AnchorProposal, StreamAppendRequest
from xphi.arch.contract.model.receptor import (
    EdgeState,
    AnchorProposalRequest,
    IntentValidationRequest,
    ExecuteComputeRequest,
    ProofGenerationRequest,
    TradeIngressRequest,
    EpochInitPayload,
    ClearingReceiptRequest
)
from xphi.watcher.receptor.edge.receipt import (
    BilledExecutionRequest,
    KernelLedgerAppendRecord
)

from xphi.kernel.dphi.broker import DphiBroker, DphiMethod
from xphi.kernel.dphi.cgroup import Tier
from xphi.eco.dphi.config import tier_config, billing_config
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.space.sandbox.profile import VerificationError
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("dphi.handler")

class WorkerContext:
    """
    FastAPI의 Request.app.state 및 Depends()를 대체하는 순수 의존성 컨테이너.
    Worker 데몬 시작 시 한 번 초기화되어 각 핸들러에 주입됩니다.
    """
    def __init__(
        self,
        broker: DphiBroker,
        store: Any, # LogStreamStore
        nexus: Any, # NexusAnchor
        exchange_adapter: Any,
        utxo_adapter: Any,
        policy_engine: Any,
        profile_service: Any
    ):
        self.broker = broker
        self.store = store
        self.nexus = nexus
        self.exchange_adapter = exchange_adapter
        self.utxo_adapter = utxo_adapter
        self.policy_engine = policy_engine
        self.profile_service = profile_service


def _build_error(code: int, message: str) -> dict:
    """RPC 표준 에러 응답 빌더"""
    return {"error": True, "code": code, "message": message}


# ==========================================
# [Core Internal] Ledger & Anchor Handlers
# ==========================================

async def handle_ledger_stream_append(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = StreamAppendRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    request_id = f"ledg_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="LEDGER_INTERNAL_APPEND", bound="edge.internal", req_id=request_id):
        events_dicts = [e.model_dump(exclude_none=True) for e in req.events]
        
        is_authorized = await ctx.store.bulk_append(stream_name=req.stream_name, events=events_dicts)
        if not is_authorized:
            return _build_error(403, "Kernel Blocked Stream Append")
            
        payload_to_hash = KernelLedgerAppendRecord(
            stream_name=req.stream_name,
            timestamp=int(time.time() * 1000),
            events=events_dicts
        ).model_dump(exclude_none=True)
        
        fp_res = await ctx.broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, payload_to_hash)
        if not fp_res.success:
            return _build_error(500, f"WASM Fingerprint Failed: {fp_res.error}")
            
        event_hash = json.loads(fp_res.output)["fingerprint"]
        merkle_proof = None
        
        if req.verbose:
            proof_res = await ctx.broker.invoke(DphiMethod.GENERATE_PROOF, payload_to_hash)
            if proof_res.success:
                merkle_proof = json.loads(proof_res.output).get("current_hash")
                
        return {
            "request_id": request_id, 
            "status": "success",
            "result": {"hash": event_hash, "membership_proof": merkle_proof}
        }


async def handle_anchor_seal(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = AnchorProposalRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    proposal = AnchorProposal(
        receptor_id=req.receptor_id, 
        proposed_parity=req.proposed_parity.model_dump(),
        parent_nexus_id=req.parent_nexus_id, 
        self_parent_state=req.self_parent_state,
        repos=req.repos, 
        signers=req.signers, 
        signatures=req.signatures, 
        timestamp=req.timestamp
    )
    result = await ctx.nexus.anchor_state(proposal)
    
    if not result.is_sealed:
        return _build_error(409, f"Consensus Failed: {result.rupture_reason}")
        
    return {
        "status": EdgeState.SEALED_AND_COMMITTED, 
        "nexus_id": result.nexus_id,
        "commit_hash": result.commit_hash, 
        "receipt": result.receipt.__dict__ if hasattr(result.receipt, "__dict__") else dict(result.receipt)
    }


async def handle_ledger_verify(params: dict, ctx: WorkerContext) -> dict:
    try:
        parity_req = StateAdapter.build_parity_request(
            topos_id_low32=params.get("topos_id_low32"),
            phase_id=params.get("phase_id"),
            nexus_id=params.get("nexus_id")
        )
    except ValueError as ve:
        return _build_error(422, f"Payload Format Error: {str(ve)}")

    canonical_payload = StateAdapter.to_canonical_bytes(parity_req).decode('utf-8')
    res = await ctx.broker.invoke(DphiMethod.VERIFY_PARITY, canonical_payload)
    
    if not res.success:
        log.warning(f"Receipt verification crashed in WASM: {res.error}")
        return _build_error(422, f"Verification execution failed: {res.error}")
    
    output = json.loads(res.output)
    is_valid = output.get("is_valid", False)
    
    return {
        "status": "SUCCESS",
        "is_valid": is_valid,
        "message": "Cryptographically verified via WASM Parity Rule" if is_valid else "Mathematical verification failed (Tampered or Invalid)"
    }


# ==========================================
# [Eco Compute] Computation & Validation
# ==========================================

async def handle_intent_validate(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = IntentValidationRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    if not req.requester_id or not req.action:
        return _build_error(401, "Missing Critical Boundaries (Agent ID or Action)")

    if req.max_fuel_budget and req.max_fuel_budget > 10_000_000:
        return _build_error(401, "Topological Fuel Limit Exceeded (> 10M)")

    if not getattr(req, 'signature', None):
        return _build_error(401, "Signature missing. Request must be cryptographically signed.")

    expected_msg = f"EXECUTE:{req.requester_id}:{req.action}:{req.max_fuel_budget or 1000000}"
    
    try:
        sig_algo = getattr(req, 'sig_algo', 'ECDSA_SECP256K1').upper()
        if sig_algo == "ECDSA_SECP256K1":
            from eth_account import Account
            from eth_account.messages import encode_defunct
            
            msg_hash = encode_defunct(text=expected_msg)
            recovered_address = Account.recover_message(msg_hash, signature=req.signature)
            
            if recovered_address.lower() != req.requester_id.lower():
                log.warning(f"Signature mismatch: Recovered {recovered_address} != Expected {req.requester_id}")
                raise ValueError("Address mismatch")
                
        elif sig_algo == "ED25519":
            log.warning("Ed25519 verification is currently bypassed in mock mode.")
            pass
        else:
            return _build_error(400, f"Unsupported signature algorithm: {sig_algo}")
            
    except ValueError:
        return _build_error(401, "WASM Rejected Intent: CRYPTOGRAPHIC_SIGNATURE_MISMATCH")
    except Exception as e:
        log.error(f"Intent validation crashed: {str(e)}")
        return _build_error(401, "Malformed cryptographic signature")

    return {
        "status": EdgeState.INTENT_VALIDATED, 
        "clearance": {
            "is_valid": True,
            "verified_at": int(time.time() * 1000),
            "agent": req.requester_id,
            "fuel_authorized": req.max_fuel_budget
        }
    }


async def handle_execute_compute(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = ExecuteComputeRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    res = await ctx.broker.execute(code=req.code, variables=req.variables)
    if not res.success:
        return _build_error(422, str(res.error))
        
    return {"status": EdgeState.EXECUTION_SUCCESS, "output": res.output}


# ==========================================
# [Eco Exchange & Profile] Billing & Economy
# ==========================================

async def handle_trade_ingress(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = TradeIngressRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    context = await ctx.policy_engine.resolve_context(agent_id=req.agent_id, action=req.action)
    if context.is_ruptured:
        return _build_error(503, f"Topology Ruptured: {context.reason}")

    press_limit = context.press_limit if hasattr(context, 'press_limit') and context.press_limit > 0 else tier_config.fallback_fuel
    payload_obj = EpochInitPayload(
        ts=int(time.time() * 1000), topo=context.topo_id, press=press_limit,
        rupture=context.is_ruptured, injected_intent=req
    )
    
    res = await ctx.broker.invoke(DphiMethod.INIT_EPOCH, payload_obj.model_dump(exclude_none=True))
    if not res.success:
        return _build_error(400, str(res.error))
        
    return {"status": EdgeState.INTENT_ACCEPTED, "session": json.loads(res.output)}


async def handle_clearing_receipt_generate(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = ClearingReceiptRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    receipt = ctx.exchange_adapter.finalize_settlement(
        entangled_state=req.entangled_state, 
        signatures=req.signatures,
        cost_metrics=req.cost_metrics, 
        tier=Tier.SYSTEM  
    )
    return {
        "status": EdgeState.RECEIPT_GENERATED, 
        "rollup_payload": ctx.exchange_adapter.generate_settlement_payload(receipt)
    }


async def handle_invoice_issue(params: dict, ctx: WorkerContext) -> dict:
    payee_address = params.get("payee_address")
    amount_usdc = params.get("amount_usdc")
    resource_id = params.get("resource_id")
    
    if not all([payee_address, amount_usdc, resource_id]):
        return _build_error(422, "Missing required invoice parameters")

    try:
        from xphi.eco.dphi.settlement import EcoAdapter
        invoice = EcoAdapter.build_x402_invoice(
            payee_address=payee_address,
            amount_usdc=amount_usdc,
            resource_id=resource_id
        )
        return {
            "status": "INVOICE_ISSUED", 
            "invoice": invoice.model_dump() if hasattr(invoice, "model_dump") else invoice.__dict__
        }
    except Exception as e:
        return _build_error(500, f"Invoice Issue Failed: {str(e)}")


async def handle_utxo_balance(params: dict, ctx: WorkerContext) -> dict:
    agent_id = params.get("agent_id")
    asset_type = params.get("asset_type", "fuel")
    
    if not agent_id:
        return _build_error(422, "Missing 'agent_id' parameter")

    try:
        balance = await ctx.utxo_adapter.get_balance(owner_address=agent_id, asset_type=asset_type)
        return {
            "agent_id": agent_id,
            "asset_type": asset_type,
            "balance": balance
        }
    except Exception as e:
        log.error(f"UTXO Balance check failed for {agent_id}: {str(e)}")
        return _build_error(500, "Failed to read hot state balance.")


async def handle_profile_quote(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = BilledExecutionRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    client_project_id = params.get("client_project_id", "generative-language-client-1234")
    
    try:
        result = await ctx.profile_service.execute(
            client_project_id=client_project_id, 
            schema=req.agent_schema,
            entry=req.target_entry, 
            depth=req.context_depth, 
            dry_run=True 
        )
        
        if result.status != "COHERENCE":
            log.warning(f"[Quote] Execution Divergence: {result.reason}")
            return _build_error(422, f"Quotation Rejected: {result.reason}")
            
    except VerificationError as ve:
        return _build_error(401, str(ve))
    except Exception as e:
        log.error(f"[Quote] Unhandled Error: {e}")
        return _build_error(500, "Internal sandbox error")
        
    estimated_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return {
        "status": "QUOTE_READY", 
        "tier_applied": result.tier_applied, 
        "fuel_estimated": result.fuel_consumed,
        "estimated_cost_usd": estimated_cost, 
        "reason": result.reason
    }


async def handle_profile_execute_billed(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = BilledExecutionRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    client_project_id = params.get("client_project_id", "generative-language-client-1234")
    
    try:
        result = await ctx.profile_service.execute(
            client_project_id=client_project_id, 
            schema=req.agent_schema,
            entry=req.target_entry, 
            depth=req.context_depth, 
            dry_run=False
        )
        
        if result.status != "COHERENCE":
            log.error(f"[Execute] Execution Failed/Diverged: {result.reason}")
            return _build_error(422, f"Billed Execution Failed: {result.reason}")
            
    except VerificationError as ve:
        return _build_error(401, str(ve))
    except Exception as e:
        log.error(f"[Execute] Unhandled Sandbox Error: {e}")
        return _build_error(500, "Sandbox execution crashed unexpectedly")
        
    billed_cost = (result.fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
    return {
        "status": "BILLED_EXECUTION_SUCCESS", 
        "tier_applied": result.tier_applied, 
        "fuel_billed": result.fuel_consumed,
        "billed_cost_usd": billed_cost, 
        "reason": result.reason
    }

# ==========================================
# 라우팅 테이블 (Dispatcher용)
# ==========================================
INTERNAL_HANDLERS_REGISTRY = {
    "core.ledger.append": handle_ledger_stream_append,
    "core.anchor.seal": handle_anchor_seal,
    "core.ledger.verify": handle_ledger_verify,
    
    "eco.compute.intent.validate": handle_intent_validate,
    "eco.compute.execute": handle_execute_compute,
    
    "eco.exchange.order.ingress": handle_trade_ingress,
    "eco.exchange.clearing.receipt.generate": handle_clearing_receipt_generate,
    "eco.exchange.invoice.issue": handle_invoice_issue,
    "eco.exchange.balance": handle_utxo_balance,
    
    "eco.profile.quote": handle_profile_quote,
    "eco.profile.execute.billed": handle_profile_execute_billed
}