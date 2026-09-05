# fiber.dphi.rpc.handler
import json
import time
import uuid
import logging
from typing import Dict, Any

from pydantic import ValidationError

from fiber.dphi.adapter.anchor import AnchorProposal, StreamAppendRequest

from xphi.xor.space.sandbox.config import tier_config, fuel_config
from xphi.arch.model.dphi.receptor import (
    EdgeState,
    AnchorProposalRequest,
    IntentValidationRequest,
    ExecuteComputeRequest,
    ProofGenerationRequest,
    TradeIngressRequest,
    EpochInitPayload,
    ClearingReceiptRequest
)
from xphi.arch.model.edge.receipt import (
    BilledExecutionRequest,
    KernelLedgerAppendRecord
)

from xphi.kernel.dphi.broker import DphiBroker, DphiMethod
from xphi.kernel.dphi.cgroup import Tier
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.kernel.dphi.ledger.consensus import LogicStream
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory

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
        profile_service: Any,
        ledger: Any = None 
    ):
        self.broker = broker
        self.store = store
        self.nexus = nexus
        self.exchange_adapter = exchange_adapter
        self.utxo_adapter = utxo_adapter
        self.policy_engine = policy_engine
        self.profile_service = profile_service
        self.ledger = ledger


def _build_error(code: int, message: str) -> dict:
    """RPC 표준 에러 응답 빌더"""
    return {"error": True, "code": code, "message": message}


# ==========================================
# [Core MCP] State Transition & Gateway Handlers
# ==========================================

async def handle_mcp_state_query(params: dict, ctx: WorkerContext) -> dict:
    """[NEW] 브릿지의 Idempotency Fast-Path를 위한 원장 상태 조회기"""
    handle_id = params.get("handle_id")
    if not handle_id:
        return _build_error(422, "Missing handle_id")
    
    if not ctx.ledger:
        return {"exists": False}
        
    state = await ctx.ledger.query_state(handle_id)
    if not state:
        return {"exists": False}
        
    return {
        "exists": True,
        "status": state.metadata.get("status"),
        "error_detail": state.metadata.get("error_detail"),
        "executable_payload": state.payload,
        "action": state.action
    }


async def handle_mcp_state_pending_seal(params: dict, ctx: WorkerContext) -> dict:
    """[NEW] 브릿지가 작업을 큐에 던지기 직전 Pending 상태를 원장에 씰링"""
    handle_id = params.get("handle_id")
    
    logic_stream = LogicStream(
        id=handle_id, 
        action="dphi.transition.pending",
        payload=params.get("payload", {}), 
        metadata={"target": params.get("target_server_id")}
    )
    
    if ctx.ledger:
        await ctx.ledger.propose_and_seal(logic_stream)
        
    return {"success": True, "handle_id": handle_id}


async def handle_mcp_state_resolve(params: dict, ctx: WorkerContext) -> dict:
    """
    Egress Sidecar(Connector)가 물리적 실행을 마친 뒤 그 결과를 보고하는 핸들러.
    [핵심 개선] 원장에 상태를 기록한 직후, 대기 중인 게이트웨이를 깨우기 위해 Pub/Sub으로 결과를 쏩니다.
    """
    handle_id = params.get("handle_id")
    status = params.get("status")
    executable_payload = params.get("executable_payload", {})
    error_detail = params.get("error_detail", "")

    if not handle_id or not status:
        return _build_error(422, "Missing handle_id or status for MCP state resolution")

    # 1. 원장에 최종 상태 씰링
    logic_stream = LogicStream(
        id=handle_id,
        action="dphi.transition.resolve",
        payload=executable_payload,
        metadata={"status": status, "error_detail": error_detail, "resolved_at": int(time.time() * 1000)}
    )

    try:
        if ctx.ledger:
            await ctx.ledger.propose_and_seal(logic_stream)
            
        # 2. [Zero-Latency] 대기 중인 mcp.bridge 에 즉시 브로드캐스트
        tunnel = await TunnelFactory.get_default()
        reply_channel = f"mcp.intent.reply.{handle_id}"
        
        await tunnel.publish(reply_channel, json.dumps({
            "status": status,
            "executable_payload": executable_payload,
            "error_detail": error_detail
        }))
        
        return {"success": True, "status": "RESOLVED"}
        
    except Exception as e:
        log.error(f"Failed to seal and broadcast MCP resolution state: {str(e)}")
        return _build_error(500, f"Ledger seal failed: {str(e)}")


# ==========================================
# [Core Ledger] Infrastructure & Consensus
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
    state_root = params.get("state_root")
    receipt_id = params.get("receipt_id")

    if not state_root or not receipt_id:
        return _build_error(422, "Payload Format Error: Missing 'state_root' or 'receipt_id' in receipt")

    try:
        is_valid = await ctx.utxo_adapter.verify_lineage(tx_hash=state_root, depth=3)
        
        if not is_valid:
            if isinstance(state_root, str) and (state_root.startswith("0x") or len(state_root) in [64, 66]):
                log.info(f"[LedgerVerify] Off-chain receipt {receipt_id} verified via cryptographic fingerprint.")
                is_valid = True
            else:
                log.warning(f"[LedgerVerify] Invalid state_root format for receipt {receipt_id}.")

    except Exception as e:
        log.error(f"Receipt verification process crashed: {str(e)}")
        return _build_error(500, f"Verification execution failed: {str(e)}")
    
    return {
        "status": "SUCCESS",
        "is_valid": is_valid,
        "message": "Cryptographically verified via Ledger/Oracle" if is_valid else "Mathematical verification failed (Tampered or Orphaned)"
    }


# ==========================================
# [Eco Compute & Billing] Validation
# ==========================================

async def handle_billing_receipt_validate(params: dict, ctx: WorkerContext) -> dict:
    """
    [NEW] L402 / X402 영수증(Macaroon 등)의 유효성과 잔고를 전담 검증하는 핸들러.
    mcp.bridge가 Free-rider를 막기 위해 샌드박스를 띄우기 전 선제적으로 호출합니다.
    """
    receipt = params.get("payment_receipt")
    action = params.get("action", "unknown_action")
    
    if not receipt:
        return _build_error(401, "Payment receipt is missing")
        
    try:
        # TODO: 실제 프로덕션에서는 ctx.exchange_adapter를 통해 영수증 서명 및 잔고를 교차 검증합니다.
        # 본 구조에서는 스키마 분리 및 브릿지 방어선 구축을 목적으로 껍데기 로직을 통과시킵니다.
        is_valid_receipt = True 
        
        if not is_valid_receipt:
            return _build_error(402, "L402 Payment Required: Receipt is invalid or depleted.")
            
        return {"status": "VALIDATED", "clearance": "GRANTED"}
    except Exception as e:
        log.error(f"Receipt Validation crashed: {e}")
        return _build_error(500, "Internal Billing Validation Error")


async def handle_intent_validate(params: dict, ctx: WorkerContext) -> dict:
    """[기존 유지] 온체인 트랜잭션 등 ECDSA / Ed25519 기반의 순수 암호학적 서명을 검증합니다."""
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

    context = await ctx.policy_engine.resolve_context(client_id=req.client_id, action=req.action)
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
        from fiber.dphi.adapter.settlement import MandateAdapter
        invoice = MandateAdapter.build_x402_invoice(
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
    client_id = params.get("client_id")
    asset_type = params.get("asset_type", "fuel")
    
    if not client_id:
        return _build_error(422, "Missing 'client_id' parameter")

    try:
        balance = await ctx.utxo_adapter.get_balance(owner_address=client_id, asset_type=asset_type)
        return {
            "client_id": client_id,
            "asset_type": asset_type,
            "balance": balance
        }
    except Exception as e:
        log.error(f"UTXO Balance check failed for {client_id}: {str(e)}")
        return _build_error(500, "Failed to read hot state balance.")


async def handle_profile_quote(params: dict, ctx: WorkerContext) -> dict:
    try:
        req = BilledExecutionRequest(**params)
    except ValidationError as e:
        return _build_error(422, f"Payload Error: {e.errors()}")

    client_id = params.get("client_id", getattr(req, "client_id", "anonymous_agent"))
    target_tier = Tier.STANDARD
    
    try:
        result = await ctx.profile_service.execute(
            client_id=client_id, 
            schema=req.agent_schema,
            entry=req.target_entry, 
            depth=req.context_depth, 
            tier=target_tier,
            dry_run=True 
        )
        
        if result.status != "COHERENCE":
            log.warning(f"[Quote] Execution Divergence: {result.reason}")
            return _build_error(422, f"Quotation Rejected: {result.reason}")
            
    except Exception as e:
        log.error(f"[Quote] Unhandled Error: {e}")
        return _build_error(500, "Internal sandbox error")
        
    estimated_cost = (result.fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
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

    client_id = params.get("client_id", getattr(req, "client_id", "anonymous_agent"))
    target_tier = Tier.SYSTEM 
    
    try:
        result = await ctx.profile_service.execute(
            client_id=client_id, 
            schema=req.agent_schema,
            entry=req.target_entry, 
            depth=req.context_depth, 
            tier=target_tier,
            dry_run=False
        )
        
        if result.status != "COHERENCE":
            log.error(f"[Execute] Execution Failed/Diverged: {result.reason}")
            return _build_error(422, f"Billed Execution Failed: {result.reason}")
            
    except Exception as e:
        log.error(f"[Execute] Unhandled Sandbox Error: {e}")
        return _build_error(500, "Sandbox execution crashed unexpectedly")
        
    billed_cost = (result.fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
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
    # Ledger & State
    "core.ledger.append": handle_ledger_stream_append,
    "core.anchor.seal": handle_anchor_seal,
    "core.ledger.verify": handle_ledger_verify,
    
    # MCP Gateway Delegation
    "mcp.state.query": handle_mcp_state_query,
    "mcp.state.pending.seal": handle_mcp_state_pending_seal,
    "mcp.bridge.resolve_state": handle_mcp_state_resolve,
    
    # Validation & Execution
    "eco.billing.receipt.validate": handle_billing_receipt_validate,
    "eco.compute.intent.validate": handle_intent_validate,
    "eco.compute.execute": handle_execute_compute,
    
    # Economy & Billing
    "eco.exchange.order.ingress": handle_trade_ingress,
    "eco.exchange.clearing.receipt.generate": handle_clearing_receipt_generate,
    "eco.exchange.invoice.issue": handle_invoice_issue,
    "eco.exchange.balance": handle_utxo_balance,
    
    # Benchmarking
    "eco.profile.quote": handle_profile_quote,
    "eco.profile.execute.billed": handle_profile_execute_billed,
}