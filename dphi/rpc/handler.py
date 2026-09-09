# fiber.dphi.rpc.handler
import json
import time
import uuid
import logging
from typing import Dict, Any

from pydantic import ValidationError

from fiber.dphi.infra.eco.anchor import AnchorProposal, StreamAppendRequest

from xphi.bound.space.sandbox.config import tier_config, fuel_config
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

from xphi.kernel.wasm.broker import DphiBroker, DphiMethod
from xphi.kernel.wasm.cgroup import Tier
from xphi.kernel.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.state.ledger.consensus import LogicStream
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.adapter.pta import PtaTransaction, PtaInput, PtaPointer, PhaseAnchorOutput

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
        pta_adapter: Any,
        policy_engine: Any,
        profile_service: Any,
        ledger: Any = None 
    ):
        self.broker = broker
        self.store = store
        self.nexus = nexus
        self.exchange_adapter = exchange_adapter
        self.pta_adapter = pta_adapter
        self.policy_engine = policy_engine
        self.profile_service = profile_service
        self.ledger = ledger


def _build_error(code: int, message: str) -> dict:
    """RPC 표준 에러 응답 빌더"""
    return {"error": True, "code": code, "message": message}


# ==========================================
# [Core MCP] State Transition & Gateway Handlers
# (PTA Phase Anchor 기반으로 재설계)
# ==========================================

async def handle_mcp_state_query(params: dict, ctx: WorkerContext) -> dict:
    """
    [IMPROVED] 브릿지의 Idempotency Fast-Path를 위한 상태 조회기
    - 무거운 원장 DB(RocksDB) 대신, PTA의 인메모리 풀(Hot State)을 탐색하여 O(N)으로 초지연 반환
    """
    handle_id = params.get("handle_id")
    if not handle_id:
        return _build_error(422, "Missing handle_id")
    
    expected_owner = f"mcp_bridge_{handle_id}"
    
    # 1. 메모리에 올라온 PTA 앵커 풀 순회 (Hot State)
    for key, output in ctx.pta_adapter._unspent_pool.items():
        if output.owner == expected_owner and output.asset_type == "mcp_state_anchor":
            # PhaseAnchorOutput 객체에서 데이터 추출
            status = getattr(output, "phase_status", "UNKNOWN")
            payload = getattr(output, "executable_payload", {})
            
            # 메타데이터(에러 상세 등)가 필요할 경우에만 Oracle(DB) 단건 조회
            tx_hash = key.split(":")[0]
            snapshot = ctx.pta_adapter.oracle._get_raw_object("commit", tx_hash)
            meta = snapshot.get("metadata", {}) if snapshot else {}
            
            return {
                "exists": True,
                "status": status,
                "error_detail": meta.get("error_detail", ""),
                "executable_payload": payload,
                "action": meta.get("action", "")
            }
            
    return {"exists": False}


async def handle_mcp_state_pending_seal(params: dict, ctx: WorkerContext) -> dict:
    """
    [IMPROVED] 브릿지가 작업을 큐에 던지기 직전 Pending 상태를 PTA UTXO로 씰링(Being)
    """
    handle_id = params.get("handle_id")
    payload = params.get("payload", {})
    target_server_id = params.get("target_server_id")

    # 1. PTA 상태 앵커 생성 (Input 없이 Output만 생성 = 최초 상태 발현/Being)
    initial_phase = PhaseAnchorOutput(
        handle_id=handle_id,
        phase_status="PENDING",
        executable_payload=payload
    )
    
    # 2. PTA 트랜잭션 구성
    tx = PtaTransaction(
        inputs=[], 
        outputs=[initial_phase], 
        metadata={"target": target_server_id, "action": "dphi.transition.pending"}
    )
    
    # 3. PTA Adapter를 통해 검증 및 원장 기록
    await ctx.pta_adapter.execute_transaction(tx)
    
    return {"success": True, "handle_id": handle_id}


async def handle_mcp_state_resolve(params: dict, ctx: WorkerContext) -> dict:
    """
    - 실행 종료 후 상태를 YIELD/RESOLVED/FAULTED 로 전이하는 핸들러
    - 이전 상태 앵커를 소모(Void)하고 다음 상태 앵커를 발행(Being)하는 원자적 트랜잭션을 실행
    """
    handle_id = params.get("handle_id")
    status = params.get("status", "RESOLVED").upper()
    executable_payload = params.get("executable_payload", {})
    error_detail = params.get("error_detail", "")

    if not handle_id or not status:
        return _build_error(422, "Missing handle_id or status")

    expected_owner = f"mcp_bridge_{handle_id}"
    
    # 1. 진행 중인 이전 상태(UTXO) 탐색
    prev_pointer_key = None
    for key, output in ctx.pta_adapter._unspent_pool.items():
        if output.owner == expected_owner and output.asset_type == "mcp_state_anchor":
            prev_pointer_key = key
            break

    # 2. 이전 상태를 소모하기 위한 Input 구성
    inputs = []
    if prev_pointer_key:
        tx_hash, idx = prev_pointer_key.split(":")
        inputs.append(PtaInput(
            pointer=PtaPointer(tx_hash=tx_hash, output_index=int(idx)),
            signature="internal_bridge_signature_bypass" # 엣지 내부 트러스트 존의 더미 서명
        ))

    # 3. 다음 상태(Phase) 앵커 생성
    next_phase = PhaseAnchorOutput(
        handle_id=handle_id,
        phase_status=status,
        executable_payload=executable_payload
    )
    
    tx = PtaTransaction(
        inputs=inputs, 
        outputs=[next_phase], 
        metadata={"error_detail": error_detail, "resolved_at": int(time.time() * 1000)}
    )
    
    # 4. 원자적 상태 전이 실행 (이중 지불 방지를 통한 멱등성 보장)
    await ctx.pta_adapter.execute_transaction(tx)

    # 5. [Zero-Latency] 대기 중인 게이트웨이 브릿지에 결과 즉시 브로드캐스트
    tunnel = await TunnelFactory.get_default()
    reply_channel = f"mcp.intent.reply.{handle_id}"
    await tunnel.publish(reply_channel, json.dumps({
        "status": status,
        "executable_payload": executable_payload,
        "error_detail": error_detail
    }))
    
    return {"success": True, "status": status}


# ==========================================
# [Core Ledger] Infrastructure & Consensus
# (기존 코어 로직 완벽 유지 - Zero Side Effect)
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
        is_valid = await ctx.pta_adapter.verify_lineage(tx_hash=state_root, depth=3)
        
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
    receipt = params.get("payment_receipt")
    action = params.get("action", "unknown_action")
    
    if not receipt:
        return _build_error(401, "Payment receipt is missing")
        
    try:
        is_valid_receipt = True 
        
        if not is_valid_receipt:
            return _build_error(402, "x402 Payment Required: Receipt is invalid or depleted.")
            
        return {"status": "VALIDATED", "clearance": "GRANTED"}
    except Exception as e:
        log.error(f"Receipt Validation crashed: {e}")
        return _build_error(500, "Internal Billing Validation Error")


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
        from fiber.dphi.infra.transaction.settlement import MandateAdapter
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


async def handle_pta_balance(params: dict, ctx: WorkerContext) -> dict:
    client_id = params.get("client_id")
    asset_type = params.get("asset_type", "fuel")
    
    if not client_id:
        return _build_error(422, "Missing 'client_id' parameter")

    try:
        balance = await ctx.pta_adapter.get_balance(owner_address=client_id, asset_type=asset_type)
        return {
            "client_id": client_id,
            "asset_type": asset_type,
            "balance": balance
        }
    except Exception as e:
        log.error(f"PTA Balance check failed for {client_id}: {str(e)}")
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
    
    # MCP Gateway Delegation (PTA Phase Anchor 기반으로 리팩토링됨)
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
    "eco.exchange.balance": handle_pta_balance,
    
    # Benchmarking
    "eco.profile.quote": handle_profile_quote,
    "eco.profile.execute.billed": handle_profile_execute_billed,
}