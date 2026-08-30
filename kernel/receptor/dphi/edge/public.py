# fiber.kernel.receptor.dphi.edge.public
import os
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import orjson

from fastapi import Body, Header, Response, status, Depends, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel

from fiber.dphi.eco.builder import NotarySwarm
from fiber.kernel.receptor.dphi.depend import (
    get_wasm_broker, 
    get_pubsub, 
    get_otlp_engine, 
    get_secret_auditor, 
    get_rpc_client
)
from fiber.dphi.rpc.client import InternalRpcClient

from xphi.arch.contract.interface import ContractRouter
from xphi.arch.contract.model.receptor import EdgeState, EdgeHeader, IntentValidationRequest
from xphi.xor.parser.ruleset.otlp import StrictOtlpExtractionEngine

from xphi.kernel.space.topos.tunnel.subs import DistributedPubSub
from xphi.kernel.dphi.broker import DphiBroker, DphiMethod
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.arch.eco.edge.receipt import (
    CodebotIntent, 
    AuditReceipt,
    ExportLogsServiceRequest, 
    AuditLogRequest, 
    AuditLogResponse, 
    AuditResult, 
    AuditEnvelope,
    BilledExecutionRequest,
    KernelExecutionRecord,
    KernelOtlpRecord
)
from fiber.kernel.receptor.audit.secret import SecretAuditor
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.public")

public_edge = ContractRouter(
    namespace="public", 
    prefix="/v1/public", 
    tags=["Public Gateway"],
    description="Deterministic Zero-Trust Gateway for Agentic Workloads"
)

"""DATA TRANSFER OBJECTS (DTO)"""
class InvoiceIssueRequest(BaseModel):
    payee_address: str
    amount_usdc: str
    resource_id: str

class AgentHandshakeResponse(BaseModel):
    status: str
    estimated_fuel: int
    estimated_cost_usd: float
    invoice: Dict[str, Any]
    macaroon: Optional[str] = None
    next_action: str = "POST /v1/public/agent/execute with X-X402-Receipt header"


"""BASE INFRASTRUCTURE (TRUST ANCHOR)"""
@public_edge.get(
    "/keys", 
    summary="Get Trusted Signer Keys (Strictly Pre-Signed)"
)
async def get_public_keys():
    active_signers_env = os.getenv("DPHI_ACTIVE_SIGNERS")
    root_signature = os.getenv("DPHI_PRE_SIGNED_ROOT_SIG")

    if not active_signers_env or not root_signature:
        log.critical("[Security] DPHI_ACTIVE_SIGNERS or DPHI_PRE_SIGNED_ROOT_SIG not configured.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Security misconfiguration: Trusted registry is offline."
        )

    payload_dict = {"active_signers": [key.strip() for key in active_signers_env.split(",")]}
    
    return Response(
        content=orjson.dumps(payload_dict),
        media_type="application/json",
        headers={"X-Dphi-Root-Signature": root_signature}
    )


"""COMPUTE SYMMETRY (QUOTE ↔ EXECUTE)"""
@public_edge.post(
    "/agent/quote", 
    summary="Get Pre-flight Execution Quotation (Dry-run)"
)
async def public_agent_quote(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    if x_x402_receipt:
        val_req = IntentValidationRequest(
            requester_id=intent.agent_id,
            responder_id=intent.responder_id or "edge-gateway-01",
            action=intent.action,
            max_fuel_budget=intent.max_fuel,
            signature=intent.signature,
            payment_receipt=x_x402_receipt
        )
        # [IMPROVED] RPC 에러(401)를 잡아서 외부 API 스펙에 맞게 422로 변환
        try:
            await rpc.call("eco.compute.intent.validate", val_req.model_dump(exclude_none=True))
        except HTTPException as e:
            raise HTTPException(status_code=422, detail=f"Intent Validation Failed: {{\"detail\":\"{e.detail}\"}}")

    exec_req = {
        "agent_schema": {
            "runtime": "python3.11-wasm",
            "files": {"main.py": intent.source_code}, 
            "limits": {"max_fuel": intent.max_fuel}
        },
        "target_entry": "main.py",
        "context_depth": 2
    }
    
    return await rpc.call("eco.profile.quote", exec_req)


@public_edge.post(
    "/agent/execute", 
    summary="Execute Billed AI Agent Intent & Issue Cryptographic Proof-of-Action",
    response_model=AuditReceipt
)
async def public_agent_execute(
    intent: CodebotIntent,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    rpc: InternalRpcClient = Depends(get_rpc_client),
    broker: DphiBroker = Depends(get_wasm_broker)
):
    request_id = f"cbot_{uuid.uuid4().hex[:8]}"
    with flow_scope(phase="GATEWAY_ORCHESTRATION", bound="edge.public", req_id=request_id):
        val_req = IntentValidationRequest(
            requester_id=intent.agent_id,
            responder_id=intent.responder_id or "edge-gateway-01",
            action=intent.action,
            max_fuel_budget=intent.max_fuel,
            signature=intent.signature,
            payment_receipt=x_x402_receipt
        )

        try:
            await rpc.call("eco.compute.intent.validate", val_req.model_dump(exclude_none=True))
        except HTTPException as e:
            raise HTTPException(status_code=401, detail=f"Intent Rejected: {{\"detail\":\"{e.detail}\"}}")

        # 2. 과금 연산 실행 (RPC)
        exec_req = BilledExecutionRequest(
            agent_schema={
                "runtime": "python3.11-wasm",
                "files": {"main.py": intent.source_code}, 
                "limits": {"max_fuel": intent.max_fuel}
            },
            target_entry="main.py",
            context_depth=2
        )
        # [IMPROVED] Compute Failed 에러는 422 래핑 유지
        try:
            exec_data = await rpc.call("eco.profile.execute.billed", exec_req.model_dump())
        except HTTPException as e:
            raise HTTPException(status_code=422, detail=f"Compute Failed: {{\"detail\":\"{e.detail}\"}}")
        
        # 3. 암호학적 영수증(AuditReceipt) 발행 
        fuel_metered = exec_data.get("fuel_billed", 0)
        cost_usd = exec_data.get("billed_cost_usd", 0.0)
        exec_hash = hashlib.sha256(json.dumps(exec_data).encode()).hexdigest()
        repos = {"vm_trace_hash": exec_hash, "metered_fuel": str(fuel_metered)}

        swarm = NotarySwarm(size=3)
        canonical_hash = StateAdapter.to_canonical_bytes(repos)
        commit_hash_bytes = hashlib.sha256(canonical_hash).digest()
        attested_signatures = swarm.attest_payload(commit_hash_bytes)

        kernel_req_dict = KernelExecutionRecord(
            receipt_id=request_id,
            repos=repos,
            signatures=attested_signatures,
            timestamp=float(time.time() * 1000)
        ).model_dump(exclude_none=True)
        
        evo_ctx = StateAdapter.build_evolution_context(phase_root={})
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="record_agent_execution",
            intent_payload=kernel_req_dict,
            evolution_ctx=evo_ctx
        )

        canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
        
        if not fp_res.success:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Kernel Record Failed: {fp_res.error}")
            
        commit_hash = json.loads(fp_res.output).get("fingerprint", "0x_sealed_root")
        
        return AuditReceipt(
            receipt_id=request_id,
            receipt_type="Proof-of-Agent-Action",
            status="SUCCESS",
            fuel_consumed=fuel_metered,
            metered_cost_usd=cost_usd,
            state_root=commit_hash,
            audit_trail=["Gateway", "PolicyEngine", "WasmSandbox", "CoreLedger"]
        )


"""ECONOMY SYMMETRY (INVOICE ↔ BALANCE & HANDSHAKE)"""
@public_edge.post(
    "/agent/handshake", 
    summary="Agent Pre-flight Handshake (Quote & Invoice)",
    response_model=AgentHandshakeResponse
)
async def public_agent_handshake(
    intent: CodebotIntent,
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    # 1. 견적 조회 (Quote)
    quote_req = {
        "agent_schema": {
            "runtime": "python3.11-wasm",
            "files": {"main.py": intent.source_code}, 
            "limits": {"max_fuel": intent.max_fuel}
        },
        "target_entry": "main.py",
        "context_depth": 2
    }
    
    try:
        quote_data = await rpc.call("eco.profile.quote", quote_req)
    except HTTPException as e:
        raise HTTPException(status_code=422, detail=f"Quotation Failed: {e.detail}")
    
    cost_usd = quote_data.get("estimated_cost_usd", 0.0)
    fuel = quote_data.get("fuel_estimated", 0)

    # 2. 인보이스 발급 (Invoice)
    invoice_req = {
        "payee_address": "0x000000000000000000000000000000000000dEaD",
        "amount_usdc": str(cost_usd),
        "resource_id": f"res_intent_{uuid.uuid4().hex[:8]}"
    }
    
    try:
        invoice_data = await rpc.call("eco.exchange.invoice.issue", invoice_req)
    except HTTPException as e:
        raise HTTPException(status_code=500, detail=f"Invoice Issue Failed: {e.detail}")

    return AgentHandshakeResponse(
        status="HANDSHAKE_READY",
        estimated_fuel=fuel,
        estimated_cost_usd=cost_usd,
        invoice=invoice_data.get("invoice", {}),
        macaroon=invoice_data.get("macaroon")
    )


@public_edge.post(
    "/billing/invoice", 
    summary="Issue L402 Invoice for Resource Access"
)
async def public_issue_invoice(
    req: InvoiceIssueRequest,
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        return await rpc.call("eco.exchange.invoice.issue", req.model_dump())
    except HTTPException as e:
        # 워커에서 올라온 에러를 그대로 패스스루
        raise


@public_edge.get(
    "/billing/balance", 
    summary="Check UTXO Fuel Balance"
)
async def public_get_balance(
    agent_id: str = Query(..., description="조회할 에이전트 주소"),
    asset_type: str = Query("fuel", description="조회할 자산 타입"),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        return await rpc.call("eco.exchange.balance", {"agent_id": agent_id, "asset_type": asset_type})
    except HTTPException as e:
        raise


"""COMPLIANCE SYMMETRY (RECORD ↔ VERIFY)"""
@public_edge.post(
    "/telemetry/logs", 
    tags=["Log Ingress"], 
    summary="Ingest OTLP Telemetry, Verify Integrity & Seal Global Stream",
    status_code=status.HTTP_200_OK
)
async def public_otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: DphiBroker = Depends(get_wasm_broker),
    otlp_engine: StrictOtlpExtractionEngine = Depends(get_otlp_engine)
):
    try:
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = orjson.dumps(payload_dict)
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()
        
        try:
            extracted_metrics = otlp_engine.execute(raw_json_bytes)
        except ValueError as e:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

        kernel_req_dict = KernelOtlpRecord(
            content_hash=content_hash,
            metrics_summary=extracted_metrics,
            receipt_ref=x_x402_receipt
        ).model_dump(exclude_none=True)
        
        evo_ctx = StateAdapter.build_evolution_context(phase_root={})
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="record_otlp_telemetry",
            intent_payload=kernel_req_dict,
            evolution_ctx=evo_ctx
        )

        canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
        
        if not res.success:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Kernel Seal Rejected")
            
        fingerprint = orjson.loads(res.output).get("fingerprint")
        bg_tasks.add_task(pubsub.publish_batch, topic="otlp_global_stream", events=[payload_dict])
        
        return Response(
            status_code=status.HTTP_200_OK, 
            headers={
                EdgeHeader.STATE: EdgeState.SUCCESS,
                EdgeHeader.CONTENT_HASH: content_hash,
                EdgeHeader.FINGERPRINT: fingerprint
            }, 
            content=b"{}"
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[Public OTLP] Processing failed: {str(e)}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Stream processing error")


@public_edge.post(
    "/audit/event", 
    tags=["Log Ingress"], 
    summary="Secure Audit Event Recording & Conditional Cryptographic Proof Issuance"
)
async def public_audit_log(
    payload: AuditLogRequest,
    x_x402_receipt: Optional[str] = Header(None, alias="X-X402-Receipt"),
    secret_auditor: SecretAuditor = Depends(get_secret_auditor),
    broker: DphiBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    sanitized_event = secret_auditor._encrypt_sensitive_data(event_dict)
    sanitized_event["_billing_ref"] = x_x402_receipt
    
    evo_ctx = StateAdapter.build_evolution_context(phase_root={})
    transition_payload = StateAdapter.build_transition_payload(
        intent_action="record_audit_event",
        intent_payload=sanitized_event,
        evolution_ctx=evo_ctx
    )

    canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
    fp_res = await broker.invoke(DphiMethod.COMPUTE_ROOT_FINGERPRINT, canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    if payload.verbose:
        proof_res = await broker.invoke(DphiMethod.GENERATE_PROOF, canonical_payload)
        if proof_res.success:
            merkle_proof = json.loads(proof_res.output).get("current_hash")

    envelope = AuditEnvelope(event=payload.event, received_at=datetime.now(timezone.utc).isoformat())
    audit_result = AuditResult(
        envelope=envelope, hash=event_hash, membership_proof=merkle_proof, consistency_proof=[]
    )
    
    return AuditLogResponse(
        request_id=f"req_{uuid.uuid4().hex[:8]}",
        request_time=request_time,
        response_time=str(time.time()),
        status="success",
        result=audit_result
    )

@public_edge.post(
    "/audit/verify", 
    summary="Verify AuditReceipt Authenticity"
)
async def public_audit_verify(
    receipt: Dict[str, Any] = Body(...),
    rpc: InternalRpcClient = Depends(get_rpc_client)
):
    try:
        return await rpc.call("core.ledger.verify", receipt)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"Verification Failed: {{\"detail\":\"{e.detail}\"}}")