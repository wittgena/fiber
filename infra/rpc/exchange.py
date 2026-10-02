# fiber.infra.rpc.exchange
import json
import time
from typing import Dict, Any

from pydantic import ValidationError

from fiber.infra.rpc.handler import WorkerContext, _build_error

from xphi.kernel.space.sandbox.config import tier_config, fuel_config
from xphi.arch.model.edge.receptor import EdgeState, TradeIngressRequest, EpochInitPayload, ClearingReceiptRequest
from xphi.arch.model.edge.receipt import BilledExecutionRequest

from xphi.kernel.wasm.broker import DphiMethod
from xphi.kernel.wasm.quota import Tier
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.exchange")

async def handle_clearing_settlement(params: dict, ctx: WorkerContext) -> dict:
    try: req = ClearingReceiptRequest(**params)
    except ValidationError as e: return _build_error(422, f"Payload Error: {e.errors()}")

    receipt = ctx.exchange_adapter.finalize_settlement(
        entangled_state=req.entangled_state, 
        signatures=req.signatures, 
        cost_metrics=req.cost_metrics, 
        tier=Tier.SYSTEM
    )
    return {"status": EdgeState.RECEIPT_GENERATED, "rollup_payload": ctx.exchange_adapter.generate_settlement_payload(receipt)}


async def handle_invoice_issue(params: dict, ctx: WorkerContext) -> dict:
    payee_address, amount_usdc, resource_id = params.get("payee_address"), params.get("amount_usdc"), params.get("resource_id")
    if not all([payee_address, amount_usdc, resource_id]): return _build_error(422, "Missing required invoice parameters")

    try:
        from xphi.arch.bound.adapter.settlement import MandateAdapter
        invoice = MandateAdapter.build_x402_invoice(payee_address=payee_address, amount_usdc=amount_usdc, resource_id=resource_id)
        return {"status": "INVOICE_ISSUED", "invoice": invoice.model_dump() if hasattr(invoice, "model_dump") else invoice.__dict__}
    except Exception as e:
        return _build_error(500, f"Invoice Issue Failed: {str(e)}")


async def handle_get_balance(params: dict, ctx: WorkerContext) -> dict:
    client_id = params.get("client_id")
    asset_type = params.get("asset_type", "fuel")
    if not client_id: return _build_error(422, "Missing 'client_id' parameter")

    try:
        balance = await ctx.pta_adapter.get_balance(owner_address=client_id, asset_type=asset_type)
        return {"client_id": client_id, "asset_type": asset_type, "balance": balance}
    except Exception as e:
        log.error(f"PTA Balance check failed for {client_id}: {str(e)}")
        return _build_error(500, "Failed to read hot state balance.")


async def handle_intent_estimate(params: dict, ctx: WorkerContext) -> dict:
    try: req = BilledExecutionRequest(**params)
    except ValidationError as e: return _build_error(422, f"Payload Error: {e.errors()}")
    client_id = params.get("client_id", getattr(req, "client_id", "anonymous_agent"))

    try:
        result = await ctx.profile_service.execute(
            client_id=client_id, 
            schema=req.sandbox_schema, 
            entry=req.target_entry,
            depth=req.context_depth,
            tier=Tier.STANDARD,
            dry_run=True
        )
        if result.status != "COHERENCE":
            log.warning(f"[Quote] Execution Divergence: {result.reason}")
            return _build_error(422, f"Quotation Rejected: {result.reason}")
    except Exception as e:
        log.error(f"[Quote] Unhandled Error: {e}")
        return _build_error(500, "Internal sandbox error")
        
    estimated_cost = (result.fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
    return {"status": "QUOTE_READY", "tier_applied": result.tier_applied, "fuel_estimated": result.fuel_consumed, "estimated_cost_usd": estimated_cost, "reason": result.reason}


async def handle_profile_execute(params: dict, ctx: WorkerContext) -> dict:
    try: req = BilledExecutionRequest(**params)
    except ValidationError as e: return _build_error(422, f"Payload Error: {e.errors()}")

    client_id = params.get("client_id", getattr(req, "client_id", "anonymous_agent"))
    try:
        result = await ctx.profile_service.execute(
            client_id=client_id, schema=req.sandbox_schema, entry=req.target_entry, 
            depth=req.context_depth, tier=Tier.SYSTEM, dry_run=False
        )
        if result.status != "COHERENCE":
            log.error(f"[Execute] Execution Failed/Diverged: {result.reason}")
            return _build_error(422, f"Billed Execution Failed: {result.reason}")
    except Exception as e:
        log.error(f"[Execute] Unhandled Sandbox Error: {e}")
        return _build_error(500, "Sandbox execution crashed unexpectedly")
        
    billed_cost = (result.fuel_consumed / fuel_config.fuel_unit) * fuel_config.usd_per_fuel_unit
    return {"status": "BILLED_EXECUTION_SUCCESS", "tier_applied": result.tier_applied, "fuel_billed": result.fuel_consumed, "billed_cost_usd": billed_cost, "reason": result.reason}