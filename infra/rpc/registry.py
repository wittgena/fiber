# fiber.infra.rpc.registry
from typing import Dict, Callable, Any, Optional
from fiber.infra.rpc.handler import (
    handle_ledger_stream_append,
    handle_anchor_seal,
    handle_receipt_verify,
    handle_mcp_state_query,
    handle_mcp_state_pending_seal,
    handle_mcp_state_resolve,
    handle_compute_intent_validate,
    handle_execute_compute,
    handle_trade_ingress,
    handle_clearing_receipt_generate,
    handle_invoice_issue,
    handle_pta_balance,
    handle_intent_estimate,
    handle_profile_execute_billed,
)
from fiber.infra.rpc.validator import handle_fuel_receipt_validate, handle_compute_margin_calculate, ValidatorService
from fiber.infra.rpc.fuel import handle_fuel_deduction

def build_internal_rpc_registry(validator_service: Optional[ValidatorService] = None) -> Dict[str, Callable]:
    registry = {
        # Ledger & State
        "core.ledger.append": handle_ledger_stream_append,
        "core.receipt.verify": handle_receipt_verify,
        "core.anchor.seal": handle_anchor_seal,
        
        # MCP Gateway Delegation
        "mcp.state.query": handle_mcp_state_query,
        "mcp.state.pending.seal": handle_mcp_state_pending_seal,
        "mcp.bridge.resolve_state": handle_mcp_state_resolve,
        
        # Exchange & Billing
        "eco.exchange.order.ingress": handle_trade_ingress,
        "eco.exchange.clearing.receipt.generate": handle_clearing_receipt_generate,
        "eco.exchange.invoice.issue": handle_invoice_issue,
        "eco.exchange.balance": handle_pta_balance,
        "eco.exchange.fuel.deduct": handle_fuel_deduction,

        "eco.compute.execute": handle_execute_compute,
        "eco.margin.calculate": handle_compute_margin_calculate,
        
        # Benchmarking
        "eco.intent.estimate": handle_intent_estimate,
        "eco.profile.execute.billed": handle_profile_execute_billed,

        # Validation & Execution
        "validate.fuel.receipt": handle_fuel_receipt_validate,
        "validate.compute.intent": handle_compute_intent_validate,
    }

    if validator_service:
        registry["validate.attest"] = validator_service.handle_attestation

    return registry