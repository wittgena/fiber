# fiber.infra.rpc.registry
## @lineage: fiber.dev.infra.rpc.registry
## @lineage: fiber.gateway.edge.rpc.registry
from typing import Dict, Callable, Any, Optional
from fiber.infra.rpc.handler import (
    handle_ledger_stream_append,
    handle_anchor_seal,
    handle_ledger_verify,
    handle_mcp_state_query,
    handle_mcp_state_pending_seal,
    handle_mcp_state_resolve,
    handle_intent_validate,
    handle_execute_compute,
    handle_trade_ingress,
    handle_clearing_receipt_generate,
    handle_invoice_issue,
    handle_pta_balance,
    handle_profile_quote,
    handle_profile_execute_billed,
    WorkerContext
)
from fiber.infra.rpc.validator import handle_billing_receipt_validate, handle_compute_margin_calculate, ValidatorService

def build_internal_rpc_registry(
    validator_service: Optional[ValidatorService] = None
) -> Dict[str, Callable]:
    """내부 RPC 라우팅 테이블을 동적으로 생성하여 반환"""
    registry = {
        # Ledger & State
        "core.ledger.append": handle_ledger_stream_append,
        "core.anchor.seal": handle_anchor_seal,
        "core.ledger.verify": handle_ledger_verify,
        
        # MCP Gateway Delegation
        "mcp.state.query": handle_mcp_state_query,
        "mcp.state.pending.seal": handle_mcp_state_pending_seal,
        "mcp.bridge.resolve_state": handle_mcp_state_resolve,
        
        # Economy & Billing
        "eco.exchange.order.ingress": handle_trade_ingress,
        "eco.exchange.clearing.receipt.generate": handle_clearing_receipt_generate,
        "eco.exchange.invoice.issue": handle_invoice_issue,
        "eco.exchange.balance": handle_pta_balance,

        "eco.compute.execute": handle_execute_compute,
        "eco.margin.calculate": handle_compute_margin_calculate,
        
        # Benchmarking
        "eco.profile.quote": handle_profile_quote,
        "eco.profile.execute.billed": handle_profile_execute_billed,

        # Validation & Execution
        "validate.billing.receipt": handle_billing_receipt_validate,
        "validate.compute.intent": handle_intent_validate,
    }

    ## 2. 상태를 유지하는 외부 서비스(External Service) 동적 바인딩
    if validator_service:
        registry["validate.attest"] = validator_service.handle_attestation

    return registry