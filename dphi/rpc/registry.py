# fiber.dphi.rpc.registry
from typing import Dict, Callable, Any, Optional
from fiber.dphi.rpc.handler import (
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
from fiber.dphi.rpc.receipt import handle_billing_receipt_validate
from fiber.dphi.rpc.margin import handle_compute_margin_calculate
from fiber.dphi.rpc.legacy.validator import AuthValidatorService

def build_internal_rpc_registry(
    validator_service: Optional[AuthValidatorService] = None
) -> Dict[str, Callable]:
    """
    내부 RPC 라우팅 테이블을 동적으로 생성하여 반환합니다.
    Args:
        validator_service: 데몬에서 인스턴스화된 AuthValidatorService (상태를 가지는 서비스)
    """
    # 1. 상태가 없는 순수 함수형(Core) 핸들러 매핑
    registry = {
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

        "eco.margin.calculate": handle_compute_margin_calculate,
        
        # Economy & Billing
        "eco.exchange.order.ingress": handle_trade_ingress,
        "eco.exchange.clearing.receipt.generate": handle_clearing_receipt_generate,
        "eco.exchange.invoice.issue": handle_invoice_issue,
        "eco.exchange.balance": handle_pta_balance,
        
        # Benchmarking
        "eco.profile.quote": handle_profile_quote,
        "eco.profile.execute.billed": handle_profile_execute_billed,
    }

    ## 2. 상태를 유지하는 외부 서비스(External Service) 동적 바인딩
    if validator_service:
        registry["validator.attest"] = validator_service.handle_attestation

    return registry