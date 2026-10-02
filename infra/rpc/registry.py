# fiber.infra.rpc.registry
from enum import Enum
from typing import Dict, Callable, Optional

from fiber.infra.rpc.method import RpcMethod
from fiber.infra.rpc.handler import (
    handle_phase_store_stream_append,
    handle_phase_store_anchor_seal,
    handle_phase_store_receipt_verify,
    handle_mcp_state_query,
    handle_mcp_state_pending_seal,
    handle_mcp_state_resolve,
    handle_compute_intent_validate
)

from fiber.infra.rpc.exchange import (
    handle_clearing_settlement,
    handle_invoice_issue,
    handle_get_balance,
    handle_intent_estimate,
    handle_profile_execute,
)

from fiber.infra.rpc.validator import handle_compute_margin_calculate, ValidatorService, handle_fuel_receipt_validate
from fiber.infra.rpc.fuel import handle_fuel_deduction

def build_internal_rpc_registry(validator_service: Optional[ValidatorService] = None) -> Dict[str, Callable]:
    registry = {
        RpcMethod.PHASE_STORE_STREAM_APPEND: handle_phase_store_stream_append,
        RpcMethod.PHASE_STORE_RECEIPT_VERIFY: handle_phase_store_receipt_verify,
        RpcMethod.PHASE_STORE_ANCHOR_SEAL: handle_phase_store_anchor_seal,
        
        RpcMethod.MCP_STATE_QUERY: handle_mcp_state_query,
        RpcMethod.MCP_STATE_PENDING_SEAL: handle_mcp_state_pending_seal,
        RpcMethod.MCP_BRIDGE_RESOLVE_STATE: handle_mcp_state_resolve,
        
        RpcMethod.EXCHANGE_CLEARING_SETTLEMENT: handle_clearing_settlement,
        RpcMethod.EXCHANGE_INVOICE_ISSUE: handle_invoice_issue,
        RpcMethod.EXCHANGE_GET_BALANCE: handle_get_balance,
        RpcMethod.EXCHANGE_FUEL_DEDUCT: handle_fuel_deduction,

        RpcMethod.ECO_INTENT_ESTIMATE: handle_intent_estimate,
        RpcMethod.ECO_PROFILE_EXECUTE: handle_profile_execute,

        RpcMethod.VALIDATE_FUEL_RECEIPT: handle_fuel_receipt_validate,
        RpcMethod.VALIDATE_COMPUTE_INTENT: handle_compute_intent_validate,
        RpcMethod.VALIDATE_MARGIN_CALCULATE: handle_compute_margin_calculate,
    }

    if validator_service:
        registry[RpcMethod.VALIDATE_ATTEST] = validator_service.handle_attestation

    return registry