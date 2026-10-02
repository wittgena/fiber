# fiber.infra.rpc.method
from enum import Enum
from typing import Dict, Callable, Optional

class RpcMethod(str, Enum):
    # phase store & State
    PHASE_STORE_STREAM_APPEND = "phase.store.stream.append"
    PHASE_STORE_RECEIPT_VERIFY = "phase.store.receipt.verify"
    PHASE_STORE_ANCHOR_SEAL = "phase.store.anchor.seal"
    
    # MCP Gateway Delegation
    MCP_STATE_QUERY = "mcp.state.query"
    MCP_STATE_PENDING_SEAL = "mcp.state.pending.seal"
    MCP_BRIDGE_RESOLVE_STATE = "mcp.bridge.resolve_state"
    
    # Exchange & Billing
    EXCHANGE_CLEARING_SETTLEMENT = "exchange.clearing.settlement"
    EXCHANGE_INVOICE_ISSUE = "exchange.invoice.issue"
    EXCHANGE_GET_BALANCE = "exchange.get.balance"
    EXCHANGE_FUEL_DEDUCT = "exchange.fuel.deduct"

    # Compute & Execution
    ECO_INTENT_ESTIMATE = "eco.intent.estimate"
    ECO_PROFILE_EXECUTE = "eco.profile.execute"

    # Validation & Execution
    VALIDATE_FUEL_RECEIPT = "validate.fuel.receipt"
    VALIDATE_COMPUTE_INTENT = "validate.compute.intent"
    VALIDATE_MARGIN_CALCULATE = "validate.margin.calculate"
    VALIDATE_ATTEST = "validate.attest"