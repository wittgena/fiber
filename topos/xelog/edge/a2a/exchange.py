# topos.xelog.edge.a2a.exchange
import json
import time
from fastapi import APIRouter, Depends, HTTPException, status

from topos.xelog.depend import get_wasm_broker, get_exchange_adapter
from topos.xelog.topos.ingress.policy import IngressPolicyEngine, get_ingress_policy
from topos.xelog.topos.state.edge import (
    EdgeState,
    TradeIngressRequest, TradeIngressResponse,
    EpochInitPayload,
    ClearingReceiptRequest, ClearingReceiptResponse
)
from watcher.dphi.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.adapter.exchange import ExchangeAdapter
from watcher.dphi.cgroup import Tier

# 중앙 설정 객체 임포트 (초기 한도 설정을 위해)
from arch.topos.bound.exchange.config import tier_config

exchange_edge = APIRouter()

@exchange_edge.post("/order/ingress", summary="거래 인텐트 인입 및 Session 발급", response_model=TradeIngressResponse)
async def submit_trade_intent(
    req: TradeIngressRequest,
    broker: WasmBroker = Depends(get_wasm_broker),
    policy_engine: IngressPolicyEngine = Depends(get_ingress_policy)
):
    context = await policy_engine.resolve_context(agent_id=req.agent_id, action=req.action)

    if context.is_ruptured:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Topology Ruptured: {context.reason}")

    # context.press_limit가 없거나 유효하지 않은 경우, config의 Fallback Fuel로 대체하여 일관성 유지
    press_limit = context.press_limit if hasattr(context, 'press_limit') and context.press_limit > 0 else tier_config.fallback_fuel

    payload_obj = EpochInitPayload(
        ts=int(time.time() * 1000), topo=context.topo_id, press=press_limit,
        rupture=context.is_ruptured, injected_intent=req
    )
    
    canonical_payload = StateAdapter.to_canonical_bytes(payload_obj.model_dump()).decode('utf-8')
    res = await broker.invoke("init_epoch", canonical_payload)
    
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    return TradeIngressResponse(status=EdgeState.INTENT_ACCEPTED, session=json.loads(res.output))

@exchange_edge.post("/clearing/receipt/generate", summary="외부 네트워크용 정산 영수증 발급", response_model=ClearingReceiptResponse)
async def generate_external_receipt(
    req: ClearingReceiptRequest,
    exchange: ExchangeAdapter = Depends(get_exchange_adapter)
):
    receipt = exchange.finalize_settlement(
        entangled_state=req.entangled_state, 
        signatures=req.signatures,
        cost_metrics=req.cost_metrics, 
        tier=Tier.SYSTEM  
    )
    
    external_payload = exchange.generate_settlement_payload(receipt)
    return ClearingReceiptResponse(status=EdgeState.RECEIPT_GENERATED, rollup_payload=external_payload)