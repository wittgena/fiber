# ops.xelog.edge.d3fi
import json
import time
from fastapi import APIRouter, Depends, HTTPException, status
from ops.xelog.depend import get_wasm_broker, get_exchange_adapter
from ops.xelog.ingress.policy import IngressPolicyEngine, get_ingress_policy
from ops.xelog.state.edge import (
    EdgeState,
    TradeIngressRequest,
    TradeIngressResponse,
    EpochInitPayload,
    ClearingReceiptRequest,
    ClearingReceiptResponse
)

from phase.wasm.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.adapter.exchange import D3fiExchangeAdapter

d3fi_edge = APIRouter(prefix="/d3fi/v1", tags=["D3Fi Exchange & Clearing"])

@d3fi_edge.post(
    "/order/ingress", 
    summary="거래 인텐트 인입 및 Session 발급",
    response_model=TradeIngressResponse
)
async def submit_trade_intent(
    req: TradeIngressRequest,
    broker: WasmBroker = Depends(get_wasm_broker),
    policy_engine: IngressPolicyEngine = Depends(get_ingress_policy)
):
    # 1. 통합 정책 엔진을 통한 동적 컨텍스트 도출 (Hash/Random 기반 Mock 연산)
    context = await policy_engine.resolve_context(agent_id=req.agent_id, action=req.action)

    # 2. Rupture 감지 시 즉시 차단 (HealthMonitor 결과)
    if context.is_ruptured:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Topology Ruptured: {context.reason}"
        )

    # 3. 명시적 Pydantic 모델을 통한 Payload 구성
    payload_obj = EpochInitPayload(
        ts=int(time.time() * 1000),
        topo=context.topo_id,
        press=context.press_limit,
        rupture=context.is_ruptured,
        injected_intent=req
    )
    
    # 4. Pydantic 덤프 후 Canonical JSON으로 변환
    canonical_payload = StateAdapter.to_canonical_bytes(payload_obj.model_dump()).decode('utf-8')
    res = await broker.invoke("init_epoch", canonical_payload)
    
    if not res.success:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=res.error.message)
        
    return TradeIngressResponse(
        status=EdgeState.INTENT_ACCEPTED,
        session=json.loads(res.output)
    )


@d3fi_edge.post(
    "/clearing/receipt/generate", 
    summary="외부 네트워크용 정산 영수증 발급",
    response_model=ClearingReceiptResponse
)
async def generate_external_receipt(
    req: ClearingReceiptRequest,
    exchange: D3fiExchangeAdapter = Depends(get_exchange_adapter)
):
    """ExchangeAdapter를 사용하여 매칭된 상태를 EVM/Rollup 전송용 페이로드로 변환"""
    receipt = exchange.finalize_settlement(
        entangled_state=req.entangled_state,
        signatures=req.signatures,
        cost_metrics=req.cost_metrics,
        tier="SYSTEM"
    )
    
    external_payload = exchange.generate_settlement_payload(receipt)
    return ClearingReceiptResponse(
        status=EdgeState.RECEIPT_GENERATED,
        rollup_payload=external_payload
    )