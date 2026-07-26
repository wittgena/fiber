# ops.xelog.edge.otlp
import json
import hashlib
from typing import Annotated
from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException

from ops.xelog.depend import get_wasm_broker, get_pubsub
from ops.xelog.topos.tenant import TenantEco, get_tenant_eco
from ops.xelog.state.edge import EdgeState, EdgeHeader

from arch.topos.bound.interface.subs import DistributedPubSub
from arch.contract.audit.otlp import ExportLogsServiceRequest
from phase.wasm.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.otlp")

otlp_edge = APIRouter()

@otlp_edge.post("/v1/logs", status_code=status.HTTP_200_OK)
async def otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: WasmBroker = Depends(get_wasm_broker),
    tenant_eco: TenantEco = Depends(get_tenant_eco),
    auth_token: Annotated[str | None, Header(alias="Authorization")] = None, 
):
    try:
        ## [1. 원본 해싱 - Python Edge 단에서 수행하여 WASM 부하 최소화]
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = json.dumps(payload_dict, sort_keys=True).encode('utf-8')
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()

        ## [2. 경제적 계산 (Billing Intent 도출)]
        genai_metrics = payload.extract_genai_metrics()
        billing_result = await tenant_eco.calculate_tenant_billing(
            tenant_id=genai_metrics.get("tenant_id", "anonymous"),
            usage=genai_metrics.get("usage", {}),
            model_name=genai_metrics.get("model", "default-model")
        )

        if billing_result.get("status") == EdgeState.ERROR:
            log.warning(f"Billing calculation skipped or failed: {billing_result.get('message')}")

        ## [3. WASM Kernel: 경량화된 영수증 봉인 (Fat Payload 분리)]
        kernel_payload = {
            "action": "seal_otlp_transaction",
            "content_hash": content_hash,
            "billing_intent": billing_result.get("billing_intent", {}),
            "metrics_summary": genai_metrics
        }
        
        canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
        res = await broker.invoke("compute_root_fingerprint", canonical_payload)
        
        if not res.success:
            # 실패 시에도 표준 포맷의 헤더를 달아서 에러 발생
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail=f"Kernel Seal Rejected: {res.error.message}",
                headers={
                    EdgeHeader.STATE: EdgeState.ERROR,
                    EdgeHeader.ERROR_DETAIL: "Kernel Seal Rejected"
                }
            )
            
        fingerprint = json.loads(res.output).get("fingerprint")
        
        with flow_scope(phase="OTLP_INGRESS", bound="edge"):
            log.info(f"[OTLP Anchor] Secured batch. ContentHash: {content_hash[:8]}, Fingerprint: {fingerprint[:16]}")

        ## [4. 브로드캐스트: 원본 데이터 방출]
        topic_name = "otlp_global_stream" 
        bg_tasks.add_task(pubsub.publish_batch, topic=topic_name, events=[payload_dict])
        
        # 🔥 Enum을 사용한 명시적 헤더 구성
        response_headers = {
            EdgeHeader.STATE: EdgeState.SUCCESS,
            EdgeHeader.CONTENT_HASH: content_hash,
            EdgeHeader.FINGERPRINT: fingerprint
        }
        
        # OTLP 표준(빈 JSON 바디)을 만족하면서 커스텀 헤더를 통해 증명 반환
        return Response(
            status_code=status.HTTP_200_OK, 
            headers=response_headers,
            content=b"{}" 
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[OTLP Anchor] Processing failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal stream processing error",
            headers={EdgeHeader.STATE: EdgeState.ERROR}
        )