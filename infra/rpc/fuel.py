# fiber.infra.rpc.fuel
import asyncio
import logging
from typing import Dict, Any, List

from fiber.infra.rpc.handler import WorkerContext
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import (
    PtaAdapter, 
    PtaTransaction, 
    PtaInput, 
    PtaPointer, 
    PtaOutput
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.fuel")

# Stateless Direct Execution (Daemon-less / Import Mode)
async def execute_direct_fuel_deduction(tenant_id: str, consumed_fuel: int, trace_id: str) -> None:
    if consumed_fuel <= 0 or tenant_id in ("anonymous", "internal_system", "unknown"):
        return

    # 외부 WorkerContext에 의존하지 않고 자체적으로 최소 인프라(Broker, Adapter)를 프로비저닝
    broker = DphiBroker(timeout=5.0)
    pta_adapter = PtaAdapter(broker=broker)
    
    try:
        # 1. 대상 테넌트의 사용 가능한 잔고(PTA Output) 추출
        available_outputs = []
        for pointer_key, output in pta_adapter._unfold_pool.items():
            if output.owner == tenant_id and output.asset_type == "fuel":
                tx_hash, idx = pointer_key.split(":")
                available_outputs.append({
                    "pointer": PtaPointer(tx_hash, int(idx)), 
                    "amount": output.amount
                })
        
        total_available = sum(item["amount"] for item in available_outputs)
        
        # 잔고가 마이너스 통장이 되는 것을 방지하기 위해 실제 차감 가능액 계산
        actual_consume = min(consumed_fuel, total_available) if total_available > 0 else consumed_fuel
        
        inputs: List[PtaInput] = []
        outputs: List[PtaOutput] = []
        accumulated = 0
        
        # 기존 상태 소모 (Burn)
        for item in available_outputs:
            inputs.append(
                PtaInput(
                    pointer=item["pointer"], 
                    signature="system_bypass_sig", # 시스템 권한에 의한 강제 차감 대리 서명
                    owner_address=tenant_id
                )
            )
            accumulated += item["amount"]
            if accumulated >= actual_consume:
                break
                
        # 새로운 상태 발행 (consume: 거스름돈 및 트레저리 귀속)
        change = accumulated - actual_consume
        if change > 0:
            outputs.append(PtaOutput(amount=change, owner=tenant_id, asset_type="fuel"))
            
        outputs.append(PtaOutput(amount=actual_consume, owner="system_treasury", asset_type="fuel"))
        
        # 차감 트랜잭션
        tx = PtaTransaction(
            inputs=inputs, 
            outputs=outputs, 
            metadata={"action": "direct_deduction", "trace_id": trace_id}
        )
        await pta_adapter.execute_transaction(tx)
        log.info(f"[Billing:Direct] Auto-deducted {actual_consume} fuel from '{tenant_id}'. Trace: {trace_id}")
    except Exception as e:
        log.error(f"[Billing:Direct] Background deduction failed for '{tenant_id}': {e}", exc_info=True)
    finally:
        await broker.close()


# RPC Queue Handler (Daemon Mode)
def _build_error(code: int, message: str) -> dict:
    """RPC 에러 포맷 규격화"""
    return {"error": True, "code": code, "message": message}

async def handle_fuel_deduction(params: dict, ctx: WorkerContext) -> dict:
    tenant_id = params.get("tenant_id")
    consumed_fuel = params.get("consumed_fuel", 0)
    trace_id = params.get("trace_id", "unknown")

    if not tenant_id or consumed_fuel <= 0:
        return _build_error(400, "Invalid deduction parameters")

    try:
        # WorkerContext에 이미 인스턴스화 되어있는 pta_adapter 재사용 (성능 최적화)
        pta_adapter = ctx.pta_adapter
        
        available_outputs = []
        for pointer_key, output in pta_adapter._unfold_pool.items():
            if output.owner == tenant_id and output.asset_type == "fuel":
                tx_hash, idx = pointer_key.split(":")
                available_outputs.append({
                    "pointer": PtaPointer(tx_hash=tx_hash, output_index=int(idx)), 
                    "amount": output.amount
                })

        total_available = sum(item["amount"] for item in available_outputs)
        actual_consume = min(consumed_fuel, total_available) if total_available > 0 else consumed_fuel

        inputs: List[PtaInput] = []
        outputs: List[PtaOutput] = []
        accumulated = 0

        for item in available_outputs:
            inputs.append(
                PtaInput(
                    pointer=item["pointer"], 
                    signature="system_rpc_bypass_sig", 
                    owner_address=tenant_id
                )
            )
            accumulated += item["amount"]
            if accumulated >= actual_consume:
                break

        change = accumulated - actual_consume
        if change > 0:
            outputs.append(PtaOutput(amount=change, owner=tenant_id, asset_type="fuel"))
            
        outputs.append(PtaOutput(amount=actual_consume, owner="system_treasury", asset_type="fuel"))

        tx = PtaTransaction(
            inputs=inputs, 
            outputs=outputs, 
            metadata={"action": "queue_deduction", "trace_id": trace_id}
        )

        tx_hash = await pta_adapter.execute_transaction(tx)
        log.info(f"[Billing:Queue] Deducted {actual_consume} fuel from '{tenant_id}'. Trace: {trace_id} (TX: {tx_hash[:8]})")
        return {"status": "SUCCESS", "tx_hash": tx_hash, "deducted": actual_consume}

    except Exception as e:
        log.error(f"[Billing:Queue] Fuel deduction failed for '{tenant_id}': {str(e)}", exc_info=True)
        return _build_error(500, "Internal billing deduction error")