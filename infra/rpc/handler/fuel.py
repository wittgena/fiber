# fiber.infra.rpc.handler.fuel
import asyncio
import logging
from typing import Dict, Any, List, Optional

from fiber.infra.rpc.handler.core import WorkerContext
from xphi.kernel.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import (
    PtaAdapter, 
    PtaTransaction, 
    PtaInput, 
    PtaPointer, 
    PtaOutput,
    SignStateAdapter
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.fuel")

async def execute_direct_fuel_consume(
    tenant_id: str, 
    consumed_fuel: int, 
    trace_id: str,
    pta_adapter: Optional[PtaAdapter] = None,
    broker: Optional[DphiBroker] = None
) -> None:
    if consumed_fuel <= 0 or tenant_id in ("anonymous", "internal_system", "unknown"):
        return

    owns_broker = False
    if pta_adapter is None:
        if broker is None:
            broker = DphiBroker(timeout=5.0)
            owns_broker = True
        pta_adapter = PtaAdapter(broker=broker)
    
    try:
        available_outputs = await pta_adapter.get_available_inputs(tenant_id, "fuel")
        total_available = sum(item["amount"] for item in available_outputs)
        actual_consume = min(consumed_fuel, total_available) if total_available > 0 else consumed_fuel
        
        inputs: List[PtaInput] = []
        outputs: List[PtaOutput] = []
        accumulated = 0
        
        node_pubkey = SignStateAdapter.get_signer_pubkey()
        for item in available_outputs:
            canonical_payload = item["pointer"].to_key()
            real_signature = SignStateAdapter.sign_state_payload({"payload": canonical_payload})
            
            inputs.append(
                PtaInput(
                    pointer=item["pointer"], 
                    signature=real_signature,
                    owner_address=node_pubkey  # 노드 권한으로 시스템 대리 서명(Bypass)
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
            metadata={"action": "direct_consume", "trace_id": trace_id}
        )
        await pta_adapter.execute_transaction(tx)
        log.info(f"[Billing:Direct] Auto-consumed {actual_consume} fuel from '{tenant_id}'. Trace: {trace_id}")
        
    except Exception as e:
        log.error(f"[Billing:Direct] Background consume failed for '{tenant_id}': {e}", exc_info=True)
    finally:
        if owns_broker and broker:
            await broker.close()

# RPC Queue Handler (Daemon Mode)
def _build_error(code: int, message: str) -> dict:
    """RPC 에러 포맷 규격화"""
    return {"error": True, "code": code, "message": message}

async def handle_fuel_consume(params: dict, ctx: WorkerContext) -> dict:
    tenant_id = params.get("tenant_id")
    consumed_fuel = params.get("consumed_fuel", 0)
    trace_id = params.get("trace_id", "unknown")

    if not tenant_id or consumed_fuel <= 0:
        return _build_error(400, "Invalid consume parameters")

    try:
        pta_adapter = ctx.pta_adapter
        
        available_outputs = await pta_adapter.get_available_inputs(tenant_id, "fuel")
        total_available = sum(item["amount"] for item in available_outputs)
        actual_consume = min(consumed_fuel, total_available) if total_available > 0 else consumed_fuel

        inputs: List[PtaInput] = []
        outputs: List[PtaOutput] = []
        accumulated = 0
        
        node_pubkey = SignStateAdapter.get_signer_pubkey()
        for item in available_outputs:
            canonical_payload = item["pointer"].to_key()
            real_signature = SignStateAdapter.sign_state_payload({"payload": canonical_payload})
            
            inputs.append(
                PtaInput(
                    pointer=item["pointer"], 
                    signature=real_signature, 
                    owner_address=node_pubkey  # 노드 권한으로 시스템 대리 서명(Bypass)
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
            metadata={"action": "queue_consume", "trace_id": trace_id}
        )

        tx_hash = await pta_adapter.execute_transaction(tx)
        log.info(f"[Tx:Queue] Consumed {actual_consume} fuel from '{tenant_id}'. Trace: {trace_id} (TX: {tx_hash[:8]})")
        return {"status": "SUCCESS", "tx_hash": tx_hash, "consumed": actual_consume}
    except Exception as e:
        log.error(f"[Tx:Queue] Fuel consume failed for '{tenant_id}': {str(e)}", exc_info=True)
        return _build_error(500, "Internal fuel consume error")