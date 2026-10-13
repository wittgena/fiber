# fiber.gateway.llm.channel.fuel
from __future__ import annotations

import os
import asyncio
from typing import Any, Dict, Optional

from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.arch.model.dphi.auth import DphiKey, KernelAuthPayload
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.fuel")

class FuelBreaker(DuplexChannel):
    def __init__(self, finops_watcher: Optional[Any] = None):
        self.finops_watcher = finops_watcher

    async def write(self, ctx: ChannelContext, msg: Any):
        # ContextBinder가 앞단에서 주입한 tenant_id를 신뢰
        tenant_id = ctx.get_attr("tenant_id", "anonymous")
        
        req_kwargs = ctx.get_attr("request_kwargs", {})
        metadata = req_kwargs.get("metadata", {})
        
        # 권한 인가 및 예산(Budget) 제어를 위한 순수 Auth Payload 파싱
        raw_auth = metadata.get(DphiKey.KERNEL_AUTH.value, {})
        kernel_auth = KernelAuthPayload(**raw_auth) if isinstance(raw_auth, dict) else (raw_auth or KernelAuthPayload())
        
        enforce_quota = os.getenv("FIBER_ENFORCE_QUOTA", "false").lower() == "true"
        is_enforced = getattr(kernel_auth, "is_enforced", False)

        # [Security] 보안 검증
        if enforce_quota and not is_enforced:
            log_pipeline.warning(f"[Security] Unauthorized API Access Attempt blocked. Tenant: {tenant_id}")
            raise PermissionError(f"Strict Quota Enforcement is active. Missing valid Kernel Auth for tenant: {tenant_id}")
        
        # 예산 통제 로직
        fuel_budget = getattr(kernel_auth, "fuel_budget", float('inf'))
        
        if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
            if tenant_id in self.finops_watcher.local_budget_cache:
                fuel_budget = self.finops_watcher.local_budget_cache[tenant_id]
                log_pipeline.debug(f"[FuelBreaker] Applied dynamic FinOps budget for {tenant_id}: {fuel_budget}")

        # 차단기(Breaker) 가동을 위한 Context 상태 초기화
        ctx.set_attr(DphiKey.FUEL_BUDGET.value, fuel_budget)
        ctx.set_attr(DphiKey.FUEL_CONSUMED.value, 0)
        ctx.set_attr(DphiKey.AUDIT_HASH.value, getattr(kernel_auth, "audit_hash", None))
        
        await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = req.get("stream", False) or "stream" in req.get("call_type", "")
        
        fuel_budget = ctx.get_attr(DphiKey.FUEL_BUDGET.value, float('inf'))
        audit_hash = ctx.get_attr(DphiKey.AUDIT_HASH.value)

        # 단일 응답(Non-stream) 처리 시 안전성 개선
        if not is_stream and not hasattr(msg, "__aiter__"):
            usage = getattr(msg, "usage", None)
            
            # Pydantic 객체와 일반 Dict 형식을 모두 안전하게 지원하는 방어 로직 추가
            usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (usage or {})
            total_tokens = usage_dict.get("total_tokens", getattr(usage, "total_tokens", 0)) if isinstance(usage_dict, dict) else getattr(usage, "total_tokens", 0)
            
            if total_tokens > 0:
                ctx.set_attr(DphiKey.FUEL_CONSUMED.value, total_tokens)

            if audit_hash and hasattr(msg, "system_fingerprint"):
                msg.system_fingerprint = audit_hash

        # 스트림(Stream) 킬 스위치 처리
        elif hasattr(msg, "__aiter__") and fuel_budget < float('inf'):
            msg = self._fuel_trap_generator(msg, fuel_budget, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream, budget, context):
        consumed = 0
        try:
            async for raw_chunk in raw_stream:
                consumed += 1 
                context.set_attr(DphiKey.FUEL_CONSUMED.value, consumed)
                if consumed > budget:
                    log_pipeline.warning(f"[FuelTrap] Fuel exhausted ({budget}). Killing stream physically.")
                    break
                yield raw_chunk
        except Exception as e:
            log_pipeline.error(f"Stream interrupted during fuel metering: {e}")
            raise

class BillingObserver(DuplexChannel):
    def __init__(self, tenant_eco: Optional[Any] = None, pta_adapter: Optional[Any] = None):
        self.tenant_eco = tenant_eco
        self.pta_adapter = pta_adapter

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = hasattr(msg, "__aiter__")

        def execute_billing(total_tokens: int, raw_usage: Any = None):
            fuel_consumed = ctx.get_attr(DphiKey.FUEL_CONSUMED.value, 0)
            
            if fuel_consumed == 0 and total_tokens > 0:
                fuel_consumed = total_tokens
                
            tenant_id = ctx.get_attr("tenant_id", "unknown")
            
            if fuel_consumed > 0 and tenant_id not in ("anonymous", "internal_system", "unknown"):
                async def _process_billing_task():
                    try:
                        final_consume_amount = fuel_consumed
                        
                        if self.tenant_eco:
                            model_name = req.get("model", "default-model")
                            provider = req.get("custom_llm_provider")
                            usage_payload = raw_usage if raw_usage else {"total_tokens": fuel_consumed}
                            
                            calc_res = await self.tenant_eco.calculate_tenant_billing(
                                tenant_id=tenant_id,
                                usage=usage_payload,
                                model_name=model_name,
                                provider=provider
                            )
                            
                            if calc_res.get("status") == "success":
                                intent = calc_res.get("consume_intent", {})
                                final_consume_amount = intent.get("amount_to_consume", final_consume_amount)
                        
                        from fiber.infra.rpc.handler.fuel import execute_direct_fuel_consume
                        await execute_direct_fuel_consume(
                            tenant_id=tenant_id, 
                            consumed_fuel=final_consume_amount, 
                            trace_id=meta.trace_id if meta else None,
                            pta_adapter=self.pta_adapter
                        )
                    except Exception as e:
                        log_pipeline.error(f"Failed to process billing pipeline for {tenant_id}: {e}")
                asyncio.create_task(_process_billing_task())

        if is_stream:
            if meta:
                meta.framework_flags["on_stream_complete"] = execute_billing
            await ctx.fire_channel_read(msg)
            return

        usage = getattr(msg, "usage", None)
        usage_dict = usage.model_dump() if hasattr(usage, "model_dump") else (usage or {})
        total_tokens = usage_dict.get("total_tokens", getattr(usage, "total_tokens", 0)) if isinstance(usage_dict, dict) else getattr(usage, "total_tokens", 0)

        execute_billing(total_tokens, raw_usage=usage_dict)
        await ctx.fire_channel_read(msg)