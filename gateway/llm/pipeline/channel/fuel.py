# fiber.gateway.llm.pipeline.channel.fuel
## @lineage: fiber.gateway.llm.channel.fuel
from __future__ import annotations

import os
import asyncio
from typing import Any, Dict, Optional, Tuple

from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.arch.model.auth import BrokerKey, NodeAuthPayload
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.fuel")


def _extract_usage_info(msg: Any) -> Tuple[int, Dict[str, Any]]:
    """Pydantic 객체와 Dict 응답 모두에서 안전하게 (total_tokens, usage_dict) 추출"""
    usage = msg.get("usage") if isinstance(msg, dict) else getattr(msg, "usage", None)
    if not usage:
        return 0, {}

    if hasattr(usage, "model_dump") and callable(getattr(usage, "model_dump")):
        try:
            usage_dict = usage.model_dump(exclude_unset=True)
        except Exception:
            usage_dict = {}
    elif isinstance(usage, dict):
        usage_dict = usage
    elif hasattr(usage, "__dict__"):
        usage_dict = {k: v for k, v in vars(usage).items() if not k.startswith("_") and v is not None}
    else:
        usage_dict = {}

    total_tokens = usage_dict.get("total_tokens")
    if total_tokens is None:
        total_tokens = getattr(usage, "total_tokens", None)
    if total_tokens is None:
        p = usage_dict.get("prompt_tokens") or getattr(usage, "prompt_tokens", 0) or 0
        c = usage_dict.get("completion_tokens") or getattr(usage, "completion_tokens", 0) or 0
        total_tokens = int(p) + int(c)

    try:
        return int(total_tokens), usage_dict
    except (ValueError, TypeError):
        return 0, usage_dict


class FuelBreaker(DuplexChannel):
    def __init__(self, finops_watcher: Optional[Any] = None):
        self.finops_watcher = finops_watcher

    async def write(self, ctx: ChannelContext, msg: Any) -> Any:
        tenant_id = ctx.get_attr("tenant_id", "anonymous")
        req_kwargs = ctx.get_attr("request_kwargs", {})
        metadata = req_kwargs.get("metadata", {})
        
        # 권한 인가 및 예산(Budget) 제어를 위한 순수 Auth Payload 파싱
        # raw_auth = metadata.get(BrokerKey.NODE_AUTH.value, {})
        # node_auth = NodeAuthPayload(**raw_auth) if isinstance(raw_auth, dict) else (raw_auth or NodeAuthPayload())
        enforce_quota = os.getenv("FIBER_ENFORCE_QUOTA", "false").lower() == "true"
        # is_enforced = getattr(node_auth, "is_enforced", False)

        node_auth: NodeAuthPayload = metadata.get(BrokerKey.NODE_AUTH.value)
        if node_auth.is_enforced and node_auth.is_anonymous():
            raise PermissionError("Strict Quota Enforcement blocked anonymous tenant.")
        ctx.set_attr(BrokerKey.AUDIT_HASH.value, node_auth.audit_hash)

        # 보안 검증
        # if enforce_quota and not is_enforced:
        #     log_pipeline.warning(f"[Security] Unauthorized API Access Attempt blocked. Tenant: {tenant_id}")
        #     raise PermissionError(f"Strict Quota Enforcement is active. Missing valid Kernel Auth for tenant: {tenant_id}")
        
        # 예산 통제 로직
        fuel_budget = getattr(node_auth, "fuel_budget", float('inf'))
        if fuel_budget is None:
            fuel_budget = float('inf')
        
        if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
            if tenant_id in self.finops_watcher.local_budget_cache:
                fuel_budget = self.finops_watcher.local_budget_cache[tenant_id]
                log_pipeline.debug(f"[FuelBreaker] Applied dynamic FinOps budget for {tenant_id}: {fuel_budget}")

        # 예산이 0 이하로 완전 소진된 경우 네트워크 I/O 진입 전 사전 차단
        if enforce_quota and fuel_budget <= 0:
            log_pipeline.warning(f"[FuelBreaker] Pre-flight blocked: Budget exhausted ({fuel_budget}) for tenant: {tenant_id}")
            raise PermissionError(f"Fuel budget exhausted for tenant: {tenant_id}")

        # 차단기(Breaker) 가동을 위한 Context 상태 초기화
        ctx.set_attr(BrokerKey.FUEL_BUDGET.value, fuel_budget)
        ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, 0)
        ctx.set_attr(BrokerKey.AUDIT_HASH.value, getattr(node_auth, "audit_hash", None))
        
        return await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = req.get("stream", False) or "stream" in req.get("call_type", "")
        
        fuel_budget = ctx.get_attr(BrokerKey.FUEL_BUDGET.value, float('inf'))
        audit_hash = ctx.get_attr(BrokerKey.AUDIT_HASH.value)

        # 단일 응답(Non-stream) 처리
        if not is_stream and not hasattr(msg, "__aiter__"):
            total_tokens, _ = _extract_usage_info(msg)
            if total_tokens > 0:
                ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, total_tokens)

            if audit_hash and hasattr(msg, "system_fingerprint"):
                try:
                    msg.system_fingerprint = audit_hash
                except (AttributeError, TypeError, ValueError):
                    pass

        # 스트림(Stream) 킬 스위치 처리: 초기 예산이 유한하거나 동적 감시자(finops_watcher)가 연결된 경우 트랩 장착
        elif hasattr(msg, "__aiter__") and (fuel_budget < float('inf') or self.finops_watcher is not None):
            tenant_id = ctx.get_attr("tenant_id", "anonymous")
            msg = self._fuel_trap_generator(msg, fuel_budget, tenant_id, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream: Any, budget: float, tenant_id: str, context: ChannelContext):
        consumed = 0
        current_budget = budget
        try:
            async for raw_chunk in raw_stream:
                consumed += 1 
                context.set_attr(BrokerKey.FUEL_CONSUMED.value, consumed)

                # 스트리밍 도중 Pub/Sub으로 갱신된 실시간 예산(예: Kill-Switch 0.0)을 매 청크마다 즉시 반영
                if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
                    if tenant_id in self.finops_watcher.local_budget_cache:
                        current_budget = self.finops_watcher.local_budget_cache[tenant_id]
                        context.set_attr(BrokerKey.FUEL_BUDGET.value, current_budget)

                if consumed > current_budget:
                    log_pipeline.warning(f"[FuelTrap] Fuel exhausted ({current_budget}). Killing stream physically.")
                    break
                yield raw_chunk
        except Exception as e:
            log_pipeline.error(f"Stream interrupted during fuel metering: {e}")
            raise
        finally:
            if consumed > current_budget and hasattr(raw_stream, "aclose") and callable(getattr(raw_stream, "aclose")):
                try:
                    await raw_stream.aclose()
                except Exception:
                    pass

class BillingObserver(DuplexChannel):
    def __init__(self, tenant_eco: Optional[Any] = None, pta_adapter: Optional[Any] = None):
        self.tenant_eco = tenant_eco
        self.pta_adapter = pta_adapter
        self._background_tasks: set = set()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        processed_ctx = ctx.get_attr("processed_context")
        is_stream = hasattr(msg, "__aiter__")

        def execute_billing(total_tokens: int = 0, raw_usage: Any = None):
            fuel_consumed = ctx.get_attr(BrokerKey.FUEL_CONSUMED.value, 0)
            
            # 실제 집계된 total_tokens가 있으면 최우선 적용하고, 스트림 강제 중단 등으로 0일 때만 청크 카운트(fuel_consumed)로 폴백
            effective_fuel = total_tokens if (total_tokens and total_tokens > 0) else fuel_consumed
            if effective_fuel > 0:
                ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, effective_fuel)
                
            tenant_id = ctx.get_attr("tenant_id", "unknown")
            
            if effective_fuel > 0 and tenant_id not in ("anonymous", "internal_system", "unknown"):
                async def _process_billing_task():
                    try:
                        final_consume_amount = effective_fuel
                        
                        if self.tenant_eco:
                            # SemanticRouter나 FallbackHandler에 의해 변경된 최종 모델/프로바이더 우선 반영
                            model_name = (
                                getattr(processed_ctx, "model", None)
                                or getattr(meta, "base_model", None)
                                or req.get("model", "default-model")
                            )
                            provider = (
                                getattr(processed_ctx, "custom_llm_provider", None)
                                or req.get("custom_llm_provider")
                            )
                            usage_payload = raw_usage if raw_usage else {"total_tokens": effective_fuel}
                            
                            calc_res = await self.tenant_eco.calculate_tenant_billing(
                                tenant_id=tenant_id,
                                usage=usage_payload,
                                model_name=model_name,
                                provider=provider
                            )
                            
                            if isinstance(calc_res, dict) and calc_res.get("status") == "success":
                                intent = calc_res.get("consume_intent", {})
                                final_consume_amount = intent.get("amount_to_consume", final_consume_amount)
                        
                        from xphi.node.rpc.handler.fuel import execute_direct_fuel_consume
                        await execute_direct_fuel_consume(
                            tenant_id=tenant_id, 
                            consumed_fuel=final_consume_amount, 
                            trace_id=meta.trace_id if meta else None,
                            pta_adapter=self.pta_adapter
                        )
                    except Exception as e:
                        log_pipeline.error(f"Failed to process billing pipeline for {tenant_id}: {e}")

                # 가비지 컬렉션(GC)에 의해 백그라운드 코루틴이 증발하는 현상 방지
                task = asyncio.create_task(_process_billing_task())
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

        if is_stream:
            if meta:
                meta.framework_flags["on_stream_complete"] = execute_billing
            await ctx.fire_channel_read(msg)
            return

        total_tokens, usage_dict = _extract_usage_info(msg)
        execute_billing(total_tokens, raw_usage=usage_dict)
        await ctx.fire_channel_read(msg)