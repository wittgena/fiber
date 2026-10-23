# fiber.llm.pipeline.channel.fuel_old
## @lineage: fiber.llm.pipeline.channel.fuel
from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Optional, Tuple

from fiber.dev.console.bridge.signal import UIKey, UIMetric, UISlot, UISlotStatus
from xphi.arch.model.auth import BrokerKey, NodeAuthPayload
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.fuel", phase="BILLING")


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
        t0 = time.perf_counter()
        tenant_id = ctx.get_attr("tenant_id", "anonymous")
        req_kwargs = ctx.get_attr("request_kwargs", {})
        metadata = req_kwargs.get("metadata", {}) if isinstance(req_kwargs, dict) else {}

        enforce_quota = os.getenv("FIBER_ENFORCE_QUOTA", "false").lower() == "true"
        raw_auth = metadata.get(BrokerKey.NODE_AUTH.value)
        node_auth: Optional[NodeAuthPayload] = (
            NodeAuthPayload(**raw_auth)
            if isinstance(raw_auth, dict)
            else raw_auth
        )

        if node_auth and getattr(node_auth, "is_enforced", False) and node_auth.is_anonymous():
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
            log_pipeline.signal(
                f"[FuelBreaker] Strict Quota Enforcement blocked anonymous tenant ('{tenant_id}').",
                **{
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                    UIKey.LATENCY_MS.value: elapsed_ms,
                    UIKey.TENANT_ID.value: tenant_id,
                },
            )
            raise PermissionError("Strict Quota Enforcement blocked anonymous tenant.")

        audit_hash = getattr(node_auth, "audit_hash", None) if node_auth else None
        ctx.set_attr(BrokerKey.AUDIT_HASH.value, audit_hash)

        fuel_budget = getattr(node_auth, "fuel_budget", float("inf")) if node_auth else float("inf")
        if fuel_budget is None:
            fuel_budget = float("inf")

        if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
            if tenant_id in self.finops_watcher.local_budget_cache:
                fuel_budget = float(self.finops_watcher.local_budget_cache[tenant_id])
                log_pipeline.debug(
                    f"[FuelBreaker] Applied dynamic FinOps budget for '{tenant_id}': {fuel_budget}",
                    **{UIKey.TENANT_ID.value: tenant_id, UIKey.ACTIVE_QUOTA.value: int(fuel_budget)},
                )

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        # 예산이 0 이하로 완전 소진된 경우 네트워크 I/O 진입 전 사전 차단 (배압 우회 .signal)
        if enforce_quota and fuel_budget <= 0:
            log_pipeline.signal(
                f"[FuelBreaker] Pre-flight blocked: Budget exhausted ({fuel_budget}) for tenant '{tenant_id}'.",
                **{
                    UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value,
                    UIKey.LATENCY_MS.value: elapsed_ms,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.ACTIVE_QUOTA.value: 0,
                },
            )
            raise PermissionError(f"Fuel budget exhausted for tenant: {tenant_id}")

        # 정상 통과 시 X-Ray 4번 슬롯(FuelBreaker) 점등 및 서브밀리초 오버헤드 기록
        pass_payload: Dict[str, Any] = {
            UIKey.SLOT.value: UISlot.FUEL.value,
            UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
            UIKey.LATENCY_MS.value: elapsed_ms,
            UIKey.TENANT_ID.value: tenant_id,
        }
        if math.isfinite(fuel_budget):
            pass_payload[UIKey.ACTIVE_QUOTA.value] = int(fuel_budget)

        log_pipeline.info(
            f"[FuelBreaker] Pre-flight passed for '{tenant_id}' ({elapsed_ms:.3f}ms).",
            **pass_payload,
        )

        ctx.set_attr(BrokerKey.FUEL_BUDGET.value, fuel_budget)
        ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, 0)
        return await ctx.fire_write(msg)

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        req: dict = ctx.get_attr("request_kwargs", {})
        is_stream = req.get("stream", False) or "stream" in str(req.get("call_type", ""))

        fuel_budget = ctx.get_attr(BrokerKey.FUEL_BUDGET.value, float("inf"))
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
        elif hasattr(msg, "__aiter__") and (fuel_budget < float("inf") or self.finops_watcher is not None):
            tenant_id = ctx.get_attr("tenant_id", "anonymous")
            msg = self._fuel_trap_generator(msg, fuel_budget, tenant_id, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream: Any, budget: float, tenant_id: str, context: ChannelContext):
        consumed = 0
        current_budget = budget
        severed = False
        try:
            async for raw_chunk in raw_stream:
                consumed += 1
                context.set_attr(BrokerKey.FUEL_CONSUMED.value, consumed)

                # 스트리밍 도중 Pub/Sub으로 갱신된 실시간 예산(예: Kill-Switch 0.0)을 매 청크마다 즉시 반영
                if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
                    if tenant_id in self.finops_watcher.local_budget_cache:
                        current_budget = float(self.finops_watcher.local_budget_cache[tenant_id])
                        context.set_attr(BrokerKey.FUEL_BUDGET.value, current_budget)

                if consumed > current_budget:
                    severed = True
                    log_pipeline.signal(
                        f"[FuelTrap] Fuel exhausted or revoked (budget={current_budget}, consumed={consumed}) "
                        f"for '{tenant_id}'. Severing stream physically via aclose().",
                        **{
                            UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                            UIKey.SLOT.value: UISlot.FUEL.value,
                            UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value,
                            UIKey.TENANT_ID.value: tenant_id,
                            UIKey.ACTIVE_QUOTA.value: 0,
                            UIKey.CHUNKS.value: consumed,
                        },
                    )
                    break

                # 콘솔 차트 실시간 구동 (첫 청크 및 매 10청크 단위 스로틀링으로 이벤트 버스 부하 방지)
                if math.isfinite(current_budget) and (consumed == 1 or consumed % 10 == 0):
                    remaining = max(0, int(current_budget - consumed))
                    log_pipeline.info(
                        f"[FuelTrap] Stream metering '{tenant_id}': consumed={consumed}, remaining={remaining} Fuel",
                        **{
                            UIKey.METRIC.value: UIMetric.FUEL_TICK.value,
                            UIKey.SLOT.value: UISlot.FUEL.value,
                            UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                            UIKey.TENANT_ID.value: tenant_id,
                            UIKey.ACTIVE_QUOTA.value: remaining,
                            UIKey.CHUNKS.value: consumed,
                        },
                    )

                yield raw_chunk
        except Exception as e:
            log_pipeline.error(
                f"[FuelTrap] Stream interrupted during fuel metering for '{tenant_id}': {e}",
                exc_info=True,
                **{UIKey.TENANT_ID.value: tenant_id},
            )
            raise
        finally:
            if severed and hasattr(raw_stream, "aclose") and callable(getattr(raw_stream, "aclose")):
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
                                provider=provider,
                            )

                            if isinstance(calc_res, dict) and calc_res.get("status") == "success":
                                intent = calc_res.get("consume_intent", {})
                                final_consume_amount = intent.get("amount_to_consume", final_consume_amount)

                        from xphi.node.rpc.handler.fuel import execute_direct_fuel_consume
                        await execute_direct_fuel_consume(
                            tenant_id=tenant_id,
                            consumed_fuel=final_consume_amount,
                            trace_id=meta.trace_id if meta else None,
                            pta_adapter=self.pta_adapter,
                        )
                    except Exception as e:
                        log_pipeline.error(
                            f"[BillingObserver] Failed to process billing pipeline for '{tenant_id}': {e}",
                            exc_info=True,
                            **{UIKey.TENANT_ID.value: tenant_id},
                        )

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