# fiber.llm.pipeline.channel.observer
from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Dict, Optional

from fiber.dev.console.observer.signal import UIKey, UIMetric, UISlot, UISlotStatus
from fiber.llm.pipeline.channel.breaker import extract_usage_info
from xphi.arch.model.auth import BrokerKey
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.observer", phase="METERING")

def _invoke_stream_hook(fn: Callable[..., Any], total_tokens: int, raw_usage: Any) -> None:
    """콜백의 인자 시그니처(1개 vs 2개)를 검사하여 안전하게 호출"""
    try:
        sig = inspect.signature(fn)
        params = [
            p
            for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
        if has_varargs or len(params) >= 2:
            fn(total_tokens, raw_usage)
        elif len(params) == 1:
            fn(total_tokens)
        else:
            fn()
    except Exception:
        try:
            fn(total_tokens)
        except Exception:
            pass


def _attach_stream_complete_hook(meta: Any, callback: Callable[..., Any]) -> None:
    """
    기존 `on_stream_complete`에 등록된 콜백(단일 함수 또는 리스트)을 덮어쓰지 않고
    체인(Composite) 형태로 안전하게 병합합니다.
    """
    flags = getattr(meta, "framework_flags", None)
    if not isinstance(flags, dict):
        return

    existing = flags.get("on_stream_complete")
    if existing is None:
        flags["on_stream_complete"] = callback
    elif isinstance(existing, list):
        existing.append(callback)

        def _chained_list_hook(total_tokens: int = 0, raw_usage: Any = None) -> None:
            for fn in list(existing):
                if callable(fn):
                    _invoke_stream_hook(fn, total_tokens, raw_usage)

        flags["on_stream_complete"] = _chained_list_hook
    elif callable(existing):

        def _chained_pair_hook(total_tokens: int = 0, raw_usage: Any = None) -> None:
            _invoke_stream_hook(existing, total_tokens, raw_usage)
            _invoke_stream_hook(callback, total_tokens, raw_usage)

        flags["on_stream_complete"] = _chained_pair_hook


class MeteringObserver(DuplexChannel):
    def __init__(self, tenant_eco: Optional[Any] = None, pta_adapter: Optional[Any] = None):
        self.tenant_eco = tenant_eco
        self.pta_adapter = pta_adapter
        self._background_tasks: set = set()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        meta = ctx.get_attr("system_meta")
        req: dict = ctx.get_attr("request_kwargs", {})
        processed_ctx = ctx.get_attr("processed_context")
        is_stream = hasattr(msg, "__aiter__")

        def execute_metering(total_tokens: int = 0, raw_usage: Any = None):
            fuel_consumed = ctx.get_attr(BrokerKey.FUEL_CONSUMED.value, 0)
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
                        if self.pta_adapter is not None:
                            actual_balance = await self.pta_adapter.get_balance(tenant_id, "fuel")
                            log_pipeline.info(
                                f"[MeteringObserver] AnchorStore balance synced for '{tenant_id}': {actual_balance} Fuel",
                                **{
                                    "mode": "RAW",
                                    UIKey.METRIC.value: UIMetric.LEDGER_SYNC.value,
                                    UIKey.SLOT.value: UISlot.FUEL.value,
                                    UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                                    UIKey.TENANT_ID.value: tenant_id,
                                    UIKey.LEDGER_BALANCE.value: actual_balance,
                                },
                            )
                    except Exception as e:
                        log_pipeline.error(
                            f"[MeteringObserver] Failed to process billing pipeline for '{tenant_id}': {e}",
                            exc_info=True,
                            **{UIKey.TENANT_ID.value: tenant_id},
                        )

                task = asyncio.create_task(_process_billing_task())
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

        if is_stream:
            if meta:
                _attach_stream_complete_hook(meta, execute_metering)
            await ctx.fire_channel_read(msg)
            return

        total_tokens, usage_dict = extract_usage_info(msg)
        execute_metering(total_tokens, raw_usage=usage_dict)
        await ctx.fire_channel_read(msg)