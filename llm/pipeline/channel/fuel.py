# fiber.llm.pipeline.channel.fuel
from __future__ import annotations

import asyncio
import inspect
import math
import os
import time
from typing import Any, Callable, Dict, Optional, Tuple

from fiber.dev.console.observer.signal import UIKey, UIMetric, UISlot, UISlotStatus
from fiber.llm.model.token.encoder import encode
from xphi.arch.model.auth import BrokerKey, NodeAuthPayload
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.fuel", phase="BILLING")

# 실시간 스트림 활성 연결 수 (단일 이벤트 루프 내 원자적 카운터)
_ACTIVE_STREAMS: int = 0

# 스트림 텔레메트리(FUEL_TICK) 최소 방출 간격 (초)
_STREAM_TICK_INTERVAL_SEC: float = 0.25


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

        # 1. 강제 쿼터 모드이거나 is_enforced=True일 때 익명(anonymous) 테넌트 차단
        is_explicitly_enforced = bool(node_auth and getattr(node_auth, "is_enforced", False))
        if (is_explicitly_enforced or enforce_quota) and (
            tenant_id == "anonymous" or (node_auth is not None and node_auth.is_anonymous())
        ):
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

        # 2. FinOps Watcher 원장 캐시 조회
        in_ledger = False
        if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
            if tenant_id in self.finops_watcher.local_budget_cache:
                in_ledger = True
                fuel_budget = float(self.finops_watcher.local_budget_cache[tenant_id])
                log_pipeline.debug(
                    f"[FuelBreaker] Applied dynamic FinOps budget for '{tenant_id}': {fuel_budget}",
                    **{UIKey.TENANT_ID.value: tenant_id, UIKey.ACTIVE_QUOTA.value: int(fuel_budget)},
                )

        # 3. [Quota Shield] FIBER_ENFORCE_QUOTA=true 환경에서 미인가 예산 주입 차단
        if enforce_quota and not in_ledger and not audit_hash and not is_explicitly_enforced:
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
            log_pipeline.signal(
                f"[FuelBreaker] Quota Shield blocked unverified budget injection "
                f"(tenant='{tenant_id}', claimed={fuel_budget}).",
                **{
                    UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                    UIKey.LATENCY_MS.value: elapsed_ms,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.ACTIVE_QUOTA.value: 0,
                },
            )
            raise PermissionError(f"Unauthorized fuel budget injection blocked for tenant: {tenant_id}")

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        # 4. 예산이 0 이하로 완전 소진된 경우 네트워크 I/O 진입 전 사전 차단
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

        # 5. 정상 통과 시 X-Ray 4번 슬롯(FuelBreaker) 점등 및 서브밀리초 오버헤드 기록
        pass_payload: Dict[str, Any] = {
            "mode": "RAW",
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
        tenant_id = ctx.get_attr("tenant_id", "anonymous")

        # 1. 단일 응답(Non-stream) 처리 및 토큰 카운트 방출 (기본 1 Token = 1 Fuel)
        if not is_stream and not hasattr(msg, "__aiter__"):
            total_tokens, _ = _extract_usage_info(msg)
            if total_tokens > 0:
                ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, total_tokens)
                unary_payload: Dict[str, Any] = {
                    "mode": "RAW",
                    UIKey.METRIC.value: UIMetric.FUEL_TICK.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.CHUNKS.value: total_tokens,
                    "active_streams": _ACTIVE_STREAMS,
                }
                if math.isfinite(fuel_budget):
                    unary_payload[UIKey.ACTIVE_QUOTA.value] = max(0, int(fuel_budget - total_tokens))
                log_pipeline.info(
                    f"[FuelBreaker] Unary completion '{tenant_id}': tokens={total_tokens}",
                    **unary_payload,
                )

            if audit_hash and hasattr(msg, "system_fingerprint"):
                try:
                    msg.system_fingerprint = audit_hash
                except (AttributeError, TypeError, ValueError):
                    pass

        # 2. 스트림(Stream) 킬 스위치 및 실시간 토큰 텔레메트리 트랩 장착
        elif hasattr(msg, "__aiter__"):
            msg = self._fuel_trap_generator(msg, fuel_budget, tenant_id, ctx)

        await ctx.fire_channel_read(msg)

    async def _fuel_trap_generator(self, raw_stream: Any, budget: float, tenant_id: str, context: ChannelContext):
        global _ACTIVE_STREAMS
        _ACTIVE_STREAMS += 1
        chunks = 0
        consumed = 0  # 실시간 누적 토큰 수 (기본 1 Token = 1 Fuel)
        current_budget = budget
        severed = False
        t_start = time.perf_counter()
        last_emit_ts = 0.0

        try:
            async for raw_chunk in raw_stream:
                chunks += 1
                try:
                    choices = raw_chunk.get("choices") if isinstance(raw_chunk, dict) else getattr(raw_chunk, "choices", None)
                    if choices:
                        c0 = choices[0]
                        delta = c0.get("delta") if isinstance(c0, dict) else getattr(c0, "delta", None)
                        if delta:
                            txt = (delta.get("content") if isinstance(delta, dict) else getattr(delta, "content", None)) or ""
                            if txt:
                                consumed += len(encode(txt))
                except Exception:
                    pass

                context.set_attr(BrokerKey.FUEL_CONSUMED.value, consumed)

                # 스트리밍 도중 Pub/Sub으로 갱신된 실시간 예산(예: Kill-Switch 0.0)을 매 청크마다 즉시 반영
                if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
                    if tenant_id in self.finops_watcher.local_budget_cache:
                        current_budget = float(self.finops_watcher.local_budget_cache[tenant_id])
                        context.set_attr(BrokerKey.FUEL_BUDGET.value, current_budget)

                # 첫 청크가 빈 헤더(consumed=0)이더라도 current_budget이 0.0(REVOKE)이면 즉시 차단되도록 max(1, consumed) 비교
                if max(1, consumed) > current_budget:
                    severed = True
                    log_pipeline.signal(
                        f"[FuelTrap] Fuel exhausted or revoked (budget={current_budget}, consumed={consumed} tokens) "
                        f"for '{tenant_id}'. Severing stream physically via aclose().",
                        **{
                            UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                            UIKey.SLOT.value: UISlot.FUEL.value,
                            UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value,
                            UIKey.TENANT_ID.value: tenant_id,
                            UIKey.ACTIVE_QUOTA.value: 0,
                            UIKey.CHUNKS.value: consumed,
                            "active_streams": max(0, _ACTIVE_STREAMS - 1),
                            "throughput_cps": 0.0,
                        },
                    )
                    break

                # 첫 청크이거나 20청크 단위이면서 최소 0.25초 이상 경과 시 방출 (consumed는 토큰 단위, cps는 청크 속도)
                now = time.perf_counter()
                if chunks == 1 or (chunks % 20 == 0 and (now - last_emit_ts) >= _STREAM_TICK_INTERVAL_SEC):
                    last_emit_ts = now
                    elapsed = max(now - t_start, 0.001)
                    cps = round(chunks / elapsed, 1)
                    tick_payload: Dict[str, Any] = {
                        "mode": "RAW",
                        UIKey.METRIC.value: UIMetric.FUEL_TICK.value,
                        UIKey.SLOT.value: UISlot.FUEL.value,
                        UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                        UIKey.TENANT_ID.value: tenant_id,
                        UIKey.CHUNKS.value: consumed,
                        "active_streams": _ACTIVE_STREAMS,
                        "throughput_cps": cps,
                    }
                    if math.isfinite(current_budget):
                        remaining = max(0, int(current_budget - consumed))
                        tick_payload[UIKey.ACTIVE_QUOTA.value] = remaining
                        msg_str = (
                            f"[FuelTrap] Stream metering '{tenant_id}': "
                            f"consumed={consumed} tokens, remaining={remaining} Fuel ({cps} chunks/s)"
                        )
                    else:
                        msg_str = f"[FuelTrap] Stream metering '{tenant_id}': consumed={consumed} tokens ({cps} chunks/s)"

                    log_pipeline.info(msg_str, **tick_payload)

                yield raw_chunk
        except Exception as e:
            log_pipeline.error(
                f"[FuelTrap] Stream interrupted during fuel metering for '{tenant_id}': {e}",
                exc_info=True,
                **{UIKey.TENANT_ID.value: tenant_id},
            )
            raise
        finally:
            _ACTIVE_STREAMS = max(0, _ACTIVE_STREAMS - 1)
            if severed and hasattr(raw_stream, "aclose") and callable(getattr(raw_stream, "aclose")):
                try:
                    await raw_stream.aclose()
                except Exception:
                    pass

            # 스트림 정상 완료 시 활성 연결 수 감소 및 속도(0.0) 초기화 이벤트 방출
            if not severed and chunks > 0:
                close_payload: Dict[str, Any] = {
                    "mode": "RAW",
                    UIKey.METRIC.value: UIMetric.FUEL_TICK.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.CHUNKS.value: consumed,
                    "active_streams": _ACTIVE_STREAMS,
                    "throughput_cps": 0.0 if _ACTIVE_STREAMS == 0 else None,
                }
                if math.isfinite(current_budget):
                    close_payload[UIKey.ACTIVE_QUOTA.value] = max(0, int(current_budget - consumed))
                log_pipeline.info(
                    f"[FuelTrap] Stream completed for '{tenant_id}' (total={consumed} tokens, chunks={chunks}, active={_ACTIVE_STREAMS}).",
                    **close_payload,
                )


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

            # 실제 집계된 total_tokens가 있으면 최우선 적용하고, 스트림 강제 중단 등으로 0일 때만 실시간 누적 토큰(fuel_consumed)로 폴백
            effective_fuel = total_tokens if (total_tokens and total_tokens > 0) else fuel_consumed
            if effective_fuel > 0:
                ctx.set_attr(BrokerKey.FUEL_CONSUMED.value, effective_fuel)

            tenant_id = ctx.get_attr("tenant_id", "unknown")

            if effective_fuel > 0 and tenant_id not in ("anonymous", "internal_system", "unknown"):
                async def _process_billing_task():
                    try:
                        # 기본값: 1 Token = 1 Fuel
                        final_consume_amount = effective_fuel

                        # tenant_eco가 주입된 경우 프로바이더/모델별 단가 계산 결과로 오버라이드
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
                _attach_stream_complete_hook(meta, execute_billing)
            await ctx.fire_channel_read(msg)
            return

        total_tokens, usage_dict = _extract_usage_info(msg)
        execute_billing(total_tokens, raw_usage=usage_dict)
        await ctx.fire_channel_read(msg)