# fiber.llm.pipeline.channel.breaker
"""
Glossary:
- fuel_budget: Fixed spending quota limit assigned via policy/Mandate (immutable until upgraded).
- ledger_balance: Real-time remaining fuel balance strictly anchored to the PTA SSOT.
- Ensure UI signals and state tracking keep these two concepts strictly decoupled.
"""
from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Optional, Tuple

from fiber.dev.console.observer.signal import UIKey, UIMetric, UISlot, UISlotStatus
from fiber.llm.model.token.encoder import encode
from xphi.arch.model.auth import BrokerKey, NodeAuthPayload
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

log_pipeline = get_emitter("channel.fuel", phase="BILLING")

_ACTIVE_STREAMS: int = 0
_STREAM_TICK_INTERVAL_SEC: float = 0.25

def extract_usage_info(msg: Any) -> Tuple[int, Dict[str, Any]]:
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


class StreamBreaker(DuplexChannel):
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

        is_explicitly_enforced = bool(node_auth and getattr(node_auth, "is_enforced", False))
        if (is_explicitly_enforced or enforce_quota) and (
            tenant_id == "anonymous" or (node_auth is not None and node_auth.is_anonymous())
        ):
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
            log_pipeline.signal(
                f"[StreamBreaker] Strict Quota Enforcement blocked anonymous tenant ('{tenant_id}').",
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

        in_ledger = False
        if self.finops_watcher and hasattr(self.finops_watcher, "local_budget_cache"):
            if tenant_id in self.finops_watcher.local_budget_cache:
                in_ledger = True
                fuel_budget = float(self.finops_watcher.local_budget_cache[tenant_id])
                log_pipeline.debug(
                    f"[StreamBreaker] Applied dynamic FinOps budget for '{tenant_id}': {fuel_budget}",
                    **{UIKey.TENANT_ID.value: tenant_id, UIKey.FUEL_BUDGET.value: int(fuel_budget)},
                )

        if enforce_quota and not in_ledger and not audit_hash and not is_explicitly_enforced:
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)
            log_pipeline.signal(
                f"[StreamBreaker] Quota Shield blocked unverified budget injection "
                f"(tenant='{tenant_id}', claimed={fuel_budget}).",
                **{
                    UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                    UIKey.LATENCY_MS.value: elapsed_ms,
                    UIKey.TENANT_ID.value: tenant_id,
                },
            )
            raise PermissionError(f"Unauthorized fuel budget injection blocked for tenant: {tenant_id}")

        elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

        # 가용 연료/예산 한도가 0 이하로 완전 소진된 경우 네트워크 I/O 진입 전 사전 차단
        if enforce_quota and fuel_budget <= 0:
            log_pipeline.signal(
                f"[StreamBreaker] Pre-flight blocked: Budget exhausted ({fuel_budget}) for tenant '{tenant_id}'.",
                **{
                    UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value,
                    UIKey.LATENCY_MS.value: elapsed_ms,
                    UIKey.TENANT_ID.value: tenant_id,
                },
            )
            raise PermissionError(f"Fuel budget exhausted for tenant: {tenant_id}")

        pass_payload: Dict[str, Any] = {
            "mode": "RAW",
            UIKey.SLOT.value: UISlot.FUEL.value,
            UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
            UIKey.LATENCY_MS.value: elapsed_ms,
            UIKey.TENANT_ID.value: tenant_id,
        }
        if math.isfinite(fuel_budget):
            pass_payload[UIKey.FUEL_BUDGET.value] = int(fuel_budget)

        log_pipeline.info(
            f"[StreamBreaker] Pre-flight passed for '{tenant_id}' ({elapsed_ms:.3f}ms).",
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

        # 단일 응답(Non-stream) 처리 및 토큰 카운트 방출
        if not is_stream and not hasattr(msg, "__aiter__"):
            total_tokens, _ = extract_usage_info(msg)
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
                log_pipeline.info(
                    f"[StreamBreaker] Unary completion '{tenant_id}': tokens={total_tokens}",
                    **unary_payload,
                )

            if audit_hash and hasattr(msg, "system_fingerprint"):
                try:
                    msg.system_fingerprint = audit_hash
                except (AttributeError, TypeError, ValueError):
                    pass

        # 스트림(Stream) 킬 스위치 및 실시간 토큰 텔레메트리 트랩 장착
        elif hasattr(msg, "__aiter__"):
            msg = self._trap_generator(msg, fuel_budget, tenant_id, ctx)

        await ctx.fire_channel_read(msg)

    async def _trap_generator(self, raw_stream: Any, budget: float, tenant_id: str, context: ChannelContext):
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

                # 스트리밍 도중 Pub/Sub으로 갱신된 동적 한도(예: Kill-Switch 0.0)를 매 청크마다 즉시 반영
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
                            UIKey.CHUNKS.value: consumed,
                            "active_streams": max(0, _ACTIVE_STREAMS - 1),
                            "throughput_cps": 0.0,
                        },
                    )
                    break

                # 첫 청크이거나 20청크 단위이면서 최소 0.25초 이상 경과 시 스트림 텔레메트리(FUEL_TICK) 방출
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
                    # log_pipeline.info(
                    #     f"[FuelTrap] Stream metering '{tenant_id}': consumed={consumed} tokens ({cps} chunks/s)",
                    #     **tick_payload,
                    # )

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
                log_pipeline.info(
                    f"[FuelTrap] Stream completed for '{tenant_id}' (total={consumed} tokens, chunks={chunks}, active={_ACTIVE_STREAMS}).",
                    **close_payload,
                )