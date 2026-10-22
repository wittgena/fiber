# fiber.dev.console.bridge.plane
from __future__ import annotations

import asyncio
from collections import deque
from contextvars import ContextVar
import json
import time
from typing import Any, Dict, Union

from fiber.dev.console.bridge.signal import (
    RoutedSurfaceMeta,
    SurfaceFilter,
    UIKey,
    UIMetric,
    UISlot,
    UISlotStatus,
)
from fiber.dev.console.state import state, refs
from fiber.dev.console.bridge.action import oob, pipeline
import fiber.dev.console.ui.diag as diag
from xphi.arch.contract.event.next import EventObserver, LogEvent
from xphi.watcher.plane.regulator import default_plane

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

surface_filter = SurfaceFilter(min_level="INFO")
_seen_fp: deque[str] = deque(maxlen=256)

# 동기적 재진입(Re-entrancy) 차단 락: ingest() 도중 발생한 파생 로그가 다시 ingest()를 부르는 것 방지
_IN_INGEST: ContextVar[bool] = ContextVar("_in_ingest", default=False)
_SELF_SOURCE_PREFIXES = ("console.ui", "console.diag", "nicegui", "uvicorn")

SLOT_INDEX: Dict[UISlot, int] = {
    UISlot.FUEL: 0,
    UISlot.GUARDRAIL: 1,
    UISlot.ROUTER: 2,
    UISlot.CACHE: 3,
}

SLOT_STYLES: Dict[UISlotStatus, str] = {
    UISlotStatus.HIT: "bg-emerald-950 border-2 border-emerald-400",
    UISlotStatus.PASS: "bg-sky-950 border-2 border-sky-400",
    UISlotStatus.BLOCK: "bg-red-950 border-2 border-red-500 animate-pulse",
    UISlotStatus.SEVER: "bg-red-950 border-2 border-red-500 animate-pulse",
}


def _is_self_diagnostic(payload: Dict[str, Any]) -> bool:
    """콘솔 자신이 발행한 UI 진단 로그이거나 재진입 호출인지 판별"""
    if _IN_INGEST.get():
        return True
    ctx = payload.get("context")
    if isinstance(ctx, dict):
        if ctx.get("ui_internal") is True or str(ctx.get("phase") or "").upper() == "UI_DIAG":
            return True
    src = str(payload.get("source_id") or "").strip().lower()
    return src.startswith(_SELF_SOURCE_PREFIXES)


def _format_line(payload: Dict[str, Any], meta: RoutedSurfaceMeta) -> str:
    p_mark = "🔥" if meta.is_bursting else ""
    gain = payload.get("gain")
    g_str = f"[G:{float(gain):.1f}]" if isinstance(gain, (int, float)) and float(gain) < 1.0 else ""
    acc = meta.ui_payload.get("acceleration")
    a_str = f"[Acc:{float(acc):.1f}]" if isinstance(acc, (int, float)) and float(acc) > 0.0 else ""
    fold = payload.get("fold_count")
    f_str = f" (x{int(fold)})" if isinstance(fold, (int, float)) and int(fold) > 1 else ""
    tags = f"{p_mark}{g_str}{a_str}"
    msg = str(payload.get("message") or "").strip()
    return f"[{meta.level}] {tags + ' ' if tags else ''}{msg}{f_str}"


def _schedule_coro_safely(coro: Any) -> None:
    """동기 함수(_apply_meta) 내부에서 비동기 액션을 안전하게 스케줄링"""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(coro)
    except RuntimeError:
        coro.close()


def _apply_meta(meta: RoutedSurfaceMeta, raw_msg: str = "") -> None:
    """`signal.SurfaceClassifier`가 분류한 `RoutedSurfaceMeta`를 소비하여 State 및 UI 반영"""
    f, s, p, d = state.finops, state.security, state.pipeline, meta.ui_payload

    if meta.ui_slot is not None:
        if card := refs.slot_cards.get(meta.ui_slot.value):
            cls_str = SLOT_STYLES.get(meta.ui_slot_status, "bg-slate-800 border border-slate-700")
            card.classes(replace=f"{cls_str} p-2 w-36 text-center")
        if (lat := d.get(UIKey.LATENCY_MS.value)) is not None and (idx := SLOT_INDEX.get(meta.ui_slot)) is not None:
            s.xray_latencies[idx] = float(lat)
            s.xray_chart_dirty = True

    if (tok := d.get(UIKey.SAVED_TOKENS.value)) is not None:
        f.saved_tokens += int(tok)
        f.saved_usd += float(d.get(UIKey.SAVED_USD.value, 0.0))
    elif meta.ui_slot is UISlot.CACHE and meta.ui_slot_status is UISlotStatus.HIT:
        f.saved_tokens += 250
        f.saved_usd += 0.004

    if meta.ui_metric in (UIMetric.LEDGER_SYNC, UIMetric.FUEL_TICK):
        f.current_fuel = int(float(d.get(UIKey.ACTIVE_QUOTA.value, f.current_fuel)))
        f.core_balance = int(float(d.get(UIKey.CORE_BALANCE.value, f.core_balance)))
        f.policy_ver = int(d.get(UIKey.POLICY_VER.value, f.policy_ver))
        if meta.ui_metric is UIMetric.FUEL_TICK:
            f.stream_status_text = "🟢 LIVE STREAMING"
        f.x_ticks.append(time.strftime("%H:%M:%S"))
        f.y_fuel.append(f.current_fuel)
        f.y_core.append(f.core_balance)
        f.chart_dirty = True
    elif meta.ui_metric is UIMetric.VCR_REPLAY and UIKey.ELAPSED_MS.value in d:
        p.vcr_bench_replay_ms = float(d[UIKey.ELAPSED_MS.value])
        p.vcr_chart_dirty = True
    elif meta.ui_metric is UIMetric.VCR_LIVE and UIKey.ELAPSED_MS.value in d:
        p.vcr_bench_live_ms = float(d[UIKey.ELAPSED_MS.value])
        p.vcr_chart_dirty = True

    if meta.is_kill_signal:
        f.current_fuel = 0
        f.chart_dirty = True
        pipeline.trigger_kill_switch()

    if meta.is_hitl_yield and not raw_msg.startswith(("⏸", "📤", "🧬", "✓", "❌", "🛑")):
        raw_args = d.get(UIKey.TOOL_ARGS.value)
        args_dict = dict(raw_args) if isinstance(raw_args, dict) else {"raw_args": str(raw_args or {})}
        if idem := d.get(UIKey.IDEM_KEY.value):
            args_dict = {UIKey.IDEM_KEY.value: idem, **args_dict}
        tool_fqn = f"{d.get(UIKey.TARGET_ID.value, 'mcp')}.{d.get(UIKey.TOOL_NAME.value, 'tool')}"
        _schedule_coro_safely(oob.act_open_hitl_modal(tool_fqn, args_dict))


# ==========================================
# Public Ingestion API (In-Memory / CLI / Redis)
# ==========================================
def ingest(event_or_dict: Union[LogEvent, Dict[str, Any]]) -> bool:
    """모든 플레인 이벤트가 통과하는 단일 진입점 (자기 진단 로그 루프 차단 내장)"""
    payload = event_or_dict if isinstance(event_or_dict, dict) else event_or_dict.__dict__
    if _is_self_diagnostic(payload):
        return False

    token = _IN_INGEST.set(True)
    try:
        passed, meta = surface_filter.match(payload)
        if not passed:
            return False

        raw_msg = str(payload.get("message") or "").strip()
        if meta.ui_metric is None and not meta.is_kill_signal:
            ev_id = payload.get("id") or payload.get("event_id")
            fp = f"id:{ev_id}" if ev_id else f"t:{int(time.time() // 2)}:{payload.get('source_id')}:{raw_msg}"
            if fp in _seen_fp:
                return False
            _seen_fp.append(fp)

        refs.push_pubsub(meta.channel, _format_line(payload, meta))
        _apply_meta(meta, raw_msg=raw_msg)
        return True
    except Exception as exc:
        diag.log_ui_exception("plane.ingest", exc)
        return False
    finally:
        _IN_INGEST.reset(token)


def emit(source_id: str, message: str, level: str = "INFO", **ctx: Any) -> None:
    ingest({"source_id": source_id, "message": message, "level": level, "context": ctx})


def emit_ledger(source_id: str, message: str, fuel: int, core: int, ver: int, level: str = "INFO") -> None:
    emit(
        source_id,
        message,
        level,
        **{
            UIKey.METRIC.value: UIMetric.LEDGER_SYNC.value,
            UIKey.ACTIVE_QUOTA.value: fuel,
            UIKey.CORE_BALANCE.value: core,
            UIKey.POLICY_VER.value: ver,
        },
    )


def bridge_cli_line(line: str) -> None:
    """CLI stdout의 `LOG | PHASE | LEVEL | src: msg` 포맷을 파싱하여 ingest로 연결"""
    if "|" not in line or ":" not in line:
        return
    parts = [p.strip() for p in line.split("|")]
    if len(parts) >= 4 and ":" in parts[3]:
        if parts[1].upper() == "UI_DIAG":
            return
        src, msg = parts[3].split(":", 1)
        emit(src.split()[-1], msg.strip(), parts[2], phase=parts[1])


class StudioPlaneObserver(EventObserver):
    def update(self, event: LogEvent) -> None:
        ingest(event)


default_plane.attach(StudioPlaneObserver())


async def check_redis_health() -> bool:
    p = state.pipeline
    if redis is None:
        p.redis_online = False
        return False
    client = None
    try:
        client = redis.from_url(p.redis_url, decode_responses=True)
        await asyncio.wait_for(client.ping(), timeout=0.4)
        p.redis_online = True
    except Exception:
        p.redis_online = False
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass
    return p.redis_online


async def redis_pubsub_worker() -> None:
    p = state.pipeline
    while True:
        if not await check_redis_health():
            await asyncio.sleep(3.0)
            continue
        client = None
        pubsub = None
        try:
            client = redis.from_url(p.redis_url, decode_responses=True)
            pubsub = client.pubsub()
            await pubsub.psubscribe("log:*", "finops:*", "mcp:*", "sign:*", "vcr:*", "kernel:*")
            refs.push_pubsub("plane:bus", "Subscribed to TunnelSurface ('log:*') & Control Channels")
            async for msg in pubsub.listen():
                if msg.get("type") != "pmessage":
                    continue
                ch, data = str(msg.get("channel") or ""), str(msg.get("data") or "")
                if ch.startswith("log:"):
                    try:
                        if isinstance(ev := json.loads(data), dict):
                            ingest(ev)
                            continue
                    except Exception:
                        pass
                ingest({"source_id": ch.replace(":", "."), "message": data, "level": "INFO"})
        except asyncio.CancelledError:
            break
        except Exception as e:
            p.redis_online = False
            refs.push_pubsub("plane:bus", f"Redis offline ({e}) -> In-Memory StudioPlaneObserver Active")
            await asyncio.sleep(2.0)
        finally:
            if pubsub is not None:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass