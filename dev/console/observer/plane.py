# fiber.dev.console.observer.plane
from __future__ import annotations

import asyncio
from collections import deque
from contextvars import ContextVar
import json
import time
from typing import Any, Dict, Union
from nicegui import ui

from fiber.dev.console.observer.signal import (
    RoutedSurfaceMeta,
    SurfaceFilter,
    UIKey,
    UIMetric,
    UISlot,
    UISlotStatus,
)
from fiber.infra.ops.account.state import state, refs
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


def _apply_meta(meta: RoutedSurfaceMeta, payload: Dict[str, Any], raw_msg: str = "") -> None:
    """`signal.SurfaceClassifier`가 분류한 `RoutedSurfaceMeta`를 소비하여 State 및 UI 반영"""
    f, s, p, d = state.finops, state.security, state.pipeline, meta.ui_payload

    # 0. PlaneRegulator 압력(Density) 및 가속도(Acceleration) 실시간 반영
    if (density := payload.get("density")) is not None:
        s.plane_density = float(density)
    if (accel := d.get("acceleration")) is not None:
        s.plane_acceleration = float(accel)
    s.plane_bursting = bool(meta.is_bursting)

    # 1. Pipeline X-Ray 4-Slot 상태 점등 및 서브밀리초 레이턴시 갱신
    if meta.ui_slot is not None:
        if card := refs.slot_cards.get(meta.ui_slot.value):
            cls_str = SLOT_STYLES.get(meta.ui_slot_status, "bg-slate-800 border border-slate-700")
            card.classes(replace=f"{cls_str} p-2 w-36 text-center")
        if (lat := d.get(UIKey.LATENCY_MS.value)) is not None and (idx := SLOT_INDEX.get(meta.ui_slot)) is not None:
            s.xray_latencies[idx] = float(lat)
            s.xray_chart_dirty = True

    # 2. EdgeCache & VCR 토큰/비용 절감액 누적
    if (tok := d.get(UIKey.SAVED_TOKENS.value)) is not None:
        f.saved_tokens += int(tok)
        f.saved_usd += float(d.get(UIKey.SAVED_USD.value, 0.0))
    elif meta.ui_slot is UISlot.CACHE and meta.ui_slot_status is UISlotStatus.HIT:
        f.saved_tokens += 250
        f.saved_usd += 0.004

    # 3. FinOps 원장 동기화 & LLM Live 실시간 스트림 펄스(FUEL_TICK) 처리
    #    - 주의: 실시간 이벤트는 현재 남은 연료(f.ledger_balance)만 갱신하며,
    #      본래 할당 예산 기준선(f.fuel_quota)은 건드리지 않는다. (단, 잔액이 기준선보다 높게 들어온 경우만 상향)
    if meta.ui_metric in (UIMetric.LEDGER_SYNC, UIMetric.FUEL_TICK):
        if UIKey.LEDGER_BALANCE.value in d and d[UIKey.LEDGER_BALANCE.value] is not None:
            f.set_balance(int(float(d[UIKey.LEDGER_BALANCE.value])))
        elif UIKey.ACTIVE_QUOTA.value in d and d[UIKey.ACTIVE_QUOTA.value] is not None:
            f.set_balance(int(float(d[UIKey.ACTIVE_QUOTA.value])))

        if f.ledger_balance > getattr(f, "fuel_quota", 0):
            f.fuel_quota = f.ledger_balance

        if UIKey.POLICY_VER.value in d and d[UIKey.POLICY_VER.value] is not None:
            f.policy_ver = max(f.policy_ver, int(d[UIKey.POLICY_VER.value]))

        if meta.ui_metric is UIMetric.FUEL_TICK:
            active_cnt = int(d.get("active_streams", getattr(s, "active_streams", 0)))
            s.active_streams = active_cnt

            if (cps := d.get("throughput_cps")) is not None:
                s.throughput_cps = float(cps)
            elif active_cnt == 0:
                s.throughput_cps = 0.0

            if (chunks := d.get(UIKey.CHUNKS.value)) is not None:
                chunk_val = int(chunks)
                s.live_stream_tokens = chunk_val
                # 이전 청크 대비 증분(Delta) 계산하여 누적 처리 토큰에 합산
                prev_chunks = getattr(s, "_last_stream_chunks", 0)
                delta = chunk_val if chunk_val < prev_chunks else (chunk_val - prev_chunks)
                s.total_processed_tokens = getattr(s, "total_processed_tokens", 0) + max(0, delta)
                s._last_stream_chunks = 0 if active_cnt == 0 else chunk_val

            if active_cnt > 0:
                f.stream_active = True
                f.stream_status_text = f"🟢 LIVE STREAMING ({active_cnt} active | {s.throughput_cps:,.1f} c/s)"
            else:
                f.stream_active = False
                f.stream_status_text = "✓ STREAM COMPLETE"

            s.xray_chart_dirty = True

        # y_fuel에는 f.ledger_balance, y_core에는 f.fuel_quota가 정확히 기록되도록 push_chart_point() 호출
        f.push_chart_point()

    # 4. VCR 벤치마크 차트 갱신
    elif meta.ui_metric is UIMetric.VCR_REPLAY and UIKey.ELAPSED_MS.value in d:
        p.vcr_bench_replay_ms = float(d[UIKey.ELAPSED_MS.value])
        p.vcr_chart_dirty = True
    elif meta.ui_metric is UIMetric.VCR_LIVE and UIKey.ELAPSED_MS.value in d:
        p.vcr_bench_live_ms = float(d[UIKey.ELAPSED_MS.value])
        p.vcr_chart_dirty = True

    # 5. 실시간 Kill-Switch (HTTP 402 절단) 인터럽트 처리
    if meta.is_kill_signal:
        # [역할 분리]
        # - 워커(channel.fuel)가 보내는 단순 스트림 차단 에러 로그는 policy_ver가 없으므로 원장 잔액(ledger_balance)을 건드리지 않는다.
        # - 오직 FinOpsWatcher가 발행한 공식 REVOKE 시그널이면서 버전(policy_ver)이 최신일 때만 잔액을 0으로 반영한다.
        ev_ver = d.get(UIKey.POLICY_VER.value)
        if ev_ver is not None and int(ev_ver) >= f.policy_ver:
            f.set_balance(0)
            f.policy_ver = int(ev_ver)
            f.push_chart_point()

        # 이미 10초 자동 복구(AUTO_RESTORE)나 수동 충전으로 잔액이 양수(> 0)로 복원된 상태라면,
        # 과거(0원 시절)에 출발해 뒤늦게 도착한 워커의 차단 에러 로그는 UI 상태를 덮어쓰지 않고 무시한다.
        if f.ledger_balance > 0:
            return

        s.active_streams = int(d.get("active_streams", 0))
        s.throughput_cps = 0.0
        s._last_stream_chunks = 0
        f.stream_active = False
        f.stream_status_text = "🛑 SEVERED MID-FLIGHT (HTTP 402)"
        if refs.live_stream_log:
            refs.live_stream_log.push("\n🛑 [FUEL BREAKER] Stream physically severed via aclose()!")
            refs.live_stream_log.push("↩ Agent Response: 'HTTP 402: Remaining fuel exhausted. Halted safely.'")
        ui.notify("🚨 Kill-Switch Activated! Active stream severed.", type="negative", position="top")

    # 6. HTTP 202 HITL Cryptographic Gate 모달 자동 팝업
    if meta.is_hitl_yield and not raw_msg.startswith(("⏸", "📤", "🧬", "✓", "❌", "🛑")):
        raw_args = d.get(UIKey.TOOL_ARGS.value)
        args_dict = dict(raw_args) if isinstance(raw_args, dict) else {"raw_args": str(raw_args or {})}
        if idem := d.get(UIKey.IDEM_KEY.value):
            args_dict = {UIKey.IDEM_KEY.value: idem, **args_dict}
        tool_fqn = f"{d.get(UIKey.TARGET_ID.value, 'mcp')}.{d.get(UIKey.TOOL_NAME.value, 'tool')}"
        if refs.hitl_code:
            refs.hitl_code.set_content(
                json.dumps({"tool": tool_fqn, "args": args_dict, "state": "HTTP 202 Parked"}, indent=2)
            )
        if refs.hitl_dialog:
            refs.hitl_dialog.open()
        refs.push_pubsub("mcp:hitl", f"⏸ [HTTP 202] Sandbox suspended for '{tool_fqn}'")


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
        is_raw_tick = meta.ui_payload.get("mode") == "RAW"

        # UI 메트릭, 킬 시그널, RAW 스트림 틱이 아닌 일반 로그에 대해서만 2초 윈도우 중복 제거 수행
        if meta.ui_metric is None and not meta.is_kill_signal and not is_raw_tick:
            ev_id = payload.get("id") or payload.get("event_id")
            fp = f"id:{ev_id}" if ev_id else f"t:{int(time.time() // 2)}:{payload.get('source_id')}:{raw_msg}"
            if fp in _seen_fp:
                return False
            _seen_fp.append(fp)

        refs.push_pubsub(meta.channel, _format_line(payload, meta))
        _apply_meta(meta, payload=payload, raw_msg=raw_msg)
        return True
    except Exception as exc:
        if hasattr(diag, "exception"):
            diag.exception("plane.ingest error", exc=exc, silent=True)
        elif hasattr(diag, "log_ui_exception"):
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
            "mode": "RAW",
            UIKey.METRIC.value: UIMetric.LEDGER_SYNC.value,
            UIKey.ACTIVE_QUOTA.value: fuel,
            UIKey.LEDGER_BALANCE.value: fuel,
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


_studio_observer: StudioPlaneObserver | None = None


def mount_studio_plane() -> None:
    """콘솔(Studio UI) 시작 시에만 명시적으로 호출하여 default_plane에 옵저버를 연결합니다."""
    global _studio_observer
    if _studio_observer is None:
        _studio_observer = StudioPlaneObserver()
        default_plane.attach(_studio_observer)


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
    mount_studio_plane()
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
            await pubsub.psubscribe("log:*", "finops:*", "mcp:*", "sign:*", "vcr:*", "node:*")
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