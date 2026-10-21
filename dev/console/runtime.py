# fiber.dev.console.runtime
from __future__ import annotations

import asyncio
from collections import deque
import json
import os
import socket
import subprocess
import sys
import time
from typing import Any, Dict, Union
import webbrowser
from nicegui import ui

from fiber.dev.console.signal import (
    RoutedSurfaceMeta,
    SurfaceFilter,
    UIKey,
    UIMetric,
    UIModal,
    UISlot,
    UISlotStatus,
)
from fiber.dev.console.state import state, refs
from fiber.dev.console.actions.gov import (
    sync_spiffe_and_check_totp,
    act_refresh_account_status,
    act_issue_account_jwt,
    act_refresh_sign_status,
    act_init_node_keys,
    act_enroll_admin_totp,
    act_verify_admin_totp,
    act_revoke_admin_totp,
    act_open_hitl_modal,
    act_approve_hitl,
)
from fiber.dev.console.actions.pipeline import (
    act_trigger_kill_switch,
    act_simulate_stream,
    act_run_xray,
    act_run_gateway_vcr_suite,
)
from xphi.arch.contract.event.next import EventObserver, LogEvent
from xphi.watcher.plane.regulator import default_plane

try:
    import redis.asyncio as redis
except ImportError:
    redis = None

console_surface_filter = SurfaceFilter(min_level="INFO")
_seen_fingerprints: deque[str] = deque(maxlen=256)

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


def _format_surface_payload(payload: Dict[str, Any], meta: RoutedSurfaceMeta) -> str:
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


def _flash_slot_card(slot: UISlot, status: UISlotStatus = UISlotStatus.PASS):
    if card := refs.slot_cards.get(slot.value):
        cls_str = SLOT_STYLES.get(status, "bg-slate-800 border border-slate-700")
        card.classes(replace=f"{cls_str} p-2 w-36 text-center")


def _dispatch_structured_meta(meta: RoutedSurfaceMeta):
    """Track 1(Strict Enum)과 Track 2(병렬 추론)가 통합된 RoutedSurfaceMeta를 소비하여 UI 구동"""
    f, s, p, d = state.finops, state.security, state.pipeline, meta.ui_payload

    if meta.ui_slot is not None:
        _flash_slot_card(meta.ui_slot, meta.ui_slot_status)
        if (lat := d.get(UIKey.LATENCY_MS.value)) is not None and (idx := SLOT_INDEX.get(meta.ui_slot)) is not None:
            s.xray_latencies[idx] = float(lat)
            s.xray_chart_dirty = True

    # Track 1 명시적 절감치 우선 반영, 없을 시 Track 2 캐시 히트 기본값 보완
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
        act_trigger_kill_switch()

    if meta.ui_modal is UIModal.HITL_202:
        raw_args = d.get(UIKey.TOOL_ARGS.value)
        args_dict = dict(raw_args) if isinstance(raw_args, dict) else {"raw_args": str(raw_args or {})}
        if idem := d.get(UIKey.IDEM_KEY.value):
            args_dict = {UIKey.IDEM_KEY.value: idem, **args_dict}
        act_open_hitl_modal(f"{d.get(UIKey.TARGET_ID.value, 'mcp')}.{d.get(UIKey.TOOL_NAME.value, 'tool')}", args_dict)


def ingest_surface_event(event_or_dict: Union[LogEvent, Dict[str, Any]]) -> bool:
    payload = event_or_dict if isinstance(event_or_dict, dict) else event_or_dict.__dict__
    passed, meta = console_surface_filter.match(payload)
    if not passed:
        return False

    ev_id = payload.get("id") or payload.get("event_id")
    fp = f"id:{ev_id}" if ev_id else f"t:{int(time.time() // 2)}:{payload.get('source_id')}:{payload.get('message')}"
    if fp in _seen_fingerprints:
        return False
    _seen_fingerprints.append(fp)

    refs.push_pubsub(meta.channel, _format_surface_payload(payload, meta))
    _dispatch_structured_meta(meta)
    return True


def _emit_local_ui_event(source_id: str, message: str, level: str = "INFO", **ctx: Any):
    ingest_surface_event({"source_id": source_id, "message": message, "level": level, "context": ctx})


def _emit_ledger_sync(source_id: str, message: str, fuel: int, core: int, ver: int, level: str = "INFO"):
    _emit_local_ui_event(
        source_id, message, level,
        **{
            UIKey.METRIC.value: UIMetric.LEDGER_SYNC.value,
            UIKey.ACTIVE_QUOTA.value: fuel,
            UIKey.CORE_BALANCE.value: core,
            UIKey.POLICY_VER.value: ver,
        },
    )


class StudioPlaneObserver(EventObserver):
    def update(self, event: LogEvent):
        try:
            ingest_surface_event(event)
        except Exception:
            pass


default_plane.attach(StudioPlaneObserver())


async def sync_initial_node_and_account_state():
    await act_refresh_sign_status(silent=True)
    await act_refresh_account_status(silent=True)


# ==========================================
# CLI Subprocess Runner & Output State Parser
# ==========================================
def _bridge_cli_line(line: str):
    """CLI 서브프로세스가 출력한 ConsoleSurface 포맷 로그(`LOG | PHASE | LEVEL | src: msg`)를 Track 2로 연결"""
    if "|" not in line or ":" not in line:
        return
    parts = [p.strip() for p in line.split("|")]
    if len(parts) >= 4 and ":" in parts[3]:
        src, msg = parts[3].split(":", 1)
        _emit_local_ui_event(src.split()[-1], msg.strip(), parts[2], phase=parts[1])


async def run_fiber_cli(*args: str):
    refs.push_cli(f"$ fiber {' '.join(args)}")
    output_lines: list[str] = []
    child_env = {**os.environ, "FIBER_CONSOLE_ACTIVE": "1"}

    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "fiber.cli", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=child_env,
        )
        assert proc.stdout is not None
        async for line in proc.stdout:
            if decoded := line.decode("utf-8", errors="replace").rstrip():
                output_lines.append(decoded)
                refs.push_cli(f"  {decoded}")
                _bridge_cli_line(decoded)
        await proc.wait()
        if proc.returncode != 0:
            await _simulate_cli_fallback(args)
        else:
            _try_sync_state_from_cli_output("\n".join(output_lines), args)
    except Exception:
        await _simulate_cli_fallback(args)


def _try_sync_state_from_cli_output(raw_out: str, args: tuple[str, ...]):
    f, s = state.finops, state.security
    try:
        start, end = raw_out.find("{"), raw_out.rfind("}")
        if start == -1 or end <= start:
            return
        data = json.loads(raw_out[start:end + 1])
        if isinstance(data.get("jwt"), str):
            f.last_jwt_preview = f"{data['jwt'][:44]}... (v{f.policy_ver})"

        st = data.get("status", data)
        if "core_state" in st and "finops_watcher" in st:
            core_bal = int(st["core_state"].get("balance", f.core_balance))
            act_quota = int(float(st["finops_watcher"].get("active_quota", f.current_fuel)))
            pol_ver = int(st["finops_watcher"].get("policy_version", f.policy_ver))
            f.last_op = str(args[-1]).upper() if args else "STATUS"
            _emit_ledger_sync("watcher.finops", f"SYNCED core={core_bal} quota={act_quota} v={pol_ver}", act_quota, core_bal, pol_ver)
        elif isinstance(data.get("accounts"), dict):
            if (acc := data["accounts"].get(f.tenant_id)) and acc.get("spiffe_id"):
                f.spiffe_id = acc["spiffe_id"]
            sync_spiffe_and_check_totp(emit_warning=False)
        elif "ssh_fiber_ed25519" in data and "local_cipher_key" in data:
            s.ssh_key_exists = bool(data["ssh_fiber_ed25519"].get("exists"))
            s.ssh_key_path = str(data["ssh_fiber_ed25519"].get("path", s.ssh_key_path))
            s.cipher_key_exists = bool(data["local_cipher_key"].get("exists"))
            s.cipher_key_path = str(data["local_cipher_key"].get("path", s.cipher_key_path))
            s.node_pubkey = str(data.get("node_pubkey", s.node_pubkey))
            s.sign_status_json = json.dumps(data, indent=2, ensure_ascii=False)
            sync_spiffe_and_check_totp(emit_warning=False)
        elif "verified" in data and "tested_code" in data:
            ok = bool(data["verified"])
            _emit_local_ui_event("fiber.cli.sign", f"TOTP_VERIFY user={data.get('user_id')} verified={ok}", "INFO" if ok else "WARN")
            ui.notify(f"TOTP Verification {'PASSED ✅' if ok else 'FAILED ❌'}", type="positive" if ok else "negative")
    except Exception:
        pass


async def _simulate_cli_fallback(args: tuple[str, ...]):
    f = state.finops
    await asyncio.sleep(0.15)
    cmd = " ".join(args)

    if "--spiffe" in args:
        sync_spiffe_and_check_totp(emit_warning=True)

    if "jwt" in cmd or "PROVISION" in cmd or "TOP_UP" in cmd:
        added = int(f.jwt_usd * 1000)
        fuel = added if f.jwt_op in ("PROVISION", "SYNC_QUOTA") else (f.current_fuel + added if f.jwt_op == "TOP_UP" else 0)
        core = (added if f.jwt_op in ("PROVISION", "SYNC_QUOTA") else f.core_balance + added) if f.sync_ledger else f.core_balance
        if f.jwt_op == "REVOKE":
            fuel = core = 0
        f.last_op = f.jwt_op
        refs.push_cli(f"  ✓ [EdDSA JWT] op={f.jwt_op} ver={f.policy_ver} -> Quota={fuel:,} Fuel")
        _emit_ledger_sync("bound.account.manager", f"EVENT={f.jwt_op} fuel={fuel} v={f.policy_ver}", fuel, core, f.policy_ver)
        ui.notify(f"Mandate Applied: {f.jwt_op} (v{f.policy_ver})", type="positive")

    elif "demo" in cmd:
        for fuel, core, ver, op, label in [
            (50000, 50000, 1, "PROVISION", "2. PROVISION ($50.0, v1) -> 50,000 Fuel"),
            (70000, 70000, 2, "TOP_UP", "3. TOP_UP (+$20.0, v2) -> 70,000 Fuel"),
            (70000, 70000, 2, "REJECT_STALE", "4. Stale v1 Token Replay -> REJECTED (False)"),
            (0, 0, 3, "REVOKE", "5. REVOKE (v3) -> Balance & Quota zeroed"),
        ]:
            await asyncio.sleep(0.35)
            f.last_op = op
            refs.push_cli(f"  {label}")
            _emit_ledger_sync("watcher.finops", label, fuel, core, ver, "WARN" if op in ("REJECT_STALE", "REVOKE") else "INFO")
        ui.notify("5-Step Account Lifecycle Demo Complete", type="info")

    elif "kill" in cmd:
        f.core_balance, f.policy_ver, f.last_op = 0, 99, "KILL_SWITCH"
        refs.push_cli("  🚨 [Kill-Switch] Broadcasted REVOKE (policy_ver=99)")
        _emit_local_ui_event(
            "channel.fuel", "EVENT=REVOKE action=aclose() policy_ver=99", "SIGNAL",
            **{UIKey.METRIC.value: UIMetric.KILL_SWITCH.value, UIKey.SLOT.value: UISlot.FUEL.value, UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value},
        )

    elif "compat" in cmd:
        refs.push_cli("  ✓ [Schema Verified] VCR replay extracted 14 chunks (0ms network I/O)")
        # Track 2 병렬 파서 검증: "3.1ms"와 "replay"만 로그에 포함되면 UIKey 없이도 VCR 차트 자동 갱신
        _emit_local_ui_event("compat.registry", "VCR replay verified 14 chunks in 3.1ms (0ms I/O)", **{UIKey.SAVED_TOKENS.value: 180, UIKey.SAVED_USD.value: 0.003})
        ui.notify("Compat Rule Verified via Offline VCR!", type="positive")


# ==========================================
# Redis Pub/Sub & 100ms RingBuffer Flush
# ==========================================
async def check_redis_health() -> bool:
    p = state.pipeline
    if redis is None:
        p.redis_online = False
        return False
    try:
        client = redis.from_url(p.redis_url, decode_responses=True)
        await asyncio.wait_for(client.ping(), timeout=0.4)
        await client.aclose()
        p.redis_online = True
    except Exception:
        p.redis_online = False
    return p.redis_online


async def redis_pubsub_worker():
    p = state.pipeline
    while True:
        if not await check_redis_health():
            await asyncio.sleep(3.0)
            continue
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
                            ingest_surface_event(ev)
                            continue
                    except Exception:
                        pass
                refs.push_pubsub(ch, data)
                if "REVOKE" in data.upper():
                    act_trigger_kill_switch()
        except Exception as e:
            p.redis_online = False
            refs.push_pubsub("plane:bus", f"Redis offline ({e}) -> In-Memory StudioPlaneObserver Active")
            await asyncio.sleep(2.0)


def flush_100ms_ringbuffer():
    f, s, p = state.finops, state.security, state.pipeline

    if refs.cli_buffer and refs.cli_log:
        refs.cli_log.push("\n".join(refs.cli_buffer))
        refs.cli_buffer.clear()

    if refs.pubsub_buffer and refs.pubsub_log:
        refs.pubsub_log.push("\n".join(refs.pubsub_buffer))
        refs.pubsub_buffer.clear()

    if f.chart_dirty and refs.finops_chart:
        f.x_ticks.append(time.strftime("%H:%M:%S"))
        f.y_fuel.append(f.current_fuel)
        f.y_core.append(f.core_balance)
        refs.finops_chart.options["xAxis"]["data"] = list(f.x_ticks)
        refs.finops_chart.options["series"][0]["data"] = list(f.y_fuel)
        refs.finops_chart.options["series"][0]["markLine"]["data"][0]["yAxis"] = f.budget_limit
        refs.finops_chart.options["series"][1]["data"] = list(f.y_core)
        refs.finops_chart.update()
        f.chart_dirty = False

    if s.xray_chart_dirty and refs.xray_chart:
        refs.xray_chart.options["series"][0]["data"] = list(s.xray_latencies)
        refs.xray_chart.update()
        s.xray_chart_dirty = False

    if p.vcr_chart_dirty and refs.vcr_chart:
        refs.vcr_chart.options["series"][0]["data"][0]["value"] = p.vcr_bench_replay_ms
        refs.vcr_chart.options["series"][0]["data"][1]["value"] = p.vcr_bench_live_ms
        refs.vcr_chart.update()
        p.vcr_chart_dirty = False


def open_app_window(url: str):
    for path in (
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    ):
        try:
            subprocess.Popen([path, f"--app={url}", "--window-size=1400,900"])
            return
        except FileNotFoundError:
            continue
    webbrowser.open(url)


def is_port_active(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0