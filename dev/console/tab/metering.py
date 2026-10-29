# fiber.dev.console.tab.metering
from __future__ import annotations

from pathlib import Path
from nicegui import ui

from fiber.infra.ops.finops.console import finops_manager
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import StudioTab
from fiber.infra.ops.account.state import state, refs, USD_TO_FUEL_RATE
from xphi.node.space.bind.resolver import resolve_path

FINOPS_ACTION_LABELS = {
    "SYNC_QUOTA": "Set to Budget",
    "TOP_UP": "+Balance",
    "REVOKE": "0 Balance",
}


def _resolve_accounts_registry_path() -> Path:
    try:
        from fiber.infra.ops.account.registry import LocalAccountRegistry
        return Path(LocalAccountRegistry.get_registry_path())
    except Exception:
        return resolve_path("anchor") / "sign" / "local_accounts.json"


def _sync_finops_echart() -> None:
    """f.chart_dirty 상태이거나 데이터가 갱신되었을 때 EChart 위젯을 즉시 리렌더링"""
    f = state.finops
    chart = refs.finops_chart
    if chart is None or not getattr(f, "chart_dirty", False):
        return
    try:
        opts = chart.options
        if isinstance(opts.get("xAxis"), dict):
            opts["xAxis"]["data"] = list(f.x_ticks)
        series = opts.get("series")
        if isinstance(series, list):
            if len(series) > 0 and isinstance(series[0], dict):
                series[0]["data"] = list(f.y_fuel)
            if len(series) > 1 and isinstance(series[1], dict):
                series[1]["data"] = list(f.y_core)
        chart.update()
        f.chart_dirty = False
    except Exception:
        pass


class MeteringTab(StudioTab):
    title = "Metering"
    left_header = "Quota & Token Governance"
    right_header = "LLM Telemetry & Circuit Breaker"

    @staticmethod
    def _ensure_telemetry_attrs(s: object) -> None:
        defaults = {
            "active_streams": 0,
            "throughput_cps": 0.0,
            "live_stream_tokens": 0,
            "total_processed_tokens": 0,
            "plane_density": 0.0,
            "plane_acceleration": 0.0,
            "plane_bursting": False,
        }
        for k, v in defaults.items():
            if not hasattr(s, k):
                setattr(s, k, v)

    def compose_left(self) -> None:
        f = state.finops
        s = state.security
        self._ensure_telemetry_attrs(s)

        if f.jwt_op not in FINOPS_ACTION_LABELS:
            f.jwt_op = "SYNC_QUOTA"

        # 1. 상단: 테넌트 식별자 및 로컬 계정 레지스트리 파일 링크
        with kit.bar():
            ui.label("Target Account Identity").classes("text-[11px] font-bold text-slate-300 shrink-0")
            kit.file_link(
                _resolve_accounts_registry_path,
                label="local_accounts.json",
                tooltip="Open LocalAccountRegistry JSON file",
                create_if_missing=True,
            )

        with kit.row():
            kit.text("Target Tenant ID", f, "tenant_id")

        # 2. 중단: 예산 할당 및 EdDSA Mandate 제어
        with kit.row(align="items-center justify-between"):
            ui.label("Fuel Balance Control ($1 = 1,000,000 Fuel)").classes(
                "text-[11px] font-bold text-emerald-400 shrink-0"
            )
            ui.label().bind_text_from(
                f,
                "jwt_usd",
                lambda u: (
                    "-> Target Balance: 0 Fuel (REVOKE)"
                    if f.jwt_op == "REVOKE"
                    else (
                        f"-> Add +{int((u or 0) * USD_TO_FUEL_RATE):,} Fuel (New: {f.ledger_balance + int((u or 0) * USD_TO_FUEL_RATE):,})"
                        if f.jwt_op == "TOP_UP"
                        else f"-> Match Balance to {int((u or 0) * USD_TO_FUEL_RATE):,} Fuel"
                    )
                ),
            ).classes("text-[11px] font-mono font-bold text-amber-300")

        with kit.row():
            kit.select(FINOPS_ACTION_LABELS, "Balance Action", f, "jwt_op", width="w-64")
            kit.number("Budget ($)", f, "jwt_usd", min_val=1.0, max_val=500.0, step=5.0, width="w-24")
            kit.button(
                "Apply",
                finops_manager.issue_account_jwt,
                color="emerald",
                flex=True,
            ).tooltip("Update AnchorStore Fuel Balance via signed EdDSA mandate")

        with kit.row(gap="gap-1.5"):
            kit.button(
                "Stream Break (10s)",
                finops_manager.temporary_stream_kill,
                color="amber",
                text_color="black",
                flex=True,
            ).tooltip("Temporarily break active streams for 10s and auto-restore balance")
            kit.button(
                "Stream Kill (Set Zero)",
                finops_manager.revoke_account_kill,
                color="red",
                outline=True,
                flex=True,
            ).tooltip("Immediately burn all remaining fuel to 0 and sever active streams")

        ui.label().bind_text_from(
            f, "last_jwt_preview", lambda j: f"Signed Mandate Token (EdDSA): {j}"
        ).classes(kit.CLS_MONO_LABEL)

        # 3. 하단: 잔여 Fuel 게이지 & LLM 호출 누적 텔레메트리 요약 카드
        with ui.column().classes("w-full gap-1.5 bg-slate-950/90 px-3 py-2 rounded border border-slate-800 mt-1 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label("Remaining Balance vs Quota Baseline:").classes(
                    "text-[11px] font-bold text-amber-400"
                )
                ui.label().bind_text_from(
                    f,
                    "ledger_balance",
                    lambda bal: (
                        f"{int(bal):,} / {int(f.fuel_budget):,} Fuel "
                        f"({round((bal / f.fuel_budget) * 100, 1) if f.fuel_budget > 0 else 0.0}%)"
                    ),
                ).classes("text-xs font-mono font-bold text-cyan-300")

            ui.linear_progress(value=1.0, show_value=False, size="8px", color="emerald").bind_value_from(
                f,
                "ledger_balance",
                lambda bal: min(1.0, max(0.0, round(float(bal) / max(float(f.fuel_budget), 1.0), 3))),
            ).classes("w-full rounded bg-slate-900")

            # LLM 호출 관련 핵심 Metering KPI (Tokens, Saved Tokens/USD, Policy Ver)
            with ui.row().classes("w-full items-center justify-between pt-1 border-t border-slate-800/80 text-[11px] font-mono"):
                ui.label().bind_text_from(
                    s,
                    "total_processed_tokens",
                    lambda tot: f"🪙 Processed: {int(getattr(s, 'live_stream_tokens', 0)):,} live / {int(tot or 0):,} tok",
                ).classes("text-amber-300 font-bold")

                ui.label().bind_text_from(
                    f,
                    "saved_tokens",
                    lambda st: f"♻️ Saved: {int(st or 0):,} tok (${float(getattr(f, 'saved_usd', 0.0)):.2f})",
                ).classes("text-emerald-300 font-bold")

    def compose_right(self) -> None:
        f = state.finops
        s = state.security
        self._ensure_telemetry_attrs(s)

        # 1. 상단 텔레메트리 바: Active Streams, Plane 이벤트 밀도, Circuit Breaker 상태
        with kit.bar():
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.badge().bind_text_from(
                    s,
                    "active_streams",
                    lambda n: f"🟢 ACTIVE STREAMS: {int(n or 0)}" if (n or 0) > 0 else "⚪ IDLE (0 STREAMS)",
                ).props("color=slate-8 text-color=emerald-3").classes("text-[11px] font-mono font-bold px-2 py-0.5")

                ui.badge().bind_text_from(
                    s,
                    "plane_density",
                    lambda d: (
                        f"🔥 BURST ({float(d or 0.0):.1f} ev/s)"
                        if getattr(s, "plane_bursting", False)
                        else f"📡 Plane: {float(d or 0.0):.1f} ev/s"
                    ),
                ).props("color=slate-9 text-color=cyan-3").classes("text-[10px] font-mono border border-slate-700 px-1.5")

            ui.badge().bind_text_from(f, "stream_status_text").props("color=slate-8 text-color=amber-3").classes(
                "text-[11px] font-mono font-bold"
            )

        # 2. 실시간 LLM Stream Velocity (chunks/s) 바
        with ui.column().classes("w-full gap-0.5 bg-slate-950/90 px-3 py-1.5 rounded border border-slate-800 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label("⚡ LLM Stream Velocity & Throughput").classes("text-[11px] font-mono text-slate-400")
                ui.label().bind_text_from(
                    s, "throughput_cps", lambda v: f"{float(v or 0.0):,.1f} chunks/s"
                ).classes("text-xs font-mono font-extrabold text-cyan-400")

            ui.linear_progress(value=0.0, show_value=False, size="6px", color="cyan").bind_value_from(
                s, "throughput_cps", lambda v: min(1.0, round(float(v or 0.0) / 2200.0, 3))
            ).classes("w-full rounded bg-slate-900")

        # 3. 시계열 라인 차트 (Fuel Balance vs Quota Baseline) + 자동 리렌더링 타이머
        refs.finops_chart = kit.line_chart(
            list(f.x_ticks),
            list(f.y_fuel),
            list(f.y_core),
            redline=f.fuel_budget,
        )
        ui.timer(0.5, _sync_finops_echart)

        # 4. 실시간 스트림 / 서킷 브레이커 이벤트 로그
        refs.live_stream_log = kit.log(max_lines=60, color="text-emerald-300")