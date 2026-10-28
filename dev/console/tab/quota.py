# fiber.dev.console.tab.qouta
from __future__ import annotations

from pathlib import Path
from nicegui import ui

from fiber.gateway.bridge.cli import run_fiber_cli
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

class QuotaTab(StudioTab):
    title = "Quota"
    left_header = "Quota Control"
    right_header = "Circuit Breaker"

    def compose_left(self) -> None:
        f = state.finops
        if f.jwt_op not in FINOPS_ACTION_LABELS:
            f.jwt_op = "SYNC_QUOTA"

        # [Step 1] 대상 계정(Tenant) 식별
        ui.label("Target Account Identity").classes("text-[11px] font-bold text-slate-400 shrink-0")
        with kit.row():
            kit.text("Target Tenant ID", f, "tenant_id")

        # [Step 2] 직관적 3단계 잔액 제어 (예산과 일치 / 추가 충전 / 잔액 0으로 초기화)
        with kit.row(align="items-center justify-between"):
            ui.label("Fuel Balance Control ($1 = 1,000,000 Fuel)").classes(
                "text-[11px] font-bold text-emerald-400 shrink-0"
            )
            ui.label().bind_text_from(
                f,
                "jwt_usd",
                lambda u: (
                    "➔ Target Balance: 0 Fuel (REVOKE)"
                    if f.jwt_op == "REVOKE"
                    else (
                        f"➔ Add +{int((u or 0) * USD_TO_FUEL_RATE):,} Fuel (New: {f.ledger_balance + int((u or 0) * USD_TO_FUEL_RATE):,})"
                        if f.jwt_op == "TOP_UP"
                        else f"➔ Match Balance to {int((u or 0) * USD_TO_FUEL_RATE):,} Fuel"
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
            ).tooltip("Update Ledger Fuel Balance via signed EdDSA mandate")

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

        # [Step 3] 할당 예산 사본(fuel_budget) 대비 현재 남은 연료(ledger_balance) 비교 게이지
        with ui.column().classes("w-full gap-1 bg-slate-950/90 px-3 py-2 rounded border border-slate-800 mt-1 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label("Remaining Balance vs Allocated Quota Baseline:").classes(
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

    def compose_right(self) -> None:
        f = state.finops
        with kit.bar():
            ui.label().bind_text_from(f, "account_summary_badge").classes("text-xs font-mono text-cyan-300 font-bold")
            with ui.row().classes("items-center gap-2"):
                kit.file_link(
                    _resolve_accounts_registry_path,
                    label="local_accounts.json",
                    tooltip="Open LocalAccountRegistry JSON file in external editor",
                    create_if_missing=True,
                )
                ui.badge().bind_text_from(f, "stream_status_text").props("color=slate-8 text-color=amber-3")

        refs.finops_chart = kit.line_chart(
            list(f.x_ticks),
            list(f.y_fuel),
            list(f.y_core),
            redline=f.fuel_budget,
        )
        refs.live_stream_log = kit.log(max_lines=60, color="text-emerald-300")