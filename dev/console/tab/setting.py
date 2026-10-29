# fiber.dev.console.tab.setting
from __future__ import annotations

from nicegui import ui

from fiber.infra.ops.account.state import state
from fiber.dev.console.bridge.sign import oob
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import StudioTab


class ModelNSettingTab(StudioTab):
    title = "Model & Setting"
    left_header = "Model Pricing"
    right_header = "Settings"
    ratio = 56

    def compose_left(self) -> None:
        from fiber.infra.ops.finops.exchange import default_exchange
        from fiber.infra.ops.finops.model.filter import CORE_KEYWORDS
        from fiber.infra.ops.finops.cost.registry import (
            ModelCostRegistry,
            REGISTRY_ROOT,
            DEFAULT_REGISTRY_FILENAME,
        )

        with ui.expansion("Fuel Unit", icon="info").props(
            "dense dark header-class='text-xs font-mono text-cyan-400 bg-slate-900/80 rounded px-2'"
        ).classes("w-full border border-slate-800 rounded bg-slate-950/80 mt-1"):
            with ui.column().classes("w-full p-2 gap-1 text-[11px] font-mono"):
                ui.label("• $1.00 USD = 1,000,000 Fuel  |  $0.000001 (1 μ$) = 1.0 Fuel").classes("text-emerald-300")
                ui.label("• Rate Par     : Price ($ / 1M Tokens) == Burn Rate (Fuel / Token)").classes("text-amber-300")
                ui.label("• Priced Models: Consumed Fuel = ceil(Billed_USD × 1,000,000)").classes("text-slate-300")
                ui.label("• Local Models : Consumed Fuel = ceil(Total_Tokens × 1.0) [when $0.0]").classes("text-cyan-300")

        total_mapped = len(getattr(ModelCostRegistry, "_model_cost", {}) or {})
        usd_rate = getattr(default_exchange, "usd_rate", 1_000_000)

        with kit.bar():
            with ui.row().classes("items-center gap-1.5 no-wrap"):
                ui.badge(f"Models: {total_mapped:,}").props("color=slate-8 text-color=cyan-3").classes(
                    "text-[10px] font-mono font-bold px-1.5 py-0.5"
                )
                ui.badge(f"providers: {len(CORE_KEYWORDS)}").props("color=slate-8 text-color=emerald-3").classes(
                    "text-[10px] font-mono font-bold px-1.5 py-0.5"
                )
                ui.badge(f"$1 = {usd_rate:,} Fuel (1:1 Local Par)").props(
                    "color=slate-9 text-color=amber-3"
                ).classes("text-[10px] font-mono border border-amber-500/40 px-1.5 py-0.5")

            kit.file_link(
                REGISTRY_ROOT / DEFAULT_REGISTRY_FILENAME,
                label=DEFAULT_REGISTRY_FILENAME,
                tooltip="Open local model cost registry JSON",
                create_if_missing=True,
            )

        columns = [
            {"name": "model", "label": "Model", "field": "model", "align": "left", "sortable": True},
            {"name": "provider", "label": "Provider", "field": "provider", "align": "left", "sortable": True},
            {"name": "in_usd", "label": "In ($/1M)", "field": "input_usd_per_1m", "align": "right", "sortable": True},
            {"name": "out_usd", "label": "Out ($/1M)", "field": "output_usd_per_1m", "align": "right", "sortable": True},
            {"name": "in_tok", "label": "In (Fuel/tok)", "field": "input_fuel_per_tok", "align": "right", "sortable": True},
            {"name": "out_tok", "label": "Out (Fuel/tok)", "field": "output_fuel_per_tok", "align": "right", "sortable": True},
            {"name": "mode", "label": "Mode", "field": "billing_mode", "align": "left"},
        ]
        rows = default_exchange.build_catalog_rows(limit=20)
        ui.table(columns=columns, rows=rows, row_key="model").props(
            "dense flat bordered dark virtual-scroll rows-per-page=0"
        ).classes("w-full flex-grow text-xs font-mono bg-slate-950")

    def compose_right(self) -> None:
        s = state.security
        with kit.bar():
            ui.label().bind_text_from(s, "gateway_health_badge_text").classes(
                "text-xs font-mono font-bold text-emerald-400"
            )
            ui.label().bind_text_from(s, "slack_oob_badge_text").classes(
                "text-[11px] font-mono font-bold text-cyan-300"
            )

        ui.label("Slack Setting").classes("text-xs font-bold text-amber-400 mt-0.5 shrink-0")
        with kit.row():
            kit.text("Slack Channel", s, "slack_channel", width="w-32")
            # kit.text("Slack UID ('*')", s, "slack_user_map", width="w-28")
            kit.text("Mapped SPIFFE ID", s, "admin_user")

        with kit.bar():
            ui.label("TOTP").classes("text-xs font-mono text-slate-300")
            kit.switch("Require TOTP", s, "oob_require_totp", color="cyan")

        ui.label("Test HITL").classes("text-xs font-bold text-slate-400 mt-0.5 shrink-0")
        with kit.row():
            kit.button(
                "Test 'drop_table' - Popup",
                lambda: oob.open_hitl_modal(
                    "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="amber",
                text_color="black",
                flex=True,
            )
            kit.button(
                "Test 'drop_table' - Slack",
                lambda: oob.dispatch_slack_oob(
                    "postgres-prod-db", "drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="indigo",
                outline=True,
                flex=True,
            )

        with kit.footer():
            kit.button("Key Init", oob.init_node_keys, color="slate-4", outline=True)
            kit.button("Enroll TOTP(QR)", oob.enroll_admin_totp, color="cyan", flex=True)
            kit.button("Verify", oob.verify_admin_totp, color="emerald", outline=True, flex=True)
            kit.button("Revoke", oob.revoke_admin_totp, color="red", outline=True)