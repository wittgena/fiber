# fiber.dev.console.tab.core
from __future__ import annotations

from pathlib import Path
from nicegui import ui

from fiber.infra.ops.account.state import state, refs
from fiber.gateway.bridge.cli import run_fiber_cli
from fiber.infra.ops.finops.console import finops_manager
from fiber.dev.console.action import pipeline
from fiber.dev.console.sign import oob

import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate, StudioTab
from fiber.dev.console.ui.guide import GuideModal
from fiber.dev.console.tab.mcp import HitlModal, QrModal, McpGatewayTab
from fiber.dev.console.tab.vcr import VcrTab
from fiber.dev.console.tab.finops import FinOpsTab
from xphi.node.space.bind.resolver import resolve_path


def _resolve_accounts_registry_path() -> Path:
    try:
        from fiber.infra.ops.account.registry import LocalAccountRegistry
        return Path(LocalAccountRegistry.get_registry_path())
    except Exception:
        return resolve_path("anchor") / "sign" / "local_accounts.json"

class LLMGuardrailTab(StudioTab):
    title = "Guardrail"
    left_header = "Guardrail"
    right_header = "Pipeline"

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
        s = state.security
        self._ensure_telemetry_attrs(s)

        ui.label("Secret Guard").classes("text-[11px] font-bold text-slate-400 shrink-0")
        kit.switch("Active Blocking", s, "block_api_keys", color="red")
        kit.text("Guard Pattern (regex)", s, "custom_regex", flex=False, width="w-full")
        kit.select(
            ["Strict Block (PermissionError)", "Redact (Mask with ***)"],
            "Guardrail Mode",
            s,
            "guardrail_mode",
            width="w-full",
        )

        ui.label("Test Prompt").classes("text-[11px] font-bold text-cyan-400 mt-1 shrink-0")
        kit.text("Input Prompt", s, "test_prompt", flex=False, width="w-full")

        with kit.row(gap="gap-1.5"):
            kit.button("CALL LLM", lambda: pipeline.run_pipeline_xray(s.test_prompt), color="red", flex=True)

    def compose_right(self) -> None:
        s = state.security
        self._ensure_telemetry_attrs(s)
        with kit.bar():
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.badge().bind_text_from(
                    s,
                    "active_streams",
                    lambda n: f"🟢 ACTIVE STREAMS: {int(n or 0)}" if (n or 0) > 0 else "⚪ IDLE (0 STREAMS)",
                ).props("color=slate-8 text-color=emerald-3").classes("text-[11px] font-mono font-bold px-2 py-0.5")

                ui.label().bind_text_from(
                    s,
                    "total_processed_tokens",
                    lambda tot: f"🪙 Tokens: {int(getattr(s, 'live_stream_tokens', 0)):,} live / {int(tot or 0):,} total",
                ).classes("text-xs font-mono font-bold text-amber-300")

            ui.badge().bind_text_from(
                s,
                "plane_density",
                lambda d: (
                    f"🔥 BURST ({float(d or 0.0):.1f} ev/s)"
                    if getattr(s, "plane_bursting", False)
                    else f"📡 Plane: {float(d or 0.0):.1f} ev/s"
                ),
            ).props("color=slate-9 text-color=cyan-3").classes("text-[10px] font-mono border border-slate-700 px-1.5")

        with ui.column().classes("w-full gap-0.5 bg-slate-950/90 px-3 py-1.5 rounded border border-slate-800 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label("⚡ Stream Velocity").classes(
                    "text-[11px] font-mono text-slate-400"
                )
                ui.label().bind_text_from(
                    s, "throughput_cps", lambda v: f"{float(v or 0.0):,.1f} chunks/s"
                ).classes("text-xs font-mono font-extrabold text-cyan-400")

            ui.linear_progress(value=0.0, show_value=False, size="6px", color="cyan").bind_value_from(
                s, "throughput_cps", lambda v: min(1.0, round(float(v or 0.0) / 2200.0, 3))
            ).classes("w-full rounded bg-slate-900")

        with kit.bar():
            for key, title, slot in [
                ("cache", "EdgeCache", "PRE_TRANSLATE"),
                ("router", "ONNX Router", "PRE_TRANSLATE"),
                ("guardrail", "Secret Guard", "POST_TRANSLATE"),
                ("fuel", "FuelBreaker", "STREAM_IO"),
            ]:
                with ui.card().classes("bg-slate-800 border border-slate-700 p-2 w-36 text-center") as c:
                    refs.slot_cards[key] = c
                    ui.label(title).classes("text-xs font-bold text-white")
                    ui.label(slot).classes("text-[10px] font-mono text-slate-400")

        ui.label("Slot Overhead (ms)").classes(
            "text-xs text-slate-400 mt-0.5 shrink-0"
        )
        refs.xray_chart = kit.bar_chart(
            categories=["FuelBreaker", "SecretGuard", "ONNX Router", "EdgeCache"],
            data=list(s.xray_latencies),
            x_max=1.0,
        )

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

        total_mapped = len(getattr(ModelCostRegistry, "_model_cost", {}) or {})
        usd_rate = getattr(default_exchange, "usd_rate", 1_000_000)

        with kit.bar():
            with ui.row().classes("items-center gap-1.5 no-wrap"):
                ui.badge(f"Models: {total_mapped:,}").props("color=slate-8 text-color=cyan-3").classes(
                    "text-[10px] font-mono font-bold px-1.5 py-0.5"
                )
                ui.badge(f"roviders: {len(CORE_KEYWORDS)}").props("color=slate-8 text-color=emerald-3").classes(
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

        ui.label("Slack HITL Setting").classes("text-xs font-bold text-amber-400 mt-0.5 shrink-0")
        with kit.row():
            kit.text("Slack Channel", s, "slack_channel", width="w-32")
            kit.text("Slack UID ('*')", s, "slack_user_map", width="w-28")
            kit.text("Mapped SPIFFE ID", s, "admin_user")

        with kit.bar():
            ui.label("TOTP").classes("text-xs font-mono text-slate-300")
            kit.switch("Require TOTP", s, "oob_require_totp", color="cyan")

        ui.label("Test HITL").classes("text-xs font-bold text-slate-400 mt-0.5 shrink-0")
        with kit.row():
            kit.button(
                "Test 'drop_table' Message",
                lambda: oob.open_hitl_modal(
                    "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="amber",
                text_color="black",
                flex=True,
            )
            kit.button(
                "Dispatch Slack",
                lambda: oob.dispatch_slack_oob(
                    "postgres-prod-db", "drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="indigo",
                outline=True,
                flex=True,
            )

        with ui.expansion("Unit Parity (FuelExchange)", icon="info").props(
            "dense dark header-class='text-xs font-mono text-cyan-400 bg-slate-900/80 rounded px-2'"
        ).classes("w-full border border-slate-800 rounded bg-slate-950/80 mt-1"):
            with ui.column().classes("w-full p-2 gap-1 text-[11px] font-mono"):
                ui.label("• $1.00 USD = 1,000,000 Fuel  |  $0.000001 (1 μ$) = 1.0 Fuel").classes("text-emerald-300")
                ui.label("• Rate Par     : Price ($ / 1M Tokens) == Burn Rate (Fuel / Token)").classes("text-amber-300")
                ui.label("• Priced Models: Consumed Fuel = ceil(Billed_USD × 1,000,000)").classes("text-slate-300")
                ui.label("• Local Models : Consumed Fuel = ceil(Total_Tokens × 1.0) [when $0.0]").classes("text-cyan-300")

        with kit.footer():
            kit.button("KeyGen", oob.init_node_keys, color="slate-4", outline=True)
            kit.button("Enroll TOTP(QR)", oob.enroll_admin_totp, color="cyan", flex=True)
            kit.button("Verify", oob.verify_admin_totp, color="emerald", outline=True, flex=True)
            kit.button("Revoke", oob.revoke_admin_totp, color="red", outline=True)

def build_tabs() -> list[StudioTab]:
    return [McpGatewayTab(), LLMGuardrailTab(), FinOpsTab(), VcrTab(), ModelNSettingTab()]

def build_modals() -> list[ModalTemplate]:
    return [HitlModal(), QrModal(), GuideModal()]