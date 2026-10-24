# fiber.dev.console.ui.header
from __future__ import annotations

from nicegui import ui

from fiber.infra.account.state import (
    STATE_FILE_PATH,
    load_or_init_state,
    refs,
    save_state_to_disk,
    state,
)
from fiber.phase.bridge.cli import run_fiber_cli
import fiber.dev.console.ui.diag as diag
import fiber.dev.console.ui.kit as kit
from xphi.node.space.bind.resolver import resolve_path

DIAG_LOG_PATH = resolve_path("log") / "ui_diag.log"


def act_manual_save_state() -> None:
    """Persist current console state to disk immediately."""
    ok = save_state_to_disk(force=True)
    if ok:
        refs.push_cli(f"💾 Saved state to {STATE_FILE_PATH} (at {state.last_saved_at})")
        diag.info("state manually saved", path=str(STATE_FILE_PATH))
        ui.notify(f"💾 Saved to {STATE_FILE_PATH.name}", type="positive")
    else:
        diag.error("Failed to save state", path=str(STATE_FILE_PATH))
        ui.notify("Failed to save state.json", type="negative")


def act_reload_state_from_disk() -> None:
    """Reload state.json after external editor modifications."""
    if load_or_init_state(STATE_FILE_PATH):
        state.finops.chart_dirty = True
        refs.push_cli(f"🔄 Reloaded state from {STATE_FILE_PATH}")
        diag.info("State reloaded from disk", path=str(STATE_FILE_PATH))
        ui.notify(f"🔄 Reloaded {STATE_FILE_PATH.name} from disk", type="info")
    else:
        ui.notify("Failed to reload state file", type="warning")


def build_header() -> None:
    """Render the 2-deck cockpit header (Row 1: Identity & Emergency / Row 2: Live KPIs & State/Diag Clusters)."""
    diag.install_browser_hooks()
    with ui.header().classes("bg-slate-900 border-b border-slate-800 px-5 py-2"):
        # Explicit full-width column to override Quasar q-header's default row layout
        with ui.column().classes("w-full gap-1.5"):
            # [Deck 1: Brand Title, Node/Redis Badges & Emergency Stream Breaker]
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                with ui.row().classes("items-center gap-3"):
                    ui.label("⚡ Fiber Runtime Harness").classes(
                        "text-base font-extrabold tracking-wider text-white"
                    )
                    ui.badge().bind_text_from(state, "redis_badge_text").props(
                        "color=slate-8 text-color=emerald-3"
                    ).classes("text-xs px-2 py-0.5")
                    ui.badge().bind_text_from(state.security, "ssh_badge_text").props(
                        "color=slate-8 text-color=cyan-3"
                    ).classes("text-xs px-2 py-0.5")

                with ui.row().classes("items-center gap-2.5"):
                    kit.button(
                        "🚨 LLM STREAM BREAK",
                        lambda: run_fiber_cli("account", "--tenant", state.finops.tenant_id, "kill", "--ver", "99"),
                        color="red",
                    ).classes("font-bold px-3")

            # [Deck 2: Live KPI Bar & Grouped State / Diag File Controls]
            with ui.row().classes(
                "w-full items-center justify-between bg-slate-950 px-3 py-1 rounded border border-slate-800 no-wrap"
            ):
                # Left: Real-time FinOps Quota & Token Savings KPIs
                with ui.row().classes("items-center gap-4"):
                    ui.label().bind_text_from(state, "fuel_kpi_text").classes(
                        "text-xs font-mono font-bold text-emerald-400"
                    )
                    ui.label("|").classes("text-xs text-slate-700")
                    ui.label().bind_text_from(state, "saved_kpi_text").classes(
                        "text-xs font-mono font-bold text-cyan-400"
                    )

                # Right: [Cluster 1: State Persistence] | [Cluster 2: UI Diagnostics]
                with ui.row().classes("items-center gap-2"):
                    ui.label().bind_text_from(
                        state,
                        "last_saved_at",
                        lambda ts: f"State ({STATE_FILE_PATH.name} @ {ts}):",
                    ).classes("text-[11px] font-mono text-slate-400")
                    kit.file_link(
                        STATE_FILE_PATH,
                        label="",
                        tooltip=f"Edit {STATE_FILE_PATH} in external editor",
                    )
                    ui.button(icon="refresh", on_click=act_reload_state_from_disk).props(
                        "flat dense round size=xs color=slate-4"
                    ).tooltip(f"Reload modified {STATE_FILE_PATH.name} from disk")
                    kit.button("💾 Save", act_manual_save_state, color="emerald", outline=True).classes("px-2")

                    ui.label("|").classes("text-xs text-slate-700 mx-0.5")

                    # Cluster 2: UI Diagnostics (ui_diag.log + Snapshot Trigger)
                    ui.label(f"Diag ({DIAG_LOG_PATH.name}):").classes("text-[11px] font-mono text-slate-400")
                    kit.file_link(
                        DIAG_LOG_PATH,
                        label="",
                        tooltip=f"Open UI diagnostic log ({DIAG_LOG_PATH})",
                        default_content="",
                    )
                    kit.button(
                        "🩺 Snapshot",
                        lambda: diag.snapshot("header_btn"),
                        color="cyan",
                        outline=True,
                    ).classes("px-2")