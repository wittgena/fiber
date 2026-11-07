# fiber.dev.console.ui.sim
from __future__ import annotations

from nicegui import ui

from fiber.infra.ops.account.state import state, refs
from fiber.dev.console.bridge.sign import oob
from fiber.dev.console.env import get_slack_status
from fiber.dev.console.ui.guide import open_guide_modal
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate

SIM_TARGET = "postgres-prod-db"
SIM_TOOL = "drop_table"
SIM_ARGS = {"table": "customer_ledgers", "cascade": True}


def _open_hitl_guide() -> None:
    try:
        open_guide_modal("hitl")  # type: ignore[arg-type]
    except TypeError:
        open_guide_modal()


async def _run_console_sim(dialog: ui.dialog) -> None:
    dialog.close()
    await oob.open_hitl_modal(f"{SIM_TARGET}.{SIM_TOOL}", SIM_ARGS)


async def _run_slack_sim() -> None:
    st = get_slack_status()
    if not st["ok"]:
        ui.notify(f"[SLACK] Missing: {', '.join(st['missing'])}", type="warning", position="top")
        _open_hitl_guide()
        return

    state.security.slack_channel = st["channel"]
    await oob.dispatch_slack_oob(SIM_TARGET, SIM_TOOL, SIM_ARGS)


class HitlSimulatorModal(ModalTemplate):
    width_cls = "w-[460px] max-w-[95vw]"
    border_cls = "border border-amber-500/70"
    persistent = False

    def compose(self, dialog: ui.dialog) -> None:
        setattr(refs, "hitl_sim_dialog", dialog)
        s = state.security

        with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-2 mb-1"):
            ui.label("🧪 HITL Dry-Run Simulator").classes("text-sm font-bold text-amber-400")
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm color=slate-4")

        ui.label(f"Payload: {SIM_TARGET}.{SIM_TOOL}({SIM_ARGS})").classes(
            "w-full px-2 py-1 rounded bg-slate-900 border border-slate-800 text-[11px] font-mono text-slate-300"
        )

        with kit.bar():
            ui.label().bind_text_from(s, "admin_user", lambda u: f"Attester: {u}").classes(
                "text-xs font-mono text-cyan-300"
            )
            kit.switch("Require 6-Digit TOTP", s, "oob_require_totp", color="cyan")

        with ui.row().classes("w-full items-center gap-2 mt-1"):
            kit.button(
                "🖥️ Console Popup",
                lambda: _run_console_sim(dialog),
                color="amber",
                text_color="black",
                flex=True,
            )
            kit.button(
                "💬 Slack OOB Test",
                _run_slack_sim,
                color="indigo",
                outline=True,
                flex=True,
            )

def open_hitl_simulator_modal() -> None:
    dlg = getattr(refs, "hitl_sim_dialog", None) or HitlSimulatorModal().mount()
    dlg.open()