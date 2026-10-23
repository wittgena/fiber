# fiber.dev.console.app
from __future__ import annotations

import asyncio
import os
from typing import Sequence
from nicegui import app, ui

from fiber.dev.console.state import (
    STATE_FILE_PATH,
    load_or_init_state,
    refs,
    save_state_to_disk,
    state,
)
import fiber.dev.console.runtime as rt
import fiber.dev.console.ui.diag as diag
import fiber.dev.console.ui.header as header
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ConsoleTemplate, StudioTab
import fiber.dev.console.ui.tab as tab

_tick_counter: int = 0
_bg_tasks: list[asyncio.Task] = []


def _build_drawer() -> None:
    with kit.dual_drawer() as (left_col, right_col):
        with left_col:
            with ui.row().classes("w-full justify-between items-center mb-1"):
                ui.label("💻 CLI Output").classes("text-xs text-cyan-400 font-mono font-bold")
                ui.button("Clear", on_click=lambda: refs.cli_log.clear() if refs.cli_log else None).props(
                    "flat dense size=xs color=slate-4"
                )
            refs.cli_log = kit.log(max_lines=250, color="text-slate-200", height="h-64")

        with right_col:
            with ui.row().classes("w-full justify-between items-center mb-1"):
                ui.label("📡 Control Plane Event (finops:* / mcp:* / sign:*)").classes(
                    "text-xs text-amber-400 font-mono font-bold"
                )
                with ui.row().classes("items-center gap-2"):
                    ui.badge().bind_text_from(state.pipeline, "pubsub_eps", lambda n: f"Events: {n}").props(
                        "color=slate-8 text-color=amber-3"
                    )
                    ui.button("Clear", on_click=lambda: refs.pubsub_log.clear() if refs.pubsub_log else None).props(
                        "flat dense size=xs color=slate-4"
                    )
            refs.pubsub_log = kit.log(max_lines=250, color="text-amber-200", height="h-64")

    refs.push_cli(
        f"Fiber-Console initialized (State file: {STATE_FILE_PATH}). Syncing local keys, accounts, and MCP topology..."
    )

def _on_tick(tabs: Sequence[StudioTab]) -> None:
    """Flush 100ms ringbuffers, trigger dirty tab re-renders, and auto-sync state.json every 5s (50 ticks)."""
    global _tick_counter
    try:
        rt.flush_100ms_ringbuffer()
        for t in tabs:
            try:
                t.flush()
            except Exception as tab_exc:
                diag.exception(f"Tab flush error in '{getattr(t, 'title', 'unknown')}'", exc=tab_exc)

        _tick_counter = (_tick_counter + 1) % 50
        if _tick_counter == 0:
            if save_state_to_disk(force=False):
                diag.debug("Auto-synced modified console state to disk", path=str(STATE_FILE_PATH))
    except Exception as exc:
        diag.exception("100ms tick flush error", exc=exc)

def build_console_shell() -> ConsoleTemplate:
    shell = ConsoleTemplate(
        tabs=tab.build_tabs(),
        modals=tab.build_modals(),
        header_builder=header.build_header,
        drawer_builder=_build_drawer,
        on_tick=_on_tick,
        tick_interval=0.1,
    )
    shell.mount_page("/", shared=False)
    diag.info("Console UI shell mounted", tabs=len(shell.tabs), modals=len(shell.modals))
    return shell


async def _delayed_open_window(url: str) -> None:
    """Wait briefly for Uvicorn socket binding before opening the app window."""
    await asyncio.sleep(0.4)
    rt.open_app_window(url)


async def _on_startup(url: str) -> None:
    """Initialize node/account/MCP state and spawn non-blocking background workers."""
    await rt.sync_initial_node_and_account_state()
    _bg_tasks.append(asyncio.create_task(rt.redis_pubsub_worker(), name="fiber-redis-pubsub"))
    _bg_tasks.append(asyncio.create_task(rt.mcp_gateway_health_worker(), name="fiber-mcp-health"))
    _bg_tasks.append(asyncio.create_task(_delayed_open_window(url), name="fiber-open-window"))


async def _on_shutdown() -> None:
    """Cancel background polling tasks cleanly and persist final state to disk."""
    for task in _bg_tasks:
        if not task.done():
            task.cancel()
    if _bg_tasks:
        await asyncio.gather(*_bg_tasks, return_exceptions=True)
    _bg_tasks.clear()
    save_state_to_disk(force=False)


def launch_console(port: int = 8357) -> None:
    if os.environ.get("FIBER_CONSOLE_ACTIVE") == "1":
        return

    url = f"http://127.0.0.1:{port}"
    if rt.is_port_active(port):
        print(f"Attached to running Fiber instance at {url}")
        diag.info("Attached to existing Fiber Console instance", url=url)
        rt.open_app_window(url)
        return

    os.environ["FIBER_CONSOLE_ACTIVE"] = "1"

    loaded = load_or_init_state()
    diag.info("Console state initialized", path=str(STATE_FILE_PATH), loaded_existing=loaded)

    build_console_shell()
    app.on_exception(lambda exc: diag.exception("Unhandled NiceGUI server exception", exc=exc))
    app.on_startup(lambda: _on_startup(url))
    app.on_shutdown(_on_shutdown)
    diag.info("Launching Fiber Control Studio server", host="127.0.0.1", port=port)
    ui.run(host="127.0.0.1", port=port, title="Fiber Control Studio", show=False, reload=False)


if __name__ == "__main__":
    launch_console(port=8357)