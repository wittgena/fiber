# fiber.dev.console.ui.header
from __future__ import annotations

import asyncio
import io
from pathlib import Path
import tarfile
import urllib.request
from nicegui import ui

from fiber.infra.ops.account.state import (
    STATE_FILE_PATH,
    load_or_init_state,
    refs,
    save_state_to_disk,
    state,
)
from fiber.infra.ops.finops.console import finops_manager
import fiber.dev.console.ui.diag as diag
from fiber.dev.console.ui.guide import open_guide_modal
import fiber.dev.console.ui.kit as kit
from xphi.node.space.bind.resolver import resolve_path

DIAG_LOG_PATH = resolve_path("log") / "ui_diag.log"
COMPATS_REPO = "dphi-network/fiber-compats"
COMPATS_TARBALL_URL = f"https://github.com/{COMPATS_REPO}/archive/refs/heads/main.tar.gz"
COMPATS_LOCAL_DIR = Path(str(resolve_path("fixture")))

_sync_lock = asyncio.Lock()


def has_console_fixtures() -> bool:
    """로컬 fixture 디렉토리에 console용 픽스처가 존재하는지 확인합니다."""
    console_dir = COMPATS_LOCAL_DIR / "console"
    return console_dir.exists() and any(console_dir.rglob("*.json"))


def _update_compats_badge_state(status: str) -> None:
    setattr(state.pipeline, "compats_badge_text", status)


def _download_and_extract_compats() -> int:
    """GitHub에서 fiber-compats tarball을 스트리밍 다운로드하여 로컬 fixture 경로에 압축 해제합니다."""
    COMPATS_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(COMPATS_TARBALL_URL, headers={"User-Agent": "Fiber-Control-Studio"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw_bytes = resp.read()

    extracted_count = 0
    with tarfile.open(fileobj=io.BytesIO(raw_bytes), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile() or "/" not in member.name:
                continue
            # 최상위 루트 폴더명(fiber-compats-main/) 제거 후 상대 경로 추출
            rel_path = member.name.split("/", 1)[1]
            if not rel_path or ".." in Path(rel_path).parts:
                continue
            dest_path = COMPATS_LOCAL_DIR / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            if f_obj := tar.extractfile(member):
                dest_path.write_bytes(f_obj.read())
                if dest_path.suffix == ".json":
                    extracted_count += 1
    return extracted_count


async def ensure_console_compats(*, force: bool = False, silent: bool = False) -> bool:
    """
    로컬에 fiber-compats/console 픽스처가 없으면 백그라운드로 다운로드합니다.
    - silent=True: 콘솔 초기 부팅 시 조용히 프리페치
    - force=True: 헤더 배지 클릭 시 최신 픽스처 강제 재동기화
    """
    if not hasattr(state.pipeline, "compats_badge_text"):
        _update_compats_badge_state("🟢 Compats: Ready" if has_console_fixtures() else "🟡 Compats: Missing")

    if not force and has_console_fixtures():
        _update_compats_badge_state("🟢 Compats: Ready")
        return True

    if _sync_lock.locked():
        return False

    async with _sync_lock:
        _update_compats_badge_state("⏳ Compats: Syncing...")
        if not silent:
            ui.notify(f"📥 Syncing fixtures from {COMPATS_REPO}...", type="info")
        refs.push_cli(f"$ fetch https://github.com/{COMPATS_REPO} -> {COMPATS_LOCAL_DIR}")

        try:
            count = await asyncio.to_thread(_download_and_extract_compats)
            _update_compats_badge_state(f"🟢 Compats: Ready ({count})")
            refs.push_cli(f"  ✓ [fiber-compats] Synced {count} fixture/compat JSON files into {COMPATS_LOCAL_DIR}")
            if not silent:
                ui.notify(f"✅ Synced {count} fixtures from fiber-compats", type="positive")
            return True
        except Exception as exc:
            _update_compats_badge_state("🔴 Compats: Offline (Click to Retry)")
            refs.push_cli(f"  ⚠️ [fiber-compats] Sync failed: {exc}")
            diag.exception("Failed to sync fiber-compats", exc=exc)
            if not silent:
                ui.notify(f"⚠️ Failed to download fiber-compats: {exc}", type="warning")
            return False


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
    if not hasattr(state.pipeline, "compats_badge_text"):
        _update_compats_badge_state("🟢 Compats: Ready" if has_console_fixtures() else "🟡 Compats: Sync")

    with ui.header().classes("bg-slate-900 border-b border-slate-800 px-5 py-2"):
        with ui.column().classes("w-full gap-1.5"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                with ui.row().classes("items-center gap-3"):
                    ui.label("Runtime Harness").classes("text-base font-extrabold tracking-wider text-white")
                    ui.badge().bind_text_from(state, "redis_badge_text").props(
                        "color=slate-8 text-color=emerald-3"
                    ).classes("text-xs px-2 py-0.5")
                    ui.badge().bind_text_from(state.security, "ssh_badge_text").props(
                        "color=slate-8 text-color=cyan-3"
                    ).classes("text-xs px-2 py-0.5")

                    # fiber-compats 상태 확인 및 1-Click 강제 최신화 배지
                    compats_badge = (
                        ui.badge()
                        .bind_text_from(state.pipeline, "compats_badge_text")
                        .props("color=slate-8 text-color=amber-3")
                        .classes("text-xs px-2 py-0.5 cursor-pointer")
                        .tooltip(f"Click to sync latest model fixtures from github.com/{COMPATS_REPO}")
                    )
                    compats_badge.on("click", lambda: asyncio.create_task(ensure_console_compats(force=True, silent=False)))

                with ui.row().classes("items-center gap-2.5"):
                    kit.button(
                        "Setup Guide",
                        open_guide_modal,
                        color="cyan",
                        outline=True,
                    ).classes("px-2.5").tooltip("IDE LLM Gateway & MCP Gateway configuration guide")

                    kit.button(
                        "LLM STREAM BREAK (10s)",
                        finops_manager.temporary_stream_kill,
                        color="red",
                    ).classes("font-bold px-3").tooltip(
                        "Sever active streams immediately (0 Fuel) and auto-restore previous balance after 10s"
                    )

            with ui.row().classes(
                "w-full items-center justify-between bg-slate-950 px-3 py-1 rounded border border-slate-800 no-wrap"
            ):
                with ui.row().classes("items-center gap-4"):
                    ui.label().bind_text_from(state, "fuel_kpi_text").classes(
                        "text-xs font-mono font-bold text-emerald-400"
                    )
                    ui.label("|").classes("text-xs text-slate-700")
                    ui.label().bind_text_from(state, "saved_kpi_text").classes(
                        "text-xs font-mono font-bold text-cyan-400"
                    )

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
                    ui.label(f"Diag ({DIAG_LOG_PATH.name}):").classes("text-[11px] font-mono text-slate-400")
                    kit.file_link(
                        DIAG_LOG_PATH,
                        label="",
                        tooltip=f"Open UI diagnostic log ({DIAG_LOG_PATH})",
                        default_content="",
                    )
                    kit.button(
                        "Snapshot",
                        lambda: diag.snapshot("header_btn"),
                        color="cyan",
                        outline=True,
                    ).classes("px-2")