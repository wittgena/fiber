# fiber.dev.console.tab.hitl
from __future__ import annotations

from pathlib import Path
import re
from typing import Dict, List
from nicegui import ui
import orjson

from fiber.infra.ops.account.state import state, refs
from fiber.dev.console.bridge.sign import oob
from fiber.dev.console.env import get_slack_status
from fiber.dev.console.ui.guide import open_guide_modal
from fiber.dev.console.ui.sim import HitlSimulatorModal, open_hitl_simulator_modal
from fiber.infra.mcp.guard.preflight import PreflightGuard
from fiber.infra.mcp.guard.preset import RAW_BUILTIN_PRESETS, PreflightPresetRegistry
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import StudioTab, ModalTemplate
from xphi.node.space.bind.resolver import resolve_path


def _open_hitl_guide() -> None:
    try:
        open_guide_modal("hitl")  # type: ignore[arg-type]
    except TypeError:
        open_guide_modal()


def _get_anchor_mcp_path() -> Path:
    return resolve_path("anchor") / "mcp.json"


def _load_worker_guard_rows() -> List[Dict[str, str]]:
    p = _get_anchor_mcp_path()
    if not p.is_file():
        return []
    try:
        data = orjson.loads(p.read_bytes())
        servers = data.get("mcpServers", {}) if isinstance(data, dict) else {}
        rows: List[Dict[str, str]] = []
        for tid, spec in servers.items():
            if not isinstance(spec, dict) or tid == "fiber-gateway":
                continue
            env_dict = spec.get("env") if isinstance(spec.get("env"), dict) else {}
            raw_pat, _ = PreflightGuard.extract_from_env_dict(env_dict)
            clean_pat, req_totp = PreflightGuard._parse_mode_and_pattern(tid, raw_pat)
            rows.append(
                {
                    "target_id": str(tid),
                    "exec_cmd": str(spec.get("command") or spec.get("url") or ""),
                    "mode": "open" if not clean_pat else ("totp" if req_totp else "button"),
                    "pattern": clean_pat or "",
                }
            )
        return rows
    except Exception:
        return []


def _save_worker_preflight(target_id: str, mode: str, pattern: str) -> bool:
    p = _get_anchor_mcp_path()
    if not p.is_file() or not target_id:
        ui.notify("~/.anchor/mcp.json not found. Sync workers in MCP Gateway tab first.", type="warning")
        return False

    clean_pat = re.sub(
        r"^(totp|button|simple|nototp|1click)\s*:\s*", "", (pattern or "").strip(), flags=re.IGNORECASE
    ).strip()

    if mode != "open" and not clean_pat:
        ui.notify("Enter a regex pattern or click a preset below.", type="warning")
        return False

    if mode != "open" and not clean_pat.startswith("@"):
        try:
            re.compile(clean_pat, re.IGNORECASE)
        except re.error as exc:
            ui.notify(f"Invalid Regex: {exc}", type="negative")
            return False

    try:
        data = orjson.loads(p.read_bytes())
        servers = data.setdefault("mcpServers", {})
        if target_id not in servers or not isinstance(servers[target_id], dict):
            return False

        env_dict = servers[target_id].setdefault("env", {})
        for k in (*PreflightGuard.ENV_KEYS, *PreflightGuard.MODE_ENV_KEYS):
            env_dict.pop(k, None)

        if mode == "button":
            env_dict["FIBER_PREFLIGHT_PATTERN"] = f"button:{clean_pat}"
        elif mode == "totp":
            env_dict["FIBER_PREFLIGHT_PATTERN"] = clean_pat

        p.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
        state.security.mcp_topology_dirty = True
        badge = "OPEN (Disarmed)" if mode == "open" else f"{mode.upper()} Guard Armed"
        ui.notify(f"[{target_id}] {badge}", type="positive")
        return True
    except Exception as exc:
        ui.notify(f"Failed to save mcp.json: {exc}", type="negative")
        return False


class ModelCatalogModal(ModalTemplate):
    width_cls = "w-[860px] max-w-[95vw]"
    border_cls = "border border-slate-700"
    persistent = False

    def compose(self, dialog: ui.dialog) -> None:
        setattr(refs, "model_catalog_dialog", dialog)
        from fiber.infra.ops.finops.exchange import default_exchange
        from fiber.infra.ops.finops.model.filter import CORE_KEYWORDS
        from fiber.infra.ops.finops.cost.registry import (
            ModelCostRegistry,
            REGISTRY_ROOT,
            DEFAULT_REGISTRY_FILENAME,
        )

        with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-2 mb-1"):
            ui.label("LLM Model Pricing & Fuel Catalog").classes("text-sm font-bold text-white")
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm color=slate-4")

        with ui.column().classes("w-full p-2 gap-1 text-[11px] font-mono bg-slate-900/80 border border-slate-800 rounded"):
            ui.label("• $1.00 USD = 1,000,000 Fuel  |  $0.000001 (1 μ$) = 1.0 Fuel").classes("text-emerald-300")
            ui.label("• Rate Par     : Price ($ / 1M Tokens) == Burn Rate (Fuel / Token)").classes("text-amber-300")
            ui.label("• Priced Models: Consumed Fuel = ceil(Billed_USD × 1,000,000)").classes("text-slate-300")
            ui.label("• Local Models : Consumed Fuel = ceil(Total_Tokens × 1.0) [when $0.0]").classes("text-cyan-300")

        total_mapped = len(getattr(ModelCostRegistry, "_model_cost", {}) or {})
        usd_rate = getattr(default_exchange, "usd_rate", 1_000_000)

        with kit.bar():
            with ui.row().classes("items-center gap-1.5 no-wrap"):
                ui.badge(f"Models: {total_mapped:,}").props("color=slate-8 text-color=cyan-3").classes("text-[10px] font-mono font-bold px-1.5")
                ui.badge(f"providers: {len(CORE_KEYWORDS)}").props("color=slate-8 text-color=emerald-3").classes("text-[10px] font-mono font-bold px-1.5")
                ui.badge(f"$1 = {usd_rate:,} Fuel").props("color=slate-9 text-color=amber-3").classes("text-[10px] font-mono border border-amber-500/40 px-1.5")
            kit.file_link(REGISTRY_ROOT / DEFAULT_REGISTRY_FILENAME, label=DEFAULT_REGISTRY_FILENAME, create_if_missing=True)

        columns = [
            {"name": "model", "label": "Model", "field": "model", "align": "left", "sortable": True},
            {"name": "provider", "label": "Provider", "field": "provider", "align": "left", "sortable": True},
            {"name": "in_usd", "label": "In ($/1M)", "field": "input_usd_per_1m", "align": "right", "sortable": True},
            {"name": "out_usd", "label": "Out ($/1M)", "field": "output_usd_per_1m", "align": "right", "sortable": True},
            {"name": "in_tok", "label": "In (Fuel/tok)", "field": "input_fuel_per_tok", "align": "right", "sortable": True},
            {"name": "out_tok", "label": "Out (Fuel/tok)", "field": "output_fuel_per_tok", "align": "right", "sortable": True},
            {"name": "mode", "label": "Mode", "field": "billing_mode", "align": "left"},
        ]
        ui.table(columns=columns, rows=default_exchange.build_catalog_rows(limit=30), row_key="model").props(
            "dense flat bordered dark virtual-scroll rows-per-page=0"
        ).classes("w-full h-80 text-xs font-mono bg-slate-950")


def open_model_catalog_modal() -> None:
    dlg = getattr(refs, "model_catalog_dialog", None) or ModelCatalogModal().mount()
    dlg.open()


class HitlNSettingTab(StudioTab):
    title = "HITL & Setting"
    left_header = "MCP Pre-Flight Guard"
    right_header = "Slack HITL & Admin Identity"
    ratio = 52

    def __init__(self) -> None:
        self._workers_box: ui.column | None = None
        self._summary_label: ui.label | None = None
        self._edit_dialog: ui.dialog | None = None
        self._edit_target_label: ui.label | None = None
        self._edit_mode_select: ui.select | None = None
        self._edit_pattern_input: ui.input | None = None
        self._editing_target_id: str = ""

    def _mount_local_dialogs(self) -> None:
        if getattr(refs, "model_catalog_dialog", None) is None:
            ModelCatalogModal().mount()
        if getattr(refs, "hitl_sim_dialog", None) is None:
            HitlSimulatorModal().mount()

        with ui.dialog() as dlg, ui.card().classes("w-[500px] max-w-[95vw] bg-slate-950 border border-slate-700 p-3.5 gap-2.5"):
            self._edit_dialog = dlg
            with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-1.5"):
                self._edit_target_label = ui.label("Configure Pre-Flight Guard").classes("text-sm font-mono font-bold text-cyan-400")
                ui.button(icon="close", on_click=dlg.close).props("flat dense round size=xs color=slate-4")

            with ui.row().classes("w-full items-center gap-2 no-wrap"):
                self._edit_mode_select = ui.select(
                    options={"totp": "🔒 TOTP", "button": "1-Click Button", "open": "Open (Disarmed)"},
                    value="totp",
                    label="Worker Auth Mode",
                ).props("dense outlined dark options-dense").classes("w-44 text-xs font-mono")

                self._edit_pattern_input = ui.input(
                    label="Intercept Regex Pattern",
                    placeholder=r"\b(DROP|TRUNCATE|DELETE)\b",
                ).props("dense outlined dark").classes("flex-grow text-xs font-mono")

            ui.label("Presets (Click to populate mode & regex):").classes("text-xs font-mono text-slate-400")
            with ui.row().classes("w-full items-center gap-1.5 flex-wrap"):
                for p in PreflightPresetRegistry.load_all().values():
                    icon = "🔒" if p.mode == "totp" else "⚡"
                    kit.button(
                        f"{icon} {p.label}",
                        lambda m=p.mode, pat=p.pattern: self._set_dialog_preset(m, pat),
                        color="amber" if p.mode == "totp" else "cyan",
                        outline=True,
                    ).tooltip(f"{p.description}\nRegex: /{p.pattern}/i")

            with ui.row().classes("w-full justify-between items-center mt-2 pt-2 border-t border-slate-800"):
                kit.button("Disarm (Set Open)", lambda: self._set_dialog_preset("open", ""), color="red", outline=True)
                kit.button("💾 Save Guard", self._save_dialog_changes, color="emerald")

    def _set_dialog_preset(self, mode: str, pattern: str) -> None:
        if self._edit_mode_select:
            self._edit_mode_select.value = mode
        if self._edit_pattern_input:
            self._edit_pattern_input.value = pattern

    def _open_edit_dialog(self, row: Dict[str, str]) -> None:
        self._editing_target_id = row["target_id"]
        if self._edit_target_label:
            self._edit_target_label.set_text(f"Pre-Flight Guard — {row['target_id']}")
        if self._edit_mode_select:
            self._edit_mode_select.value = row["mode"] if row["mode"] != "open" else "totp"
        if self._edit_pattern_input:
            self._edit_pattern_input.value = row["pattern"]
        if self._edit_dialog:
            self._edit_dialog.open()

    def _save_dialog_changes(self) -> None:
        mode = self._edit_mode_select.value if self._edit_mode_select else "totp"
        pat = self._edit_pattern_input.value if self._edit_pattern_input else ""
        if _save_worker_preflight(self._editing_target_id, mode, pat):
            if self._edit_dialog:
                self._edit_dialog.close()
            self._render_worker_guard_list()

    def _render_worker_guard_list(self) -> None:
        if self._workers_box is None:
            return
        rows = _load_worker_guard_rows()
        self._workers_box.clear()

        totp_cnt = sum(1 for r in rows if r["mode"] == "totp")
        btn_cnt = sum(1 for r in rows if r["mode"] == "button")
        open_cnt = sum(1 for r in rows if r["mode"] == "open")

        if self._summary_label:
            self._summary_label.set_text(f"🔒 {totp_cnt} TOTP · ⚡ {btn_cnt} 1-Click · 🔓 {open_cnt} Open")

        with self._workers_box:
            if not rows:
                ui.label("No MCP workers in ~/.anchor/mcp.json. Sync in 'MCP Gateway' tab first.").classes(
                    "text-xs font-mono text-slate-400 py-2"
                )
                return

            for r in rows:
                mode, pat = r["mode"], r["pattern"]
                if mode == "totp":
                    border_cls = "border-slate-800 bg-slate-900/60"
                    badge_txt, badge_color, badge_text_col = "🔒 TOTP", "slate-8", "amber-3"
                    pat_cls = "text-[11px] font-mono text-amber-200/80 bg-slate-950/90 px-1.5 py-0.5 rounded border border-slate-800"
                elif mode == "button":
                    border_cls = "border-slate-800 bg-slate-900/60"
                    badge_txt, badge_color, badge_text_col = "⚡ 1-Click", "slate-8", "cyan-3"
                    pat_cls = "text-[11px] font-mono text-cyan-200/80 bg-slate-950/90 px-1.5 py-0.5 rounded border border-slate-800"
                else:
                    border_cls = "border-slate-800/60 bg-slate-950/50"
                    badge_txt, badge_color, badge_text_col = "🔓 Open", "slate-9", "slate-4"
                    pat_cls = "text-[11px] font-mono text-slate-500"

                with ui.column().classes(f"w-full px-2.5 py-1.5 rounded border {border_cls} gap-1"):
                    with ui.row().classes("w-full items-center justify-between no-wrap"):
                        with ui.row().classes("items-center gap-1.5 overflow-hidden"):
                            ui.label(r["target_id"]).classes("text-xs font-mono font-bold text-slate-200 truncate").tooltip(r["exec_cmd"])
                            ui.badge(badge_txt).props(f"color={badge_color} text-color={badge_text_col}").classes(
                                "text-[10px] font-mono px-1.5 py-0"
                            )
                        kit.button("Configure", lambda rc=r: self._open_edit_dialog(rc), color="slate-4", outline=True)

                    ui.label(f"/{pat}/i" if pat else "Unarmed (All tool calls pass through)").classes(
                        f"{pat_cls} truncate max-w-full"
                    ).tooltip(f"/{pat}/i" if pat else "Unarmed")

    def compose_left(self) -> None:
        self._mount_local_dialogs()
        default_rules = orjson.dumps({"presets": RAW_BUILTIN_PRESETS, "disabled_presets": []}, option=orjson.OPT_INDENT_2).decode()

        with kit.bar():
            self._summary_label = ui.label("🔒 0 TOTP · ⚡ 0 1-Click · 🔓 0 Open").classes("text-xs font-mono font-bold text-cyan-400")
            with ui.row().classes("items-center gap-1.5"):
                kit.file_link(
                    PreflightPresetRegistry.get_registry_path(),
                    label="preflight_rules.json",
                    default_content=default_rules,
                    create_if_missing=True,
                )
                ui.button("🔄", on_click=self._render_worker_guard_list).props("flat dense size=xs color=cyan")

        self._workers_box = ui.column().classes("w-full gap-1.5 flex-grow max-h-80 overflow-y-auto pr-1")
        self._render_worker_guard_list()

        # with kit.footer():
        #     kit.button("Slack Setup Guide", _open_hitl_guide, color="slate-4", outline=True, flex=True)

    def compose_right(self) -> None:
        s = state.security
        from fiber.infra.ops.finops.cost.registry import ModelCostRegistry, REGISTRY_ROOT, DEFAULT_REGISTRY_FILENAME

        slk = get_slack_status()
        if slk["channel"]:
            s.slack_channel = slk["channel"]

        with kit.bar():
            ui.label().bind_text_from(s, "gateway_health_badge_text").classes("text-xs font-mono font-bold text-emerald-400")
            ui.label().bind_text_from(s, "slack_oob_badge_text").classes("text-[11px] font-mono font-bold text-cyan-300")

        ui.label("Slack OOB Approval Bridge").classes("text-xs font-bold text-amber-400")
        with ui.row().classes(
            "w-full px-2.5 py-2 rounded border border-slate-800 bg-slate-950/80 items-center justify-between no-wrap font-mono text-xs"
        ):
            with ui.row().classes("items-center gap-2 overflow-hidden"):
                ui.badge("🟢 LIVE" if slk["ok"] else "🟡 UNSET").props(
                    f"color={'emerald-9' if slk['ok'] else 'slate-8'}"
                ).classes("text-[10px] font-bold px-1.5 py-0.5")
                ui.label(slk["meta"]).classes("text-[11px] text-slate-300 truncate")
            kit.button("Setup Guide", _open_hitl_guide, color="amber", outline=True)

        ui.label("Admin Identity & TOTP Keychain").classes("text-xs font-bold text-cyan-400 mt-1")
        with ui.column().classes("w-full p-2.5 rounded border border-slate-800 bg-slate-950/80 gap-2"):
            kit.text("Mapped Admin SPIFFE ID ('*' Wildcard)", s, "admin_user", width="w-full")
            with kit.row():
                kit.button("Key Init", oob.init_node_keys, color="slate-4", outline=True)
                kit.button("Enroll TOTP(QR)", oob.enroll_admin_totp, color="cyan", flex=True)
                kit.button("Verify TOTP", oob.verify_admin_totp, color="emerald", outline=True, flex=True)
                kit.button("Revoke", oob.revoke_admin_totp, color="red", outline=True)

        # 3. FinOps Model Catalog
        total_mapped = len(getattr(ModelCostRegistry, "_model_cost", {}) or {})
        with kit.bar():
            with ui.row().classes("items-center gap-1.5"):
                ui.label("FinOps Model Catalog").classes("text-xs font-mono text-slate-300")
                ui.badge(f"{total_mapped:,} Models").props("color=slate-8 text-color=cyan-3").classes("text-[10px] font-mono")
            with ui.row().classes("items-center gap-1.5"):
                kit.file_link(REGISTRY_ROOT / DEFAULT_REGISTRY_FILENAME, label="JSON", create_if_missing=True)
                kit.button("Pricing Table", open_model_catalog_modal, color="cyan", outline=True)

        with kit.footer():
            kit.button(
                "Open HITL Simulator (Console / Slack)",
                open_hitl_simulator_modal,
                color="amber",
                text_color="black",
                flex=True,
            )

    def flush(self) -> None:
        if getattr(state.security, "mcp_topology_dirty", False):
            self._render_worker_guard_list()