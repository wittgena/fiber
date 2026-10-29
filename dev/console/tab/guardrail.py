# fiber.dev.console.tab.guardrail
from __future__ import annotations

import re
from nicegui import ui

from fiber.infra.ops.account.state import state, refs
from fiber.dev.console.bridge.pipeline import pipeline
from fiber.llm.compat.secret.registry import (
    BASE_SECRET_KEYS,
    DEFAULT_REDACT_FILENAME,
    RedactRegistryIO,
)
import fiber.llm.compat.secret.redact as _redact_mod

import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate, StudioTab
from fiber.dev.console.ui.guide import GuideModal
from fiber.dev.console.tab.mcp import HitlModal, QrModal, McpGatewayTab
from fiber.dev.console.tab.vcr import VcrTab
from fiber.dev.console.tab.metering import MeteringTab
from fiber.dev.console.tab.setting import ModelNSettingTab  # noqa: F401

class SecretRulesModal:
    """Built-in(33) + Custom Patterns(redact_rules.json) 및 Secret Keys를 단일 SSOT로 관리하는 Dialog"""

    def __init__(self, on_saved=None) -> None:
        self.on_saved = on_saved
        self.dialog = ui.dialog()
        self.disabled_set: set[str] = set()
        self.custom_patterns: dict[str, str] = {}
        self.extra_keys: list[str] = []

    def open(self, initial_tab: str = "rules") -> None:
        schema = RedactRegistryIO.load_schema()
        self.disabled_set = set(schema.disabled_rules)
        self.custom_patterns = dict(schema.patterns)
        self.extra_keys = [k.upper() for k in schema.extra_secret_keys]
        base_keys = sorted(BASE_SECRET_KEYS)

        def _merged_rules() -> dict[str, str]:
            # Custom patterns를 상단에 배치하여 추가/확인이 쉽도록 구성
            return {**self.custom_patterns, **_redact_mod.BASE_SECRET_RULES}

        self.dialog.clear()
        with self.dialog, ui.card().classes(
            "bg-slate-950 border border-slate-700 w-[740px] max-w-[94vw] p-3 gap-2"
        ):
            with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-1"):
                with ui.tabs().props("dense active-color=cyan indicator-color=cyan") as tabs:
                    tab_rules = ui.tab("rules", label="🛡️ Regex Rules (Built-in & Custom)").classes("text-xs font-mono")
                    tab_keys = ui.tab("keys", label="🔑 Secret Keys (Dict & URL)").classes("text-xs font-mono")
                self.status_label = ui.label().classes("text-xs font-mono text-slate-400")

            def _refresh_status() -> None:
                all_r = _merged_rules()
                active_r = len(all_r) - len(self.disabled_set & set(all_r.keys()))
                total_k = len(set(base_keys) | set(self.extra_keys))
                self.status_label.set_text(
                    f"Rules: {active_r}/{len(all_r)} active (Custom: {len(self.custom_patterns)})  |  Keys: {total_k}"
                )

            _refresh_status()

            with ui.tab_panels(tabs, value=tab_keys if initial_tab == "keys" else tab_rules).classes(
                "w-full bg-transparent p-0"
            ):
                # 1. Regex Rules 패널 (Built-in 토글 + Custom Pattern 추가/삭제)
                with ui.tab_panel(tab_rules).classes("p-0 gap-2 flex flex-col"):
                    # Custom Pattern 추가 입력 바 (redact_rules.json의 'patterns'와 직결)
                    with ui.row().classes("w-full items-center gap-1.5 no-wrap bg-slate-900/80 p-1.5 rounded border border-slate-800"):
                        new_id_input = (
                            ui.input(placeholder="New rule_id (e.g. corp_token)")
                            .props("dense outlined dark")
                            .classes("w-48 text-xs font-mono")
                        )
                        new_regex_input = (
                            ui.input(placeholder=r"Regex pattern (e.g. CORP-[0-9]{6})")
                            .props("dense outlined dark")
                            .classes("flex-grow text-xs font-mono")
                        )

                        def _add_custom_pattern() -> None:
                            rid = (new_id_input.value or "").strip()
                            pat = (new_regex_input.value or "").strip()
                            if not rid or not pat:
                                ui.notify("Both rule_id and regex pattern are required.", type="warning")
                                return
                            try:
                                re.compile(pat)
                            except re.error as err:
                                ui.notify(f"Invalid regex: {err}", type="negative")
                                return
                            self.custom_patterns[rid] = pat
                            self.disabled_set.discard(rid)
                            new_id_input.value = ""
                            new_regex_input.value = ""
                            _refresh_status()
                            _render_rows(search_input.value)

                        new_regex_input.on("keydown.enter", _add_custom_pattern)
                        ui.button("Add Rule", icon="add", on_click=_add_custom_pattern).props(
                            "dense color=cyan text-color=black"
                        ).classes("text-xs font-bold px-2.5 shrink-0")

                    with ui.row().classes("w-full items-center justify-between gap-2"):
                        search_input = (
                            ui.input(placeholder="Filter by rule_id or regex...")
                            .props("dense outlined dark clearable")
                            .classes("flex-grow text-xs font-mono")
                        )
                        ui.button("Enable All", on_click=lambda: _set_all(True)).props(
                            "dense flat size=sm color=emerald"
                        ).classes("text-xs font-mono")
                        ui.button("Disable All", on_click=lambda: _set_all(False)).props(
                            "dense flat size=sm color=amber"
                        ).classes("text-xs font-mono")

                    list_container = ui.scroll_area().classes(
                        "w-full h-72 border border-slate-800 rounded bg-slate-900/60 p-1"
                    )

                    def _render_rows(query: str = "") -> None:
                        q = (query or "").strip().lower()
                        all_r = _merged_rules()
                        list_container.clear()
                        with list_container:
                            for rule_id, pattern in all_r.items():
                                if q and q not in rule_id.lower() and q not in pattern.lower():
                                    continue
                                is_enabled = rule_id not in self.disabled_set
                                is_custom = rule_id in self.custom_patterns

                                with ui.row().classes(
                                    "w-full items-center justify-start px-2 py-1 border-b border-slate-800/60 hover:bg-slate-800/40 no-wrap gap-2.5"
                                ):
                                    def _toggle(e, rid=rule_id) -> None:
                                        if e.value:
                                            self.disabled_set.discard(rid)
                                        else:
                                            self.disabled_set.add(rid)
                                        _refresh_status()

                                    ui.switch(value=is_enabled, on_change=_toggle).props("dense color=cyan").classes(
                                        "shrink-0"
                                    )

                                    with ui.column().classes("gap-0 overflow-hidden flex-grow"):
                                        with ui.row().classes("items-center gap-1.5 no-wrap"):
                                            ui.label(rule_id).classes("text-xs font-mono font-bold text-slate-200 shrink-0")
                                            if is_custom:
                                                ui.badge("CUSTOM").props("color=cyan-9 text-color=cyan-2").classes(
                                                    "text-[9px]"
                                                )
                                        ui.label(pattern).classes(
                                            "text-[10px] font-mono text-slate-400 truncate w-full"
                                        ).tooltip(pattern)

                                    if is_custom:
                                        ui.button(
                                            icon="delete_outline",
                                            on_click=lambda _, rid=rule_id: (
                                                self.custom_patterns.pop(rid, None),
                                                self.disabled_set.discard(rid),
                                                _refresh_status(),
                                                _render_rows(search_input.value),
                                            ),
                                        ).props("dense flat round size=xs color=red-4").tooltip("Remove custom rule")

                    def _set_all(enable: bool) -> None:
                        if enable:
                            self.disabled_set.clear()
                        else:
                            self.disabled_set = set(_merged_rules().keys())
                        _refresh_status()
                        _render_rows(search_input.value)

                    search_input.on_value_change(lambda e: _render_rows(e.value))
                    _render_rows()

                # 2. Secret Keys 패널
                with ui.tab_panel(tab_keys).classes("p-0 gap-2 flex flex-col"):
                    ui.label(
                        "Matches dictionary keys & URL query parameters containing these tokens (case-insensitive) in PRE_TRANSLATE."
                    ).classes("text-[11px] font-mono text-slate-400")

                    with ui.row().classes("w-full items-center gap-2"):
                        new_key_input = (
                            ui.input(placeholder="Add extra secret key (min 3 chars, e.g. CORP_PIN)...")
                            .props("dense outlined dark")
                            .classes("flex-grow text-xs font-mono")
                        )

                        def _add_extra_key() -> None:
                            val = (new_key_input.value or "").strip().upper()
                            if len(val) < 3:
                                ui.notify("Key must be at least 3 characters.", type="warning")
                                return
                            if val not in base_keys and val not in self.extra_keys:
                                self.extra_keys.append(val)
                            new_key_input.value = ""
                            _refresh_status()
                            _render_keys()

                        new_key_input.on("keydown.enter", _add_extra_key)
                        ui.button("Add Key", icon="add", on_click=_add_extra_key).props(
                            "dense color=emerald text-color=black"
                        ).classes("text-xs font-bold px-2.5")

                    keys_container = ui.scroll_area().classes(
                        "w-full h-72 border border-slate-800 rounded bg-slate-900/60 p-2"
                    )

                    def _render_keys() -> None:
                        keys_container.clear()
                        with keys_container:
                            ui.label("Extra Secret Keys (Editable - saved to redact_rules.json)").classes(
                                "text-[11px] font-mono font-bold text-emerald-400 mb-1"
                            )
                            with ui.row().classes("w-full flex-wrap gap-1.5 mb-3"):
                                if not self.extra_keys:
                                    ui.label("No extra keys configured.").classes(
                                        "text-xs font-mono text-slate-500 italic"
                                    )
                                for k in sorted(self.extra_keys):
                                    with ui.badge().props("color=emerald-9 text-color=emerald-2").classes(
                                        "text-xs font-mono px-2 py-1 gap-1 items-center"
                                    ):
                                        ui.label(k)
                                        ui.button(
                                            icon="close",
                                            on_click=lambda _, target=k: (
                                                self.extra_keys.remove(target),
                                                _refresh_status(),
                                                _render_keys(),
                                            ),
                                        ).props("dense flat round size=xs color=emerald-2")

                            ui.label("Built-in Secret Keys (14 Core Patterns)").classes(
                                "text-[11px] font-mono font-bold text-slate-400 mb-1"
                            )
                            with ui.row().classes("w-full flex-wrap gap-1.5"):
                                for k in base_keys:
                                    ui.badge(k).props("color=slate-8 text-color=slate-3").classes(
                                        "text-xs font-mono px-2 py-1 border border-slate-700"
                                    )

                    _render_keys()

            with ui.row().classes("w-full justify-end gap-2 pt-1"):
                ui.button("Cancel", on_click=self.dialog.close).props("dense flat color=slate-4").classes("text-xs")

                def _save() -> None:
                    RedactRegistryIO.save_preferences(
                        disabled_rules=sorted(self.disabled_set),
                        extra_secret_keys=self.extra_keys,
                        patterns=self.custom_patterns,
                    )
                    if self.on_saved:
                        self.on_saved()
                    ui.notify(
                        f"Saved to {DEFAULT_REDACT_FILENAME} "
                        f"(Custom: {len(self.custom_patterns)}, Disabled: {len(self.disabled_set)}, +Keys: {len(self.extra_keys)})",
                        type="positive",
                    )
                    self.dialog.close()

                ui.button("Save & Apply", on_click=_save).props("dense color=cyan text-color=black").classes(
                    "text-xs font-bold px-3"
                )

        self.dialog.open()


class LLMGuardrailTab(StudioTab):
    title = "Guardrail"
    left_header = "Rule Registry & Test"
    right_header = "Pipeline X-Ray"

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
        # 일회성 custom_regex가 Built-in/Registry 룰 비활성화 테스트를 간섭하지 않도록 비움
        s.custom_regex = ""

        active_rules_cnt = len(getattr(_redact_mod, "ACTIVE_SECRET_RULES", {}) or {})
        active_keys_cnt = len(getattr(_redact_mod, "SECRET_KEY_PATTERNS", {}) or {})
        registry_file_path = RedactRegistryIO.ensure_default_file()

        rules_badge_ref = None
        keys_badge_ref = None

        def _refresh_rule_badges() -> None:
            r_cnt = len(getattr(_redact_mod, "ACTIVE_SECRET_RULES", {}) or {})
            k_cnt = len(getattr(_redact_mod, "SECRET_KEY_PATTERNS", {}) or {})
            if rules_badge_ref is not None:
                rules_badge_ref.set_text(f"Rules: {r_cnt}")
            if keys_badge_ref is not None:
                keys_badge_ref.set_text(f"Keys: {k_cnt}")

        def _reload_from_disk() -> None:
            RedactRegistryIO.initialize_and_inject()
            _refresh_rule_badges()
            ui.notify(f"Reloaded {DEFAULT_REDACT_FILENAME} into runtime engine.", type="info")

        rules_modal = SecretRulesModal(on_saved=_refresh_rule_badges)

        with kit.bar():
            with ui.row().classes("items-center gap-1 no-wrap"):
                ui.button(icon="tune", on_click=lambda: rules_modal.open("rules")).props(
                    "dense flat round size=xs color=cyan"
                ).tooltip("Inspect & Edit Built-in / Custom Secret Rules")

                rules_badge_ref = (
                    ui.badge(f"Rules: {active_rules_cnt}")
                    .props("color=slate-8 text-color=cyan-3")
                    .classes("text-[10px] font-mono font-bold px-1.5 py-0.5 cursor-pointer")
                    .tooltip("Click to manage Built-in & Custom Regex Rules")
                )
                rules_badge_ref.on("click", lambda: rules_modal.open("rules"))

                keys_badge_ref = (
                    ui.badge(f"Keys: {active_keys_cnt}")
                    .props("color=slate-8 text-color=emerald-3")
                    .classes("text-[10px] font-mono font-bold px-1.5 py-0.5 cursor-pointer")
                    .tooltip("Click to inspect & add Secret Key Patterns")
                )
                keys_badge_ref.on("click", lambda: rules_modal.open("keys"))

            with ui.row().classes("items-center gap-1 no-wrap"):
                kit.file_link(
                    registry_file_path,
                    label=DEFAULT_REDACT_FILENAME,
                    tooltip="Open declarative secret & PII registry JSON",
                    create_if_missing=True,
                )
                ui.button(icon="refresh", on_click=_reload_from_disk).props(
                    "dense flat round size=xs color=slate-4"
                ).tooltip("Reload redact_rules.json from disk")

        ui.label("Registry Policy Enforcement").classes("text-[11px] font-bold text-slate-400 shrink-0")
        kit.switch("Enforce Active Registry Rules (Built-in + Custom JSON)", s, "block_api_keys", color="red")
        kit.select(
            ["Strict Block (PermissionError)", "Redact (Mask with ***)"],
            "Guardrail Action",
            s,
            "guardrail_mode",
            width="w-full",
        )

        ui.label("Test Prompt").classes("text-[11px] font-bold text-cyan-400 mt-1 shrink-0")
        kit.text("Input Prompt", s, "test_prompt", flex=False, width="w-full")
        with kit.row(gap="gap-1.5"):
            kit.button("Test Prompt", lambda: pipeline.run_pipeline_xray(s.test_prompt), color="red", flex=True)

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
                ui.label("⚡ Stream Velocity").classes("text-[11px] font-mono text-slate-400")
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
                ("guardrail", "Secret / PII Guard", "PRE & POST"),
                ("fuel", "StreamBreaker", "STREAM_IO"),
            ]:
                with ui.card().classes("bg-slate-800 border border-slate-700 p-2 w-36 text-center") as c:
                    refs.slot_cards[key] = c
                    ui.label(title).classes("text-xs font-bold text-white")
                    ui.label(slot).classes("text-[10px] font-mono text-slate-400")

        ui.label("Slot Overhead (ms)").classes("text-xs text-slate-400 mt-0.5 shrink-0")
        refs.xray_chart = kit.bar_chart(
            categories=["StreamBreaker", "SecretGuard", "ONNX Router", "EdgeCache"],
            data=list(s.xray_latencies),
            x_max=1.0,
        )