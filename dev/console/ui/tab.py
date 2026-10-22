# fiber.dev.console.ui.tab
from __future__ import annotations

import os
from pathlib import Path
from nicegui import ui

from fiber.dev.console.state import state, refs
import fiber.dev.console.runtime as rt
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate, StudioTab
from xphi.node.space.bind.resolver import resolve_path


def _resolve_accounts_registry_path() -> Path:
    try:
        from fiber.infra.bound.account.registry import LocalAccountRegistry
        return Path(LocalAccountRegistry.get_registry_path())
    except Exception:
        return resolve_path("anchor") / "sign" / "local_accounts.json"


def _resolve_compat_rules_path() -> Path:
    env_path = os.environ.get("FIBER_COMPAT_RULES_PATH", "").strip()
    return Path(env_path) if env_path else (resolve_path("anchor") / "compat_rules.json")


# ==========================================
# Modals: 1) HTTP 202 HITL Gate & 2) TOTP QR
# ==========================================
class HitlModal(ModalTemplate):
    width_cls = "w-[520px]"
    border_cls = "border-2 border-amber-500"
    persistent = True

    def mount(self) -> ui.dialog:
        refs.hitl_dialog = super().mount()
        return refs.hitl_dialog

    def compose(self, dialog: ui.dialog) -> None:
        ui.label("⚠️ HTTP 202: Human-in-the-Loop Cryptographic Gate").classes("text-amber-400 font-bold text-base")
        ui.label().bind_text_from(
            state.security,
            "admin_user",
            lambda u: f"High-risk MCP tool call parked. Requires RFC-6238 TOTP + Ed25519 attestation ({u}):",
        ).classes("text-xs text-slate-300")
        refs.hitl_code = ui.code("{}", language="json").classes("w-full text-xs")
        kit.text(
            "6-Digit Admin TOTP Code (Leave blank for Auto-Derived Self-Verify)",
            state.security,
            "totp_input_code",
            flex=False,
            width="w-full mt-1",
        )
        with ui.row().classes("w-full justify-end gap-2 mt-3"):
            kit.button(
                "✕ Reject & Rollback",
                lambda: (
                    dialog.close(),
                    refs.push_pubsub("mcp:hitl", "🛑 REJECTED -> Transaction rolled back"),
                    ui.notify("Rejected & Rolled back", type="warning"),
                ),
                color="red",
                outline=True,
            )
            kit.button("✓ Verify TOTP & Approve", rt.oob.approve_hitl, color="emerald")


class QrModal(ModalTemplate):
    width_cls = "w-[440px] items-center"
    border_cls = "border border-cyan-500"

    def mount(self) -> ui.dialog:
        refs.qr_dialog = super().mount()
        return refs.qr_dialog

    def compose(self, dialog: ui.dialog) -> None:
        ui.label("📱 Scan Admin TOTP QR (Google Authenticator / Authy)").classes("text-cyan-400 font-bold text-sm")
        ui.label().bind_text_from(state.security, "admin_user", lambda u: f"SPIFFE ID: {u}").classes(
            "text-xs font-mono text-slate-300"
        )
        refs.qr_code_box = ui.code("", language="text").classes("text-[10px] leading-none font-mono bg-black p-3")
        ui.label().bind_text_from(state.security, "totp_secret", lambda s: f"Base32 Secret: {s}").classes(
            "text-xs font-mono text-amber-300"
        )
        kit.button("Close", dialog.close, color="cyan", outline=True).classes("w-full mt-2")

# Tab 1: MCP Gateway (`state.security`)
class McpGatewayTab(StudioTab):
    title = "MCP Gateway"
    left_header = "🔌 MCP Config Switchboard"
    right_header = "📡 Gateway Health (:8356) & Slack OOB Inspector"
    ratio = 46

    def __init__(self) -> None:
        self._configs_box: ui.row | None = None
        self._workers_box: ui.column | None = None

    def compose_left(self) -> None:
        s = state.security

        # 1. Agent Config Radar (1줄 컴팩트 헤더)
        with kit.row(align="items-center justify-between"):
            ui.label("🗂️ Agent Configs").classes(kit.CLS_SECTION_HEADER)
            with ui.row().classes("items-center gap-1"):
                self._configs_box = ui.row().classes("items-center gap-1 no-wrap")
                ui.button(
                    "🔄",
                    on_click=lambda: rt.oob.scan_mcp_topology_and_health(silent=False),
                ).props("flat dense size=xs color=cyan").tooltip("Rescan local MCP configs & health")

        # 2. Discovered & Mounted MCP Workers List
        with kit.row(align="items-center justify-between"):
            ui.label("🔌 Discovered MCP Workers & Sidecars").classes(kit.CLS_SECTION_HEADER)
            kit.button(
                "⚡ Connect All (.mcp.json)",
                lambda: rt.run_fiber_cli("connect"),
                color="emerald",
                outline=True,
            )

        self._workers_box = ui.column().classes("w-full gap-1 max-h-36 overflow-y-auto pr-1 shrink-0")

        # 3. Slack OOB Bridge & Identity Mapping
        ui.label("💬 Slack OOB Bridge & Identity Mapping").classes("text-xs font-bold text-amber-400 mt-1 shrink-0")
        with kit.row():
            kit.text("Slack Channel", s, "slack_channel", width="w-32")
            kit.text("Slack UID ('*')", s, "slack_user_map", width="w-28")
            kit.text("Mapped SPIFFE ID", s, "admin_user")

        with kit.bar():
            ui.label("OOB Auth Policy (TOTP vs 1-Click Sudo)").classes("text-xs font-mono text-slate-300")
            kit.switch("Require 6-Digit TOTP", s, "oob_require_totp", color="cyan")

        with kit.footer():
            kit.button("🔑 Init Keys", rt.oob.init_node_keys, color="slate-4", outline=True)
            kit.button("📱 Enroll TOTP QR", rt.oob.enroll_admin_totp, color="cyan", flex=True)
            kit.button("✓ Verify TOTP", rt.oob.verify_admin_totp, color="emerald", outline=True, flex=True)
            kit.button("🗑️", rt.oob.revoke_admin_totp, color="red", outline=True)

        self._render_dynamic_sections()

    def compose_right(self) -> None:
        """[복원] 우측 실시간 게이트웨이 헬스 배지 및 Slack BlockKit / HTTP 202 인스펙터"""
        s = state.security
        with kit.bar():
            ui.label().bind_text_from(s, "gateway_health_badge_text").classes(
                "text-xs font-mono font-bold text-emerald-400"
            )
            ui.label().bind_text_from(s, "slack_oob_badge_text").classes(
                "text-[11px] font-mono font-bold text-cyan-300"
            )

        with kit.row():
            kit.button(
                "🚨 Simulate 'drop_table' (HTTP 202)",
                lambda: rt.oob.open_hitl_modal(
                    "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="amber",
                text_color="black",
                flex=True,
            )
            kit.button(
                "📤 Dispatch Slack BlockKit",
                lambda: rt.oob.dispatch_slack_oob(
                    "postgres-prod-db", "drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="indigo",
                outline=True,
            )

        ui.label("Live Signed OOB StateToken & Slack BlockKit Payload (`mcp.bridge.slack`):").classes(
            "text-xs text-slate-400 shrink-0"
        )
        kit.editor(s, "oob_inspector_json")

    def _render_dynamic_sections(self) -> None:
        """발견된 설정 파일은 1줄 미니 칩으로, 없는 설정 파일은 드롭다운 메뉴 안으로 압축 렌더링"""
        s = state.security
        default_mcp_tpl = (
            '{\n  "mcpServers": {\n'
            '    "fiber-gateway": {\n      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"\n    }\n'
            "  }\n}\n"
        )

        if self._configs_box is not None:
            self._configs_box.clear()
            found_cfgs = [c for c in s.discovered_configs if c.get("exists")]
            missing_cfgs = [c for c in s.discovered_configs if not c.get("exists")]

            with self._configs_box:
                for cfg in found_cfgs:
                    ag = cfg["agent"]
                    p_str = cfg["path"]
                    with ui.row().classes(
                        "items-center gap-0.5 px-1.5 py-0 rounded border "
                        "border-emerald-500/60 bg-emerald-950/40 text-emerald-300 text-[10px] font-mono"
                    ):
                        ui.label(f"🟢 {ag}").tooltip(f"{cfg['name']} — {p_str}")
                        kit.file_link(
                            p_str,
                            label="",
                            tooltip=f"Edit {p_str}",
                            default_content=default_mcp_tpl,
                            create_if_missing=False,
                        )

                if missing_cfgs:
                    with ui.button(f"+ Init ({len(missing_cfgs)})").props(
                        "flat dense size=xs color=slate-4 no-caps"
                    ).classes("text-[10px] font-mono border border-slate-800 px-1.5 rounded"):
                        with ui.menu().classes("bg-slate-900 border border-slate-700 text-xs font-mono"):
                            for cfg in missing_cfgs:
                                p_str = cfg["path"]
                                with ui.menu_item().classes("py-1 px-2"):
                                    with ui.row().classes("items-center justify-between w-44 no-wrap"):
                                        ui.label(f"⚪ {cfg['agent']}").classes("text-slate-300 text-[11px]")
                                        kit.file_link(
                                            p_str,
                                            label="Create",
                                            tooltip=f"Create {p_str}",
                                            default_content=default_mcp_tpl,
                                            create_if_missing=True,
                                        )

        if self._workers_box is not None:
            self._workers_box.clear()
            with self._workers_box:
                if not s.discovered_workers:
                    ui.label("No MCP workers discovered yet. Click '🔄' or init a config above.").classes(
                        "text-xs font-mono text-slate-500 py-1"
                    )
                for w in s.discovered_workers:
                    tid = w["target_id"]
                    is_live = w["is_live"]
                    badge_txt = "🟢 LIVE" if is_live else "⚪ CONF"
                    guard_txt = f"🔒 {w['preflight'][:12]}" if w["preflight"] else "🔓 Open"
                    with kit.bar():
                        with ui.row().classes("items-center gap-1.5 no-wrap overflow-hidden"):
                            ui.label(badge_txt).classes(
                                "text-[10px] font-mono font-bold "
                                + ("text-emerald-400" if is_live else "text-slate-400")
                            )
                            ui.label(f"{tid}").classes("text-xs font-mono font-bold text-slate-100 truncate")
                            ui.label(f"[{w['source']}|{w['transport']}:{w['mode']}]").classes(
                                "text-[10px] font-mono text-cyan-400"
                            )
                        with ui.row().classes("items-center gap-1.5 shrink-0"):
                            ui.badge(guard_txt).props("color=slate-8 text-color=amber-3").classes(
                                "text-[10px] font-mono"
                            )
                            kit.button(
                                "▶ Mount",
                                lambda t=tid, c=w["exec_cmd"]: rt.run_fiber_cli("connect", "-t", t, "-e", c),
                                color="emerald",
                                outline=True,
                            )

    def flush(self) -> None:
        """[복원] 100ms 틱마다 토폴로지 변경 플래그를 확인하여 변경 시에만 재렌더링"""
        if state.security.mcp_topology_dirty:
            state.security.mcp_topology_dirty = False
            self._render_dynamic_sections()


# ==========================================
# Tab 2: Live LLM Pipeline X-Ray (`state.security`)
# ==========================================
class LLMLiveTab(StudioTab):
    title = "LLM Live"
    left_header = "🛡️ Secret Guardrail Config"
    right_header = "🔬 Slot Setting & Latency Breakdown"

    def compose_left(self) -> None:
        s = state.security
        kit.switch("Active Secret Blocking (POST_TRANSLATE)", s, "block_api_keys", color="red")
        kit.text("Custom C-Level Regex Pattern", s, "custom_regex", flex=False, width="w-full")
        kit.select(
            ["Strict Block (PermissionError)", "Redact (Mask with ***)"],
            "Action on Match",
            s,
            "guardrail_mode",
            width="w-full",
        )
        kit.text("Test Payload Prompt", s, "test_prompt", flex=False, width="w-full mt-2")

        with kit.footer():
            kit.button("⚡ Test Secret Leak", lambda: rt.pipeline.run_xray(s.test_prompt), color="red", flex=True)
            kit.button(
                "✓ Clean Prompt",
                lambda: rt.pipeline.run_xray("Summarize Q3 financial metrics."),
                color="emerald",
                outline=True,
                flex=True,
            )

    def compose_right(self) -> None:
        s = state.security
        with kit.bar():
            for key, title, slot in [
                ("cache", "1. EdgeCache", "PRE_TRANSLATE"),
                ("router", "2. ONNX Router", "PRE_TRANSLATE"),
                ("guardrail", "3. Secret Guard", "POST_TRANSLATE"),
                ("fuel", "4. FuelBreaker", "STREAM_IO"),
            ]:
                with ui.card().classes("bg-slate-800 border border-slate-700 p-2 w-36 text-center") as c:
                    refs.slot_cards[key] = c
                    ui.label(title).classes("text-xs font-bold text-white")
                    ui.label(slot).classes("text-[10px] font-mono text-slate-400")

        ui.label("Per-Slot Execution Overhead (ms) - Sub-Millisecond Pipeline Telemetry").classes(
            "text-xs text-slate-400 mt-1 shrink-0"
        )
        refs.xray_chart = kit.bar_chart(
            categories=["4. FuelBreaker", "3. SecretGuard", "2. ONNX Router", "1. EdgeCache"],
            data=list(s.xray_latencies),
            x_max=1.0,
        )

# Tab 3: FinOps Cockpit & Account CLI (`state.finops`)
class FinOpsTab(StudioTab):
    title = "FinOps"
    left_header = "🎛️ FinOps Account(Tenant) Control"
    right_header = "📈 FuelBreaker Telemetry"

    def compose_left(self) -> None:
        f = state.finops
        with kit.row():
            kit.text("Target Tenant ID", f, "tenant_id")
            kit.text("Bound SPIFFE ID (Optional)", f, "spiffe_id")
            kit.number("Policy Ver", f, "policy_ver", min_val=1, max_val=99, width="w-20", as_int=True)

        with kit.row():
            kit.select(["PROVISION", "TOP_UP", "SYNC_QUOTA", "REVOKE"], "Mandate Op", f, "jwt_op")
            kit.number("USD ($)", f, "jwt_usd", min_val=5.0, max_val=500.0, step=10.0, width="w-24")
            kit.switch("Sync Core", f, "sync_ledger", color="emerald")

        with kit.row(gap="gap-1.5"):
            kit.button("🔑 Issue JWT", rt.gov.issue_account_jwt, color="emerald", flex=True)
            kit.button(
                "🔄 5-Step Demo",
                lambda: rt.run_fiber_cli(
                    "account",
                    "--tenant",
                    f.tenant_id,
                    *(["--spiffe", f.spiffe_id.strip()] if f.spiffe_id and f.spiffe_id.strip() else []),
                    "demo",
                ),
                color="cyan",
                outline=True,
            )
            kit.button("📋 Status", lambda: rt.gov.refresh_account_status(silent=False), color="slate-4", outline=True)
            kit.button("📂 List", lambda: rt.run_fiber_cli("account", "list"), color="slate-4", outline=True)

        ui.label().bind_text_from(f, "last_jwt_preview", lambda j: f"Latest EdDSA JWT: {j}").classes(kit.CLS_MONO_LABEL)

        ui.label("Emergency Cutoff Threshold (Fuel Redline):").classes("text-xs text-amber-300 font-mono mt-1 shrink-0")
        ui.slider(min=0, max=60000, step=5000, on_change=lambda: setattr(f, "chart_dirty", True)).bind_value(
            f, "budget_limit"
        ).props("label-always color=red dense")

        with kit.footer():
            kit.button(
                "▶ Simulate High-Speed Stream (Test FuelBreaker)",
                rt.pipeline.simulate_stream,
                color="indigo",
                flex=True,
            )

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
            redline=f.budget_limit,
        )
        refs.live_stream_log = kit.log(max_lines=60, color="text-emerald-300")


# ==========================================
# Tab 4: Deterministic VCR & HTTP Gateway Suite (`state.pipeline`)
# ==========================================
class VcrTab(StudioTab):
    title = "VCR & Compat"
    left_header = "🧬 Config VCR & Compat Ruleset"
    right_header = "⏱️ Live Speed Benchmark"
    ratio = 46

    def compose_left(self) -> None:
        p = state.pipeline
        with kit.row():
            kit.select(["live", "record", "replay", "auto"], "Per-Request VCR Mode", p, "vcr_mode")
            kit.select(["max", "real"], "Replay Speed", p, "vcr_speed", width="w-28")
            kit.text("Scenario Name", p, "vcr_scenario")

        with kit.row(align="items-center justify-between"):
            ui.label("Declarative Provider Ruleset (FIBER_COMPAT_RULES_PATH):").classes(
                "text-xs text-slate-400 shrink-0"
            )
            kit.file_link(
                _resolve_compat_rules_path,
                label="compat_rules.json",
                tooltip="Open FIBER_COMPAT_RULES_PATH in external editor",
                default_content=p.compat_json,
                create_if_missing=True,
            )

        kit.editor(p, "compat_json")

    def compose_right(self) -> None:
        p = state.pipeline
        with kit.row():
            kit.button("▶ Run 3-Step Gateway VCR Suite", rt.pipeline.run_gateway_vcr_suite, color="emerald", flex=True)
            kit.button(
                "🧬 Verify Compat Fixture",
                lambda: rt.run_fiber_cli("compat", "fixture", "--vcr", "replay"),
                color="cyan",
                outline=True,
            )

        ui.label("Live LLM Network I/O vs. Deterministic VCR Replay Latency (ms):").classes(
            "text-xs text-slate-400 mt-1 shrink-0"
        )
        refs.vcr_chart = kit.bar_chart(
            categories=["VCR Replay", "Live LLM"],
            data=[
                {"value": p.vcr_bench_replay_ms, "itemStyle": {"color": "#10b981"}},
                {"value": p.vcr_bench_live_ms, "itemStyle": {"color": "#f59e0b"}},
            ],
            formatter="{c} ms",
        )

def build_tabs() -> list[StudioTab]:
    return [McpGatewayTab(), LLMLiveTab(), FinOpsTab(), VcrTab()]


def build_modals() -> list[ModalTemplate]:
    return [HitlModal(), QrModal()]