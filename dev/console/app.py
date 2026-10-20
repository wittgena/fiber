# fiber.dev.console.app
import asyncio
from typing import Callable
from nicegui import app, ui

from fiber.dev.console.state import state, refs
import fiber.dev.console.runtime as rt


def render_split_studio(
    left_title: str, left_pane: Callable[[], None],
    right_title: str, right_pane: Callable[[], None],
    ratio: int = 42
):
    """모든 탭이 공유하는 [좌측: 선언적 설정(Config) <-> 우측: 실시간 증명(Action & Chart)] 템플릿"""
    with ui.splitter(value=ratio).classes("w-full h-[450px] bg-slate-900 rounded border border-slate-800") as sp:
        with sp.before:
            with ui.column().classes("p-4 gap-2.5 w-full h-full overflow-y-auto"):
                ui.label(left_title).classes("text-sm font-bold text-white border-b border-slate-800 pb-1.5 w-full")
                left_pane()
        with sp.after:
            with ui.column().classes("p-4 gap-2.5 w-full h-full overflow-y-auto"):
                ui.label(right_title).classes("text-sm font-bold text-white border-b border-slate-800 pb-1.5 w-full")
                right_pane()


def render_modals():
    """1) HTTP 202 HITL 승인 모달 (TOTP 연동) & 2) Admin TOTP QR 스캔 모달"""
    with ui.dialog().props("persistent") as refs.hitl_dialog, ui.card().classes("bg-slate-900 border-2 border-amber-500 w-[520px] p-5"):
        ui.label("⚠️ HTTP 202: Human-in-the-Loop Cryptographic Gate").classes("text-amber-400 font-bold text-base")
        ui.label().bind_text_from(
            state.security,
            "admin_user",
            lambda u: f"High-risk MCP tool call parked. Requires RFC-6238 TOTP + Ed25519 attestation ({u}):",
        ).classes("text-xs text-slate-300")
        refs.hitl_code = ui.code("{}", language="json").classes("w-full text-xs")
        ui.input("6-Digit Admin TOTP Code (Leave blank for Auto-Derived Self-Verify)").bind_value(
            state.security, "totp_input_code"
        ).props("dark outlined dense").classes("w-full font-mono text-xs mt-1")
        with ui.row().classes("w-full justify-end gap-2 mt-3"):
            ui.button(
                "✕ Reject & Rollback",
                on_click=lambda: (
                    refs.hitl_dialog.close(),
                    refs.push_pubsub("mcp:hitl", "🛑 REJECTED -> Transaction rolled back"),
                    ui.notify("Rejected & Rolled back", type="warning"),
                ),
            ).props("color=red outline")
            ui.button("✓ Verify TOTP & Approve", on_click=rt.act_approve_hitl).props("color=emerald")

    with ui.dialog() as refs.qr_dialog, ui.card().classes("bg-slate-900 border border-cyan-500 w-[440px] p-5 items-center"):
        ui.label("📱 Scan Admin TOTP QR (Google Authenticator / Authy)").classes("text-cyan-400 font-bold text-sm")
        ui.label().bind_text_from(state.security, "admin_user", lambda u: f"SPIFFE ID: {u}").classes("text-xs font-mono text-slate-300")
        refs.qr_code_box = ui.code("", language="text").classes("text-[10px] leading-none font-mono bg-black p-3")
        ui.label().bind_text_from(state.security, "totp_secret", lambda s: f"Base32 Secret: {s}").classes("text-xs font-mono text-amber-300")
        ui.button("Close", on_click=lambda: refs.qr_dialog.close()).props("color=cyan outline").classes("w-full mt-2")


# ==========================================
# Tab 1: FinOps Cockpit & Account CLI (`state.finops`)
# ==========================================
def finops_left_pane():
    f = state.finops
    with ui.row().classes("w-full gap-2"):
        ui.input("Target Tenant ID").bind_value(f, "tenant_id").props("dark outlined dense").classes("flex-1 font-mono text-xs")
        ui.input("Bound SPIFFE ID (Optional)").bind_value(f, "spiffe_id").props("dark outlined dense").classes("flex-1 font-mono text-xs")
        ui.number("Policy Ver", min=1, max=99).bind_value(f, "policy_ver", forward=int).props("dark outlined dense").classes("w-20 font-mono text-xs")

    with ui.row().classes("w-full gap-2 items-center"):
        ui.select(["PROVISION", "TOP_UP", "SYNC_QUOTA", "REVOKE"], label="Mandate Op").bind_value(f, "jwt_op").props("dark outlined dense").classes("flex-1 text-xs")
        ui.number("USD ($)", min=5.0, max=500.0, step=10.0).bind_value(f, "jwt_usd").props("dark outlined dense").classes("w-24 text-xs")
        ui.switch("Sync Core").bind_value(f, "sync_ledger").props("color=emerald dense").classes("text-xs")

    with ui.row().classes("w-full gap-1.5"):
        ui.button("🔑 Issue JWT", on_click=rt.act_issue_account_jwt).props("color=emerald dense").classes("flex-1 text-xs font-bold")
        ui.button(
            "🔄 5-Step Demo",
            on_click=lambda: rt.run_fiber_cli(
                "account", "--tenant", f.tenant_id,
                *(["--spiffe", f.spiffe_id.strip()] if f.spiffe_id and f.spiffe_id.strip() else []),
                "demo"
            ),
        ).props("outline color=cyan dense").classes("text-xs px-2")
        ui.button("📋 Status", on_click=rt.act_refresh_account_status).props("outline color=slate-4 dense").classes("text-xs px-2")
        ui.button("📂 List", on_click=lambda: rt.run_fiber_cli("account", "list")).props("outline color=slate-4 dense").classes("text-xs px-2")

    ui.label().bind_text_from(f, "last_jwt_preview", lambda j: f"Latest EdDSA JWT: {j}").classes(
        "text-[11px] font-mono text-slate-400 bg-slate-950 px-2.5 py-1.5 rounded border border-slate-800 w-full truncate"
    )

    ui.label("Emergency Cutoff Threshold (Fuel Redline):").classes("text-xs text-amber-300 font-mono mt-1")
    ui.slider(min=0, max=60000, step=5000, on_change=lambda: setattr(f, "chart_dirty", True)).bind_value(f, "budget_limit").props("label-always color=red dense")
    ui.button("▶ Simulate High-Speed Stream (Test FuelBreaker)", on_click=rt.act_simulate_stream).props("color=indigo").classes("w-full font-bold mt-auto")


def finops_right_pane():
    f = state.finops
    with ui.row().classes("w-full justify-between items-center bg-slate-950 px-3 py-1.5 rounded border border-slate-800"):
        ui.label().bind_text_from(f, "account_summary_badge").classes("text-xs font-mono text-cyan-300 font-bold")
        ui.badge().bind_text_from(f, "stream_status_text").props("color=slate-8 text-color=amber-3")

    refs.finops_chart = ui.echart({
        "tooltip": {"trigger": "axis"},
        "legend": {"data": ["Active Quota (FinOps)", "Core Balance (PTA)"], "textStyle": {"color": "#94a3b8", "fontSize": 10}, "top": 0},
        "grid": {"top": 26, "bottom": 20, "left": 55, "right": 20},
        "xAxis": {"type": "category", "data": list(f.x_ticks), "axisLabel": {"color": "#64748b", "fontSize": 10}},
        "yAxis": {"type": "value", "min": 0, "max": 75000, "splitLine": {"lineStyle": {"color": "#1e293b"}}, "axisLabel": {"color": "#64748b", "fontSize": 10}},
        "series": [
            {
                "name": "Active Quota (FinOps)", "type": "line", "data": list(f.y_fuel), "smooth": True,
                "itemStyle": {"color": "#10b981"}, "areaStyle": {"color": "rgba(16, 185, 129, 0.12)"},
                "markLine": {"silent": True, "lineStyle": {"color": "#ef4444", "width": 2, "type": "dashed"}, "data": [{"yAxis": f.budget_limit}]}
            },
            {
                "name": "Core Balance (PTA)", "type": "line", "data": list(f.y_core), "step": "end",
                "lineStyle": {"color": "#06b6d4", "type": "dotted"}, "itemStyle": {"color": "#06b6d4"}
            }
        ]
    }).classes("w-full h-40")
    refs.live_stream_log = ui.log(max_lines=60).classes("w-full flex-1 bg-black border border-slate-800 p-2 font-mono text-xs text-emerald-300")


# ==========================================
# Tab 2: Live Pipeline X-Ray & Latency Chart (`state.security`)
# ==========================================
def xray_left_pane():
    s = state.security
    ui.switch("Active Secret Blocking (POST_TRANSLATE)").bind_value(s, "block_api_keys").props("color=red")
    ui.input("Custom C-Level Regex Pattern").bind_value(s, "custom_regex").classes("w-full font-mono text-xs").props("dark outlined dense")
    ui.select(["Strict Block (PermissionError)", "Redact (Mask with ***)"], label="Action on Match").bind_value(s, "guardrail_mode").classes("w-full text-xs").props("dark outlined dense")
    ui.input("Test Payload Prompt").bind_value(s, "test_prompt").classes("w-full font-mono text-xs mt-2").props("dark outlined dense")
    with ui.row().classes("w-full gap-2 mt-auto"):
        ui.button("⚡ Test Secret Leak", on_click=lambda: rt.act_run_xray(s.test_prompt)).props("color=red").classes("flex-1 text-xs font-bold")
        ui.button("✓ Clean Prompt", on_click=lambda: rt.act_run_xray("Summarize Q3 financial metrics.")).props("outline color=emerald").classes("flex-1 text-xs")


def xray_right_pane():
    s = state.security
    with ui.row().classes("w-full justify-between bg-slate-950 p-3 rounded border border-slate-800"):
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

    ui.label("Per-Slot Execution Overhead (ms) - Sub-Millisecond Pipeline Telemetry").classes("text-xs text-slate-400 mt-1")
    refs.xray_chart = ui.echart({
        "grid": {"top": 10, "bottom": 25, "left": 95, "right": 30},
        "xAxis": {"type": "value", "max": 1.0, "splitLine": {"lineStyle": {"color": "#1e293b"}}, "axisLabel": {"color": "#64748b", "fontSize": 10}},
        "yAxis": {"type": "category", "data": ["4. FuelBreaker", "3. SecretGuard", "2. ONNX Router", "1. EdgeCache"], "axisLabel": {"color": "#cbd5e1", "fontSize": 10}},
        "series": [{"type": "bar", "data": list(s.xray_latencies), "itemStyle": {"color": "#38bdf8"}, "label": {"show": True, "position": "right", "color": "#e2e8f0", "fontSize": 10}}]
    }).classes("w-full flex-1")


# ==========================================
# Tab 3: MCP Switchboard & Local Node Identity (`fiber sign`)
# ==========================================
def mcp_left_pane():
    s = state.security
    ui.label("🔌 Active MCP Workers (.mcp.json)").classes("text-xs font-bold text-cyan-400")
    for name, mode, hitl in [
        ("postgres-prod-db", "stdio | linear", True),
        ("repomix-worker", "stdio | multiplex", False),
        ("ctx7-remote", "http/sse | Bearer", False),
    ]:
        with ui.row().classes("w-full justify-between items-center bg-slate-950 px-3 py-1.5 rounded border border-slate-800"):
            ui.label(f"{name} [{mode}]").classes("text-xs font-mono text-slate-200")
            with ui.row().classes("gap-2"):
                ui.switch("HITL 202", value=hitl).props("color=amber dense").classes("text-xs")
                ui.switch("Mount", value=True).props("color=emerald dense").classes("text-xs")

    ui.label("🔐 Local Node Keys & TOTP Admin (`fiber sign`)").classes("text-xs font-bold text-amber-400 mt-1")
    with ui.row().classes("w-full gap-2 items-center"):
        ui.input("Admin SPIFFE ID").bind_value(s, "admin_user").props("dark outlined dense").classes("flex-1 font-mono text-xs")
        ui.button("🔑 Init Keys", on_click=rt.act_init_node_keys).props("color=emerald outline dense").classes("text-xs px-2.5")
        ui.button("🔄 Status", on_click=rt.act_refresh_sign_status).props("color=slate-4 outline dense").classes("text-xs px-2")

    with ui.row().classes("w-full gap-1.5 mt-auto"):
        ui.button("📱 Enroll TOTP", on_click=rt.act_enroll_admin_totp).props("color=cyan").classes("flex-1 text-xs font-bold")
        ui.button("✓ Verify TOTP", on_click=lambda: rt.act_verify_admin_totp()).props("color=emerald outline").classes("flex-1 text-xs")
        ui.button("🗑️ Revoke", on_click=rt.act_revoke_admin_totp).props("color=red outline").classes("text-xs px-2.5")


def mcp_right_pane():
    s = state.security
    # 1. ~/.ssh/fiber_ed25519 및 local_cipher.key 물리 파일 존재 여부 가시성 카드
    with ui.row().classes("w-full gap-2"):
        with ui.card().classes("flex-1 bg-slate-950 border border-slate-800 p-2.5"):
            ui.label().bind_text_from(s, "ssh_badge_text").classes("text-xs font-mono font-bold text-emerald-400")
            ui.label().bind_text_from(s, "ssh_key_path", lambda p: f"Path: {p}").classes("text-[10px] font-mono text-slate-400 truncate")
        with ui.card().classes("flex-1 bg-slate-950 border border-slate-800 p-2.5"):
            ui.label().bind_text_from(s, "cipher_badge_text").classes("text-xs font-mono font-bold text-cyan-400")
            ui.label().bind_text_from(s, "cipher_key_path", lambda p: f"Path: {p}").classes("text-[10px] font-mono text-slate-400 truncate")

    # 2. NodeSigner Public Key 지문 & HTTP 202 HITL 샌드박스 트리거
    with ui.row().classes("w-full justify-between items-center bg-slate-950 px-3 py-2 rounded border border-slate-800"):
        ui.label().bind_text_from(s, "pubkey_short_text").classes("text-xs font-mono text-amber-300 font-bold")
        ui.button("🚨 Simulate 'drop_table' (HTTP 202)", on_click=lambda: rt.act_open_hitl_modal(
            "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
        )).props("color=amber text-color=black dense").classes("text-xs font-bold px-3")

    ui.label("Live Keychain & SQLite TOTP Audit Registry (`fiber sign status`):").classes("text-xs text-slate-400")
    ui.codemirror(language="json", theme="oneDark").bind_value(s, "sign_status_json").classes("w-full flex-1 text-xs border border-slate-800")


# ==========================================
# Tab 4: VCR Time-Machine & HTTP Gateway Suite (`state.pipeline`)
# ==========================================
def vcr_left_pane():
    p = state.pipeline
    with ui.row().classes("w-full gap-2"):
        ui.select(["live", "record", "replay", "auto"], label="Per-Request VCR Mode").bind_value(p, "vcr_mode").props("dark outlined dense").classes("flex-1 text-xs")
        ui.select(["max", "real"], label="Replay Speed").bind_value(p, "vcr_speed").props("dark outlined dense").classes("w-28 text-xs")
        ui.input("Scenario Name").bind_value(p, "vcr_scenario").props("dark outlined dense").classes("flex-1 font-mono text-xs")

    ui.label("Declarative Provider Ruleset (FIBER_COMPAT_RULES_PATH):").classes("text-xs text-slate-400")
    ui.codemirror(language="json", theme="oneDark").bind_value(p, "compat_json").classes("w-full flex-1 text-xs border border-slate-800")


def vcr_right_pane():
    p = state.pipeline
    with ui.row().classes("w-full gap-2"):
        ui.button("▶ Run 3-Step Gateway VCR Suite", on_click=rt.act_run_gateway_vcr_suite).props("color=emerald").classes("flex-1 text-xs font-bold")
        ui.button("🧬 Verify Compat Fixture", on_click=lambda: rt.run_fiber_cli("compat", "fixture", "--vcr", "replay")).props("outline color=cyan").classes("text-xs")

    ui.label("Live LLM Network I/O vs. Deterministic VCR Replay Latency (ms):").classes("text-xs text-slate-400 mt-1")
    refs.vcr_chart = ui.echart({
        "grid": {"top": 15, "bottom": 25, "left": 90, "right": 45},
        "xAxis": {"type": "value", "splitLine": {"lineStyle": {"color": "#1e293b"}}, "axisLabel": {"color": "#64748b", "fontSize": 10}},
        "yAxis": {"type": "category", "data": ["VCR Replay", "Live LLM"], "axisLabel": {"color": "#cbd5e1", "fontSize": 11}},
        "series": [{
            "type": "bar",
            "data": [
                {"value": p.vcr_bench_replay_ms, "itemStyle": {"color": "#10b981"}},
                {"value": p.vcr_bench_live_ms, "itemStyle": {"color": "#f59e0b"}}
            ],
            "label": {"show": True, "position": "right", "formatter": "{c} ms", "color": "#e2e8f0", "fontSize": 11}
        }]
    }).classes("w-full flex-1")


# ==========================================
# 콘솔 쉘 조립 - 상/중/하 2-Depth 구조
# ==========================================
def build_console_shell():
    ui.dark_mode().enable()
    ui.query("body").classes("bg-slate-950 text-slate-100 select-none")
    render_modals()

    # [상단: Header Bar]
    with ui.header().classes("bg-slate-900 border-b border-slate-800 px-6 py-2.5 items-center justify-between"):
        with ui.row().classes("items-center gap-3"):
            ui.label("⚡ FIBER-CONSOLE").classes("text-base font-extrabold tracking-wider")
            ui.badge().bind_text_from(state, "redis_badge_text").props("color=slate-8 text-color=emerald-3").classes("text-xs px-2 py-1")
            ui.badge().bind_text_from(state.security, "ssh_badge_text").props("color=slate-8 text-color=cyan-3").classes("text-xs px-2 py-1")
        with ui.row().classes("items-center gap-5"):
            ui.label().bind_text_from(state, "saved_kpi_text").classes("text-xs font-mono font-bold text-cyan-400")
            ui.label().bind_text_from(state, "fuel_kpi_text").classes("text-xs font-mono font-bold text-emerald-400")
            ui.button(
                "🚨 GLOBAL KILL-SWITCH",
                on_click=lambda: rt.run_fiber_cli("account", "--tenant", state.finops.tenant_id, "kill", "--ver", "99")
            ).props("color=red dense").classes("font-bold text-xs px-3")

    # [중단: 4 Core Showcase Tabs]
    with ui.tabs().classes("w-full bg-slate-900 border-b border-slate-800") as tabs:
        t1 = ui.tab("1. FinOps & Account")
        t2 = ui.tab("2. Pipeline X-Ray")
        t3 = ui.tab("3. MCP & Node Keys")
        t4 = ui.tab("4. VCR & Gateway")

    with ui.tab_panels(tabs, value=t1).classes("w-full bg-slate-950 p-3"):
        with ui.tab_panel(t1).classes("p-0"):
            render_split_studio("🎛️ LocalAccount & Mandate Control (fiber account)", finops_left_pane, "📈 Dual-Ledger Telemetry & FuelBreaker", finops_right_pane)
        with ui.tab_panel(t2).classes("p-0"):
            render_split_studio("🛡️ Secret Guardrail & Regex Config", xray_left_pane, "🔬 Duplex Slot X-Ray & Latency Breakdown", xray_right_pane)
        with ui.tab_panel(t3).classes("p-0"):
            render_split_studio("🔌 MCP Switchboard & Keychain (fiber sign)", mcp_left_pane, "🔐 Dual-Key Status (~/.ssh & local_cipher) & HITL 202", mcp_right_pane, ratio=44)
        with ui.tab_panel(t4).classes("p-0"):
            render_split_studio("🧬 Gateway VCR & Compat Ruleset", vcr_left_pane, "⏱️ Live vs. Replay Speed Benchmark (480x)", vcr_right_pane, ratio=46)

    # [하단: Dual Terminal Drawer]
    with ui.row().classes("w-full px-3 pb-3 gap-3 no-wrap"):
        with ui.card().classes("w-1/2 bg-black border border-slate-800 p-2.5"):
            with ui.row().classes("w-full justify-between items-center mb-1"):
                ui.label("💻 CLI & Subprocess Output (sys.executable)").classes("text-xs text-cyan-400 font-mono font-bold")
                ui.button("Clear", on_click=lambda: refs.cli_log.clear()).props("flat dense size=xs color=slate-4")
            refs.cli_log = ui.log(max_lines=250).classes("w-full h-32 font-mono text-xs text-slate-200")
        with ui.card().classes("w-1/2 bg-black border border-slate-800 p-2.5"):
            with ui.row().classes("w-full justify-between items-center mb-1"):
                ui.label("📡 Control Plane Event Bus (finops:* / mcp:* / sign:*)").classes("text-xs text-amber-400 font-mono font-bold")
                with ui.row().classes("items-center gap-2"):
                    ui.badge().bind_text_from(state.pipeline, "pubsub_eps", lambda n: f"Events: {n}").props("color=slate-8 text-color=amber-3")
                    ui.button("Clear", on_click=lambda: refs.pubsub_log.clear()).props("flat dense size=xs color=slate-4")
            refs.pubsub_log = ui.log(max_lines=250).classes("w-full h-32 font-mono text-xs text-amber-200")

    ui.timer(0.1, rt.flush_100ms_ringbuffer)
    refs.push_cli("Fiber-Console initialized. Syncing local ~/.ssh/fiber_ed25519, local_cipher.key, and account ledgers...")


def launch_console(port: int = 8356):
    url = f"http://127.0.0.1:{port}"
    if rt.is_port_active(port):
        print(f"Attached to running Fiber instance at {url}")
        rt.open_app_window(url)
        return

    build_console_shell()
    app.on_startup(lambda: asyncio.create_task(rt.sync_initial_node_and_account_state()))
    app.on_startup(lambda: asyncio.create_task(rt.redis_pubsub_worker()))
    app.on_startup(lambda: rt.open_app_window(url))
    ui.run(host="127.0.0.1", port=port, title="Fiber Control Studio", show=False, reload=False)


if __name__ in {"__main__", "__mp_main__"}:
    launch_console(port=8356)