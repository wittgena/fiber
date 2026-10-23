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
                "Dispatch Slack HITL",
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

# Tab 2: Live LLM Pipeline X-Ray (`state.security`)
class LLMLiveTab(StudioTab):
    title = "LLM Live"
    left_header = "🛡️ Active Guardrail & Stream Probe"
    right_header = "🔬 Live Pipeline & Interceptor"

    @staticmethod
    def _ensure_telemetry_attrs(s: object) -> None:
        """`state.security`에 실시간 텔레메트리 필드가 없을 경우 안전하게 기본값 초기화"""
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

        ui.label("Secret Guardrail").classes("text-[11px] font-bold text-slate-400 shrink-0")
        kit.switch("Active Secret Blocking", s, "block_api_keys", color="red")
        kit.text("Guard Regex Pattern", s, "custom_regex", flex=False, width="w-full")
        kit.select(
            ["Strict Block (PermissionError)", "Redact (Mask with ***)"],
            "Guardrail Mode",
            s,
            "guardrail_mode",
            width="w-full",
        )

        ui.label("Pipeline Probe").classes("text-[11px] font-bold text-cyan-400 mt-1 shrink-0")
        kit.text("Test Prompt", s, "test_prompt", flex=False, width="w-full")

        with kit.row(gap="gap-1.5"):
            kit.button("⚡ CALL LLM", lambda: rt.pipeline.run_xray(s.test_prompt), color="red", flex=True)
            # kit.button(
            #     "✓ Clean Prompt",
            #     lambda: rt.pipeline.run_xray("Summarize Q3 financial metrics."),
            #     color="emerald",
            #     outline=True,
            #     flex=True,
            # )

        with kit.footer():
            kit.button(
                "🌊 Trigger Live Stream Pulse (2,200 c/s)",
                rt.pipeline.simulate_stream,
                color="indigo",
                outline=True,
                flex=True,
            ).tooltip("Drive live stream chunks through FuelBreaker to test velocity gauge & token counters")

    def compose_right(self) -> None:
        s = state.security
        self._ensure_telemetry_attrs(s)

        # 1. 실시간 파이프라인 활성도 KPI 바 (Active Connections, Tokens, Plane Pressure)
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

            # 우측: PlaneRegulator 실시간 이벤트 밀도(Density) 및 폭주(Burst) 인디케이터
            ui.badge().bind_text_from(
                s,
                "plane_density",
                lambda d: (
                    f"🔥 BURST ({float(d or 0.0):.1f} ev/s)"
                    if getattr(s, "plane_bursting", False)
                    else f"📡 Plane: {float(d or 0.0):.1f} ev/s"
                ),
            ).props("color=slate-9 text-color=cyan-3").classes("text-[10px] font-mono border border-slate-700 px-1.5")

        # 2. 실시간 스트림 처리 속도 게이지 (Throughput Velocity Gauge Bar)
        with ui.column().classes("w-full gap-0.5 bg-slate-950/90 px-3 py-1.5 rounded border border-slate-800 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label("⚡ Stream Interleaving Velocity (Target Max: 2,200 chunks/s)").classes(
                    "text-[11px] font-mono text-slate-400"
                )
                ui.label().bind_text_from(
                    s, "throughput_cps", lambda v: f"{float(v or 0.0):,.1f} chunks/s"
                ).classes("text-xs font-mono font-extrabold text-cyan-400")

            # 0.0 ~ 1.0 정규화 게이지 바 (2,200 chunks/s 기준 풀스케일)
            ui.linear_progress(value=0.0, show_value=False, size="6px", color="cyan").bind_value_from(
                s, "throughput_cps", lambda v: min(1.0, round(float(v or 0.0) / 2200.0, 3))
            ).classes("w-full rounded bg-slate-900")

        # 3. 4-Slot Pipeline X-Ray 상태 점등 카드
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

        # 4. 서브밀리초 슬롯별 오버헤드 바 차트
        ui.label("Per-Slot Execution Overhead (ms) - Sub-Millisecond Pipeline Telemetry").classes(
            "text-xs text-slate-400 mt-0.5 shrink-0"
        )
        refs.xray_chart = kit.bar_chart(
            categories=["4. FuelBreaker", "3. SecretGuard", "2. ONNX Router", "1. EdgeCache"],
            data=list(s.xray_latencies),
            x_max=1.0,
        )


# Tab 3: FinOps Cockpit & Account CLI (`state.finops`)
class FinOpsTab(StudioTab):
    title = "FinOps"
    left_header = "🎛️ FinOps Budget & Fuel Provisioning"
    right_header = "📈 Real-Time Fuel Burn & Circuit Breaker"

    def compose_left(self) -> None:
        f = state.finops

        # [Step 1] 대상 계정(Tenant) 식별
        ui.label("1. Target Account & Policy Identity").classes("text-[11px] font-bold text-slate-400 shrink-0")
        with kit.row():
            kit.text("Target Tenant ID", f, "tenant_id")
            kit.text("Bound SPIFFE ID (Optional)", f, "spiffe_id")
            kit.number("Policy Ver", f, "policy_ver", min_val=1, max_val=99, width="w-20", as_int=True)

        # [Step 2] 달러($) -> Fuel 환산 및 서명된 예산 토큰(Mandate) 주입
        with kit.row(align="items-center justify-between"):
            ui.label("2. Fuel Allocation Mandate ($1 = 1,000 Fuel)").classes(
                "text-[11px] font-bold text-emerald-400 shrink-0"
            )
            # 달러 입력 시 실제로 몇 Fuel이 움직이는지 실시간 프리뷰 제공 (Header 수치와 직결됨을 암시)
            ui.label().bind_text_from(
                f, "jwt_usd", lambda u: f"➔ {int((u or 0) * 1000):,} Fuel ({f.jwt_op})"
            ).classes("text-[11px] font-mono font-bold text-amber-300")

        with kit.row():
            kit.select(["PROVISION", "TOP_UP", "SYNC_QUOTA", "REVOKE"], "Mandate Action", f, "jwt_op")
            kit.number("Budget ($)", f, "jwt_usd", min_val=5.0, max_val=500.0, step=10.0, width="w-24")
            kit.switch("Sync Balance", f, "sync_ledger", color="emerald").tooltip(
                "When enabled, also updates the persistent Ledger Balance alongside Remaining Fuel"
            )

        with kit.row(gap="gap-1.5"):
            kit.button(
                "⛽ Apply Fuel Mandate (Mint JWT)",
                rt.gov.issue_account_jwt,
                color="emerald",
                flex=True,
            ).tooltip("Sign an EdDSA billing mandate JWT and update Header Remaining Fuel & Balance")
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
            ).tooltip("Walk through Provision -> Top-Up -> Stale Replay Reject -> Revoke")
            kit.button("📋 Sync", lambda: rt.gov.refresh_account_status(silent=False), color="slate-4", outline=True)
            kit.button("📂 List", lambda: rt.run_fiber_cli("account", "list"), color="slate-4", outline=True)

        ui.label().bind_text_from(
            f, "last_jwt_preview", lambda j: f"Signed Mandate Token (EdDSA): {j}"
        ).classes(kit.CLS_MONO_LABEL)

        # [Step 3] 실시간 스트림 소모 및 킬스위치(Redline) 테스트
        ui.label("3. Mid-Stream Circuit Breaker Threshold (Redline):").classes(
            "text-[11px] font-bold text-amber-400 mt-1 shrink-0"
        )
        ui.slider(min=0, max=60000, step=5000, on_change=lambda: setattr(f, "chart_dirty", True)).bind_value(
            f, "budget_limit"
        ).props("label-always color=red dense")

        with kit.footer():
            kit.button(
                "▶ Burn Fuel via High-Speed Stream (Test HTTP 402 Breaker)",
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

# Tab 5: Model Pricing & Fuel Exchange Matrix
class ModelRatesTab(StudioTab):
    title = "Model Pricing"
    left_header = "💱 Model Pricing & Fuel Rate Board(Top 20)"
    right_header = "📐 Unit Parity & Settlement Reference"
    ratio = 58

    def compose_left(self) -> None:
        from fiber.llm.provider.cost.exchange import default_exchange
        from fiber.llm.provider.cost.registry import REGISTRY_ROOT, DEFAULT_REGISTRY_FILENAME

        with kit.row(align="items-center justify-between"):
            ui.label(
                "Supply Par: $1.00 = 1,000,000 Fuel (1M)  |  Local/Fallback Par: 1 Token = 1.0 Fuel"
            ).classes("text-[11px] font-mono font-bold text-emerald-400")
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
        from fiber.llm.provider.cost.exchange import default_exchange
        from fiber.llm.provider.cost.filter import CORE_KEYWORDS
        from fiber.llm.provider.cost.registry import ModelCostRegistry

        total_mapped = len(getattr(ModelCostRegistry, "_model_cost", {}) or {})
        usd_rate = getattr(default_exchange, "usd_rate", 1_000_000)
        token_par = getattr(default_exchange, "token_par", 1.0)

        # 상단 레지스트리 및 기준 환율 요약 바
        with kit.bar():
            ui.badge(f"📚 Listed Models: {total_mapped:,}").props(
                "color=slate-8 text-color=cyan-3"
            ).classes("text-[11px] font-mono font-bold px-2 py-0.5")
            ui.badge(f"🔌 Active Providers: {len(CORE_KEYWORDS)}").props(
                "color=slate-8 text-color=emerald-3"
            ).classes("text-[11px] font-mono font-bold px-2 py-0.5")
            ui.badge(f"1 USD = {usd_rate:,} Fuel").props(
                "color=slate-9 text-color=amber-3"
            ).classes("text-[11px] font-mono border border-amber-500/40 px-2 py-0.5")

        # 기본 단위 환산표 (Micro-Dollar & Token Par Reference Cards)
        ui.label("Unit Parity & Exchange Reference").classes(
            "text-[11px] font-bold text-slate-400 shrink-0"
        )
        unit_specs = [
            ("💵 $1.00 USD", f"{usd_rate:,} Fuel", "Mandate Minting Supply (1M Fuel)"),
            ("🪙 $0.001 (1 m$)", f"{usd_rate // 1000:,} Fuel", "1K Baseline Token Equivalent"),
            ("🔬 $0.000001 (1 μ$)", "1.0 Fuel", "Atomic Ledger Unit ($1 / 1M Tokens)"),
            ("🏠 1 Local Token", f"{token_par:.1f} Fuel", "Unmapped / Local Model Fallback (1:1)"),
        ]
        with ui.element("div").classes("grid grid-cols-2 gap-1.5 w-full shrink-0"):
            for src_unit, dst_fuel, desc in unit_specs:
                with ui.column().classes("bg-slate-900/90 border border-slate-800 rounded px-2.5 py-1.5 gap-0"):
                    with ui.row().classes("w-full items-center justify-between no-wrap"):
                        ui.label(src_unit).classes("text-xs font-mono font-bold text-slate-200")
                        ui.label(f"➔ {dst_fuel}").classes("text-xs font-mono font-extrabold text-emerald-400")
                    ui.label(desc).classes("text-[10px] font-mono text-slate-400 truncate")

        # 파이프라인 2단계(Dual-Mode) 차감 수식 레퍼런스
        ui.label("Dual-Mode Settlement & Equivalence Rules (`FuelExchange`)").classes(
            "text-[11px] font-bold text-cyan-400 mt-1 shrink-0"
        )
        with ui.column().classes(
            "w-full bg-slate-950 border border-slate-800 rounded p-2 gap-1 text-[11px] font-mono shrink-0"
        ):
            ui.label("• Rate Par    : Price ($ / 1M Tokens) == Burn Rate (Fuel / Token)").classes("text-amber-300")
            ui.label("• Hot-Path Stream: FuelBreaker meters live tokens 1:1 via in-memory tiktoken").classes(
                "text-slate-300"
            )
            ui.label("• Priced Models  : Consumed Fuel = ceil(Billed_USD × 1,000,000)").classes("text-emerald-300")
            ui.label("• Local Fallback : Consumed Fuel = ceil(Total_Tokens × 1.0)  [when cost == $0.0]").classes(
                "text-cyan-300"
            )

        # 모델 티어별 1,000 토큰 호출 시 Fuel 소모량 비교 바 차트
        ui.label("Fuel Burn per 1,000 Output Tokens by Model Tier:").classes(
            "text-xs text-slate-400 mt-0.5 shrink-0"
        )
        kit.bar_chart(
            categories=[
                "Frontier ($15/1M)",
                "Flagship ($10/1M)",
                "Local 1:1 Par ($0)",
                "Flash Tier ($0.6/1M)",
            ],
            data=[
                {"value": 15000, "itemStyle": {"color": "#f43f5e"}},
                {"value": 10000, "itemStyle": {"color": "#f59e0b"}},
                {"value": 1000, "itemStyle": {"color": "#06b6d4"}},
                {"value": 600, "itemStyle": {"color": "#10b981"}},
            ],
            formatter="{c} Fuel",
        )

def build_tabs() -> list[StudioTab]:
    return [LLMLiveTab(), McpGatewayTab(), FinOpsTab(), VcrTab(), ModelRatesTab()]


def build_modals() -> list[ModalTemplate]:
    return [HitlModal(), QrModal()]