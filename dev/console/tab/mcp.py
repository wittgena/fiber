# fiber.dev.console.tab.mcp
from __future__ import annotations

from pathlib import Path
from nicegui import ui
import orjson

from fiber.infra.account.state import state, refs
from fiber.dev.console.action import oob, gov, pipeline
from fiber.gateway.bridge.cli import run_fiber_cli
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate, StudioTab
from xphi.node.space.bind.resolver import resolve_path

def _get_anchor_mcp_path() -> Path:
    return resolve_path("anchor") / "mcp.json"


def _load_anchor_specs_map() -> dict[str, dict]:
    """~/.anchor/mcp.json의 mcpServers 맵을 직접 읽어 {target_id: spec_dict} 반환"""
    p = _get_anchor_mcp_path()
    if not p.is_file():
        return {}
    try:
        data = orjson.loads(p.read_bytes())
        servers = data.get("mcpServers", {}) if isinstance(data, dict) else {}
        return servers if isinstance(servers, dict) else {}
    except Exception:
        return {}


# Modals: 1) HTTP 202 HITL Gate & 2) TOTP QR
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
            kit.button("✓ Verify TOTP & Approve", oob.approve_hitl, color="emerald")


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


# Tab 1: MCP Gateway
class McpGatewayTab(StudioTab):
    title = "MCP Gateway"
    left_header = "🔌 MCP Config & Worker Switchboard"
    right_header = "🛡️ Gateway Status & HITL Auth Gate"
    ratio = 52

    def __init__(self) -> None:
        self._configs_box: ui.row | None = None
        self._workers_box: ui.column | None = None
        self._roster_summary_label: ui.label | None = None

    def compose_left(self) -> None:
        """좌측: MCP 설정 파일 탐색 및 워커 프로세스 토폴로지 전담"""
        # 1. Fiber MCP Config (.anchor/mcp.json) & External Agent Configs
        with kit.row(align="items-center justify-between"):
            ui.label("🗂️ MCP Configs").classes(kit.CLS_SECTION_HEADER)
            with ui.row().classes("items-center gap-1"):
                self._configs_box = ui.row().classes("items-center gap-1 no-wrap")
                kit.button(
                    "📥 Copy to Fiber",
                    lambda: run_fiber_cli("connect", "copy"),
                    color="cyan",
                    outline=True,
                ).tooltip("Copy & merge all discovered configs into ~/.anchor/mcp.json")
                ui.button(
                    "🔄",
                    on_click=lambda: oob.scan_mcp_topology_and_health(silent=False),
                ).props("flat dense size=xs color=cyan").tooltip("Rescan MCP configs & daemon status")

        # 2. MCP Workers List (Running vs Standby vs Config-Only)
        # 하단 요소들이 우측으로 이동했으므로 워커 리스트가 남은 세로 공간을 넉넉히 활용하도록 확장
        with kit.row(align="items-center justify-between"):
            ui.label("🔌 MCP Workers").classes(kit.CLS_SECTION_HEADER)
            self._roster_summary_label = ui.label("").classes("text-[10px] font-mono text-slate-400")

        self._workers_box = ui.column().classes("w-full gap-1.5 flex-grow max-h-72 overflow-y-auto pr-1")

        self._render_dynamic_sections()

    def compose_right(self) -> None:
        """우측: 게이트웨이 헬스, Slack HITL 브릿지 설정, HTTP 202 시뮬레이션 및 TOTP 제어"""
        s = state.security

        # 1. 상단 실시간 게이트웨이 & Slack OOB 상태 배지
        with kit.bar():
            ui.label().bind_text_from(s, "gateway_health_badge_text").classes(
                "text-xs font-mono font-bold text-emerald-400"
            )
            ui.label().bind_text_from(s, "slack_oob_badge_text").classes(
                "text-[11px] font-mono font-bold text-cyan-300"
            )

        # 2. Slack Bridge & Auth Policy 설정 (기존 좌측에서 이동)
        ui.label("💬 Slack HITL Bridge & Auth Policy").classes("text-xs font-bold text-amber-400 mt-1 shrink-0")
        with kit.row():
            kit.text("Slack Channel", s, "slack_channel", width="w-32")
            kit.text("Slack UID ('*')", s, "slack_user_map", width="w-28")
            kit.text("Mapped SPIFFE ID", s, "admin_user")

        with kit.bar():
            ui.label("Auth Policy (TOTP vs Approve Button)").classes("text-xs font-mono text-slate-300")
            kit.switch("TOTP(6-Digit)", s, "oob_require_totp", color="cyan")

        # 3. HTTP 202 HITL Gate & Slack Dispatch 트리거
        ui.label("🚨 High-Risk Tool Call Simulation (HTTP 202)").classes(
            "text-xs font-bold text-slate-400 mt-1 shrink-0"
        )
        with kit.row():
            kit.button(
                "🚨 Simulate 'drop_table' (HTTP 202)",
                lambda: oob.open_hitl_modal(
                    "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="amber",
                text_color="black",
                flex=True,
            )
            kit.button(
                "📤 Dispatch Slack HITL",
                lambda: oob.dispatch_slack_oob(
                    "postgres-prod-db", "drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="indigo",
                outline=True,
                flex=True,
            )

        # [임시 비활성화] 공간 점유가 큰 Slack BlockKit / StateToken JSON 에디터 주석 처리
        # ui.label("Live Signed OOB StateToken & Slack BlockKit Payload (`mcp.bridge.slack`):").classes(
        #     "text-xs text-slate-400 shrink-0"
        # )
        # kit.editor(s, "oob_inspector_json")

        # 4. 하단 노드 키 및 TOTP 인증 제어 푸터 (기존 좌측에서 이동)
        with kit.footer():
            kit.button("KeyGen", oob.init_node_keys, color="slate-4", outline=True)
            kit.button("Enroll TOTP(QR)", oob.enroll_admin_totp, color="cyan", flex=True)
            kit.button("Verify TOTP", oob.verify_admin_totp, color="emerald", outline=True, flex=True)
            kit.button("Revoke", oob.revoke_admin_totp, color="red", outline=True)

    def _render_dynamic_sections(self) -> None:
        s = state.security
        anchor_path = _get_anchor_mcp_path()
        anchor_specs = _load_anchor_specs_map()
        active_set = set(s.active_target_ids or [])

        default_mcp_tpl = (
            '{\n  "mcpServers": {\n'
            '    "fiber-gateway": {\n      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"\n    }\n'
            "  }\n}\n"
        )

        # 1. Config 칩 렌더링: Fiber MCP Config (.anchor/mcp.json) 파일 링크 + 외부 IDE 설정
        if self._configs_box is not None:
            self._configs_box.clear()
            found_cfgs = [
                c for c in s.discovered_configs
                if c.get("exists") and Path(c.get("path", "")).resolve() != anchor_path.resolve()
            ]
            missing_cfgs = [
                c for c in s.discovered_configs
                if not c.get("exists") and Path(c.get("path", "")).resolve() != anchor_path.resolve()
            ]

            with self._configs_box:
                # [Primary Fiber MCP Config Chip with File Link]
                anchor_exists = anchor_path.is_file()
                with ui.row().classes(
                    "items-center gap-1 px-1.5 py-0 rounded border "
                    + (
                        "border-cyan-400/80 bg-cyan-950/50 text-cyan-200"
                        if anchor_exists
                        else "border-amber-500/60 bg-amber-950/40 text-amber-300"
                    )
                    + " text-[10px] font-mono font-bold"
                ):
                    ui.label("🟢 fiber" if anchor_exists else "🟡 fiber").tooltip(
                        f"Fiber MCP Config ({len(anchor_specs)} targets) — {anchor_path}"
                    )
                    kit.file_link(
                        str(anchor_path),
                        label="mcp.json",
                        tooltip=f"Open {anchor_path}",
                        default_content='{\n  "mcpServers": {}\n}\n',
                        create_if_missing=True,
                    )

                # [External Agent Config Chips]
                for cfg in found_cfgs:
                    ag = cfg["agent"]
                    p_str = cfg["path"]
                    with ui.row().classes(
                        "items-center gap-0.5 px-1.5 py-0 rounded border "
                        "border-emerald-500/50 bg-emerald-950/30 text-emerald-300 text-[10px] font-mono"
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

        # 2. 워커 리스트 렌더링
        if self._workers_box is not None:
            self._workers_box.clear()
            running_cnt = 0
            standby_cnt = 0
            config_cnt = 0

            with self._workers_box:
                if not s.discovered_workers:
                    ui.label("No MCP workers discovered yet. Click '📥 Copy to Fiber' or '🔄' above.").classes(
                        "text-xs font-mono text-slate-500 py-1"
                    )

                for w in s.discovered_workers:
                    tid = w["target_id"]
                    source = str(w.get("source", "ext"))
                    transport = str(w.get("transport", "stdio")).lower()
                    exec_cmd = w.get("exec_cmd", "")

                    # [방어 로직] 만약 console.py에서 넘어온 mode가 ephemeral이라도,
                    # .anchor/mcp.json의 FIBER_MODE나 내장 워커(worker.*) 규칙을 직접 대조하여 정확한 모드 복원
                    anchor_env = (
                        anchor_specs.get(tid, {}).get("env", {})
                        if isinstance(anchor_specs.get(tid), dict)
                        else {}
                    )
                    raw_mode = str(w.get("mode", "")).lower()
                    if anchor_env.get("FIBER_MODE"):
                        mode = str(anchor_env["FIBER_MODE"]).lower()
                    elif raw_mode in ("multiplex", "linear"):
                        mode = raw_mode
                    elif tid.startswith("worker."):
                        mode = "linear" if tid in ("worker.eco.finlib", "worker.reaper") else "multiplex"
                    else:
                        mode = raw_mode or "ephemeral"

                    preflight = w.get("preflight") or anchor_env.get("FIBER_PREFLIGHT_PATTERN") or ""

                    is_mounted = bool(w.get("is_live")) or (tid in active_set)
                    is_in_anchor = (tid in anchor_specs) or (source in ("fiber", "builtin"))

                    if is_mounted and mode != "ephemeral":
                        running_cnt += 1
                        status_badge = "🟢 RUNNING"
                        status_cls = "text-emerald-400"
                        status_tip = f"Process running in McpWorkerDaemon (Mode: {mode.upper()})"
                    elif is_mounted and mode == "ephemeral":
                        standby_cnt += 1
                        status_badge = "⚡ STANDBY"
                        status_cls = "text-cyan-400"
                        status_tip = "Mounted in Daemon (Ephemeral Zero-Spawn: boots on tool call)"
                    elif is_in_anchor:
                        config_cnt += 1
                        status_badge = "🟡 CONFIG"
                        status_cls = "text-amber-300"
                        status_tip = "Registered in fiber mcp.json (Start 'fiber daemon' to boot)"
                    else:
                        status_badge = "⚪ EXTERNAL"
                        status_cls = "text-slate-400"
                        status_tip = "Found in external IDE config (Click 'Copy' to add to fiber mcp.json)"

                    guard_txt = f"🔒 {preflight[:12]}" if preflight else "🔓 Open"

                    with kit.bar():
                        with ui.row().classes("items-center gap-1.5 no-wrap overflow-hidden"):
                            ui.label(status_badge).classes(f"text-[10px] font-mono font-bold {status_cls}").tooltip(
                                status_tip
                            )
                            ui.label(f"{tid}").classes("text-xs font-mono font-bold text-slate-100 truncate").tooltip(
                                exec_cmd
                            )
                            ui.label(f"[{source}|{transport}:{mode}]").classes("text-[10px] font-mono text-cyan-400")

                        with ui.row().classes("items-center gap-1.5 shrink-0"):
                            ui.badge(guard_txt).props("color=slate-8 text-color=amber-3").classes(
                                "text-[10px] font-mono"
                            ).tooltip(f"Pre-Flight Pattern: {preflight or 'None'}")

                            # 우측 상태/액션: 이미 데몬에서 돌고 있는 워커는 버튼을 띄우지 않고 상태만 명확히 표시
                            if is_mounted and mode != "ephemeral":
                                ui.badge("✓ Active").props(
                                    "outline color=emerald text-color=emerald-3"
                                ).classes("text-[10px] font-mono px-1.5")
                            elif is_mounted and mode == "ephemeral":
                                ui.badge("Zero-Spawn").props(
                                    "outline color=cyan text-color=cyan-3"
                                ).classes("text-[10px] font-mono px-1.5")
                            elif not is_in_anchor:
                                kit.button(
                                    "📥 Copy",
                                    lambda t=tid, c=exec_cmd, m=mode, p=preflight: run_fiber_cli(
                                        "connect",
                                        "copy",
                                        "-t",
                                        t,
                                        "-e",
                                        c,
                                        "-m",
                                        m,
                                        *(["-p", p] if p else []),
                                    ),
                                    color="cyan",
                                    outline=True,
                                ).tooltip("Copy into fiber mcp.json")
                            else:
                                kit.button(
                                    "▶ Run",
                                    lambda t=tid, c=exec_cmd: run_fiber_cli("connect", "-t", t, "-e", c),
                                    color="amber",
                                    outline=True,
                                ).tooltip("Daemon is offline; run this worker standalone")

            if self._roster_summary_label is not None:
                self._roster_summary_label.set_text(
                    f"🟢 {running_cnt} Running · ⚡ {standby_cnt} Standby · 🟡 {config_cnt} Stopped"
                )

    def flush(self) -> None:
        if state.security.mcp_topology_dirty:
            state.security.mcp_topology_dirty = False
            self._render_dynamic_sections()