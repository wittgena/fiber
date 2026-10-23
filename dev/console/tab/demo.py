# fiber.dev.console.tab.demo
from __future__ import annotations

import json
from pathlib import Path
from nicegui import ui

from fiber.dev.console.state import state
from fiber.dev.console.action import oob, pipeline
from fiber.phase.bridge.cli import run_fiber_cli
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import StudioTab
from xphi.node.space.bind.resolver import resolve_path


def _resolve_compat_adapters_dir() -> Path:
    """현재 작업 경로 또는 상위 경로에서 fiber-compats/adapters 디렉토리 탐색"""
    for cand in (
        Path("fiber-compats/adapters"),
        Path("../fiber-compats/adapters"),
        resolve_path("anchor").parent / "fiber-compats" / "adapters",
    ):
        if cand.is_dir():
            return cand.resolve()
    return Path("fiber-compats/adapters")


def _discover_compat_models() -> list[str]:
    """
    fiber-compats/adapters/{provider}/{provider}_{model_slug} 구조를 스캔하여
    `fiber compat fixture -m <model>`에 전달할 모델 식별자 리스트를 반환합니다.
    """
    adapters_root = _resolve_compat_adapters_dir()
    discovered: list[str] = []

    if adapters_root.is_dir():
        for prov_dir in sorted(p for p in adapters_root.iterdir() if p.is_dir() and not p.name.startswith(".")):
            prov = prov_dir.name
            prefix = f"{prov}_"
            for mod_dir in sorted(d for d in prov_dir.iterdir() if d.is_dir() and not d.name.startswith(".")):
                extracted_model: str | None = None
                for jf_name in ("sync_declarative_rule.json", "stream_compat_traverser.json"):
                    jf = mod_dir / jf_name
                    if jf.is_file():
                        try:
                            data = json.loads(jf.read_text(encoding="utf-8"))
                            if isinstance(data, dict):
                                cand = (
                                    data.get("model")
                                    or data.get("target_model")
                                    or (data.get("meta", {}) if isinstance(data.get("meta"), dict) else {}).get("model")
                                )
                                if isinstance(cand, str) and cand.strip():
                                    extracted_model = cand.strip()
                                    break
                        except Exception:
                            pass

                if extracted_model:
                    model_id = extracted_model if "/" in extracted_model else f"{prov}/{extracted_model}"
                else:
                    slug = mod_dir.name[len(prefix) :] if mod_dir.name.startswith(prefix) else mod_dir.name
                    if prov == "ollama" and "_" in slug:
                        slug = slug.replace("_", ":", 1)
                    elif prov in ("huggingface", "groq") and "_" in slug:
                        slug = slug.replace("_", "/", 1)
                    model_id = f"{prov}/{slug}"

                if model_id not in discovered:
                    discovered.append(model_id)

    if not discovered:
        discovered = [
            "gemini/gemini-3.1-flash-lite",
            "cohere/command-r-08-2024",
            "cerebras/qwen-3.8-27b",
            "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
            "lm_studio/qwen-3.5-9b",
            "mistral/open-mistral-7b",
            "ollama/gemma:2b",
            "groq/qwen/qwen3.8-27b",
            "huggingface/meta-llama/Llama-3.1-8B-Instruct",
        ]
    return discovered


class DemoTab(StudioTab):
    title = "⚡ Demo Lab"
    left_header = "🎬 4-Pillar Interactive Scenario Launcher"
    right_header = "📡 Unified Live Telemetry & OOB Inspector"
    ratio = 48

    def compose_left(self) -> None:
        s, f, p = state.security, state.finops, state.pipeline

        compat_models = _discover_compat_models()
        if not getattr(p, "compat_model", None) or p.compat_model not in compat_models:
            setattr(p, "compat_model", compat_models[0])

        # Scenario 1: LLM Guardrail & Sub-ms X-Ray
        ui.label("1. LLM Pipeline: Secret Guardrail & X-Ray Probe").classes(
            "text-[11px] font-bold text-cyan-400 shrink-0"
        )
        with kit.row(gap="gap-1.5"):
            kit.text("Test Prompt", s, "test_prompt", width="flex-grow")
            kit.button(
                "🛡️ Block Secret",
                lambda: pipeline.run_pipeline_xray("Analyze key: sk-proj-99887766554433221100"),
                color="red",
                outline=True,
            ).tooltip("Trigger POST_TRANSLATE PermissionError on leaked API key")
            kit.button(
                "✓ Safe Pass",
                lambda: pipeline.run_pipeline_xray("Summarize Q3 infrastructure latency."),
                color="emerald",
                outline=True,
            ).tooltip("Run clean prompt through all 4 slots (0.75ms overhead)")

        # Scenario 2: MCP Pre-Flight HITL & Slack OOB
        ui.label("2. MCP Gateway: HTTP 202 Parking & Slack HITL").classes(
            "text-[11px] font-bold text-amber-400 mt-1.5 shrink-0"
        )
        with kit.row(gap="gap-1.5"):
            kit.button(
                "🚨 Trap 'drop_table' (HTTP 202 Modal)",
                lambda: oob.open_hitl_modal(
                    "postgres-prod-db.drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="amber",
                text_color="black",
                flex=True,
            ).tooltip("Simulate Pre-Flight regex trap & open local TOTP/1-Click gate")
            kit.button(
                "📤 Dispatch Slack OOB",
                lambda: oob.dispatch_slack_oob(
                    "postgres-prod-db", "drop_table", {"table": "customer_ledgers", "cascade": True}
                ),
                color="indigo",
                outline=True,
                flex=True,
            ).tooltip("Mint signed StateToken & dispatch BlockKit prompt to Slack")

        # Scenario 3: FinOps Lifecycle & Mid-Stream Kill-Switch
        ui.label("3. FinOps Governance: PTA Ledger & HTTP 402 Kill-Switch").classes(
            "text-[11px] font-bold text-emerald-400 mt-1.5 shrink-0"
        )
        with kit.row(gap="gap-1.5"):
            kit.button(
                "🔄 5-Step Mandate Demo",
                lambda: run_fiber_cli(
                    "account",
                    "--tenant",
                    f.tenant_id,
                    *(["--spiffe", f.spiffe_id.strip()] if f.spiffe_id and f.spiffe_id.strip() else []),
                    "demo",
                ),
                color="cyan",
                outline=True,
                flex=True,
            ).tooltip("Provision -> Top-Up -> Reject Stale Replay -> Revoke")
            kit.button(
                "🌊 Burn Stream (2,200 c/s ➔ 402 Sever)",
                pipeline.simulate_stream,
                color="indigo",
                flex=True,
            ).tooltip("Drive high-speed stream until Fuel hits Redline and severs mid-flight")

        # Scenario 4: Deterministic VCR & Schema Compat
        ui.label("4. Deterministic VCR: Record ➔ Offline Replay (~0ms I/O)").classes(
            "text-[11px] font-bold text-purple-400 mt-1.5 shrink-0"
        )
        with kit.row(gap="gap-1.5"):
            kit.select(compat_models, "Compat Model (-m)", p, "compat_model", width="w-52")
            kit.button(
                "🧬 Verify Compat",
                lambda: run_fiber_cli("compat", "fixture", "-m", p.compat_model, "--vcr", "replay"),
                color="cyan",
                outline=True,
            ).tooltip("Run: fiber compat fixture -m <model> --vcr replay")
            kit.button(
                "⏱️ 3-Step VCR Suite",
                pipeline.run_gateway_vcr_suite,
                color="emerald",
                flex=True,
            ).tooltip("Execute live HTTP Record followed by deterministic Replay benchmark")

    def compose_right(self) -> None:
        s, f, p = state.security, state.finops, state.pipeline

        # 1. Unified Status KPI Header (충돌 없는 반응형 바인딩)
        with kit.bar():
            ui.badge().bind_text_from(
                f, "stream_status_text", lambda st: f"STREAM: {st}"
            ).props("color=slate-8 text-color=amber-3").classes("text-[11px] font-mono font-bold px-2 py-0.5")

            ui.badge().bind_text_from(
                p,
                "vcr_bench_replay_ms",
                lambda rep: (
                    f"⚡ VCR Speedup: {round(p.vcr_bench_live_ms / max(float(rep or 0.1), 0.1), 1)}x "
                    f"({p.vcr_bench_live_ms:.0f}ms ➔ {float(rep or 0.0):.1f}ms)"
                ),
            ).props("color=slate-9 text-color=emerald-3").classes(
                "text-[10px] font-mono border border-emerald-500/40 px-2 py-0.5"
            )

        # 2. Live Fuel Burn & Redline Gauge (refs.finops_chart와 충돌하지 않는 프로그레스 게이지)
        with ui.column().classes("w-full gap-1 bg-slate-950/90 px-3 py-2 rounded border border-slate-800 shrink-0"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label().bind_text_from(
                    f,
                    "current_fuel",
                    lambda cur: f"⛽ Active Fuel: {int(cur or 0):,} / {int(f.ledger_balance or 0):,} (Redline: {int(f.budget_limit):,})",
                ).classes("text-xs font-mono font-bold text-emerald-400")
                ui.label().bind_text_from(
                    s,
                    "xray_latencies",
                    lambda lats: f"🔬 Last X-Ray: {sum(lats):.2f}ms ({'BLOCKED 🛑' if lats[0] == 0.0 else 'PASSED ✓'})",
                ).classes("text-[11px] font-mono text-cyan-300")

            ui.linear_progress(value=1.0, show_value=False, size="8px", color="emerald").bind_value_from(
                f,
                "current_fuel",
                lambda cur: min(1.0, max(0.0, round(float(cur or 0) / max(float(f.ledger_balance or 50000), 1.0), 3))),
            ).classes("w-full rounded bg-slate-900")

        # 3. Live Signed OOB / Receipt JSON Inspector
        ui.label("Live Cryptographic Receipt & Slack BlockKit Inspector:").classes(
            "text-xs text-slate-400 mt-0.5 shrink-0"
        )
        kit.editor(s, "oob_inspector_json")