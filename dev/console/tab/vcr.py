# fiber.dev.console.tab.vcr
from __future__ import annotations

import json
from pathlib import Path
from nicegui import ui

from fiber.infra.account.state import state, refs
from fiber.dev.console.action import pipeline
from fiber.phase.bridge.cli import run_fiber_cli
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import StudioTab
from xphi.node.space.bind.resolver import find_current_self


def _resolve_compat_adapters_dir() -> Path:
    self_dir = find_current_self()
    if self_dir:
        cand = self_dir.parent / "fiber-compats" / "adapters"
        if cand.is_dir():
            return cand.resolve()
    for fallback in (Path("fiber-compats/adapters"), Path("../fiber-compats/adapters")):
        if fallback.is_dir():
            return fallback.resolve()
    return Path("fiber-compats/adapters")


def _discover_compat_models_and_dirs() -> dict[str, Path]:
    adapters_root = _resolve_compat_adapters_dir()
    mapping: dict[str, Path] = {}

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

                if model_id not in mapping:
                    mapping[model_id] = mod_dir

    if not mapping:
        for fallback_id in (
            "gemini/gemini-3.1-flash-lite",
            "cohere/command-r-08-2024",
            "cerebras/qwen-3.8-27b",
            "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
            "lm_studio/qwen-3.5-9b",
            "mistral/open-mistral-7b",
            "ollama/gemma:2b",
            "groq/qwen/qwen3.8-27b",
            "huggingface/meta-llama/Llama-3.1-8B-Instruct",
        ):
            mapping[fallback_id] = adapters_root
    return mapping


def discover_compat_models() -> list[str]:
    return list(_discover_compat_models_and_dirs().keys())


class VcrTab(StudioTab):
    title = "VCR"
    left_header = "🧬 Adapter Fixture & Replay Controls"
    right_header = "⏱️ Live Replay Telemetry & Speedup Benchmark"
    ratio = 46

    def __init__(self) -> None:
        self._fixture_preview_box: ui.code | None = None
        self._fixture_meta_label: ui.label | None = None
        self._last_inspected_key: str = ""

    async def _run_compat_verify(self) -> None:
        p = state.pipeline
        mode_arg = p.vcr_mode if p.vcr_mode in ("record", "replay") else "replay"
        chaos_val = float(getattr(p, "vcr_chaos", 0.0) or 0.0)

        cli_args = [
            "compat",
            "fixture",
            "-m",
            p.compat_model,
            "--vcr",
            mode_arg,
            "--vcr-speed",
            p.vcr_speed,
        ]
        if chaos_val > 0.0:
            cli_args.extend(["--vcr-chaos", str(chaos_val)])

        await run_fiber_cli(*cli_args)
        self._refresh_fixture_inspector(force=True)

    def _refresh_fixture_inspector(self, *, force: bool = False) -> None:
        p = state.pipeline
        model_id = getattr(p, "compat_model", "")
        fixture_kind = getattr(p, "compat_fixture_kind", "stream_compat_traverser.json")
        cache_key = f"{model_id}::{fixture_kind}::{p.vcr_mode}::{p.vcr_speed}"
        if not force and cache_key == self._last_inspected_key:
            return
        self._last_inspected_key = cache_key

        model_dirs = _discover_compat_models_and_dirs()
        target_dir = model_dirs.get(model_id)
        target_file = (target_dir / fixture_kind) if target_dir else None

        if target_file and target_file.is_file():
            try:
                raw_text = target_file.read_text(encoding="utf-8")
                parsed = json.loads(raw_text)
                size_kb = target_file.stat().st_size / 1024.0
                if isinstance(parsed, dict):
                    keys_info = f"keys={list(parsed.keys())[:4]}"
                elif isinstance(parsed, list):
                    keys_info = f"items={len(parsed)}"
                else:
                    keys_info = "valid JSON"

                if self._fixture_meta_label:
                    self._fixture_meta_label.set_text(
                        f"🟢 {target_file.parent.name}/{target_file.name} ({size_kb:.1f} KB | {keys_info})"
                    )
                if self._fixture_preview_box:
                    preview_lines = "\n".join(raw_text.splitlines()[:28])
                    if len(raw_text.splitlines()) > 28:
                        preview_lines += "\n// ... (truncated)"
                    self._fixture_preview_box.set_content(preview_lines)
                return
            except Exception as e:
                if self._fixture_meta_label:
                    self._fixture_meta_label.set_text(f"⚠️ Parse error in {target_file.name}: {e}")
                return

        if self._fixture_meta_label:
            self._fixture_meta_label.set_text(f"⚪ No local fixture found for '{model_id}' ({fixture_kind})")
        if self._fixture_preview_box:
            self._fixture_preview_box.set_content(
                json.dumps(
                    {
                        "target_model": model_id,
                        "--vcr": p.vcr_mode,
                        "--vcr-speed": p.vcr_speed,
                        "status": "Click 'Verify Compat Fixture' to run extraction test.",
                    },
                    indent=2,
                )
            )

    def compose_left(self) -> None:
        p = state.pipeline
        compat_models = discover_compat_models()
        if not getattr(p, "compat_model", None) or p.compat_model not in compat_models:
            setattr(p, "compat_model", compat_models[0])
        if not hasattr(p, "compat_fixture_kind"):
            setattr(p, "compat_fixture_kind", "stream_compat_traverser.json")
        if not hasattr(p, "vcr_chaos"):
            setattr(p, "vcr_chaos", 0.0)

        # 1. Standardized VCR CLI Controls (--model, --vcr, --vcr-speed, --vcr-chaos)
        ui.label("1. Target Adapter & Standardized VCR Options").classes("text-[11px] font-bold text-cyan-400 shrink-0")
        with kit.row():
            kit.select(compat_models, "Model (-m)", p, "compat_model", width="flex-grow")
            kit.select(["replay", "record"], "Mode (--vcr)", p, "vcr_mode", width="w-28")
            kit.select(["max", "real"], "Speed (--vcr-speed)", p, "vcr_speed", width="w-32")
            kit.number("Chaos (--vcr-chaos)", p, "vcr_chaos", min_val=0.0, max_val=2000.0, step=100.0, width="w-28")

        # 2. Execution Triggers
        with kit.row(gap="gap-1.5"):
            kit.button(
                "🧬 Verify Compat Fixture",
                self._run_compat_verify,
                color="cyan",
                outline=True,
                flex=True,
            ).tooltip("Run 'fiber compat fixture -m <model> --vcr <mode> --vcr-speed <speed>'")
            kit.button(
                "▶ Run 3-Step Gateway VCR Suite",
                pipeline.run_gateway_vcr_suite,
                color="emerald",
                flex=True,
            ).tooltip("Execute Unary, SSE Stream, and Dynamic Record -> Replay roundtrip")

        # 3. Live Adapter Cassette Inspector
        with kit.row(align="items-center justify-between"):
            ui.label("2. Recorded Adapter Fixture Inspector (`fiber-compats/adapters`):").classes(
                "text-[11px] font-bold text-slate-400 shrink-0"
            )
            kit.select(
                ["stream_compat_traverser.json", "sync_declarative_rule.json"],
                "Fixture File",
                p,
                "compat_fixture_kind",
                width="w-56",
            )

        self._fixture_meta_label = ui.label("").classes("text-[11px] font-mono text-emerald-400 shrink-0")
        self._fixture_preview_box = (
            ui.code("{}", language="json")
            .classes("w-full flex-grow text-[11px] font-mono bg-slate-950 border border-slate-800 rounded overflow-y-auto")
        )
        self._refresh_fixture_inspector(force=True)

    def compose_right(self) -> None:
        s, f, p = state.security, state.finops, state.pipeline

        # 1. 실시간 토큰 카운터 + VCR 배속 + 절감액 KPI 바
        with kit.bar():
            with ui.row().classes("items-center gap-2 no-wrap"):
                ui.badge().bind_text_from(
                    p,
                    "vcr_bench_replay_ms",
                    lambda rep: (
                        f"⚡ Speedup: {round(p.vcr_bench_live_ms / max(float(rep or 0.1), 0.1), 1)}x "
                        f"({p.vcr_bench_live_ms:.0f}ms ➔ {float(rep or 0.0):.1f}ms)"
                    ),
                ).props("color=slate-8 text-color=emerald-3").classes("text-[11px] font-mono font-bold px-2 py-0.5")

                ui.label().bind_text_from(
                    f,
                    "saved_tokens",
                    lambda tok: f"🪙 Replay Saved: {int(tok or 0):,} tok (${float(f.saved_usd or 0.0):.3f})",
                ).classes("text-xs font-mono font-bold text-amber-300")

            ui.badge().bind_text_from(
                s,
                "total_processed_tokens",
                lambda tot: f"📊 Total Tokens: {int(tot or 0):,}",
            ).props("color=slate-9 text-color=cyan-3").classes("text-[10px] font-mono border border-slate-700 px-1.5")

        # 2. 표준화된 `--vcr-*` CLI 커맨드 프리뷰 바
        with ui.row().classes(
            "w-full items-center justify-between bg-slate-950/90 px-3 py-1.5 rounded border border-slate-800 shrink-0"
        ):
            ui.label().bind_text_from(
                p,
                "compat_model",
                lambda m: (
                    f"$ fiber compat fixture -m {m} --vcr {p.vcr_mode} --vcr-speed {p.vcr_speed}"
                    + (f" --vcr-chaos {getattr(p, 'vcr_chaos', 0.0)}" if float(getattr(p, "vcr_chaos", 0.0) or 0.0) > 0 else "")
                ),
            ).classes("text-[11px] font-mono text-cyan-300 truncate")

            ui.badge().bind_text_from(
                p,
                "vcr_speed",
                lambda spd: "🚀 0ms Fast-Forward" if spd == "max" else "⏱️ Real-Time Pacing",
            ).props("color=slate-8 text-color=slate-3").classes("text-[10px] font-mono shrink-0")

        # 3. Live LLM vs VCR Replay 레이턴시 비교 바 차트
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

    def flush(self) -> None:
        self._refresh_fixture_inspector(force=False)