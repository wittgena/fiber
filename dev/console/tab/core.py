# fiber.dev.console.tab.core
from __future__ import annotations

from pathlib import Path
from nicegui import ui

from fiber.infra.account.state import state, refs
from fiber.gateway.bridge.cli import run_fiber_cli
from fiber.dev.console.action import gov, pipeline
import fiber.dev.console.ui.kit as kit
from fiber.dev.console.ui.kit import ModalTemplate, StudioTab
from fiber.dev.console.tab.mcp import HitlModal, QrModal, McpGatewayTab
from fiber.dev.console.tab.vcr import VcrTab
from fiber.dev.console.tab.finops import FinOpsTab
from xphi.node.space.bind.resolver import resolve_path


def _resolve_accounts_registry_path() -> Path:
    try:
        from fiber.infra.account.registry import LocalAccountRegistry
        return Path(LocalAccountRegistry.get_registry_path())
    except Exception:
        return resolve_path("anchor") / "sign" / "local_accounts.json"


# Tab 2: Live LLM Pipeline X-Ray (`state.security`)
class LiveLLMTab(StudioTab):
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

        ui.label("Test Pipeline").classes("text-[11px] font-bold text-cyan-400 mt-1 shrink-0")
        kit.text("Input Prompt", s, "test_prompt", flex=False, width="w-full")

        with kit.row(gap="gap-1.5"):
            kit.button("⚡ CALL LLM", lambda: pipeline.run_pipeline_xray(s.test_prompt), color="red", flex=True)

        with kit.footer():
            kit.button(
                "🌊 Trigger Live Stream Pulse (2,200 c/s)",
                pipeline.simulate_stream,
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

class ModelPriceTab(StudioTab):
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
    return [McpGatewayTab(), LiveLLMTab(), FinOpsTab(), VcrTab(), ModelPriceTab()]


def build_modals() -> list[ModalTemplate]:
    return [HitlModal(), QrModal()]