# fiber.dev.console.ui.kit
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from abc import ABC, abstractmethod
from contextlib import contextmanager
import inspect
from typing import Any, Callable, Iterator, Sequence
from nicegui import Client, ui

import fiber.dev.console.ui.diag as diag

# ==========================================
# 1. Design Tokens & Internal Helpers
# ==========================================
CLS_MONO_LABEL = (
    "text-[11px] font-mono text-slate-400 bg-slate-950 px-2.5 py-1.5 rounded border border-slate-800 w-full truncate"
)
CLS_SECTION_HEADER = "text-xs font-bold text-cyan-400 shrink-0"

_orig_notify = ui.notify

def _context_safe_notify(message: Any, **kwargs: Any) -> None:
    """Fallback to an active client context if ui.notify is called from a background task."""
    try:
        _orig_notify(message, **kwargs)
    except RuntimeError:
        for client in list(Client.instances.values()):
            if getattr(client, "has_socket_connection", False):
                with client:
                    _orig_notify(message, **kwargs)
                return


ui.notify = _context_safe_notify


def _wrap_async(handler: Callable[[], Any]) -> Callable[[], Any]:
    """Await coroutines directly inside an async handler to preserve NiceGUI's slot stack."""
    async def _runner() -> None:
        try:
            res = handler()
            if inspect.iscoroutine(res):
                await res
        except Exception as exc:
            diag.exception(
                "UI button handler failed",
                exc=exc,
                handler=getattr(handler, "__name__", "lambda"),
            )

    return _runner

def open_local_file(
    path: str | Path,
    *,
    default_content: str = "{}\n",
    create_if_missing: bool = False,
) -> None:
    """
    Open a local file or directory in the user's preferred external editor.
    - Does NOT create missing files unless `create_if_missing=True` is explicitly passed.
    - If the file exists but is 0-byte empty, seeds it with `default_content` to prevent JSON parse errors.
    """
    target = Path(path).expanduser().resolve()
    try:
        if not target.exists():
            if not create_if_missing:
                ui.notify(
                    f"⚠️ File does not exist yet: {target.name} (Use '+ Init' to create it)",
                    type="warning",
                )
                return
            if target.suffix:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(default_content, encoding="utf-8")
        elif target.is_file() and target.stat().st_size == 0 and default_content:
            # 0바이트 빈 파일이 이미 생성되어 파서 오류를 일으키는 경우 기본 JSON 시드 주입
            target.write_text(default_content, encoding="utf-8")

        editor_cmd = os.environ.get("FIBER_EDITOR") or os.environ.get("EDITOR")
        if editor_cmd:
            subprocess.Popen([editor_cmd, str(target)])
        elif sys.platform == "darwin":
            subprocess.Popen(["open", "-t", str(target)] if target.is_file() else ["open", str(target)])
        elif sys.platform.startswith("linux"):
            subprocess.Popen(["xdg-open", str(target)])
        else:
            os.startfile(str(target))  # type: ignore[attr-defined]

        ui.notify(f"📂 Opened in external editor: {target.name}", type="info")
    except Exception as exc:
        diag.exception("Failed to open local file in external tool", exc=exc, path=str(target))
        ui.notify(f"Cannot open {target.name}: {exc}", type="warning")

def file_link(
    path: str | Path | Callable[[], str | Path],
    *,
    label: str = "",
    tooltip: str = "",
    default_content: str = "{}\n",
    create_if_missing: bool = False,
) -> ui.row:
    """Compact inline file badge + 1-icon button ('open_in_new' or 'add') to launch external editors."""
    def _resolve() -> Path:
        raw = path() if callable(path) else path
        return Path(raw).expanduser()

    initial_path = _resolve()
    disp_text = label or initial_path.name
    tip_text = tooltip or f"Open externally: {initial_path}"
    icon_name = "add_circle_outline" if create_if_missing and not initial_path.exists() else "open_in_new"

    with ui.row().classes("items-center gap-1 no-wrap shrink-0") as container:
        if label != "":
            ui.label(disp_text).classes("text-[11px] font-mono text-slate-400 underline decoration-slate-700")
        ui.button(
            icon=icon_name,
            on_click=lambda: open_local_file(
                _resolve(),
                default_content=default_content,
                create_if_missing=create_if_missing,
            ),
        ).props("flat dense round size=xs color=cyan").tooltip(tip_text)
    return container

def trigger_viewport_resize() -> None:
    """Recalculate ECharts and CodeMirror dimensions after tab visibility changes."""
    ui.run_javascript("setTimeout(() => window.dispatchEvent(new Event('resize')), 30)")


# ==========================================
# 2. Form & Input Primitives
# ==========================================
def text(
    label: str, target: Any, attr: str, *, flex: bool = True, mono: bool = True, width: str = ""
) -> ui.input:
    w_cls = width or ("flex-1" if flex else "")
    m_cls = "font-mono" if mono else ""
    return (
        ui.input(label)
        .bind_value(target, attr)
        .props("dark outlined dense")
        .classes(f"{w_cls} {m_cls} text-xs".strip())
    )


def number(
    label: str,
    target: Any,
    attr: str,
    *,
    min_val: float = 0,
    max_val: float = 100,
    step: float = 1,
    width: str = "w-24",
    as_int: bool = False,
) -> ui.number:
    forward_fn = int if as_int else lambda x: x
    return (
        ui.number(label, min=min_val, max=max_val, step=step)
        .bind_value(target, attr, forward=forward_fn)
        .props("dark outlined dense")
        .classes(f"{width} font-mono text-xs")
    )


def select(
    options: Sequence[str], label: str, target: Any, attr: str, *, width: str = "flex-1"
) -> ui.select:
    return (
        ui.select(list(options), label=label)
        .bind_value(target, attr)
        .props("dark outlined dense")
        .classes(f"{width} text-xs")
    )


def switch(
    label: str, target: Any | None = None, attr: str = "", *, color: str = "emerald", value: bool = False
) -> ui.switch:
    sw = ui.switch(label, value=value).props(f"color={color} dense").classes("text-xs")
    if target is not None and attr:
        sw.bind_value(target, attr)
    return sw


def button(
    label: str,
    action: Callable[[], Any],
    *,
    color: str = "emerald",
    outline: bool = False,
    flex: bool = False,
    text_color: str = "",
) -> ui.button:
    props = f"color={color} dense"
    if outline:
        props += " outline"
    if text_color:
        props += f" text-color={text_color}"
    cls = f"{'flex-1 font-bold' if flex else 'px-2.5'} text-xs"
    return ui.button(label, on_click=_wrap_async(action)).props(props).classes(cls)


# ==========================================
# 3. Viewport & Data Widgets (min-h-0 guarded)
# ==========================================
def editor(target: Any, attr: str, *, lang: str = "json") -> ui.codemirror:
    """CodeMirror editor guarded with min-h-0 to prevent flexbox height overflow."""
    return (
        ui.codemirror(language=lang, theme="oneDark")
        .bind_value(target, attr)
        .classes("w-full flex-1 min-h-0 text-xs border border-slate-800")
    )


def log(max_lines: int = 100, *, color: str = "text-slate-200", height: str = "flex-1 min-h-0") -> ui.log:
    """Log viewer with isolated scroll containment inside flex containers."""
    return ui.log(max_lines=max_lines).classes(
        f"w-full {height} bg-black border border-slate-800 p-2 font-mono text-xs {color}"
    )


def bar_chart(
    categories: list[str],
    data: list[Any],
    *,
    x_max: float | None = None,
    formatter: str = "{c}",
) -> ui.echart:
    """Standardized dark-theme horizontal bar chart."""
    x_axis: dict[str, Any] = {
        "type": "value",
        "splitLine": {"lineStyle": {"color": "#1e293b"}},
        "axisLabel": {"color": "#64748b", "fontSize": 10},
    }
    if x_max is not None:
        x_axis["max"] = x_max

    return ui.echart({
        "grid": {"top": 12, "bottom": 25, "left": 95, "right": 45},
        "xAxis": x_axis,
        "yAxis": {
            "type": "category",
            "data": categories,
            "axisLabel": {"color": "#cbd5e1", "fontSize": 10},
        },
        "series": [{
            "type": "bar",
            "data": data,
            "itemStyle": {"color": "#38bdf8"},
            "label": {
                "show": True,
                "position": "right",
                "formatter": formatter,
                "color": "#e2e8f0",
                "fontSize": 10,
            },
        }],
    }).classes("w-full flex-1 min-h-0")


def line_chart(
    x_ticks: list[str],
    y_primary: list[int | float],
    y_secondary: list[int | float],
    *,
    redline: int | float = 0,
    y_max: int = 75000,
) -> ui.echart:
    """Dark-theme dual-series line chart for FinOps ledger telemetry."""
    return ui.echart({
        "tooltip": {"trigger": "axis"},
        "legend": {
            "data": ["Active Quota (FinOps)", "Core Balance (PTA)"],
            "textStyle": {"color": "#94a3b8", "fontSize": 10},
            "top": 0,
        },
        "grid": {"top": 26, "bottom": 20, "left": 55, "right": 20},
        "xAxis": {"type": "category", "data": x_ticks, "axisLabel": {"color": "#64748b", "fontSize": 10}},
        "yAxis": {
            "type": "value",
            "min": 0,
            "max": y_max,
            "splitLine": {"lineStyle": {"color": "#1e293b"}},
            "axisLabel": {"color": "#64748b", "fontSize": 10},
        },
        "series": [
            {
                "name": "Active Quota (FinOps)",
                "type": "line",
                "data": y_primary,
                "smooth": True,
                "itemStyle": {"color": "#10b981"},
                "areaStyle": {"color": "rgba(16, 185, 129, 0.12)"},
                "markLine": {
                    "silent": True,
                    "lineStyle": {"color": "#ef4444", "width": 2, "type": "dashed"},
                    "data": [{"yAxis": redline}],
                },
            },
            {
                "name": "Core Balance (PTA)",
                "type": "line",
                "data": y_secondary,
                "step": "end",
                "lineStyle": {"color": "#06b6d4", "type": "dotted"},
                "itemStyle": {"color": "#06b6d4"},
            },
        ],
    }).classes("w-full h-40 shrink-0")


# ==========================================
# 4. Spatial Layout Containers
# ==========================================
@contextmanager
def row(*, gap: str = "gap-2", align: str = "items-center") -> Iterator[ui.row]:
    """Full-width horizontal flex row."""
    with ui.row().classes(f"w-full {gap} {align} shrink-0") as r:
        yield r


@contextmanager
def bar() -> Iterator[ui.row]:
    """Compact status bar or list item container."""
    with ui.row().classes(
        "w-full justify-between items-center bg-slate-950 px-3 py-1.5 rounded border border-slate-800 shrink-0"
    ) as r:
        yield r


@contextmanager
def card(*, flex: bool = True, width: str = "") -> Iterator[ui.card]:
    """Standard dark-theme card container."""
    w_cls = width or ("flex-1" if flex else "")
    with ui.card().classes(f"{w_cls} bg-slate-950 border border-slate-800 p-2.5") as c:
        yield c


@contextmanager
def footer() -> Iterator[ui.row]:
    """Bottom-anchored action bar (`mt-auto`)."""
    with ui.row().classes("w-full gap-2 mt-auto pt-1 shrink-0") as r:
        yield r


def split_studio(
    left_title: str,
    left_builder: Callable[[], None],
    right_title: str,
    right_builder: Callable[[], None],
    *,
    ratio: int = 42,
) -> ui.splitter:
    """
    Two-pane studio splitter [Left: Declarative Config <-> Right: Live Proof & Telemetry].
    - Left pane scrolls vertically when form controls overflow.
    - Right pane enforces `min-h-0 overflow-hidden` so flex-1 charts/editors fit strictly.
    """
    with ui.splitter(value=ratio).classes("w-full h-[450px] bg-slate-900 rounded border border-slate-800") as sp:
        with sp.before:
            with ui.column().classes("p-4 gap-2.5 w-full h-full min-h-0 overflow-y-auto"):
                ui.label(left_title).classes(
                    "text-sm font-bold text-white border-b border-slate-800 pb-1.5 w-full shrink-0"
                )
                left_builder()
        with sp.after:
            with ui.column().classes("p-4 gap-2.5 w-full h-full min-h-0 overflow-hidden"):
                ui.label(right_title).classes(
                    "text-sm font-bold text-white border-b border-slate-800 pb-1.5 w-full shrink-0"
                )
                right_builder()
    return sp


@contextmanager
def dual_drawer() -> Iterator[tuple[ui.column, ui.column]]:
    """Bottom dual-terminal drawer layout for CLI and Event Bus logs."""
    with ui.row().classes("w-full px-3 pb-3 gap-3 no-wrap"):
        left_col = ui.column().classes("w-1/2 bg-black border border-slate-800 rounded p-2.5 gap-1")
        right_col = ui.column().classes("w-1/2 bg-black border border-slate-800 rounded p-2.5 gap-1")
        yield left_col, right_col


# ==========================================
# 5. Lifecycle & Structural Templates
# ==========================================
class StudioTab(ABC):
    """Base template for domain studio tabs (MCP, LLM Live, FinOps, VCR)."""
    title: str = "Tab"
    left_header: str = "🎛️ Configuration"
    right_header: str = "📈 Telemetry & Proof"
    ratio: int = 42

    @abstractmethod
    def compose_left(self) -> None:
        """Build left configuration pane."""

    @abstractmethod
    def compose_right(self) -> None:
        """Build right telemetry/chart pane."""

    def flush(self) -> None:
        """100ms ringbuffer sync hook (override if needed)."""

    def render(self) -> ui.splitter:
        """Render left and right panes inside `split_studio`."""
        return split_studio(
            self.left_header,
            self.compose_left,
            self.right_header,
            self.compose_right,
            ratio=self.ratio,
        )


class ModalTemplate(ABC):
    """Base template for modal dialogs (HITL 202 gate, TOTP QR)."""
    width_cls: str = "w-[480px]"
    border_cls: str = "border border-slate-700"
    persistent: bool = False

    @abstractmethod
    def compose(self, dialog: ui.dialog) -> None:
        """Build modal card content."""

    def mount(self) -> ui.dialog:
        props = "persistent" if self.persistent else ""
        with ui.dialog().props(props) as dialog, ui.card().classes(
            f"bg-slate-900 {self.border_cls} {self.width_cls} p-5"
        ):
            self.compose(dialog)
        return dialog


class ConsoleTemplate:
    """Top-level shell assembling header, keep-alive studio tabs, modals, and bottom drawers."""
    def __init__(
        self,
        tabs: Sequence[StudioTab],
        modals: Sequence[ModalTemplate] = (),
        *,
        header_builder: Callable[[], None] | None = None,
        drawer_builder: Callable[[], None] | None = None,
        on_tick: Callable[[Sequence[StudioTab]], None] | None = None,
        tick_interval: float = 0.1,
    ) -> None:
        self.tabs = list(tabs)
        self.modals = list(modals)
        self.header_builder = header_builder
        self.drawer_builder = drawer_builder
        self.on_tick = on_tick
        self.tick_interval = tick_interval

    def build_view(self) -> None:
        """Construct the DOM tree and start the 100ms UI flush timer."""
        ui.dark_mode().enable()
        ui.query("body").classes("bg-slate-950 text-slate-100 select-none")
        ui.query(".nicegui-content").classes("p-0 gap-0")

        for modal in self.modals:
            modal.mount()

        if self.header_builder:
            self.header_builder()

        if self.tabs:
            with ui.tabs().classes("w-full bg-slate-900 border-b border-slate-800") as tab_bar:
                tab_handles = [ui.tab(t.title) for t in self.tabs]

            with (
                ui.tab_panels(tab_bar, value=tab_handles[0], on_change=lambda _: trigger_viewport_resize())
                .props("keep-alive animated=false")
                .classes("w-full bg-slate-950 p-3")
            ):
                for tab_obj, handle in zip(self.tabs, tab_handles):
                    with ui.tab_panel(handle).classes("p-0"):
                        tab_obj.render()

        if self.drawer_builder:
            self.drawer_builder()

        if self.on_tick:
            ui.timer(self.tick_interval, lambda: self.on_tick(self.tabs))

    def mount_page(self, path: str = "/", *, shared: bool = True) -> None:
        """Mount view on shared auto-index root (`shared=True`) or isolated `@ui.page` (`shared=False`)."""
        if shared:
            self.build_view()
        else:
            @ui.page(path)
            def _page_entry() -> None:
                self.build_view()