# fiber.dev.console.ui.diag
from __future__ import annotations

from functools import wraps
import inspect
import json
from typing import Any, Callable, Dict
from nicegui import ui

from fiber.dev.console.state import state, refs
from xphi.watcher.plane.emitter import get_emitter

_emitter = get_emitter(name="console.ui.diag", phase="UI_DIAG", mode="RAW")

_JS_ERROR_HOOK = """
window.__fiber_ui_errors = window.__fiber_ui_errors || [];
window.addEventListener('error', (e) => {
    window.__fiber_ui_errors.push({ ts: Date.now(), msg: e.message, line: e.lineno });
});
window.addEventListener('unhandledrejection', (e) => {
    window.__fiber_ui_errors.push({ ts: Date.now(), msg: String(e.reason), type: 'promise' });
});
"""

_JS_DOM_INSPECTOR = """
(() => {
    const anomalies = [];
    const metrics = [];
    const targets = document.querySelectorAll('.q-splitter, .q-tab-panel, .cm-editor, [_echarts_instance_]');
    targets.forEach((el, idx) => {
        const rect = el.getBoundingClientRect();
        const style = window.getComputedStyle(el);
        if (style.display === 'none' || style.visibility === 'hidden') return;

        const tag = el.className.split(' ').slice(0, 2).join('.');
        const headerEl = el.querySelector('.text-sm.font-bold') || el.closest('.q-splitter')?.querySelector('.text-sm.font-bold');
        const hint = headerEl ? headerEl.innerText.slice(0, 28) : `node_${idx}`;

        metrics.push({ target: tag, hint: hint, w: Math.round(rect.width), h: Math.round(rect.height) });

        if (rect.width < 10 || rect.height < 10) {
            anomalies.push({ type: 'ZERO_OR_COLLAPSED_SIZE', target: tag, hint: hint, w: Math.round(rect.width), h: Math.round(rect.height) });
        }
        if (style.overflowY === 'hidden' && el.scrollHeight > el.clientHeight + 4) {
            anomalies.push({ type: 'FLEX_OVERFLOW_CLIPPED', target: tag, hint: hint, client_h: el.clientHeight, scroll_h: el.scrollHeight });
        }
    });
    return {
        viewport: { w: window.innerWidth, h: window.innerHeight },
        active_elements: metrics,
        anomalies: anomalies,
        js_errors: (window.__fiber_ui_errors || []).slice(-10)
    };
})()
"""

def _caller_tag(depth: int = 2) -> str:
    """호출자의 모듈 및 함수명을 자동 추출 (예: 'runtime.run_fiber_cli')"""
    try:
        frame = inspect.currentframe()
        for _ in range(depth):
            if frame is not None:
                frame = frame.f_back
        if frame is not None:
            mod = frame.f_globals.get("__name__", "console").split(".")[-1]
            func = frame.f_code.co_name
            return f"{mod}.{func}"
    except Exception:
        pass
    return "console"


def _emit_safe(level: str, msg: str, *args: Any, depth: int = 3, **ctx: Any) -> None:
    """
    모든 콘솔 내부 로그의 단일 출구.
    - flow_id="global": TunnelSurface의 Redis 발행을 원천 차단
    - ui_internal=True: StudioPlaneObserver(plane.ingest)의 콘솔 화면 재유입을 원천 차단
    """
    caller = ctx.pop("caller", None) or _caller_tag(depth=depth)
    log_fn = getattr(_emitter, level.lower(), _emitter.info)
    log_fn(
        f"[{caller}] {msg}",
        *args,
        flow_id="global",
        ui_internal=True,
        caller=caller,
        **ctx,
    )

# 1. Wrapped Standard Logging API (외부 모듈 호출용)
def debug(msg: str, *args: Any, **ctx: Any) -> None:
    _emit_safe("DEBUG", msg, *args, **ctx)

def info(msg: str, *args: Any, **ctx: Any) -> None:
    _emit_safe("INFO", msg, *args, **ctx)

def warn(msg: str, *args: Any, **ctx: Any) -> None:
    _emit_safe("WARN", msg, *args, **ctx)

def error(msg: str, *args: Any, **ctx: Any) -> None:
    _emit_safe("ERROR", msg, *args, **ctx)

def exception(msg: str, *args: Any, exc: BaseException | None = None, **ctx: Any) -> None:
    """try-except 블록 내부에서 스택트레이스와 함께 에러 기록"""
    suffix = f" ({type(exc).__name__}: {exc})" if exc else ""
    _emit_safe("ERROR", f"{msg}{suffix}", *args, exc_info=True, **ctx)

# 2. Automatic Exception Guard Decorator
def guard(fn: Callable[..., Any]) -> Callable[..., Any]:
    tag = f"{fn.__module__.split('.')[-1]}.{fn.__name__}"
    if inspect.iscoroutinefunction(fn):
        @wraps(fn)
        async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                _emit_safe("ERROR", f"Unhandled async UI error: {exc}", exc_info=True, caller=tag)
                return None
        return _async_wrapper

    @wraps(fn)
    def _sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            _emit_safe("ERROR", f"Unhandled sync UI error: {exc}", exc_info=True, caller=tag)
            return None
    return _sync_wrapper


# ==========================================
# 3. Browser DOM & JS Snapshot API
# ==========================================
def install_browser_hooks() -> None:
    """브라우저 헤드에 전역 JS 에러 수집기 장착"""
    ui.add_head_html(f"<script>{_JS_ERROR_HOOK}</script>")


async def snapshot(reason: str = "manual") -> Dict[str, Any]:
    """DOM 치수 결함 + JS 에러 + 콘솔 State를 스캔하여 Plane(ailog/unified.jsonl) 및 클립보드에 덤프"""
    try:
        dom_report = await ui.run_javascript(_JS_DOM_INSPECTOR, timeout=2.0)
    except Exception as exc:
        dom_report = {"inspector_error": str(exc)}

    anomalies = dom_report.get("anomalies", []) if isinstance(dom_report, dict) else []
    js_errors = dom_report.get("js_errors", []) if isinstance(dom_report, dict) else []
    has_issue = bool(anomalies or js_errors)

    state_dump = {
        "redis_online": state.pipeline.redis_online,
        "finops_fuel": state.finops.current_fuel,
        "xray_latencies": list(state.security.xray_latencies),
        "cli_buffered": len(refs.cli_buffer),
        "pubsub_buffered": len(refs.pubsub_buffer),
    }

    summary = f"UI_SNAPSHOT [{reason}] anomalies={len(anomalies)} js_errors={len(js_errors)}"
    _emit_safe(
        "WARN" if has_issue else "INFO",
        summary,
        caller="diag.snapshot",
        reason=reason,
        dom_report=dom_report,
        console_state=state_dump,
    )

    clip_text = json.dumps(
        {"summary": summary, "dom_report": dom_report, "console_state": state_dump},
        indent=2,
        ensure_ascii=False,
    )
    ui.clipboard.write(clip_text)
    ui.notify(
        f"🩺 UI Snapshot logged & copied (Anomalies: {len(anomalies)})",
        type="warning" if has_issue else "positive",
    )
    return dom_report