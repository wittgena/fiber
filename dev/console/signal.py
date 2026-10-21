# fiber.dev.console.signal
## @lineage: fiber.dev.console.filter.surface
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Any, Dict, Optional, Set, Tuple, Type, TypeVar, Union
from xphi.arch.contract.event.next import LogEvent

E = TypeVar("E", bound=Enum)
_MS_RE = re.compile(r"(\d+(?:\.\d+)?)\s*ms\b", re.IGNORECASE)


class SurfaceDomain(str, Enum):
    FINOPS = "finops"
    MCP = "mcp"
    SIGN = "sign"
    VCR = "vcr"
    KERNEL = "kernel"
    PLANE = "plane"


class UIKey(str, Enum):
    """`event.context` (`**kwargs`)에 주입되는 표준 페이로드 키"""
    SLOT = "ui_slot"
    SLOT_STATUS = "slot_status"
    LATENCY_MS = "latency_ms"
    SAVED_TOKENS = "saved_tokens"
    SAVED_USD = "saved_usd"

    METRIC = "ui_metric"
    TENANT_ID = "tenant_id"
    ACTIVE_QUOTA = "active_quota"
    CORE_BALANCE = "core_balance"
    POLICY_VER = "policy_ver"
    ELAPSED_MS = "elapsed_ms"
    CHUNKS = "chunks"

    MODAL = "ui_modal"
    TARGET_ID = "target_id"
    TOOL_NAME = "tool_name"
    TOOL_ARGS = "tool_args"
    IDEM_KEY = "idem_key"


class UISlot(str, Enum):
    """Tab 2: Pipeline X-Ray 슬롯 식별자"""
    FUEL = "fuel"
    GUARDRAIL = "guardrail"
    ROUTER = "router"
    CACHE = "cache"


class UISlotStatus(str, Enum):
    """Tab 2: Pipeline X-Ray 슬롯 점등 상태"""
    HIT = "hit"
    PASS = "pass"
    BLOCK = "block"
    SEVER = "sever"


class UIMetric(str, Enum):
    """Tab 1 & Tab 4: 차트 및 서킷 브레이커 구동 메트릭 식별자"""
    LEDGER_SYNC = "ledger_sync"
    FUEL_TICK = "fuel_tick"
    KILL_SWITCH = "kill_switch"
    VCR_REPLAY = "vcr_replay"
    VCR_LIVE = "vcr_live"


class UIModal(str, Enum):
    """Tab 3: 글로벌 인터럽트 모달 식별자"""
    HITL_202 = "hitl_202"


def _coerce_enum(enum_cls: Type[E], raw_val: Any) -> Optional[E]:
    """원시 문자열/Enum 값을 안전하게 목표 Enum 타입으로 변환 (불일치 시 None)"""
    if raw_val is None:
        return None
    if isinstance(raw_val, enum_cls):
        return raw_val
    try:
        return enum_cls(str(raw_val).strip().lower())
    except ValueError:
        return None


@dataclass(frozen=True)
class RoutedSurfaceMeta:
    domain: SurfaceDomain
    channel: str
    level: str
    ui_slot: Optional[UISlot] = None
    ui_slot_status: UISlotStatus = UISlotStatus.PASS
    ui_metric: Optional[UIMetric] = None
    ui_modal: Optional[UIModal] = None
    ui_payload: Dict[str, Any] = field(default_factory=dict)
    is_bursting: bool = False
    is_hitl_yield: bool = False
    is_kill_signal: bool = False


class SurfaceClassifier:
    PHASE_MAP: Dict[str, SurfaceDomain] = {
        "BILLING": SurfaceDomain.FINOPS,
        "DEFENSE": SurfaceDomain.SIGN,
        "AUDIT": SurfaceDomain.SIGN,
        "KERNEL": SurfaceDomain.KERNEL,
        "ANCHOR": SurfaceDomain.KERNEL,
        "COMMIT": SurfaceDomain.KERNEL,
        "LLM_INTER": SurfaceDomain.VCR,
        "LLM_EMBEDDING": SurfaceDomain.VCR,
    }

    DOMAIN_RULES: Tuple[Tuple[SurfaceDomain, Set[str]], ...] = (
        (SurfaceDomain.MCP, {"mcp", "connect", "preflight", "oob", "slack"}),
        (SurfaceDomain.FINOPS, {"finops", "account", "fuel", "pricing", "cost", "quota", "settlement", "eco", "exchange"}),
        (SurfaceDomain.SIGN, {"sign", "vault", "secret", "cipher", "sentinel", "guardrail", "compliance", "auditor", "security", "risk"}),
        (SurfaceDomain.VCR, {"vcr", "compat", "llm", "token", "provider", "chunk", "param", "embedding", "llama", "gemini", "traverser"}),
        (SurfaceDomain.KERNEL, {"anchor", "pta", "commit", "wasm", "receptor", "warden", "dvm", "oracle", "topos", "fsm"}),
    )

    SLOT_RULES: Tuple[Tuple[UISlot, Set[str]], ...] = (
        (UISlot.CACHE, {"cache"}),
        (UISlot.ROUTER, {"router", "resolver"}),
        (UISlot.GUARDRAIL, {"guardrail", "sentinel", "redact"}),
        (UISlot.FUEL, {"fuel"}),
    )

    @classmethod
    def classify(cls, ev: Union[LogEvent, Dict[str, Any]]) -> RoutedSurfaceMeta:
        d = ev if isinstance(ev, dict) else ev.__dict__
        src = str(d.get("source_id") or "unknown").strip().lower()
        msg = str(d.get("message") or "")
        msg_up = msg.upper()
        level = str(d.get("level") or "INFO").upper()
        raw_ctx = d.get("context")
        ctx = dict(raw_ctx) if isinstance(raw_ctx, dict) else {}
        phase = str(ctx.get("phase") or "SYSTEM").upper()
        tokens = set(src.replace("_", ".").split("."))

        # 1. [Track 1] 명시적 Enum 키 우선 추출 (핵심 제어 지점)
        ui_slot = _coerce_enum(UISlot, ctx.get(UIKey.SLOT.value))
        ui_slot_status = _coerce_enum(UISlotStatus, ctx.get(UIKey.SLOT_STATUS.value))
        ui_metric = _coerce_enum(UIMetric, ctx.get(UIKey.METRIC.value))
        ui_modal = _coerce_enum(UIModal, ctx.get(UIKey.MODAL.value))

        # 2. [Track 2] 경량 병렬 추론 (일반 모듈에서 UIKey 없이 로그만 찍었을 때 자동 보완)
        if ui_slot is None:
            ui_slot = next((slot for slot, keys in cls.SLOT_RULES if tokens & keys), None)

        if ui_slot_status is None:
            if level in ("ERROR", "CRIT", "SIGNAL") or "BLOCK" in msg_up or "REJECT" in msg_up:
                ui_slot_status = UISlotStatus.SEVER if ui_slot is UISlot.FUEL else UISlotStatus.BLOCK
            elif "HIT" in msg_up:
                ui_slot_status = UISlotStatus.HIT
            else:
                ui_slot_status = UISlotStatus.PASS

        if m_ms := _MS_RE.search(msg):
            ms_val = float(m_ms.group(1))
            if ui_slot is not None and UIKey.LATENCY_MS.value not in ctx:
                ctx[UIKey.LATENCY_MS.value] = ms_val
            if ("vcr" in tokens or "compat" in tokens) and UIKey.ELAPSED_MS.value not in ctx:
                ctx[UIKey.ELAPSED_MS.value] = ms_val
                if ui_metric is None:
                    ui_metric = UIMetric.VCR_REPLAY if "REPLAY" in msg_up else UIMetric.VCR_LIVE

        # 3. 도메인 및 채널 결정
        domain = next((dom for dom, keys in cls.DOMAIN_RULES if tokens & keys), None)
        if domain is None:
            domain = cls.PHASE_MAP.get(phase, SurfaceDomain.PLANE)

        noise = {"xphi", "fiber", "infra", "dev", "bound", "gateway", "phase", "cli", "watcher", "arch", "state", "node", domain.value}
        parts = [p for p in src.split(".") if p and p not in noise]
        sub_topic = parts[-1] if len(parts) <= 1 else ".".join(parts[-2:])

        # 4. 핵심 시그널 판정
        is_hitl = (
            ui_modal is UIModal.HITL_202
            or (domain is SurfaceDomain.MCP and ("202" in msg_up or "YIELD" in msg_up))
        )
        is_kill = (
            ui_metric is UIMetric.KILL_SWITCH
            or (ui_slot is UISlot.FUEL and ui_slot_status is UISlotStatus.SEVER)
            or "EVENT=REVOKE" in msg_up
            or (level in ("SIGNAL", "CRIT") and any(k in msg_up for k in ("REVOKE", "RUPTURE DETECTED", "KILL_SWITCH")))
        )

        return RoutedSurfaceMeta(
            domain=domain,
            channel=f"{domain.value}:{sub_topic or 'core'}",
            level=level,
            ui_slot=ui_slot,
            ui_slot_status=ui_slot_status,
            ui_metric=ui_metric,
            ui_modal=ui_modal,
            ui_payload=ctx,
            is_bursting=(d.get("kind") == "summary"),
            is_hitl_yield=is_hitl,
            is_kill_signal=is_kill,
        )


@dataclass
class SurfaceFilter:
    min_level: str = "INFO"
    allowed_domains: Optional[Set[SurfaceDomain]] = None

    WEIGHTS = {"TRACE": 10, "DEBUG": 20, "INFO": 30, "WARN": 40, "WARNING": 40, "ERROR": 50, "CRIT": 60, "CRITICAL": 60, "SIGNAL": 70}

    def match(self, ev: Union[LogEvent, Dict[str, Any]]) -> Tuple[bool, RoutedSurfaceMeta]:
        meta = SurfaceClassifier.classify(ev)
        if (
            meta.level in ("CRIT", "CRITICAL", "SIGNAL")
            or meta.is_kill_signal
            or meta.is_hitl_yield
            or meta.ui_slot is not None
            or meta.ui_metric is not None
            or meta.ui_modal is not None
        ):
            return True, meta
        if self.WEIGHTS.get(meta.level, 30) < self.WEIGHTS.get(self.min_level, 30):
            return False, meta
        if self.allowed_domains and meta.domain not in self.allowed_domains:
            return False, meta
        return True, meta