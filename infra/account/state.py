# fiber.infra.account.state
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, List, Tuple
from xphi.node.space.bind.resolver import resolve_path

STATE_FILE_PATH = resolve_path("anchor") / "state.json"
USD_TO_FUEL_RATE = 1_000_000

_PERSIST_SCHEMA: Dict[str, Tuple[str, ...]] = {
    "finops": (
        "tenant_id",
        "spiffe_id",
        "ledger_balance",
        "fuel_quota",
        "policy_ver",
        "last_op",
        "jwt_op",
        "jwt_usd",
        "saved_usd",
        "saved_tokens",
    ),
    "security": (
        "block_api_keys",
        "custom_regex",
        "guardrail_mode",
        "test_prompt",
        "admin_user",
        "slack_channel",
        "slack_user_map",
        "oob_require_totp",
    ),
    "pipeline": (
        "redis_url",
        "vcr_mode",
        "vcr_scenario",
        "vcr_speed",
        "compat_model",
        "compat_json",
        "vcr_bench_live_ms",
        "vcr_bench_replay_ms",
    ),
}


@dataclass
class FinOpsState:
    tenant_id: str = "accounting_dept"
    spiffe_id: str = "spiffe://self/fiber"
    totp_enrolled: bool = False

    # $10.00 = 10,000,000 Fuel 기본 스케일 ($1 = 1M Fuel)
    ledger_balance: int = 10_000_000  # 현재 남은 실제 연료 (스트림 소모 시 차감됨)
    fuel_quota: int = 10_000_000      # 할당된 예산 기준값 사본 (비교 및 Re-sync 기준)
    policy_ver: int = 1
    last_op: str = "INIT"

    jwt_op: str = "SYNC_QUOTA"
    jwt_usd: float = 10.0
    sync_ledger: bool = True
    last_jwt_preview: str = "Click 'Apply' to set or top-up fuel balance."

    saved_usd: float = 12.40
    saved_tokens: int = 384000
    stream_active: bool = False
    stream_status_text: str = "IDLE"

    chart_dirty: bool = False
    x_ticks: deque = field(default_factory=lambda: deque(["Init"], maxlen=30))
    y_fuel: deque = field(default_factory=lambda: deque([10_000_000], maxlen=30))
    y_core: deque = field(default_factory=lambda: deque([10_000_000], maxlen=30))

    def set_balance(self, fuel_amount: int) -> None:
        val = max(0, int(fuel_amount))
        self.ledger_balance = val

    def push_chart_point(self, label: str | None = None) -> None:
        """현재 잔액(ledger_balance)을 차트 시계열에 즉시 반영한다."""
        tick_label = label or time.strftime("%H:%M:%S")
        self.x_ticks.append(tick_label)
        self.y_fuel.append(self.ledger_balance)
        self.y_core.append(self.fuel_quota)
        self.chart_dirty = True
    
    @property
    def balance_usd(self) -> float:
        return self.ledger_balance / USD_TO_FUEL_RATE

    @property
    def quota_usd(self) -> float:
        return self.fuel_quota / USD_TO_FUEL_RATE

    @property
    def account_summary_badge(self) -> str:
        return (
            f"Tenant: {self.tenant_id} (v{self.policy_ver}) | "
            f"Balance: {self.ledger_balance:,} / Quota: {self.fuel_quota:,} Fuel "
            f"(${self.balance_usd:,.2f} / ${self.quota_usd:,.2f})"
        )

@dataclass
class SecurityState:
    block_api_keys: bool = True
    custom_regex: str = r"sk-[a-zA-Z0-9]{20,}"
    guardrail_mode: str = "Strict Block (PermissionError)"
    test_prompt: str = "Analyze this key: sk-proj-99887766554433221100"
    xray_latencies: List[float] = field(default_factory=lambda: [0.12, 0.45, 0.14, 0.08])
    xray_chart_dirty: bool = False

    ssh_key_exists: bool = False
    ssh_key_path: str = "~/.ssh/fiber_ed25519"
    cipher_key_exists: bool = False
    cipher_key_path: str = "<sign>/local_cipher.key"
    node_pubkey: str = "uninitialized"

    admin_user: str = "spiffe://self/fiber"
    enrolled_admins: List[str] = field(default_factory=list)
    enrolled_admins_count: int = 0
    totp_secret: str = ""
    totp_uri: str = ""
    totp_input_code: str = ""
    sign_status_json: str = "{}"

    slack_channel: str = os.environ.get("FIBER_SLACK_CHANNEL_ID", "") or "#mcp-sec-ops"
    slack_user_map: str = "*"
    oob_require_totp: bool = True
    slack_live_configured: bool = bool(
        os.environ.get("FIBER_SLACK_BOT_TOKEN") and os.environ.get("FIBER_SLACK_CHANNEL_ID")
    )
    oob_inspector_json: str = (
        '{\n  "oob_bridge": "SlackHitlBridge Ready",\n'
        '  "hint": "Click \'📤 Dispatch OOB\' or \'🚨 Simulate 202\' to inspect signed State Token & Slack Block Kit."\n}'
    )

    gateway_url: str = "http://127.0.0.1:8356"
    gateway_online: bool = False
    gateway_latency_ms: float = 0.0
    gateway_last_check: str = "Never"
    active_target_ids: List[str] = field(default_factory=list)
    discovered_configs: List[Dict[str, Any]] = field(default_factory=list)
    discovered_workers: List[Dict[str, Any]] = field(default_factory=list)
    mcp_topology_dirty: bool = True

    @property
    def gateway_health_badge_text(self) -> str:
        if self.gateway_online:
            return (
                f"🟢 Gateway(:8356) ONLINE ({self.gateway_latency_ms:.1f}ms) | "
                f"Active: {len(self.active_target_ids)} | Checked: {self.gateway_last_check}"
            )
        return f"⚪ Gateway(:8356) STANDBY (Run 'fiber daemon') | Checked: {self.gateway_last_check}"

    @property
    def ssh_badge_text(self) -> str:
        keys_ok = self.ssh_key_exists and self.cipher_key_exists
        fp = self.node_pubkey[:12] if self.node_pubkey and self.node_pubkey != "uninitialized" else "uninit"
        return f"🟢 Node FP: {fp} (Keys Ready)" if keys_ok else f"🟡 Node FP: {fp} (Keys Missing)"

    @property
    def slack_oob_badge_text(self) -> str:
        mode = "TOTP+Ed25519" if self.oob_require_totp else "1-Click+Ed25519"
        conn = "🟢 Slack: Live SocketMode" if self.slack_live_configured else "🟣 Slack OOB: Local/Webhook Ready"
        return f"{conn} [{mode}]"

    @property
    def cipher_badge_text(self) -> str:
        return "🟢 local_cipher.key: FOUND (0600)" if self.cipher_key_exists else "🔴 local_cipher.key: MISSING"

    @property
    def pubkey_short_text(self) -> str:
        if not self.node_pubkey or self.node_pubkey == "uninitialized":
            return "Node PubKey: Not Loaded"
        return f"Node Ed25519 PubKey: {self.node_pubkey[:16]}...{self.node_pubkey[-8:]}"


@dataclass
class PipelineState:
    redis_online: bool = False
    redis_url: str = "redis://localhost:6379"
    pubsub_eps: int = 0

    vcr_mode: str = "record"
    vcr_scenario: str = "console_suite"
    vcr_speed: str = "max"
    compat_model: str = "gemini/gemini-3.1-flash-lite"
    compat_json: str = (
        '{\n  "extraction_rules": {\n    "llama_server": {\n'
        '      "text": ["choices.0.delta.content", "choices.0.message.content"]\n'
        "    }\n  }\n}"
    )
    vcr_bench_live_ms: float = 1539.2
    vcr_bench_replay_ms: float = 3.2
    vcr_chart_dirty: bool = False


@dataclass
class StudioState:
    finops: FinOpsState = field(default_factory=FinOpsState)
    security: SecurityState = field(default_factory=SecurityState)
    pipeline: PipelineState = field(default_factory=PipelineState)
    last_saved_at: str = "Never"

    @property
    def redis_badge_text(self) -> str:
        return "🟢 Redis: Live Pub/Sub" if self.pipeline.redis_online else "🟡 Mode: Standalone (In-Memory)"

    @property
    def fuel_kpi_text(self) -> str:
        f = self.finops
        return f"[{f.tenant_id}] Balance: {f.ledger_balance:,} / {f.fuel_quota:,} Fuel (v{f.policy_ver})"

    @property
    def saved_kpi_text(self) -> str:
        f = self.finops
        return f"Saved: ${f.saved_usd:.2f} ({f.saved_tokens:,} tok)"

    def to_persist_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for section_name, keys in _PERSIST_SCHEMA.items():
            sub_obj = getattr(self, section_name)
            data[section_name] = {k: getattr(sub_obj, k) for k in keys if hasattr(sub_obj, k)}
        return data

    def apply_persist_dict(self, payload: Dict[str, Any]) -> None:
        for section_name, keys in _PERSIST_SCHEMA.items():
            sec_data = payload.get(section_name)
            if not isinstance(sec_data, dict):
                continue
            sub_obj = getattr(self, section_name)
            for k in keys:
                if k in sec_data and sec_data[k] is not None:
                    setattr(sub_obj, k, sec_data[k])

        self.finops.set_balance(self.finops.ledger_balance)
        if self.finops.fuel_quota <= 0 and self.finops.ledger_balance > 0:
            self.finops.fuel_quota = self.finops.ledger_balance

        self.finops.y_fuel.clear()
        self.finops.y_fuel.append(self.finops.ledger_balance)
        self.finops.y_core.clear()
        self.finops.y_core.append(self.finops.fuel_quota)

@dataclass
class UIRefs:
    finops_chart: Any = None
    xray_chart: Any = None
    vcr_chart: Any = None
    live_stream_log: Any = None
    cli_log: Any = None
    pubsub_log: Any = None
    hitl_dialog: Any = None
    hitl_code: Any = None
    qr_dialog: Any = None
    qr_code_box: Any = None
    slot_cards: Dict[str, Any] = field(default_factory=dict)

    cli_buffer: deque = field(default_factory=lambda: deque(maxlen=500))
    pubsub_buffer: deque = field(default_factory=lambda: deque(maxlen=500))

    def push_cli(self, msg: str) -> None:
        self.cli_buffer.append(f"[{time.strftime('%H:%M:%S')}] {msg}")

    def push_pubsub(self, channel: str, msg: str) -> None:
        state.pipeline.pubsub_eps += 1
        self.pubsub_buffer.append(f"[{time.strftime('%H:%M:%S.%f')[:-3]}] [{channel}] {msg}")


state = StudioState()
refs = UIRefs()
_last_serialized_snapshot: str = ""


def save_state_to_disk(path: Path = STATE_FILE_PATH, *, force: bool = True) -> bool:
    global _last_serialized_snapshot
    try:
        payload = state.to_persist_dict()
        serialized = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
        if not force and serialized == _last_serialized_snapshot:
            return False

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(serialized, encoding="utf-8")
        tmp_path.replace(path)

        _last_serialized_snapshot = serialized
        state.last_saved_at = time.strftime("%H:%M:%S")
        return True
    except Exception:
        return False


def load_or_init_state(path: Path = STATE_FILE_PATH) -> bool:
    global _last_serialized_snapshot
    if path.exists():
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if isinstance(data, dict):
                state.apply_persist_dict(data)
                _last_serialized_snapshot = json.dumps(
                    state.to_persist_dict(), indent=2, ensure_ascii=False, sort_keys=True
                )
                state.last_saved_at = time.strftime("%H:%M:%S")
                return True
        except Exception:
            pass

    save_state_to_disk(path, force=True)
    return False