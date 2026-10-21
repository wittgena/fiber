# fiber.dev.console.state
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class FinOpsState:
    tenant_id: str = "accounting_dept"
    spiffe_id: str = "spiffe://self/fiber"
    totp_enrolled: bool = False

    current_fuel: int = 50000
    core_balance: int = 50000
    budget_limit: int = 15000
    policy_ver: int = 1
    last_op: str = "INIT"

    jwt_op: str = "PROVISION"
    jwt_usd: float = 50.0
    sync_ledger: bool = True
    last_jwt_preview: str = "Click 'Issue EdDSA JWT' to mint a NodeSigner-signed mandate token."

    saved_usd: float = 12.40
    saved_tokens: int = 384000
    stream_active: bool = False
    stream_status_text: str = "IDLE"

    chart_dirty: bool = False
    x_ticks: deque = field(default_factory=lambda: deque(["Init"], maxlen=30))
    y_fuel: deque = field(default_factory=lambda: deque([50000], maxlen=30))
    y_core: deque = field(default_factory=lambda: deque([50000], maxlen=30))

    @property
    def account_summary_badge(self) -> str:
        totp_tag = "🟢 TOTP" if self.totp_enrolled else "⚠️ No-TOTP"
        spiffe_short = self.spiffe_id.replace("spiffe://self/", "") if self.spiffe_id else "unassigned"
        return f"Tenant: {self.tenant_id} ({spiffe_short} | {totp_tag}) | v{self.policy_ver} | Quota: {self.current_fuel:,} / Core: {self.core_balance:,}"


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
    totp_secret: str = ""
    totp_input_code: str = ""
    sign_status_json: str = '{\n  "info": "Click \'Status\' or \'Init Keys\' to inspect local identity."\n}'

    @property
    def ssh_badge_text(self) -> str:
        return "🟢 ~/.ssh/fiber_ed25519: FOUND (0600)" if self.ssh_key_exists else "🔴 ~/.ssh/fiber_ed25519: MISSING"

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
    compat_json: str = (
        '{\n  "extraction_rules": {\n    "llama_server": {\n'
        '      "text": ["choices.0.delta.content", "choices.0.message.content"]\n'
        '    }\n  }\n}'
    )
    vcr_bench_live_ms: float = 1539.2
    vcr_bench_replay_ms: float = 3.2
    vcr_chart_dirty: bool = False


@dataclass
class StudioState:
    finops: FinOpsState = field(default_factory=FinOpsState)
    security: SecurityState = field(default_factory=SecurityState)
    pipeline: PipelineState = field(default_factory=PipelineState)

    @property
    def redis_badge_text(self) -> str:
        return "🟢 Redis: Live Pub/Sub" if self.pipeline.redis_online else "🟡 Mode: Standalone (In-Memory)"

    @property
    def fuel_kpi_text(self) -> str:
        f = self.finops
        return f"[{f.tenant_id}] Quota: {f.current_fuel:,} | Core: {f.core_balance:,} Fuel (v{f.policy_ver})"

    @property
    def saved_kpi_text(self) -> str:
        f = self.finops
        return f"Saved: ${f.saved_usd:.2f} ({f.saved_tokens:,} tok)"


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

    def push_cli(self, msg: str):
        self.cli_buffer.append(f"[{time.strftime('%H:%M:%S')}] {msg}")

    def push_pubsub(self, channel: str, msg: str):
        state.pipeline.pubsub_eps += 1
        self.pubsub_buffer.append(f"[{time.strftime('%H:%M:%S.%f')[:-3]}] [{channel}] {msg}")


state = StudioState()
refs = UIRefs()