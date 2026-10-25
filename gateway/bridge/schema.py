# fiber.gateway.bridge.schema
## @lineage: fiber.phase.bridge.schema
from __future__ import annotations
from typing import Any
from pydantic import Field
from xphi.arch.model.surge.model import SurgeBaseModel


# 1. FinOps / Account Schemas
class AccountStatusResult(SurgeBaseModel):
    tenant_id: str = ""
    registry_file: str = ""
    raw_status: dict[str, Any] = Field(default_factory=dict)
    last_op: str | None = None
    updated_at: str | None = None
    spiffe_id: str | None = None
    ledger_balance: int = 0
    active_allocations: int = 0
    active_quota: int = 0
    policy_ver: int = 0


class AccountMandateResult(SurgeBaseModel):
    jwt: str = ""
    compute_ticket_issued: bool = False
    compute_ticket: dict[str, Any] | None = None
    balance: int = 0
    active_quota: int = 0
    policy_ver: int = 1
    status: AccountStatusResult | None = None

    @property
    def ledger_balance(self) -> int:
        return self.balance


class AccountAllocateResult(SurgeBaseModel):
    tx_hash: str | None = None
    allocated_amount: int = 0
    status: AccountStatusResult


class AccountRevokeResult(SurgeBaseModel):
    tenant_id: str
    burn_tx_hash: str | None = None
    signal: str = "REVOKE_PUBLISHED"
    status: AccountStatusResult


class AccountListResult(SurgeBaseModel):
    default_tenant: str
    registry_file: str
    accounts: dict[str, Any] = Field(default_factory=dict)


# 2. Sign / TOTP / Slack OOB Schemas
class KeyFileInfo(SurgeBaseModel):
    path: str
    exists: bool


class EnrolledAdminInfo(SurgeBaseModel):
    user_id: str
    cipher_prefix: str


class SignStatusResult(SurgeBaseModel):
    ssh_fiber_ed25519: KeyFileInfo
    local_cipher_key: KeyFileInfo
    node_pubkey: str
    audit_db: str
    enrolled_admins: list[EnrolledAdminInfo] = Field(default_factory=list)


class NodeKeysInitResult(SurgeBaseModel):
    ssh_key_status: str = "unknown"
    local_cipher_status: str = "unknown"
    raw_info: dict[str, Any] = Field(default_factory=dict)


class TotpEnrollmentResult(SurgeBaseModel):
    canonical_id: str
    secret: str
    totp_uri: str
    display_label: str
    issuer: str
    qr_ascii: str


class TotpVerificationResult(SurgeBaseModel):
    registered: bool
    valid: bool
    code: str
    pubkey: str
    user_id: str


class SlackOobDispatchResult(SurgeBaseModel):
    handle_id: str
    is_live: bool
    node_fp: str
    ts: str | None = None
    blocks: list[dict[str, Any]] | dict[str, Any] = Field(default_factory=list)


# 3. MCP Topology & Gateway Health Schemas
class McpConfigCandidate(SurgeBaseModel):
    agent: str
    path: str
    name: str
    exists: bool
    file_present: bool


class McpWorkerInfo(SurgeBaseModel):
    target_id: str
    source: str
    transport: str
    mode: str
    preflight: str = ""
    is_live: bool = False
    exec_cmd: str = ""


class McpTopologyScanResult(SurgeBaseModel):
    configs: list[McpConfigCandidate] = Field(default_factory=list)
    workers: list[McpWorkerInfo] = Field(default_factory=list)
    active_target_ids: list[str] = Field(default_factory=list)
    gateway_online: bool = False
    gateway_latency_ms: float = 0.0
    checked_at: str = ""