# fiber.phase.action.core
from __future__ import annotations

import asyncio
import io
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
from typing import Any

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_last_config_stat_sig: tuple[tuple[str, int, int], ...] = ()
_cached_mapped_entries: list[Any] = []

def safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

# FinOps / Account Actions (Headless)
async def fetch_account_status(tenant_id: str) -> dict[str, Any]:
    from fiber.infra.bound.account.manager import LocalAccountManager
    from fiber.infra.bound.account.registry import LocalAccountRegistry

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        st = await mgr.get_status()
    accounts = LocalAccountRegistry.list_accounts()
    acc_info = accounts.get(tenant_id, {})
    return {
        "raw_status": st,
        "last_op": acc_info.get("last_op"),
        "spiffe_id": acc_info.get("spiffe_id"),
        "core_balance": safe_int(st.get("core_state", {}).get("balance"), 0),
        "active_quota": safe_int(st.get("finops_watcher", {}).get("active_quota"), 0),
        "policy_ver": safe_int(st.get("finops_watcher", {}).get("policy_version"), 1),
    }

async def issue_account_mandate(
    tenant_id: str,
    *,
    budget_usd: float,
    op: str,
    policy_ver: int,
    sync_ledger: bool,
    spiffe_id: str | None = None,
) -> dict[str, Any]:
    from fiber.infra.bound.account.manager import LocalAccountManager, SyncQuotaCommand
    from fiber.infra.bound.account.registry import LocalAccountRegistry

    if spiffe_id:
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id)

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        cmd = SyncQuotaCommand(
            budget_usd=budget_usd,
            op=op,
            policy_ver=policy_ver,
            sync_ledger=sync_ledger,
        )
        res = await mgr.sync_quota(cmd)

    st = res.get("status", {})
    return {
        "jwt": res.get("jwt", ""),
        "core_balance": safe_int(st.get("core_state", {}).get("balance"), 0),
        "active_quota": safe_int(st.get("finops_watcher", {}).get("active_quota"), 0),
        "policy_ver": safe_int(st.get("finops_watcher", {}).get("policy_version"), policy_ver),
    }

# 2. Sign / TOTP / Slack OOB Actions (Headless)
async def inspect_sign_status() -> dict[str, Any]:
    from fiber.phase.cli.sign import (
        SSH_PRIV_KEY_PATH,
        LOCAL_CIPHER_KEY_PATH,
        DEFAULT_DB_PATH,
        _open_audit_db,
    )
    from xphi.arch.bound.adapter.pta import NodeSigner

    def _inspect() -> dict[str, Any]:
        signer = NodeSigner.get_instance()
        conn = _open_audit_db(DEFAULT_DB_PATH)
        rows = conn.execute(
            "SELECT user_id, substr(ciphertext, 1, 16) FROM admin_users ORDER BY rowid DESC"
        ).fetchall()
        conn.close()
        return {
            "ssh_fiber_ed25519": {"path": str(SSH_PRIV_KEY_PATH), "exists": SSH_PRIV_KEY_PATH.exists()},
            "local_cipher_key": {"path": str(LOCAL_CIPHER_KEY_PATH), "exists": LOCAL_CIPHER_KEY_PATH.exists()},
            "node_pubkey": signer.pubkey_hex,
            "audit_db": DEFAULT_DB_PATH,
            "enrolled_admins": [{"user_id": r[0], "cipher_prefix": f"{r[1]}..."} for r in rows],
        }

    return await asyncio.to_thread(_inspect)

async def init_node_keys() -> dict[str, Any]:
    from fiber.phase.cli.sign import ensure_node_keys
    return await asyncio.to_thread(ensure_node_keys)

async def enroll_admin_totp(admin_user: str) -> dict[str, Any]:
    import pyotp
    import qrcode
    from fiber.phase.cli.sign import (
        AdminSecretVault,
        _normalize_spiffe_id,
        _build_totp_provisioning_uri,
        _resolve_vault_context,
        _open_audit_db,
        DEFAULT_DB_PATH,
        TOTP_ISSUER_NAME,
    )

    canonical_id = _normalize_spiffe_id(admin_user)
    secret = pyotp.random_base32()

    def _provision() -> str:
        active_passphrase, pubkey_hex = _resolve_vault_context()
        vault = AdminSecretVault(active_passphrase)
        salt_hex, nonce_hex, cipher_hex = vault.encrypt(secret)
        conn = _open_audit_db(DEFAULT_DB_PATH)
        conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
        conn.execute(
            "INSERT INTO admin_users (user_id, salt, nonce, ciphertext) VALUES (?, ?, ?, ?)",
            (canonical_id, salt_hex, nonce_hex, cipher_hex),
        )
        conn.commit()
        conn.close()
        return pubkey_hex

    pubkey_hex = await asyncio.to_thread(_provision)
    totp_uri, display_label = _build_totp_provisioning_uri(secret, canonical_id, pubkey_hex)

    qr = qrcode.QRCode(version=1, box_size=1, border=1)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    buf = io.StringIO()
    qr.print_ascii(out=buf, invert=True)

    return {
        "canonical_id": canonical_id,
        "secret": secret,
        "totp_uri": totp_uri,
        "display_label": display_label,
        "issuer": TOTP_ISSUER_NAME,
        "qr_ascii": buf.getvalue(),
    }

async def verify_admin_totp(admin_user: str, code_override: str | None = None) -> dict[str, Any]:
    import pyotp
    from fiber.phase.cli.sign import (
        AdminSecretVault,
        _normalize_spiffe_id,
        _resolve_vault_context,
        _open_audit_db,
        DEFAULT_DB_PATH,
    )

    canonical_id = _normalize_spiffe_id(admin_user)

    def _check() -> dict[str, Any]:
        active_passphrase, pubkey_hex = _resolve_vault_context()
        conn = _open_audit_db(DEFAULT_DB_PATH)
        row = conn.execute(
            "SELECT salt, nonce, ciphertext FROM admin_users WHERE user_id = ?",
            (canonical_id,),
        ).fetchone()
        conn.close()
        if not row:
            return {"registered": False, "valid": False, "code": "UNREGISTERED", "pubkey": pubkey_hex, "user_id": canonical_id}
        vault = AdminSecretVault(active_passphrase)
        secret = vault.decrypt(*row)
        totp = pyotp.TOTP(secret)
        target_code = code_override.strip() if code_override else totp.now()
        is_ok = totp.verify(str(target_code).zfill(6), valid_window=1)
        return {"registered": True, "valid": is_ok, "code": target_code, "pubkey": pubkey_hex, "user_id": canonical_id}

    return await asyncio.to_thread(_check)

async def revoke_admin_totp(admin_user: str) -> str:
    from fiber.phase.cli.sign import _normalize_spiffe_id, _open_audit_db, DEFAULT_DB_PATH

    canonical_id = _normalize_spiffe_id(admin_user)

    def _delete() -> None:
        conn = _open_audit_db(DEFAULT_DB_PATH)
        conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
        conn.commit()
        conn.close()

    await asyncio.to_thread(_delete)
    return canonical_id

async def dispatch_slack_oob_payload(
    *,
    slack_channel: str,
    slack_user_map: str,
    admin_user: str,
    require_totp: bool,
    target_id: str = "postgres-prod-db",
    tool_name: str = "drop_table",
    tool_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from fiber.gateway.bridge.mcp.slack import SlackHitlBridge

    args_payload = tool_args or {"table": "customer_ledgers", "cascade": True}
    handle_id = f"h_{int(time.time() * 1000) % 1000000:06d}"

    bridge = SlackHitlBridge(default_channel=slack_channel)
    bridge.register_identity_mapping(slack_user_map.strip() or "*", admin_user)

    yield_event = {
        "event": "MCP_PHASE_YIELD",
        "handle_id": handle_id,
        "target_server_id": f"{target_id}.{tool_name}",
        "agent": admin_user,
        "prompt": {
            "id": f"req_{handle_id}",
            "params": {
                "require_totp": require_totp,
                "message": f"Execute '{tool_name}' with args {json.dumps(args_payload)}",
            },
        },
    }
    blocks = bridge.build_yield_message_blocks(yield_event, channel=slack_channel)
    ts = await bridge.post_yield_notification(yield_event) if bridge.is_live_configured else None
    return {
        "handle_id": handle_id,
        "is_live": bridge.is_live_configured,
        "node_fp": bridge.node_fp,
        "ts": ts,
        "blocks": blocks,
    }


def build_hitl_resolution_receipt(
    *, slack_channel: str, slack_user_map: str, admin_user: str, require_totp: bool
) -> dict[str, Any]:
    from fiber.gateway.bridge.mcp.slack import SlackHitlBridge

    bridge = SlackHitlBridge(default_channel=slack_channel)
    token = bridge.mint_approval_state_token("h_active", "postgres-prod-db", "req_1", require_totp=require_totp)
    return bridge.build_resolution_receipt_blocks(
        {"state_token": token, "slack_user_id": slack_user_map or "operator"},
        {"status": "RESUMED", "handle_id": "h_active", "attested_by": admin_user},
    )

# MCP Topology & Gateway Health (Headless)
def _inspect_and_sanitize_config_candidates(
    candidate_map: dict[str, list[Path]],
) -> tuple[list[dict[str, Any]], tuple[tuple[str, int, int], ...]]:
    config_list: list[dict[str, Any]] = []
    stat_sig: list[tuple[str, int, int]] = []

    for agent_name, paths in candidate_map.items():
        existing = next((p for p in paths if p.is_file()), None)
        primary_path = existing or (paths[0] if paths else None)
        if primary_path is None:
            continue

        has_valid_content = False
        mtime_ns = 0
        size = 0

        if existing is not None:
            try:
                raw_text = existing.read_text(encoding="utf-8", errors="replace").strip()
                if not raw_text:
                    existing.write_text("{}\n", encoding="utf-8")
                    raw_text = "{}"
                st = existing.stat()
                mtime_ns = st.st_mtime_ns
                size = st.st_size
                has_valid_content = raw_text not in ("", "{}")
            except OSError:
                pass
        elif agent_name != "fiber" and not primary_path.parent.exists():
            continue

        stat_sig.append((str(primary_path), mtime_ns, size))
        config_list.append({
            "agent": agent_name,
            "path": str(primary_path),
            "name": primary_path.name,
            "exists": existing is not None and has_valid_content,
            "file_present": existing is not None,
        })

    return config_list, tuple(stat_sig)

async def scan_mcp_topology_and_health(gateway_url: str, *, force_parse: bool = False) -> dict[str, Any]:
    global _last_config_stat_sig, _cached_mapped_entries
    import httpx
    from fiber.infra.mcp.mapper.config import McpConfigMapper
    from fiber.phase.cli.connect import discover_builtin_workers, resolve_connector_spec
    from xphi.node.space.tunnel.factory import TunnelFactory

    candidate_map = McpConfigMapper._resolve_candidate_paths()
    config_list, current_stat_sig = await asyncio.to_thread(
        _inspect_and_sanitize_config_candidates, candidate_map
    )

    live_targets: set[str] = set()
    try:
        tunnel = await TunnelFactory.get_default()
        if tunnel:
            raw = await tunnel.get("mcp:registry:active_targets")
            if raw:
                parsed = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                if isinstance(parsed, list):
                    live_targets = {str(x) for x in parsed}
    except Exception:
        pass

    if force_parse or current_stat_sig != _last_config_stat_sig:
        _cached_mapped_entries = await asyncio.to_thread(McpConfigMapper.scan_and_parse)
        _last_config_stat_sig = current_stat_sig

    workers: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for entry in _cached_mapped_entries:
        spec = resolve_connector_spec(target=entry.target_id, exec_cmd=entry.exec_cmd)
        workers.append({
            "target_id": spec.target_id,
            "source": entry.source_agent,
            "transport": spec.transport,
            "mode": spec.mode,
            "preflight": entry.env.get("FIBER_PREFLIGHT_PATTERN") or spec.preflight or "",
            "is_live": spec.target_id in live_targets,
            "exec_cmd": spec.exec_cmd,
        })
        seen_ids.add(spec.target_id)

    if not workers:
        builtins = await asyncio.to_thread(discover_builtin_workers)
        for _, (canon_id, cmd, def_mode) in builtins.items():
            if canon_id not in seen_ids:
                seen_ids.add(canon_id)
                workers.append({
                    "target_id": canon_id,
                    "source": "builtin",
                    "transport": "stdio",
                    "mode": def_mode,
                    "preflight": "",
                    "is_live": canon_id in live_targets,
                    "exec_cmd": cmd,
                })

    t0 = time.perf_counter()
    gw_online = False
    gw_lat = 0.0
    try:
        async with httpx.AsyncClient(timeout=0.45) as client:
            resp = await client.get(f"{gateway_url}/_health")
            if resp.status_code == 200:
                gw_online = True
                gw_lat = round((time.perf_counter() - t0) * 1000, 1)
    except Exception:
        pass

    return {
        "configs": config_list,
        "workers": workers,
        "active_target_ids": sorted(live_targets),
        "gateway_online": gw_online,
        "gateway_latency_ms": gw_lat,
        "checked_at": time.strftime("%H:%M:%S"),
    }

# Pipeline Guardrail & VCR Runner (Headless)
def matches_secret_pattern(prompt: str, custom_regex: str) -> bool:
    if "sk-" in prompt or "Bearer " in prompt:
        return True
    pattern = custom_regex.strip()
    if not pattern:
        return False
    try:
        return bool(re.search(pattern, prompt))
    except re.error:
        return False