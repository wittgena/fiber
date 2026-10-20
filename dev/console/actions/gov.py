# fiber.dev.console.actions.gov
"""
@desc: Governance, Identity & FinOps Actions (Tab 1 Account & Tab 3 Sign/HITL)
- Coordinates Account <-> SPIFFE <-> TOTP state transitions, non-blocking warnings, and HITL 202 attestation.
"""
from __future__ import annotations

import asyncio
import io
import json
from nicegui import ui

from fiber.dev.console.state import state, refs


# ==========================================
# Shared Helper: SPIFFE Normalization & Non-blocking TOTP Check
# ==========================================
def normalize_console_spiffe(raw_spiffe: str) -> str:
    raw = raw_spiffe.strip()
    if not raw:
        return ""
    return raw if raw.startswith("spiffe://") else f"spiffe://self/{raw}"


def sync_spiffe_and_check_totp(emit_warning: bool = True) -> None:
    """
    Checks if `state.finops.spiffe_id` is enrolled in `state.security.enrolled_admins`.
    Syncs the target SPIFFE ID across Tab 1 and Tab 3, and emits a non-blocking warning if missing.
    """
    f = state.finops
    s = state.security
    if not f.spiffe_id or not f.spiffe_id.strip():
        f.totp_enrolled = False
        return

    norm_id = normalize_console_spiffe(f.spiffe_id)
    f.spiffe_id = norm_id
    s.admin_user = norm_id  # Sync target SPIFFE with Tab 3 & HITL 202 modal
    f.totp_enrolled = norm_id in s.enrolled_admins

    if not f.totp_enrolled and emit_warning:
        warn_msg = (
            f"⚠️ [AccountRegistry] SPIFFE '{norm_id}' bound to '{f.tenant_id}', "
            f"but TOTP is not enrolled (Enroll in Tab 3)."
        )
        refs.push_cli(f"  {warn_msg}")
        refs.push_pubsub("finops:warning", f"UNENROLLED_SPIFFE tenant={f.tenant_id} spiffe={norm_id}")
        ui.notify(warn_msg, type="warning")


# ==========================================
# Tab 1: FinOps & LocalAccount Direct Actions
# ==========================================
async def act_refresh_account_status(silent: bool = False):
    """LocalAccountManager 및 Registry를 조회하여 Balance, Quota, Bound SPIFFE를 UI에 반영"""
    from fiber.dev.console.runtime import run_fiber_cli
    f = state.finops
    try:
        from fiber.infra.bound.account.manager import LocalAccountManager
        from fiber.infra.bound.account.registry import LocalAccountRegistry

        async with LocalAccountManager(tenant_id=f.tenant_id) as mgr:
            st = await mgr.get_status()
        accounts = LocalAccountRegistry.list_accounts()
        if f.tenant_id in accounts:
            acc_info = accounts[f.tenant_id]
            f.last_op = acc_info.get("last_op", f.last_op)
            if acc_info.get("spiffe_id"):
                f.spiffe_id = acc_info["spiffe_id"]

        sync_spiffe_and_check_totp(emit_warning=not silent)

        f.core_balance = int(st.get("core_state", {}).get("balance", f.core_balance))
        f.current_fuel = int(float(st.get("finops_watcher", {}).get("active_quota", f.current_fuel)))
        f.policy_ver = int(st.get("finops_watcher", {}).get("policy_version", f.policy_ver))
        f.chart_dirty = True
        if not silent:
            refs.push_cli(f"$ fiber account --tenant {f.tenant_id} status\n  {json.dumps(st, ensure_ascii=False)}")
            ui.notify(f"Synced '{f.tenant_id}': Quota={f.current_fuel:,} / Core={f.core_balance:,} Fuel", type="info")
    except Exception:
        if not silent:
            await run_fiber_cli("account", "--tenant", f.tenant_id, "status")


async def act_issue_account_jwt():
    """EdDSA JWT 발급, SPIFFE 단일 바인딩(TOTP 미등록 시 Warning) 및 원장 동기화를 수행"""
    from fiber.dev.console.runtime import run_fiber_cli
    f = state.finops
    spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None
    try:
        from fiber.infra.bound.account.manager import LocalAccountManager, SyncQuotaCommand
        from fiber.infra.bound.account.registry import LocalAccountRegistry

        if spiffe_arg:
            LocalAccountRegistry.record(f.tenant_id, spiffe_id=spiffe_arg)
            sync_spiffe_and_check_totp(emit_warning=True)

        async with LocalAccountManager(tenant_id=f.tenant_id) as mgr:
            cmd = SyncQuotaCommand(
                budget_usd=f.jwt_usd,
                op=f.jwt_op,
                policy_ver=f.policy_ver,
                sync_ledger=f.sync_ledger,
            )
            res = await mgr.sync_quota(cmd)
        jwt_str = res.get("jwt", "")
        st = res.get("status", {})
        f.last_jwt_preview = f"{jwt_str[:42]}... (v{f.policy_ver}, {f.jwt_op})"
        f.core_balance = int(st.get("core_state", {}).get("balance", f.core_balance))
        f.current_fuel = int(float(st.get("finops_watcher", {}).get("active_quota", f.current_fuel)))
        f.policy_ver = int(st.get("finops_watcher", {}).get("policy_version", f.policy_ver))
        f.last_op = f.jwt_op
        f.chart_dirty = True

        spiffe_flag = f" --spiffe {f.spiffe_id}" if spiffe_arg else ""
        refs.push_cli(
            f"$ fiber account --tenant {f.tenant_id}{spiffe_flag} "
            f"jwt --usd {f.jwt_usd} --op {f.jwt_op} --ver {f.policy_ver}"
        )
        refs.push_cli(f"  ✓ [EdDSA JWT] {f.last_jwt_preview} -> Quota={f.current_fuel:,} | Core={f.core_balance:,}")
        refs.push_pubsub(
            "finops:control_plane",
            f"EVENT={f.jwt_op} tenant={f.tenant_id} spiffe={f.spiffe_id} fuel={f.current_fuel} v={f.policy_ver}",
        )
        ui.notify(f"Mandate Applied: {f.jwt_op} (${f.jwt_usd:.1f}, v{f.policy_ver})", type="positive")
    except Exception:
        args = ["account", "--tenant", f.tenant_id]
        if spiffe_arg:
            args.extend(["--spiffe", spiffe_arg])
            sync_spiffe_and_check_totp(emit_warning=True)
        args.extend(["jwt", "--usd", str(f.jwt_usd), "--op", f.jwt_op, "--ver", str(f.policy_ver)])
        if f.sync_ledger:
            args.append("--sync")
        await run_fiber_cli(*args)


# ==========================================
# Tab 3: Local Node Identity (`fiber sign`) & HITL 202 Actions
# ==========================================
async def act_refresh_sign_status(silent: bool = False):
    """`~/.ssh/fiber_ed25519`, `local_cipher.key`, `NodeSigner.pubkey_hex`, `deploy_audit.sqlite` 상태를 직접 검사"""
    s = state.security
    try:
        from fiber.phase.cli.sign import (
            SSH_PRIV_KEY_PATH,
            LOCAL_CIPHER_KEY_PATH,
            DEFAULT_DB_PATH,
            _open_audit_db,
        )
        from xphi.arch.bound.adapter.pta import NodeSigner

        def _inspect() -> dict:
            signer = NodeSigner.get_instance()
            conn = _open_audit_db(DEFAULT_DB_PATH)
            rows = conn.execute(
                "SELECT user_id, substr(ciphertext, 1, 16) FROM admin_users ORDER BY rowid DESC"
            ).fetchall()
            conn.close()
            return {
                "ssh_fiber_ed25519": {
                    "path": str(SSH_PRIV_KEY_PATH),
                    "exists": SSH_PRIV_KEY_PATH.exists(),
                },
                "local_cipher_key": {
                    "path": str(LOCAL_CIPHER_KEY_PATH),
                    "exists": LOCAL_CIPHER_KEY_PATH.exists(),
                },
                "node_pubkey": signer.pubkey_hex,
                "audit_db": DEFAULT_DB_PATH,
                "enrolled_admins": [{"user_id": r[0], "cipher_prefix": f"{r[1]}..."} for r in rows],
            }

        data = await asyncio.to_thread(_inspect)
        s.ssh_key_exists = data["ssh_fiber_ed25519"]["exists"]
        s.ssh_key_path = data["ssh_fiber_ed25519"]["path"]
        s.cipher_key_exists = data["local_cipher_key"]["exists"]
        s.cipher_key_path = data["local_cipher_key"]["path"]
        s.node_pubkey = data["node_pubkey"]
        s.enrolled_admins = [r["user_id"] for r in data["enrolled_admins"]]
        s.enrolled_admins_count = len(s.enrolled_admins)
        s.sign_status_json = json.dumps(data, indent=2, ensure_ascii=False)

        sync_spiffe_and_check_totp(emit_warning=False)

        if not silent:
            refs.push_cli(f"$ fiber sign status\n  PubKey={s.node_pubkey[:16]}... | Admins={s.enrolled_admins_count}")
            ui.notify("Keychain & Audit DB Status Refreshed", type="info")
    except Exception as e:
        if not silent:
            refs.push_cli(f"  [Sign Status Error] {e}")


async def act_init_node_keys():
    """`ensure_node_keys()`를 실행해 `~/.ssh/fiber_ed25519` 및 `local_cipher.key`가 없으면 안전하게 생성(0600)"""
    try:
        from fiber.phase.cli.sign import ensure_node_keys
        info = await asyncio.to_thread(ensure_node_keys)
        await act_refresh_sign_status(silent=True)
        refs.push_cli(f"$ fiber sign init\n  {json.dumps(info, ensure_ascii=False)}")
        refs.push_pubsub("sign:init", f"KEYS_READY ssh={info['ssh_key_status']} cipher={info['local_cipher_status']}")
        ui.notify(
            f"Keys Ready! SSH: {info['ssh_key_status']} | Cipher: {info['local_cipher_status']}",
            type="positive",
        )
    except Exception as e:
        refs.push_cli(f"  [Key Init Error] {e}")
        ui.notify(f"Key Initialization Failed: {e}", type="negative")


async def act_enroll_admin_totp():
    """Zero-Exposure 파생 키로 TOTP를 암호화해 SQLite에 저장하고 QR 모달 표시 및 Tab 1 TOTP 배지 갱신"""
    s = state.security
    try:
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

        canonical_id = _normalize_spiffe_id(s.admin_user)
        secret = pyotp.random_base32()

        def _provision_in_sqlite() -> str:
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

        pubkey_hex = await asyncio.to_thread(_provision_in_sqlite)
        totp_uri, display_label = _build_totp_provisioning_uri(secret, canonical_id, pubkey_hex)
        await act_refresh_sign_status(silent=True)

        qr = qrcode.QRCode(version=1, box_size=1, border=1)
        qr.add_data(totp_uri)
        qr.make(fit=True)
        buf = io.StringIO()
        qr.print_ascii(out=buf, invert=True)

        s.totp_secret = secret
        s.totp_uri = totp_uri
        if refs.qr_code_box:
            refs.qr_code_box.set_content(buf.getvalue())
        if refs.qr_dialog:
            refs.qr_dialog.open()

        refs.push_cli(
            f"$ fiber sign enroll -u {canonical_id}\n"
            f"  ✓ Zero-Exposure AES-GCM TOTP saved (App Label: {TOTP_ISSUER_NAME} / {display_label})"
        )
        refs.push_pubsub("sign:enroll", f"TOTP_ENROLLED user={canonical_id} label='{display_label}'")
        ui.notify(f"TOTP Enrolled: {TOTP_ISSUER_NAME} ({display_label})", type="positive")
    except Exception as e:
        refs.push_cli(f"  [TOTP Enroll Error] {e}")
        ui.notify(f"TOTP Enrollment Error: {e}", type="warning")


async def act_verify_admin_totp(code_override: str | None = None) -> bool:
    """SQLite DB에서 Zero-Exposure 파생 키로 TOTP 시크릿을 복호화해 검증"""
    s = state.security
    try:
        import pyotp
        from fiber.phase.cli.sign import (
            AdminSecretVault,
            _normalize_spiffe_id,
            _resolve_vault_context,
            _open_audit_db,
            DEFAULT_DB_PATH,
        )

        canonical_id = _normalize_spiffe_id(s.admin_user)

        def _check() -> tuple[bool, str, str]:
            active_passphrase, pubkey_hex = _resolve_vault_context()
            conn = _open_audit_db(DEFAULT_DB_PATH)
            row = conn.execute(
                "SELECT salt, nonce, ciphertext FROM admin_users WHERE user_id = ?",
                (canonical_id,),
            ).fetchone()
            conn.close()
            if not row:
                return False, "UNREGISTERED", pubkey_hex
            vault = AdminSecretVault(active_passphrase)
            secret = vault.decrypt(*row)
            totp = pyotp.TOTP(secret)
            target_code = code_override.strip() if code_override else totp.now()
            return totp.verify(str(target_code).zfill(6), valid_window=1), target_code, pubkey_hex

        is_valid, tested_code, pubkey_hex = await asyncio.to_thread(_check)
        if tested_code == "UNREGISTERED":
            ui.notify(f"Unregistered Admin: {canonical_id} (Click 'Enroll TOTP' first)", type="warning")
            return False

        refs.push_cli(
            f"$ fiber sign verify -u {canonical_id}\n"
            f"  ✓ Decrypted & Verified: code={tested_code} | valid={is_valid} | pubkey={pubkey_hex[:16]}..."
        )
        refs.push_pubsub("sign:verify", f"TOTP_VERIFY user={canonical_id} valid={is_valid}")
        ui.notify(
            f"TOTP Verification {'PASSED ✅' if is_valid else 'FAILED ❌'} (Code: {tested_code})",
            type="positive" if is_valid else "negative",
        )
        return is_valid
    except Exception as e:
        refs.push_cli(f"  [TOTP Verify Error] {e}")
        return False


async def act_revoke_admin_totp():
    """SQLite DB에서 지정된 관리자의 TOTP 레코드 삭제 후 상태 갱신"""
    s = state.security
    try:
        from fiber.phase.cli.sign import _normalize_spiffe_id, _open_audit_db, DEFAULT_DB_PATH
        canonical_id = _normalize_spiffe_id(s.admin_user)

        def _delete():
            conn = _open_audit_db(DEFAULT_DB_PATH)
            conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
            conn.commit()
            conn.close()

        await asyncio.to_thread(_delete)
        s.totp_secret = ""
        await act_refresh_sign_status(silent=True)
        refs.push_cli(f"$ fiber sign revoke -u {canonical_id}\n  ✓ Revoked from SQLite Audit DB.")
        refs.push_pubsub("sign:revoke", f"TOTP_REVOKED user={canonical_id}")
        ui.notify(f"Revoked TOTP for '{canonical_id}'", type="warning")
    except Exception as e:
        refs.push_cli(f"  [TOTP Revoke Error] {e}")


def act_open_hitl_modal(tool: str, args: dict):
    if refs.hitl_code:
        refs.hitl_code.set_content(json.dumps({"tool": tool, "args": args, "state": "HTTP 202 Parked"}, indent=2))
    if refs.hitl_dialog:
        refs.hitl_dialog.open()
    refs.push_pubsub("mcp:hitl", f"⏸ [HTTP 202] Sandbox suspended for '{tool}'")


async def act_approve_hitl():
    """HITL 모달에서 6자리 TOTP 코드를 검증(또는 DB 복호화 Self-Verify)한 후 승인 처리"""
    s = state.security
    input_code = s.totp_input_code.strip()

    valid = await act_verify_admin_totp(input_code if input_code else None)
    if not valid:
        refs.push_pubsub("mcp:hitl", "❌ TOTP_VERIFY_FAILED -> Sandbox remains suspended")
        return

    if refs.hitl_dialog:
        refs.hitl_dialog.close()
    refs.push_pubsub("mcp:hitl", "✓ ATTESTATION_PASSED -> Resuming suspended sandbox execution")
    ui.notify("✓ Approved! Cryptographic attestation verified.", type="positive")