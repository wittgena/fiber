# fiber.phase.action.sign
"""
@desc: Local Node Keychain & Admin TOTP Attestation CLI
- Provisions Zero-Env foundational keys (~/.ssh/fiber_ed25519 & local_cipher.key) as the node's Root of Trust.
- Manages AES-GCM encrypted Admin TOTP secrets in SQLite via deterministic HMAC key derivation (distinct from tenant FinOps quotas in `fiber account`).
"""
import os
import sys
import socket
import secrets
import sqlite3
import qrcode
import hashlib
import hmac
import json
from pathlib import Path
from typing import Annotated, Optional, Tuple

import typer
import pyotp
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.space.bind.resolver import resolve_path

SIGN_ROOT = Path(str(resolve_path("sign")))
SSH_DIR = Path.home() / ".ssh"
SSH_PRIV_KEY_PATH = SSH_DIR / "fiber_ed25519"
SSH_PUB_KEY_PATH = SSH_DIR / "fiber_ed25519.pub"
LOCAL_CIPHER_KEY_PATH = SIGN_ROOT / "local_cipher.key"
DEFAULT_DB_PATH = str(SIGN_ROOT / "deploy_audit.sqlite")
BASE_SPIFFE_DOMAIN = "spiffe://self/"
TOTP_ISSUER_NAME = "Fiber"

log_sign = get_emitter("fiber.cli.sign")

app = typer.Typer(
    name="sign",
    help="Fiber Local Node Identity (~/.ssh/fiber_ed25519 & local_cipher.key) and Admin TOTP CLI",
    no_args_is_help=True,
    add_completion=False,
)


# ==========================================
# 1. Core Key Provisioning (~/.ssh/fiber_ed25519 & local_cipher.key)
# ==========================================
def _normalize_spiffe_id(user_id: str) -> str:
    if user_id.startswith("spiffe://"):
        return user_id
    return f"{BASE_SPIFFE_DOMAIN}{user_id}"


def _resolve_target_user(conn: sqlite3.Connection, user_id: Optional[str] = None) -> str:
    """Uses explicit user_id if given; otherwise picks the latest enrolled admin from SQLite."""
    if user_id and user_id.strip():
        return _normalize_spiffe_id(user_id.strip())
    row = conn.execute(
        "SELECT user_id FROM admin_users WHERE user_id NOT LIKE '%_e2e_%' ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else _normalize_spiffe_id("fiber")


def _build_totp_provisioning_uri(secret: str, canonical_id: str, pubkey_hex: str = "") -> Tuple[str, str]:
    """
    Strips 'spiffe://self/' and colons (which break Authenticator app label parsing)
    to produce a clean, human-readable label: e.g. 'fiber@MacBook-Pro (a1b2c3)' under issuer 'Fiber'.
    """
    short_user = canonical_id.replace(BASE_SPIFFE_DOMAIN, "").replace("spiffe://", "").replace(":", "/")
    host = socket.gethostname().split(".")[0] or "local"
    key_tag = f" ({pubkey_hex[:6]})" if pubkey_hex else ""
    display_label = f"{short_user}@{host}{key_tag}"

    uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=display_label,
        issuer_name=TOTP_ISSUER_NAME,
    )
    return uri, display_label


def ensure_node_keys(force_regenerate_cipher: bool = False) -> dict:
    """
    Ensures the two foundational keys exist on disk with strict 0600 permissions:
      1. ~/.ssh/fiber_ed25519 (OpenSSH format, loaded by NodeSigner)
      2. <sign>/local_cipher.key (32-byte hex seed for local AES-GCM vault derivation)
    Never overwrites an existing ~/.ssh/fiber_ed25519.
    """
    SSH_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(SSH_DIR, 0o700)
    except Exception:
        pass

    ssh_created = False
    if not SSH_PRIV_KEY_PATH.exists():
        priv_key = ed25519.Ed25519PrivateKey.generate()
        openssh_priv = priv_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.OpenSSH,
            encryption_algorithm=serialization.NoEncryption(),
        )
        openssh_pub = priv_key.public_key().public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH,
        )
        SSH_PRIV_KEY_PATH.write_bytes(openssh_priv)
        os.chmod(SSH_PRIV_KEY_PATH, 0o600)

        if not SSH_PUB_KEY_PATH.exists():
            SSH_PUB_KEY_PATH.write_bytes(openssh_pub + b" fiber-node\n")
            os.chmod(SSH_PUB_KEY_PATH, 0o644)

        ssh_created = True
        NodeSigner._instance = None

    SIGN_ROOT.mkdir(parents=True, exist_ok=True)
    cipher_created = False
    if force_regenerate_cipher or not LOCAL_CIPHER_KEY_PATH.exists():
        LOCAL_CIPHER_KEY_PATH.write_text(secrets.token_hex(32), encoding="utf-8")
        os.chmod(LOCAL_CIPHER_KEY_PATH, 0o600)
        cipher_created = True

    signer = NodeSigner.get_instance()
    return {
        "ssh_key_path": str(SSH_PRIV_KEY_PATH),
        "ssh_key_status": "CREATED" if ssh_created else "EXISTING",
        "local_cipher_path": str(LOCAL_CIPHER_KEY_PATH),
        "local_cipher_status": "CREATED" if cipher_created else "EXISTING",
        "node_pubkey": signer.pubkey_hex,
    }


def _open_audit_db(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_users (
            user_id TEXT PRIMARY KEY,
            salt TEXT,
            nonce TEXT,
            ciphertext TEXT
        )
    """)
    conn.commit()
    return conn


# ==========================================
# 2. Zero-Exposure Crypto Vault (HMAC Derivation)
# ==========================================
class AdminSecretVault:
    def __init__(self, passphrase: str):
        self.passphrase = passphrase.encode("utf-8")

    def encrypt(self, secret: str) -> tuple[str, str, str]:
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        aesgcm = AESGCM(kdf.derive(self.passphrase))
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, secret.encode("utf-8"), None)
        return salt.hex(), nonce.hex(), ciphertext.hex()

    def decrypt(self, salt_hex: str, nonce_hex: str, ciphertext_hex: str) -> str:
        salt = bytes.fromhex(salt_hex)
        nonce = bytes.fromhex(nonce_hex)
        ciphertext = bytes.fromhex(ciphertext_hex)
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=600_000)
        aesgcm = AESGCM(kdf.derive(self.passphrase))
        return aesgcm.decrypt(nonce, ciphertext, None).decode("utf-8")


def _resolve_vault_context() -> Tuple[str, str]:
    """
    Deterministically derives the vault passphrase strictly from `local_cipher.key`
    and `~/.ssh/fiber_ed25519` (via NodeSigner) without any environment variable dependencies.
    """
    ensure_node_keys()
    signer = NodeSigner.get_instance()
    pubkey_hex = signer.pubkey_hex

    raw_local_key = LOCAL_CIPHER_KEY_PATH.read_text(encoding="utf-8").strip()
    ikm = raw_local_key.encode("utf-8") if raw_local_key else bytes(signer.signing_key)

    domain_context = f"dphi:domain:admin_secret_vault:v1:{pubkey_hex}".encode("utf-8")
    derived_passphrase = hmac.new(ikm, domain_context, hashlib.sha256).hexdigest()
    return derived_passphrase, pubkey_hex


# ==========================================
# 3. CLI Commands (init, status, enroll, verify, revoke)
# ==========================================
@app.command("init")
def cmd_init(
    rotate_cipher: Annotated[bool, typer.Option("--rotate-cipher", help="Regenerate local_cipher.key (invalidates existing TOTP DB)")] = False,
):
    """Initialize local node cryptographic keys (~/.ssh/fiber_ed25519 & local_cipher.key)."""
    info = ensure_node_keys(force_regenerate_cipher=rotate_cipher)
    log_sign.info("🔐 Node Cryptographic Identity Initialized:")
    print(json.dumps(info, indent=2, ensure_ascii=False))


@app.command("status")
def cmd_status(
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
):
    """Inspect local node key files, active Public Key, and enrolled TOTP administrators."""
    signer = NodeSigner.get_instance()
    conn = _open_audit_db(db_path)
    rows = conn.execute("SELECT user_id, substr(ciphertext, 1, 16) FROM admin_users ORDER BY rowid DESC").fetchall()
    active_admin = _resolve_target_user(conn, None) if rows else None
    conn.close()

    status_payload = {
        "ssh_fiber_ed25519": {
            "path": str(SSH_PRIV_KEY_PATH),
            "exists": SSH_PRIV_KEY_PATH.exists(),
        },
        "local_cipher_key": {
            "path": str(LOCAL_CIPHER_KEY_PATH),
            "exists": LOCAL_CIPHER_KEY_PATH.exists(),
        },
        "node_pubkey": signer.pubkey_hex,
        "audit_db": db_path,
        "active_default_admin": active_admin,
        "enrolled_admins": [{"user_id": r[0], "cipher_prefix": f"{r[1]}..."} for r in rows],
    }
    print(json.dumps(status_payload, indent=2, ensure_ascii=False))


@app.command("enroll")
def cmd_enroll(
    user_id: Annotated[Optional[str], typer.Option("--user", "-u", help="Administrator SPIFFE ID (defaults to latest or 'fiber')")] = None,
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
    secret_override: Annotated[Optional[str], typer.Option("--secret", "-s", help="Optional Base32 secret override")] = None,
):
    """Ensure local keys exist, enroll an administrator TOTP secret into SQLite, and display the QR code."""
    active_passphrase, pubkey_hex = _resolve_vault_context()
    conn = _open_audit_db(db_path)
    canonical_id = _resolve_target_user(conn, user_id)

    secret = secret_override or pyotp.random_base32()
    totp_uri, display_label = _build_totp_provisioning_uri(secret, canonical_id, pubkey_hex)

    vault = AdminSecretVault(active_passphrase)
    salt_hex, nonce_hex, cipher_hex = vault.encrypt(secret)

    # Delete existing row first so re-enrolling moves rowid to the latest (active default)
    conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
    conn.execute(
        "INSERT INTO admin_users (user_id, salt, nonce, ciphertext) VALUES (?, ?, ?, ?)",
        (canonical_id, salt_hex, nonce_hex, cipher_hex),
    )
    conn.commit()
    conn.close()

    log_sign.info(f"\n✅ TOTP Enrolled for '{canonical_id}' (Active Default Admin)")
    log_sign.info(f"🏷️  Authenticator App Label : {TOTP_ISSUER_NAME} ({display_label})")
    log_sign.info("📱 Scan the QR code below with Google Authenticator or Authy:\n")
    qr = qrcode.QRCode(version=1, box_size=1, border=2)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    qr.print_ascii(out=sys.stdout, invert=True)
    print(f"\nBase32 Manual Key: {secret}\n")


@app.command("verify")
def cmd_verify(
    user_id: Annotated[Optional[str], typer.Option("--user", "-u", help="Administrator SPIFFE ID (omit to use latest enrolled admin)")] = None,
    code: Annotated[Optional[str], typer.Option("--code", "-c", help="6-digit OTP code (omit for auto self-test)")] = None,
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
):
    """Verify decryption parity and TOTP code against the local SQLite Audit DB."""
    active_passphrase, pubkey_hex = _resolve_vault_context()

    conn = _open_audit_db(db_path)
    canonical_id = _resolve_target_user(conn, user_id)
    row = conn.execute(
        "SELECT salt, nonce, ciphertext FROM admin_users WHERE user_id = ?", (canonical_id,)
    ).fetchone()
    conn.close()

    if not row:
        log_sign.error(f"🚨 Unregistered Admin: {canonical_id}. Run 'fiber sign enroll' first.")
        sys.exit(1)

    vault = AdminSecretVault(active_passphrase)
    secret = vault.decrypt(*row)
    totp = pyotp.TOTP(secret)
    target_code = code or totp.now()
    is_valid = totp.verify(str(target_code).zfill(6), valid_window=1)

    print(json.dumps({
        "user_id": canonical_id,
        "node_pubkey_prefix": pubkey_hex[:16],
        "tested_code": target_code,
        "verified": is_valid,
    }, indent=2))
    if not is_valid:
        sys.exit(1)


@app.command("revoke")
def cmd_revoke(
    user_id: Annotated[Optional[str], typer.Option("--user", "-u", help="Administrator SPIFFE ID to revoke (omit for latest enrolled)")] = None,
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
):
    """Remove an administrator's TOTP record from the local SQLite Audit DB."""
    conn = _open_audit_db(db_path)
    canonical_id = _resolve_target_user(conn, user_id)
    conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
    conn.commit()
    conn.close()
    log_sign.info(f"✅ Revoked TOTP access for '{canonical_id}'.")


# Maintain `fiber sign admin ...` sub-group alias for backward compatibility with existing scripts
admin_app = typer.Typer(help="Alias group for admin TOTP commands")
admin_app.command("totp-enroll")(cmd_enroll)
admin_app.command("totp-verify")(cmd_verify)
admin_app.command("totp-revoke")(cmd_revoke)
admin_app.command("totp-list")(cmd_status)
app.add_typer(admin_app, name="admin")

def main():
    app()

if __name__ == "__main__":
    main()