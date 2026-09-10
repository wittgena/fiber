# fiber.phase.cli.sign
import os
import sys
import secrets
import sqlite3
import qrcode
import hashlib
import json
import nacl.signing
import nacl.encoding
from typing import Annotated, Optional

import typer
import pyotp
from eth_account import Account
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:
    import dotenv
except ImportError:
    dotenv = None

from xphi.kernel.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

SIGN_ROOT = resolve_path("sign")
DEFAULT_DB_PATH = os.path.join(str(SIGN_ROOT), "deploy_audit.sqlite")
BASE_SPIFFE_DOMAIN = "spiffe://self/"

log_sign = get_emitter("fiber.cli.sign")

app = typer.Typer(
    name="sign",
    help="Fiber Unified Identity, Cryptographic Keys & Security CLI Tools",
    no_args_is_help=True,
    add_completion=False,
)

# ==========================================
# Common Utilities
# ==========================================
def _load_env(env_file: Optional[str]):
    """Injects environment variables from a .env file into the runtime context."""
    if env_file:
        if dotenv:
            try:
                dotenv.load_dotenv(env_file)
                log_sign.info(f"[Fiber] Loaded environment from {env_file}")
            except Exception as e:
                log_sign.error(f"[Fiber] Failed to load .env file: {e}")
                raise typer.Exit(1)
        else:
            log_sign.warning("[Fiber] python-dotenv is not installed. Ignoring --env-file option.")

def _normalize_spiffe_id(user_id: str) -> str:
    """Canonicalizes the identifier into a valid SPIFFE URI."""
    if user_id.startswith("spiffe://"):
        return user_id
    return f"{BASE_SPIFFE_DOMAIN}{user_id}"

def _write_secure_file(filepath: str, content: str, is_private: bool = True):
    """Writes content to a file with strict OS-level permissions."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        f.write(content)
    # chmod 600 for private keys (Owner Read/Write only), 644 for public
    if is_private:
        os.chmod(filepath, 0o600)
    else:
        os.chmod(filepath, 0o644)

# ==========================================
# Crypto Vault (AES-GCM for TOTP)
# ==========================================
class AdminSecretVault:
    def __init__(self, passphrase: str):
        self.passphrase = passphrase.encode('utf-8')

    def encrypt(self, secret: str) -> tuple[str, str, str]:
        """Encrypts a plaintext TOTP secret using AES-GCM."""
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=600_000,
        )
        aes_key = kdf.derive(self.passphrase)
        aesgcm = AESGCM(aes_key)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, secret.encode('utf-8'), None)
        return salt.hex(), nonce.hex(), ciphertext.hex()

# ==========================================
# Domain 1: Machine Identity (EVM & Node Keys)
# ==========================================
machine_app = typer.Typer(
    help="Manage Machine Identities (EVM Billing Wallets & Ed25519 Consensus Keys)",
    no_args_is_help=True
)
app.add_typer(machine_app, name="machine")

def _generate_evm_wallet(name: str) -> tuple[str, str]:
    """Generates a secp256k1 keypair for EVM-compatible billing and smart contracts."""
    priv = secrets.token_hex(32)
    private_key = "0x" + priv
    account = Account.from_key(private_key)

    log_sign.info(f"[{name} - EVM Billing Identity]")
    log_sign.info(f"EVM Address : {account.address}")
    log_sign.info(f"Private Key : {private_key}")
    log_sign.info(f"DID Format  : did:pkh:eip155:84532:{account.address}")
    log_sign.info("-" * 60)
    return private_key, account.address

@machine_app.command("gen-evm")
def flow_gen_evm(
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")] = None,
):
    """Generate secp256k1 EOA Wallets for X402 Billing and Agent Identities."""
    _load_env(env_file)
    log_sign.info("🚀 Generating EVM Wallets for X402 Billing & A2A Economy...\n")

    alpha_pkey, _ = _generate_evm_wallet("Alpha (Compute Provider)")
    beta_pkey, _ = _generate_evm_wallet("Data Consumer Beta")
    master_pkey, _ = _generate_evm_wallet("System Clearinghouse Master")

    log_sign.info("📋 Copy & Paste this to your .env file:")
    log_sign.info("=" * 60)
    log_sign.info(f'AGENT_PKEY_ALPHA="{alpha_pkey}"')
    log_sign.info(f'AGENT_PKEY_BETA="{beta_pkey}"')
    log_sign.info(f'SYSTEM_CLEARING_PKEY="{master_pkey}"')
    log_sign.info("=" * 60)


def _generate_ed25519_node(name: str) -> tuple[str, str]:
    """Generates an Ed25519 keypair and returns raw hex (no terminal exposure)."""
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    priv_hex = private_key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption()
    ).hex()

    pub_hex = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw
    ).hex()

    log_sign.info(f"[{name}] - Keys successfully generated in memory.")
    return priv_hex, pub_hex

@machine_app.command("bootstrap-origin")
def flow_bootstrap_origin(
    out_dir: Annotated[str, typer.Option("--out-dir", "-o", help="Directory for secure file staging")] = os.path.expanduser("~/.ssh/fiber"),
    count: Annotated[int, typer.Option("--count", "-c", help="Number of validator nodes to generate")] = 3,
    location: Annotated[str, typer.Option("--location", "-l", help="Logical repository path (e.g., fiber.phase.abc.validator.origin_config)")] = "fiber.phase.abc.validator.origin_config",
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")] = None,
):
    """Automates Origin Bootstrapping: Generates Root, Validators, and signs them instantly (1-Step Process)."""
    _load_env(env_file)
    log_sign.info(f"🛡️ Bootstrapping Origin Infrastructure ({count} Validators) securely in {out_dir} ...\n")

    os.makedirs(out_dir, exist_ok=True)
    os.chmod(out_dir, 0o700)

    # 1. Master Root Key 생성 및 저장
    root_priv, root_pub = _generate_ed25519_node("Master Root Key")
    _write_secure_file(os.path.join(out_dir, "master_root.key"), root_priv, is_private=True)
    _write_secure_file(os.path.join(out_dir, "master_root.pub"), root_pub, is_private=False)

    # 2. Validator Nodes 생성 및 저장
    val_pubs = []
    validators_metadata = []
    for i in range(1, count + 1):
        v_priv, v_pub = _generate_ed25519_node(f"Validator Node {i}")
        _write_secure_file(os.path.join(out_dir, f"validator_{i}.key"), v_priv, is_private=True)
        _write_secure_file(os.path.join(out_dir, f"validator_{i}.pub"), v_pub, is_private=False)
        val_pubs.append(v_pub)
        
        # [수정완료] 순수 무결성 식별 데이터만 남김 (로컬 key_path 제거)
        validators_metadata.append({
            "node_id": f"validator_{i}",
            "pubkey": v_pub
        })

    # 3. 즉각적인 인메모리 서명 (NodeSigner 로직과 100% 동일한 방식)
    log_sign.info("\n🔒 [Offline Signer] Generating Pre-signed Payload in memory...")
    try:
        payload_dict = {"active_signers": val_pubs}
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)

        signing_key = nacl.signing.SigningKey(root_priv, encoder=nacl.encoding.HexEncoder)
        payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
        signed = signing_key.sign(payload_hash_str.encode('utf-8'))
        root_signature = signed.signature.hex()
    except Exception as e:
        log_sign.error(f"🚨 Signature Generation Failed: {e}")
        sys.exit(1)

    # 4. JSON 포맷 설정 파일 생성 (GitOps 및 무결성 검증용)
    json_config_path = os.path.join(out_dir, "origin_config.json")
    
    # [수정완료] CLI 옵션(location) 반영 및 불필요한 env_injection, root_key_path 완벽 제거
    config_data = {
        "location": location,
        "network": "fiber.origin",
        "attestation": {
            "root_pubkey": root_pub,
            "pre_signed_root_sig": root_signature
        },
        "validators": validators_metadata
    }
    
    _write_secure_file(json_config_path, json.dumps(config_data, indent=4), is_private=False)

    # 5. 결과 출력 및 격리 가이드 (로거 텔레메트리 차단을 피하기 위해 print() 사용)
    print("\n✅ Origin Bootstrapping Complete. Keys are written to disk with chmod 600/644.")
    print("=" * 70)
    print(f"Root Key      : {os.path.join(out_dir, 'master_root.key')}")
    print(f"JSON Config   : {json_config_path}")
    print(f"Validators    : {count} pairs generated")
    print("=" * 70)

    print("\n🔥 [HOT SERVER] - Legacy / Env Inject (If needed):")
    print("-" * 70)
    print(f'COMMITTEE_VALIDATORS="{",".join(val_pubs)}"')
    print(f'DPHI_ACTIVE_SIGNERS="{",".join(val_pubs)}"')
    print(f'DPHI_PRE_SIGNED_ROOT_SIG="{root_signature}"')
    print("-" * 70)

    print("\n⚠️  [SECURITY WARNING] The staging process is complete.")
    print(f"1. Commit '{json_config_path}' to your Git Repository.")
    print(f"2. Move your Master Root Key to Cold Storage immediately:")
    print(f"-> mv {os.path.join(out_dir, 'master_root.key')} /Volumes/SECURE_USB/\n")


# ==========================================
# Domain 2: Human Identity (Admin TOTP)
# ==========================================
admin_app = typer.Typer(
    help="Manage Human Administrators and Encrypted TOTP Provisioning",
    no_args_is_help=True
)
app.add_typer(admin_app, name="admin")

@admin_app.command("totp-enroll")
def enroll_admin(
    user_id: Annotated[str, typer.Option("--user", "-u", help="Administrator identifier (e.g. fiber or spiffe://self/fiber)")],
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")] = None,
):
    """Generate a new encrypted TOTP secret for an administrator and inject it into SQLite."""
    _load_env(env_file)
    canonical_id = _normalize_spiffe_id(user_id)

    log_sign.info(f"🔐 Provisioning TOTP for Administrator: {canonical_id}")
    log_sign.info(f"📁 Target Database: {db_path}")

    secret = pyotp.random_base32()
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=canonical_id,
        issuer_name="DPHI_Enterprise_Deploy"
    )

    passphrase = typer.prompt("🔑 Enter Master Passphrase to encrypt the TOTP secret", hide_input=True, confirmation_prompt=True)
    vault = AdminSecretVault(passphrase)

    try:
        salt_hex, nonce_hex, cipher_hex = vault.encrypt(secret)
    except Exception as e:
        log_sign.error(f"🚨 Encryption Failed: {e}")
        sys.exit(1)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id TEXT PRIMARY KEY,
                salt TEXT,
                nonce TEXT,
                ciphertext TEXT
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO admin_users (user_id, salt, nonce, ciphertext) 
            VALUES (?, ?, ?, ?)
        """, (canonical_id, salt_hex, nonce_hex, cipher_hex))
        conn.commit()
        conn.close()
    except Exception as e:
        log_sign.error(f"🚨 DB Injection Failed: {e}")
        sys.exit(1)

    log_sign.info("\n✅ Encrypted TOTP secret successfully injected into database.")
    log_sign.info("=" * 70)
    log_sign.info(f"Admin ID    : {canonical_id}")
    log_sign.info("🔒 (Encrypted securely under Master Passphrase)")
    log_sign.info("=" * 70)

    log_sign.info("📱 Open your Authenticator app (Google/Authy) and scan the QR code below:\n")
    qr = qrcode.QRCode(version=1, box_size=1, border=2)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    qr.print_ascii(out=sys.stdout, invert=True)
    print("\n")

    log_sign.info("=" * 70)
    log_sign.info(f"Base32 Manual Key: {secret}")
    log_sign.info("=" * 70)

@admin_app.command("totp-revoke")
def revoke_admin(
    user_id: Annotated[str, typer.Option("--user", "-u", help="Administrator identifier to revoke (e.g. fiber)")],
    db_path: Annotated[str, typer.Option("--db", "-d", help="Path to SQLite Audit DB")] = DEFAULT_DB_PATH,
    env_file: Annotated[Optional[str], typer.Option("--env-file", "-f", exists=True, help="Path to .env file")] = None,
):
    """Revoke (delete) an administrator's TOTP secret from SQLite."""
    _load_env(env_file)
    canonical_id = _normalize_spiffe_id(user_id)

    log_sign.info(f"🗑️ Revoking TOTP access for Administrator: {canonical_id}")
    log_sign.info(f"📁 Target Database: {db_path}")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    try:
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id TEXT PRIMARY KEY,
                salt TEXT,
                nonce TEXT,
                ciphertext TEXT
            )
        """)
        conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
        conn.commit()

        log_sign.info(f"✅ Administrator '{canonical_id}' successfully revoked from database.")
        conn.close()
    except Exception as e:
        log_sign.error(f"🚨 Revocation Failed: {e}")
        sys.exit(1)

def main():
    app()

if __name__ == "__main__":
    main()