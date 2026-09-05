# fiber.cli.sign
import os
import sys
import secrets
import pyotp
import sqlite3
import qrcode
from typing import Annotated

import typer
from eth_account import Account

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import NodeSigner
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

SIGN_ROOT = resolve_path("sign")
DEFAULT_DB_PATH = os.path.join(str(SIGN_ROOT), "deploy_audit.sqlite")
BASE_SPIFFE_DOMAIN = "spiffe://self/"

log_sign = get_emitter("cli.sign")

app = typer.Typer(
    name="flow",
    help="Flow Unified Identity & Security CLI Tools",
    add_completion=False,
    no_args_is_help=True,
)

# ==========================================
# Helper: SPIFFE ID 정규화 (Canonicalization)
# ==========================================
def _normalize_spiffe_id(user_id: str) -> str:
    """
    사용자가 'fiber'만 입력해도 'spiffe://self/fiber'로 자동 변환합니다.
    이미 'spiffe://'를 포함하여 입력한 경우 원본을 유지합니다.
    """
    if user_id.startswith("spiffe://"):
        return user_id
    return f"{BASE_SPIFFE_DOMAIN}{user_id}"


# ==========================================
# Crypto Vault (관리자 시크릿 암호화 전담)
# ==========================================
class AdminSecretVault:
    def __init__(self, passphrase: str):
        self.passphrase = passphrase.encode('utf-8')

    def encrypt(self, secret: str) -> tuple[str, str, str]:
        """TOTP 평문 시크릿을 AES-GCM으로 암호화하여 반환"""
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
# Domain 1: Machine Identity (기계 신원 및 노드 서명)
# ==========================================
machine_app = typer.Typer(help="Manage Machine Identities (EVM Keys, Node Signatures)")
app.add_typer(machine_app, name="machine")

def _generate_agent(name: str) -> tuple[str, str]:
    priv = secrets.token_hex(32)
    private_key = "0x" + priv
    account = Account.from_key(private_key)
    
    log_sign.info(f"[{name} Agent]")
    log_sign.info(f"EVM Address : {account.address}")
    log_sign.info(f"Private Key : {private_key}")
    log_sign.info(f"DID Format  : did:pkh:eip155:84532:{account.address}")
    log_sign.info("-" * 50)
    
    return private_key, account.address

@machine_app.command("genkey")
def flow_genkey():
    """Generate EOA Wallets for Testnet Agents."""
    log_sign.info("🚀 Generating EOA Wallets for Testnet Agents...\n")
    
    alpha_pkey, _ = _generate_agent("Alpha (Compute Provider)")
    beta_pkey, _ = _generate_agent("Beta (Data Consumer)")
    
    log_sign.info("📋 Copy & Paste this to your .env file:")
    log_sign.info("=" * 50)
    log_sign.info(f'AGENT_ALPHA_PKEY="{alpha_pkey}"')
    log_sign.info(f'AGENT_BETA_PKEY="{beta_pkey}"')
    log_sign.info("=" * 50)


@machine_app.command("signature")
def flow_signature(
    keys: Annotated[str, typer.Option("--keys", "-k", help="신뢰할 Edge 노드들의 Public Key 목록 (쉼표 구분)")],
    root_key: Annotated[str, typer.Option("--root-key", "-r", help="보안 격리된 Master Root Private Key (Hex)")]
):
    """Generate offline Root Signature for DPHI Edge."""
    signers_list = [k.strip() for k in keys.split(",")]
    
    log_sign.info("\n🔒 [Offline Signer] Generating Pre-signed Payload...")
    payload_dict = {"active_signers": signers_list}
    canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
    
    try:
        temp_signer = NodeSigner(private_key_hex=root_key)
        root_signature = temp_signer.sign_payload(canonical_bytes)
        root_pubkey = temp_signer.pubkey_hex
    except Exception as e:
        log_sign.error(f"🚨 서명 생성 실패: {e}")
        sys.exit(1)

    log_sign.info("\n✅ 성공적으로 서명이 생성되었습니다. Edge 서버 환경변수에 아래 내용을 주입하세요.\n")
    log_sign.info("=" * 60)
    log_sign.info(f"DPHI_ACTIVE_SIGNERS={','.join(signers_list)}")
    log_sign.info(f"DPHI_PRE_SIGNED_ROOT_SIG={root_signature}")
    log_sign.info("=" * 60)
    log_sign.info("\n[참고] 이 서명을 검증하기 위해 클라이언트(SDK)에 주입해야 할 Root Public Key:")
    log_sign.info(f"DPHI_ROOT_PUBKEY={root_pubkey}")
    log_sign.info("=" * 60)


# ==========================================
# Domain 2: Human Identity (인간 관리자 및 TOTP 프로비저닝)
# ==========================================
admin_app = typer.Typer(help="Manage Human Administrators and TOTP Secrets")
app.add_typer(admin_app, name="admin")

@admin_app.command("totp-enroll")
def enroll_admin(
    user_id: Annotated[str, typer.Option("--user", "-u", help="관리자 식별자 (예: fiber 또는 spiffe://self/fiber)")],
    db_path: Annotated[str, typer.Option("--db", "-d", help="SQLite DB 파일 경로")] = DEFAULT_DB_PATH
):
    """Generate a new encrypted TOTP secret for an administrator and inject it into SQLite."""
    # [수정] 식별자 정규화 처리
    canonical_id = _normalize_spiffe_id(user_id)
    
    log_sign.info(f"🔐 Provisioning TOTP for Administrator: {canonical_id}")
    log_sign.info(f"📁 Target Database: {db_path}")
    
    # 1. 시크릿 발급 및 Vault 암호화 준비
    secret = pyotp.random_base32()
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=canonical_id,  # Authenticator 앱에도 정규화된 이름으로 표기됨
        issuer_name="DPHI_Enterprise_Deploy"
    )
    
    # 마스터 패스프레이즈를 인터랙티브하게 입력 받음
    passphrase = typer.prompt("🔑 Enter Master Passphrase to encrypt the TOTP secret", hide_input=True, confirmation_prompt=True)
    vault = AdminSecretVault(passphrase)
    
    try:
        # AES-GCM 암호화 수행
        salt_hex, nonce_hex, cipher_hex = vault.encrypt(secret)
    except Exception as e:
        log_sign.error(f"🚨 Encryption Failed: {e}")
        sys.exit(1)
    
    # 2. SQLite 연결 및 암호화된 데이터 주입
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    try:
        conn = sqlite3.connect(db_path)
        # 평문 대신 암호화 요소(salt, nonce, ciphertext)를 저장
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id TEXT PRIMARY KEY,
                salt TEXT,
                nonce TEXT,
                ciphertext TEXT
            )
        """)
        # INSERT OR REPLACE로 기존 키가 있으면 덮어쓰기 (정규화된 ID 사용)
        conn.execute("""
            INSERT OR REPLACE INTO admin_users (user_id, salt, nonce, ciphertext) 
            VALUES (?, ?, ?, ?)
        """, (canonical_id, salt_hex, nonce_hex, cipher_hex))
        conn.commit()
        conn.close()
    except Exception as e:
        log_sign.error(f"🚨 DB Injection Failed: {e}")
        sys.exit(1)

    log_sign.info("\n✅ 성공적으로 DB에 '암호화된' TOTP 시크릿이 주입되었습니다.")
    log_sign.info("=" * 60)
    log_sign.info(f"Admin ID    : {canonical_id}")
    log_sign.info("🔒 (The Secret is encrypted. It cannot be recovered without the Master Passphrase)")
    log_sign.info("=" * 60)
    
    # 터미널 QR 코드 렌더링
    log_sign.info("📱 스마트폰 Authenticator (Google/Authy 등) 앱을 열고 아래 QR 코드를 스캔하세요:\n")
    qr = qrcode.QRCode(version=1, box_size=1, border=2)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    qr.print_ascii(out=sys.stdout, invert=True)
    print("\n")
    
    log_sign.info("=" * 60)
    log_sign.info("💡 QR 스캔이 불가능한 경우, 아래 수동 입력 키를 사용하세요 (단 1회성 출력):")
    log_sign.info(f"Base32 Key : {secret}")
    log_sign.info("=" * 60)


@admin_app.command("totp-revoke")
def revoke_admin(
    user_id: Annotated[str, typer.Option("--user", "-u", help="권한을 폐기할 관리자 식별자 (예: fiber)")],
    db_path: Annotated[str, typer.Option("--db", "-d", help="SQLite DB 파일 경로")] = DEFAULT_DB_PATH
):
    """Revoke (delete) a TOTP secret for an administrator."""
    # [수정] 식별자 정규화 처리 (폐기할 때도 짧게 입력 가능하도록)
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
        # 정규화된 ID로 삭제
        conn.execute("DELETE FROM admin_users WHERE user_id = ?", (canonical_id,))
        conn.commit()
        
        log_sign.info(f"✅ 관리자 '{canonical_id}'의 TOTP 권한이 DB에서 완전히 삭제되었습니다.")
        conn.close()
    except Exception as e:
        log_sign.error(f"🚨 Revocation Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    app()