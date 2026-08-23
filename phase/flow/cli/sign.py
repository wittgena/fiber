# phase.flow.cli.sign
import sys
import secrets
from typing import Annotated

import typer
from eth_account import Account

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

# --- Loggers ---
log_genkey = get_emitter("evm.genkey")
log_sign = get_emitter("sign.signature")

# --- Typer App ---
app = typer.Typer(
    name="flow",
    help="Flow Crypto & Signature CLI Tools",
    add_completion=False,
    no_args_is_help=True,
)


# ==========================================
# 1. Genkey Command
# ==========================================

def _generate_agent(name: str) -> tuple[str, str]:
    priv = secrets.token_hex(32)
    private_key = "0x" + priv
    account = Account.from_key(private_key)
    
    log_genkey.info(f"[{name} Agent]")
    log_genkey.info(f"EVM Address : {account.address}")
    log_genkey.info(f"Private Key : {private_key}")
    log_genkey.info(f"DID Format  : did:pkh:eip155:84532:{account.address}")
    log_genkey.info("-" * 50)
    
    return private_key, account.address

@app.command("genkey")
def flow_genkey():
    """Generate EOA Wallets for Testnet Agents."""
    log_genkey.info("🚀 Generating EOA Wallets for Testnet Agents...\n")
    
    alpha_pkey, _ = _generate_agent("Alpha (Compute Provider)")
    beta_pkey, _ = _generate_agent("Beta (Data Consumer)")
    
    log_genkey.info("📋 Copy & Paste this to your .env file:")
    log_genkey.info("=" * 50)
    log_genkey.info(f'AGENT_ALPHA_PKEY="{alpha_pkey}"')
    log_genkey.info(f'AGENT_BETA_PKEY="{beta_pkey}"')
    log_genkey.info("=" * 50)


# ==========================================
# 2. Signature Command
# ==========================================

@app.command("signature")
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


if __name__ == "__main__":
    app()