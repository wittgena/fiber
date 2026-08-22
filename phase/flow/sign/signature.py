# phase.flow.sign.signature
## @lineage: flow.sign.signature
## @lineage: meta.flow.sign.signature
## @lineage: meta.cli.sign.signature
import argparse
import sys
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

log = get_emitter("sign.signature")

def generate_offline_signature(active_signers: list[str], root_private_key: str):
    log.info("\n🔒 [Offline Signer] Generating Pre-signed Payload...")
    payload_dict = {"active_signers": active_signers}
    canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
    try:
        temp_signer = NodeSigner(private_key_hex=root_private_key)
        root_signature = temp_signer.sign_payload(canonical_bytes)
        root_pubkey = temp_signer.pubkey_hex
    except Exception as e:
        log.info(f"🚨 서명 생성 실패: {e}")
        sys.exit(1)

    log.info("\n✅ 성공적으로 서명이 생성되었습니다. Edge 서버 환경변수에 아래 내용을 주입하세요.\n")
    log.info("=" * 60)
    log.info(f"DPHI_ACTIVE_SIGNERS={','.join(active_signers)}")
    log.info(f"DPHI_PRE_SIGNED_ROOT_SIG={root_signature}")
    log.info("=" * 60)
    log.info(f"\n[참고] 이 서명을 검증하기 위해 클라이언트(SDK)에 주입해야 할 Root Public Key:")
    log.info(f"DPHI_ROOT_PUBKEY={root_pubkey}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate offline Root Signature for DPHI Edge.")
    parser.add_argument("--keys", required=True, help="신뢰할 Edge 노드들의 Public Key 목록 (쉼표 구분)")
    parser.add_argument("--root-key", required=True, help="보안 격리된 Master Root Private Key (Hex)")
    args = parser.parse_args()
    signers_list = [k.strip() for k in args.keys.split(",")]
    generate_offline_signature(signers_list, args.root_key)