# phase.flow.sign.genkey
## @lineage: flow.sign.genkey
## @lineage: meta.flow.sign.genkey
## @lineage: meta.cli.sign.genkey
## @lineage: phase.dphi.evm.genkey
from eth_account import Account
import secrets
from watcher.plane.emitter import get_emitter

log = get_emitter("evm.genkey")

def generate_agent(name: str):
    priv = secrets.token_hex(32)
    private_key = "0x" + priv
    account = Account.from_key(private_key)
    
    log.info(f"[{name} Agent]")
    log.info(f"EVM Address : {account.address}")
    log.info(f"Private Key : {private_key}")
    log.info(f"DID Format  : did:pkh:eip155:84532:{account.address}")
    log.info("-" * 50)
    
    return private_key, account.address

if __name__ == "__main__":
    log.info("🚀 Generating EOA Wallets for Testnet Agents...\n")
    alpha_pkey, alpha_addr = generate_agent("Alpha (Compute Provider)")
    beta_pkey, beta_addr = generate_agent("Beta (Data Consumer)")
    
    log.info("📋 Copy & Paste this to your .env file:")
    log.info("=" * 50)
    log.info(f"AGENT_ALPHA_PKEY=\"{alpha_pkey}\"")
    log.info(f"AGENT_BETA_PKEY=\"{beta_pkey}\"")
    log.info("=" * 50)