# dphi.adapter.config.validator
## @lineage: phase.epoch.config.validator
import re
import asyncio
import httpx
from typing import List, Tuple

from watcher.plane.emitter import get_emitter
from dphi.adapter.config.dphi import mock_env, DphiEnv

log = get_emitter("config.validator")

class ConfigValidator:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def _is_evm_address(self, address: str) -> bool:
        """EVM 호환 주소 형식(0x + 40자리 헥사데시멀) 검증"""
        return bool(re.match(r"^0x[a-fA-F0-9]{40}$", address))

    def _is_hex_private_key(self, pkey: str) -> bool:
        """표준 256비트 프라이빗 키(0x + 64자리 헥사데시멀) 검증"""
        return bool(re.match(r"^(0x)?[a-fA-F0-9]{64}$", pkey))

    def _is_pem_key(self, key_str: str) -> bool:
        """CDP API 키용 PEM 포맷 검증"""
        return "-----BEGIN" in key_str and "-----END" in key_str

    def validate_agents(self):
        """에이전트 계정의 식별자 및 키 포맷 검증"""
        log.info("[Validator] Checking Agent Registry...")
        for agent_key in ["alpha", "beta"]:
            agent = getattr(mock_env.agents, agent_key)
            
            # 1. EVM 주소 포맷 체크
            if not self._is_evm_address(agent.evm_address):
                self.errors.append(f"Agent '{agent.name}' has invalid EVM address: {agent.evm_address}")
            
            # 2. DID 포맷 체크 (eip155 체인 ID 일치 여부)
            expected_did_prefix = f"did:pkh:eip155:{mock_env.l2_network.chain_id}:"
            if not agent.did.startswith(expected_did_prefix):
                self.warnings.append(f"Agent '{agent.name}' DID doesn't match current Chain ID ({mock_env.l2_network.chain_id})")

            # 3. 프라이빗 키 체크 (주입된 값 or 폴백)
            pkey = mock_env.get_agent_pkey(agent_key)
            if not self._is_hex_private_key(pkey):
                self.errors.append(f"Agent '{agent.name}' private key is not a valid 256-bit hex string.")

    def validate_contracts(self):
        """오피셜 스마트 컨트랙트 주소 포맷 검증"""
        log.info("[Validator] Checking L2 Smart Contracts...")
        contracts = [
            ("Nexus Anchor", mock_env.l2_network.nexus_anchor_contract),
            ("Exchange Clearing", mock_env.l2_network.exchange_clearing_contract)
        ]
        for name, address in contracts:
            if not self._is_evm_address(address):
                self.errors.append(f"{name} Contract has invalid EVM address: {address}")

    def validate_cdp_credentials(self):
        """Coinbase AgentKit API 자격 증명 검증"""
        log.info("[Validator] Checking CDP Wallet Credentials...")
        api_name = mock_env.cdp_wallet.api_name
        api_pkey = mock_env.cdp_wallet.api_private_key

        if mock_env.mode in [DphiEnv.TESTNET, DphiEnv.MAINNET]:
            if not api_name or not api_pkey:
                self.warnings.append(f"Running in {mock_env.mode.upper()} but CDP credentials are missing. Wallet will run in SIMULATE mode.")
            elif not self._is_pem_key(api_pkey):
                self.errors.append("CDP API Private Key is not in valid PEM format (missing BEGIN/END boundaries).")
        else:
            if not api_name or not api_pkey:
                log.info("  └─ Running in LOCAL mode without CDP keys (Simulation).")

    async def ping_l2_rpc(self):
        """L2 RPC 엔드포인트 생존 여부 및 체인 ID 정합성 검증"""
        rpc_url = mock_env.l2_network.rpc_url
        expected_chain_id = mock_env.l2_network.chain_id
        log.info(f"[Validator] Pinging L2 RPC ({rpc_url})...")
        
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_chainId",
            "params": [],
            "id": 1
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.post(rpc_url, json=payload)
                if res.status_code == 200:
                    result_hex = res.json().get("result")
                    actual_chain_id = int(result_hex, 16)
                    if actual_chain_id != expected_chain_id:
                        self.errors.append(f"RPC Chain ID mismatch. Expected {expected_chain_id}, Got {actual_chain_id}.")
                    else:
                        log.info(f"  └─ RPC Alive. Chain ID matches: {actual_chain_id}")
                else:
                    self.errors.append(f"RPC HTTP Error: {res.status_code}")
        except Exception as e:
            self.errors.append(f"Failed to connect to L2 RPC ({rpc_url}): {str(e)}")

    async def execute(self) -> bool:
        log.info(f"\n{'='*60}\n🔍 [ENV VALIDATOR] Checking Dimension: {mock_env.mode.upper()}\n{'='*60}")
        
        self.validate_agents()
        self.validate_contracts()
        self.validate_cdp_credentials()
        await self.ping_l2_rpc()
        
        if self.warnings:
            log.warning("\n⚠️  Warnings found:")
            for w in self.warnings:
                log.warning(f"  - {w}")
                
        if self.errors:
            log.error("\n❌ Validation FAILED. Misconfigurations found:")
            for e in self.errors:
                log.error(f"  - {e}")
            log.info("="*60 + "\n")
            return False
            
        log.info("\n✅ All configurations are cryptographically and structurally valid.")
        log.info("="*60 + "\n")
        return True