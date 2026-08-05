# exchange.net.config
## @lineage: exchange.mock.config
## @lineage: dphi.exchange.mock.config
import os
from enum import Enum
from typing import Dict, List
from pydantic import BaseModel, Field

class DphiEnv(str, Enum):
    LOCAL = "local"
    TESTNET = "testnet"
    MAINNET = "mainnet"

CURRENT_ENV = DphiEnv(os.getenv("DPHI_ENV", DphiEnv.TESTNET.value))

## mock pkey
AGENT_PKEY_ALPHA = os.getenv("AGENT_PKEY_ALPHA", "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
AGENT_PKEY_BETA  = os.getenv("AGENT_PKEY_BETA", "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")

class AgentAccount(BaseModel):
    name: str
    did: str
    evm_address: str
    private_key_env_var: str
    fallback_pkey: str

class AgentRegistry(BaseModel):
    """실제 Base Sepolia 등에서 테스트용으로 사용할 고정 계정들 (X402 결제 주체)"""
    alpha: AgentAccount = AgentAccount(
        name="Compute_Provider_Agent",
        did="did:pkh:eip155:84532:0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        evm_address="0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        private_key_env_var="AGENT_PKEY_ALPHA",
        fallback_pkey=AGENT_PKEY_ALPHA
    )
    beta: AgentAccount = AgentAccount(
        name="Data_Consumer_Agent",
        did="did:pkh:eip155:84532:0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        evm_address="0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        private_key_env_var="AGENT_PKEY_BETA",
        fallback_pkey=AGENT_PKEY_BETA
    )

class CDPWalletConfig(BaseModel):
    """실제 송금 및 서명을 위한 CDP API 자격 증명"""
    network_id: str = Field(default_factory=lambda: "base-mainnet" if CURRENT_ENV == DphiEnv.MAINNET else "base-sepolia")
    api_name: str = Field(default_factory=lambda: os.getenv("TEST_CDP_API_NAME", ""))
    api_private_key: str = Field(default_factory=lambda: os.getenv("TEST_CDP_API_PRIVATE_KEY", ""))

class ExportAttestationConfig(BaseModel):
    """WASM 코어가 만든 순수 증명을 외부(EVM 등)로 제출할 때, 외부 스마트 컨트랙트가 형식적으로 요구하는 증인(Notary/Witness)들의 Ed25519 공개키 목록"""
    @property
    def witness_pubkeys(self) -> List[str]:
        env_validators = os.getenv("COMMITTEE_VALIDATORS")
        if env_validators:
            return [v.strip() for v in env_validators.split(",")]
        
        ## 로컬 SSH 키 등에서 추출된 Ed25519 Raw Hex
        return [
            "d9b397e16418eaead7782aaef98dc8b64b550b61c3e1f5f393089da77601a142", 
            "e8c460d3d52c2ab7eb79f42b322a30bb9133a8c66eef4ec3a1d9b3a31c618b7a",
            "1c53e020462002cd43e33d4da3d61ea15a9992d9f4c3bece7d2b2c3a5d848721"
        ]

class SettlementTargetConfig(BaseModel):
    """external sink - ex) evm, solana, cosmos"""
    chain_id: int = Field(default_factory=lambda: 8453 if CURRENT_ENV == DphiEnv.MAINNET else 84532)
    rpc_url: str = Field(default_factory=lambda: os.getenv(
        "L2_RPC_URL", 
        "https://mainnet.base.org" if CURRENT_ENV == DphiEnv.MAINNET else "https://sepolia.base.org"
    ))
    nexus_contract_address: str = Field(default_factory=lambda: os.getenv("NEXUS_CONTRACT", "0x1111111111111111111111111111111111111111"))
    clearing_contract_address: str = Field(default_factory=lambda: os.getenv("CLEARING_CONTRACT", "0x2222222222222222222222222222222222222222"))

class DAConfig(BaseModel):
    """Celestia DAaaS (Alchemy Bridge) 연동 설정"""
    provider: str = Field(default_factory=lambda: "celestia-mainnet" if CURRENT_ENV == DphiEnv.MAINNET else "celestia-mocha-testnet")
    rpc_url: str = Field(default_factory=lambda: os.getenv(
        "DA_RPC_URL",
        "https://celestiabridge-mainnet.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8" if CURRENT_ENV == DphiEnv.MAINNET else "https://celestiabridge-mocha.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8"
    ))
    namespace_id: str = os.getenv("DA_NAMESPACE", "0000000000000000000000000000000000000000000000000000d941")
    auth_token_env_var: str = "CELESTIA_NODE_AUTH_TOKEN"

class OTLPConfig(BaseModel):
    metrics_endpoint: str = os.getenv("OTLP_ENDPOINT", "https://otlp.datadoghq.com/api/v0.2/traces")
    @property
    def headers(self) -> Dict[str, str]:
        return {
            "DD-API-KEY": os.getenv("DATADOG_API_KEY", "mock_dd_key_12345")
        }

class RealisticMockConfig(BaseModel):
    mode: DphiEnv = CURRENT_ENV
    agents: AgentRegistry = Field(default_factory=AgentRegistry)
    cdp_wallet: CDPWalletConfig = Field(default_factory=CDPWalletConfig)
    
    export_attestation: ExportAttestationConfig = Field(default_factory=ExportAttestationConfig)
    settlement_target: SettlementTargetConfig = Field(default_factory=SettlementTargetConfig)
    da_layer: DAConfig = Field(default_factory=DAConfig)
    otlp: OTLPConfig = Field(default_factory=OTLPConfig)
    
    def get_agent_pkey(self, agent_name: str) -> str:
        agent = getattr(self.agents, agent_name, None)
        if not agent:
            raise ValueError(f"Unknown agent: {agent_name}")
            
        env_val = os.getenv(agent.private_key_env_var)
        if env_val:
            return env_val.replace('\\n', '\n')
        return agent.fallback_pkey

mock_env = RealisticMockConfig()