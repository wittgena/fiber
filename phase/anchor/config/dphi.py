# phase.anchor.config.dphi
import os
from enum import Enum
from typing import Dict, List
from pydantic import BaseModel, Field
from dataclasses import dataclass, field

class DphiEnv(str, Enum):
    LOCAL = "local"
    TESTNET = "testnet"
    MAINNET = "mainnet"

CURRENT_ENV = DphiEnv(os.getenv("DPHI_ENV", DphiEnv.TESTNET.value))
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
DEFAULT_PKEY_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEFAULT_PKEY_1 = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"

class AgentAccount(BaseModel):
    """Unified account structure serving both D3Fi Intent workflows and DVM Shadow executions."""
    name: str
    did: str
    evm_address: str
    private_key_env_var: str
    fallback_pkey: str

class AgentRegistry(BaseModel):
    """Registry for AI Agents (Alpha/Alice & Beta/Bob) participating in the network."""
    alpha: AgentAccount = AgentAccount(
        name="Compute_Provider_Alpha",
        did="did:pkh:eip155:84532:0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        evm_address="0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        private_key_env_var="AGENT_PKEY_ALPHA",
        fallback_pkey=os.getenv("AGENT_PKEY_ALPHA", DEFAULT_PKEY_0)
    )
    beta: AgentAccount = AgentAccount(
        name="Data_Consumer_Beta",
        did="did:pkh:eip155:84532:0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        evm_address="0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        private_key_env_var="AGENT_PKEY_BETA",
        fallback_pkey=os.getenv("AGENT_PKEY_BETA", DEFAULT_PKEY_1)
    )

def _get_default_rpc() -> str:
    """Dynamically construct RPC URL prioritizing Alchemy if key is provided."""
    env_override = os.getenv("L2_RPC_URL") or os.getenv("DVM_RPC_URL")
    if env_override:
        return env_override

    if ALCHEMY_API_KEY:
        if CURRENT_ENV == DphiEnv.MAINNET:
            return f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
        else:
            return f"https://eth-sepolia.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    
    if CURRENT_ENV == DphiEnv.MAINNET:
        return "https://eth-mainnet.public.blastapi.io"
    return "https://ethereum-sepolia-rpc.publicnode.com"

class NetworkConfig(BaseModel):
    """Unified network configuration for Web3 communication and Settlement."""
    chain_id: int = Field(default_factory=lambda: 1 if CURRENT_ENV == DphiEnv.MAINNET else 11155111)
    rpc_url: str = Field(default_factory=_get_default_rpc)
    use_poa_middleware: bool = Field(default_factory=lambda: CURRENT_ENV != DphiEnv.MAINNET)

class ContractRegistry(BaseModel):
    """Addresses of target smart contracts on the settlement layer."""
    nexus_clearing: str = Field(default_factory=lambda: os.getenv("NEXUS_CONTRACT", "0x3333333333333333333333333333333333333333"))
    target_erc20: str = Field(default_factory=lambda: os.getenv(
        "TARGET_ERC20", 
        "0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14" if CURRENT_ENV == DphiEnv.TESTNET else "0x1111111111111111111111111111111111111111"
    ))
    dex_router: str = Field(default_factory=lambda: os.getenv("TARGET_DEX", "0x2222222222222222222222222222222222222222"))

class WasmBrokerConfig(BaseModel):
    """Execution parameters for the dvm.wasm engine."""
    tier: str = "SYSTEM"
    max_gas_limit: int = 30_000_000
    timeout_ms: int = 5000

class CDPWalletConfig(BaseModel):
    """Coinbase Developer Platform API credentials for automated live transactions."""
    network_id: str = Field(default_factory=lambda: "base-mainnet" if CURRENT_ENV == DphiEnv.MAINNET else "base-sepolia")
    api_name: str = Field(default_factory=lambda: os.getenv("TEST_CDP_API_NAME", ""))
    api_private_key: str = Field(default_factory=lambda: os.getenv("TEST_CDP_API_PRIVATE_KEY", ""))

class ExportAttestationConfig(BaseModel):
    """Ed25519 Public Keys of Notary nodes required to sign state transitions."""
    @property
    def witness_pubkeys(self) -> List[str]:
        env_validators = os.getenv("COMMITTEE_VALIDATORS")
        if env_validators:
            return [v.strip() for v in env_validators.split(",")]
        return [
            "d9b397e16418eaead7782aaef98dc8b64b550b61c3e1f5f393089da77601a142", 
            "e8c460d3d52c2ab7eb79f42b322a30bb9133a8c66eef4ec3a1d9b3a31c618b7a",
            "1c53e020462002cd43e33d4da3d61ea15a9992d9f4c3bece7d2b2c3a5d848721"
        ]

class DAConfig(BaseModel):
    """Celestia DAaaS configurations."""
    provider: str = Field(default_factory=lambda: "celestia-mainnet" if CURRENT_ENV == DphiEnv.MAINNET else "celestia-mocha-testnet")
    rpc_url: str = Field(default_factory=lambda: os.getenv(
        "DA_RPC_URL",
        "https://celestiabridge-mainnet.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8" if CURRENT_ENV == DphiEnv.MAINNET else "https://celestiabridge-mocha.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8"
    ))
    namespace_id: str = os.getenv("DA_NAMESPACE", "0000000000000000000000000000000000000000000000000000d941")
    auth_token_env_var: str = "CELESTIA_NODE_AUTH_TOKEN"

class OTLPConfig(BaseModel):
    """Datadog/OTLP telemetry ingestion points."""
    metrics_endpoint: str = os.getenv("OTLP_ENDPOINT", "https://otlp.datadoghq.com/api/v0.2/traces")
    @property
    def headers(self) -> Dict[str, str]:
        return {
            "DD-API-KEY": os.getenv("DATADOG_API_KEY", "mock_dd_key_12345")
        }

class UnifiedExchangeConfig(BaseModel):
    """Master configuration object integrating Network, WASM execution, and External Plugs."""
    mode: DphiEnv = CURRENT_ENV
    
    # Execution & Agents
    network: NetworkConfig = Field(default_factory=NetworkConfig)
    contracts: ContractRegistry = Field(default_factory=ContractRegistry)
    agents: AgentRegistry = Field(default_factory=AgentRegistry)
    wasm: WasmBrokerConfig = Field(default_factory=WasmBrokerConfig)
    
    # External Sinks
    cdp_wallet: CDPWalletConfig = Field(default_factory=CDPWalletConfig)
    export_attestation: ExportAttestationConfig = Field(default_factory=ExportAttestationConfig)
    da_layer: DAConfig = Field(default_factory=DAConfig)
    otlp: OTLPConfig = Field(default_factory=OTLPConfig)
    
    def get_agent_pkey(self, agent_name: str) -> str:
        """Retrieves the private key for a requested agent."""
        agent = getattr(self.agents, agent_name, None)
        if not agent:
            raise ValueError(f"Unknown agent: {agent_name}")
            
        env_val = os.getenv(agent.private_key_env_var)
        if env_val:
            return env_val.replace('\\n', '\n')
        return agent.fallback_pkey

mock_env = UnifiedExchangeConfig()

@dataclass
class DvmConfig:
    mode: str = "suite"
    scenario: str = "ERC20_TRANSFER"
    revert: bool = False
    
    rpc_url: str = field(default_factory=lambda: mock_env.network.rpc_url)
    target: str = field(default_factory=lambda: mock_env.contracts.target_erc20)
    caller: str = field(default_factory=lambda: mock_env.agents.alpha.evm_address)
    
    value: str = "0"
    calldata: str = "0x"
    slots: List[str] = field(default_factory=list)