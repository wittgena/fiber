# dphi.adapter.config
## @lineage: dphi.bound.config
## @lineage: bound.config.dphi
import os
from enum import Enum
from typing import Dict, List, Union
from pydantic import BaseModel, Field
from dataclasses import dataclass, field

class NetEnv(str, Enum):
    LOCAL = "local"
    TESTNET = "testnet"
    MAINNET = "mainnet"

"""NETWORK CONSTANTS & ADDRESS REGISTRY"""
ETH_MAINNET_ID = 1
ETH_SEPOLIA_ID = 11155111
BASE_SEPOLIA_ID = 84532

# 신규 Cosmos / Akash / Osmosis 추가
AKASH_MAINNET_ID = "akashnet-2"
OSMOSIS_MAINNET_ID = "osmosis-1"

# Chain ID는 int(EVM) 또는 str(Cosmos)이 될 수 있음
ChainIdType = Union[int, str]

# 현재 테스트넷 대상을 Ethereum Sepolia로 할지, Base Sepolia로 할지 명시적 결정
TARGET_TESTNET_ID = ETH_SEPOLIA_ID
CURRENT_ENV = NetEnv(os.getenv("DPHI_ENV", NetEnv.TESTNET.value))
ACTIVE_CHAIN_ID = ETH_MAINNET_ID if CURRENT_ENV == NetEnv.MAINNET else TARGET_TESTNET_ID

"""[주소록] 체인 ID를 키(Key)로 사용하여 주소를 매핑"""
KNOWN_USDC_ADDRESSES: Dict[ChainIdType, str] = {
    ETH_MAINNET_ID: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ETH_SEPOLIA_ID: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
    BASE_SEPOLIA_ID: "0x036CbD53842c5426634e7929541eC2318f3dCF7e"
}

KNOWN_RPC_URLS: Dict[ChainIdType, str] = {
    # EVM RPCs
    ETH_MAINNET_ID: "https://eth-mainnet.public.blastapi.io",
    ETH_SEPOLIA_ID: "https://ethereum-sepolia-rpc.publicnode.com",
    BASE_SEPOLIA_ID: "https://sepolia.base.org",
    
    # Cosmos Tendermint RPCs
    AKASH_MAINNET_ID: "https://rpc.akash.forbole.com:443",
    OSMOSIS_MAINNET_ID: "https://rpc.osmosis.zone:443"
}

KNOWN_REST_URLS: Dict[ChainIdType, str] = {
    AKASH_MAINNET_ID: "https://api.akash.forbole.com:443",
    OSMOSIS_MAINNET_ID: "https://lcd.osmosis.zone:443"
}

ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
# Anvil/Hardhat 표준 테스트 계정 0, 1, 2
DEFAULT_PKEY_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEFAULT_PKEY_1 = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
DEFAULT_PKEY_2 = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"

class AgentAccount(BaseModel):
    """Unified account structure serving both D3Fi Intent workflows and DVM Shadow executions"""
    name: str
    did: str
    evm_address: str
    cosmos_address: str = ""  # 추가된 필드 (akash1... or osmo1...)
    private_key_env_var: str
    fallback_pkey: str

class AgentRegistry(BaseModel):
    """Registry for AI Agents (Alpha/Alice & Beta/Bob) participating in the network"""
    alpha: AgentAccount = AgentAccount(
        name="Compute_Provider_Alpha",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        evm_address="0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        cosmos_address="akash1provideraddressmock1234567890",
        private_key_env_var="AGENT_PKEY_ALPHA",
        fallback_pkey=os.getenv("AGENT_PKEY_ALPHA", DEFAULT_PKEY_0)
    )
    beta: AgentAccount = AgentAccount(
        name="Data_Consumer_Beta",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        evm_address="0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        cosmos_address="akash1consumeraddressmock0987654321",
        private_key_env_var="AGENT_PKEY_BETA",
        fallback_pkey=os.getenv("AGENT_PKEY_BETA", DEFAULT_PKEY_1)
    )
    system_clearing: AgentAccount = AgentAccount(
        name="System_Clearinghouse_Master",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        evm_address="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        cosmos_address="akash1clearingmastermock1122334455",
        private_key_env_var="SYSTEM_CLEARING_PKEY",
        fallback_pkey=os.getenv("SYSTEM_CLEARING_PKEY", DEFAULT_PKEY_2)
    )

def _get_default_rpc() -> str:
    """Dynamically construct EVM RPC URL based on ACTIVE_CHAIN_ID."""
    env_override = os.getenv("L2_RPC_URL") or os.getenv("DVM_RPC_URL")
    if env_override:
        return env_override

    if ALCHEMY_API_KEY:
        if ACTIVE_CHAIN_ID == ETH_MAINNET_ID:
            return f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
        elif ACTIVE_CHAIN_ID == ETH_SEPOLIA_ID:
            return f"https://eth-sepolia.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
        elif ACTIVE_CHAIN_ID == BASE_SEPOLIA_ID:
            return f"https://base-sepolia.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    
    return KNOWN_RPC_URLS.get(ACTIVE_CHAIN_ID, KNOWN_RPC_URLS[ETH_SEPOLIA_ID])

class NetworkConfig(BaseModel):
    """Unified network configuration for Web3 communication and Settlement."""
    chain_id: ChainIdType = Field(default_factory=lambda: ACTIVE_CHAIN_ID)
    rpc_url: str = Field(default_factory=_get_default_rpc)
    use_poa_middleware: bool = Field(default_factory=lambda: CURRENT_ENV != NetEnv.MAINNET)

    # Cosmos 통신을 위한 엔드포인트 추가
    @property
    def cosm_rpc_url(self) -> str:
        target_chain = os.getenv("TARGET_COSMOS_CHAIN", AKASH_MAINNET_ID)
        return os.getenv("COSM_RPC_URL", KNOWN_RPC_URLS.get(target_chain, ""))

    @property
    def cosm_rest_url(self) -> str:
        target_chain = os.getenv("TARGET_COSMOS_CHAIN", AKASH_MAINNET_ID)
        return KNOWN_REST_URLS.get(target_chain, "")

class ContractRegistry(BaseModel):
    """Addresses of target smart contracts on the settlement layer."""
    nexus_clearing: str = Field(default_factory=lambda: os.getenv("NEXUS_CONTRACT", "0x3333333333333333333333333333333333333333"))
    target_erc20: str = Field(default_factory=lambda: os.getenv(
        "TARGET_ERC20", 
        KNOWN_USDC_ADDRESSES.get(ACTIVE_CHAIN_ID, "0x0000000000000000000000000000000000000000")
    ))
    target_usdc: str = Field(default_factory=lambda: os.getenv(
        "TARGET_USDC",
        KNOWN_USDC_ADDRESSES.get(ACTIVE_CHAIN_ID, "0x0000000000000000000000000000000000000000")
    ))
    dex_router: str = Field(default_factory=lambda: os.getenv("TARGET_DEX", "0x2222222222222222222222222222222222222222"))
    
    # [신규 Cosmos]
    target_cw20: str = Field(default_factory=lambda: os.getenv("TARGET_CW20", "akash1cw20tokenaddressmock1234"))
    escrow_contract: str = Field(default_factory=lambda: os.getenv("TARGET_ESCROW", "akash1escrowcontractmock5678"))

class WasmBrokerConfig(BaseModel):
    """Execution parameters for the dvm.wasm engine."""
    tier: str = "SYSTEM"
    max_gas_limit: int = 30_000_000
    timeout_ms: int = 5000

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
    provider: str = Field(default_factory=lambda: "celestia-mainnet" if CURRENT_ENV == NetEnv.MAINNET else "celestia-mocha-testnet")
    rpc_url: str = Field(default_factory=lambda: os.getenv(
        "DA_RPC_URL",
        "https://celestiabridge-mainnet.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8" if CURRENT_ENV == NetEnv.MAINNET else "https://celestiabridge-mocha.g.alchemy.com/v2/XW1KY_hfiAz5RRK7sQCs8"
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

class ExchangeConfig(BaseModel):
    """Master configuration object integrating Network, WASM execution, and External Plugs."""
    mode: NetEnv = CURRENT_ENV
    
    network: NetworkConfig = Field(default_factory=NetworkConfig)
    contracts: ContractRegistry = Field(default_factory=ContractRegistry)
    agents: AgentRegistry = Field(default_factory=AgentRegistry)
    wasm: WasmBrokerConfig = Field(default_factory=WasmBrokerConfig)
    
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

dphi_env = ExchangeConfig()

@dataclass
class DvmConfig:
    mode: str = "suite"
    scenario: str = "ERC20_TRANSFER"
    revert: bool = False
    
    rpc_url: str = field(default_factory=lambda: dphi_env.network.rpc_url)
    target: str = field(default_factory=lambda: dphi_env.contracts.target_erc20)
    caller: str = field(default_factory=lambda: dphi_env.agents.alpha.evm_address)
    
    value: str = "0"
    calldata: str = "0x"
    slots: List[str] = field(default_factory=list)