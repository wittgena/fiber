# fiber.dphi.eco.config
import os
from enum import Enum
from typing import Dict, List, Union
from pydantic import BaseModel, Field

class NetEnv(str, Enum):
    LOCAL = "local"
    TESTNET = "testnet"
    MAINNET = "mainnet"

"""NETWORK CONSTANTS & ADDRESS REGISTRY (Internal Hidden)"""
_ETH_MAINNET_ID = 1
_ETH_SEPOLIA_ID = 11155111
_BASE_SEPOLIA_ID = 84532

# Chain ID는 int(EVM) 또는 str(Cosmos)이 될 수 있음
ChainIdType = Union[int, str]

CURRENT_ENV = NetEnv(os.getenv("DPHI_ENV", NetEnv.TESTNET.value))
_TARGET_TESTNET_ID = _ETH_SEPOLIA_ID
ACTIVE_CHAIN_ID = _ETH_MAINNET_ID if CURRENT_ENV == NetEnv.MAINNET else _TARGET_TESTNET_ID

"""[주소록] 체인 ID를 키(Key)로 사용하여 주소를 매핑"""
_KNOWN_USDC_ADDRESSES: Dict[ChainIdType, str] = {
    _ETH_MAINNET_ID: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    _ETH_SEPOLIA_ID: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
    _BASE_SEPOLIA_ID: "0x036CbD53842c5426634e7929541eC2318f3dCF7e"
}

_KNOWN_RPC_URLS: Dict[ChainIdType, str] = {
    _ETH_MAINNET_ID: "https://eth-mainnet.public.blastapi.io",
    _ETH_SEPOLIA_ID: "https://ethereum-sepolia-rpc.publicnode.com",
    _BASE_SEPOLIA_ID: "https://sepolia.base.org",
}

_ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")

# Anvil/Hardhat 표준 테스트 계정 0, 1, 2
_DEFAULT_PKEY_0 = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
_DEFAULT_PKEY_1 = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
_DEFAULT_PKEY_2 = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"


class AgentAccount(BaseModel):
    """Unified account structure serving D3Fi Intent workflows"""
    name: str
    did: str
    evm_address: str
    private_key_env_var: str
    fallback_pkey: str

class AgentRegistry(BaseModel):
    """Registry for AI Agents (Alpha/Alice & Beta/Bob) participating in the network"""
    alpha: AgentAccount = AgentAccount(
        name="Compute_Provider_Alpha",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        evm_address="0x4331626df4B45B695ef5F56670c2e2f2C6A02e3B",
        private_key_env_var="AGENT_PKEY_ALPHA",
        fallback_pkey=os.getenv("AGENT_PKEY_ALPHA", _DEFAULT_PKEY_0)
    )
    beta: AgentAccount = AgentAccount(
        name="Data_Consumer_Beta",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        evm_address="0xDD43a52B5Cf94fA2E65Cd5aC7820614C31C6c097",
        private_key_env_var="AGENT_PKEY_BETA",
        fallback_pkey=os.getenv("AGENT_PKEY_BETA", _DEFAULT_PKEY_1)
    )
    system_clearing: AgentAccount = AgentAccount(
        name="System_Clearinghouse_Master",
        did=f"did:pkh:eip155:{ACTIVE_CHAIN_ID}:0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        evm_address="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        private_key_env_var="SYSTEM_CLEARING_PKEY",
        fallback_pkey=os.getenv("SYSTEM_CLEARING_PKEY", _DEFAULT_PKEY_2)
    )

def _get_default_rpc() -> str:
    """Dynamically construct EVM RPC URL based on ACTIVE_CHAIN_ID."""
    env_override = os.getenv("L2_RPC_URL") or os.getenv("DVM_RPC_URL")
    if env_override:
        return env_override

    if _ALCHEMY_API_KEY:
        if ACTIVE_CHAIN_ID == _ETH_MAINNET_ID:
            return f"https://eth-mainnet.g.alchemy.com/v2/{_ALCHEMY_API_KEY}"
        elif ACTIVE_CHAIN_ID == _ETH_SEPOLIA_ID:
            return f"https://eth-sepolia.g.alchemy.com/v2/{_ALCHEMY_API_KEY}"
        elif ACTIVE_CHAIN_ID == _BASE_SEPOLIA_ID:
            return f"https://base-sepolia.g.alchemy.com/v2/{_ALCHEMY_API_KEY}"
    
    return _KNOWN_RPC_URLS.get(ACTIVE_CHAIN_ID, _KNOWN_RPC_URLS[_ETH_SEPOLIA_ID])

class NetworkConfig(BaseModel):
    """Unified network configuration for Web3 communication and Settlement."""
    chain_id: ChainIdType = Field(default_factory=lambda: ACTIVE_CHAIN_ID)
    rpc_url: str = Field(default_factory=_get_default_rpc)
    use_poa_middleware: bool = Field(default_factory=lambda: CURRENT_ENV != NetEnv.MAINNET)

class ContractRegistry(BaseModel):
    """Addresses of target smart contracts on the settlement layer."""
    nexus_clearing: str = Field(default_factory=lambda: os.getenv("NEXUS_CONTRACT", "0x3333333333333333333333333333333333333333"))
    
    # getattr를 통해 동적으로 로드됨 (target_erc20, target_usdc)
    target_erc20: str = Field(default_factory=lambda: os.getenv(
        "TARGET_ERC20", 
        _KNOWN_USDC_ADDRESSES.get(ACTIVE_CHAIN_ID, "0x0000000000000000000000000000000000000000")
    ))
    target_usdc: str = Field(default_factory=lambda: os.getenv(
        "TARGET_USDC",
        _KNOWN_USDC_ADDRESSES.get(ACTIVE_CHAIN_ID, "0x0000000000000000000000000000000000000000")
    ))
    
    # CosmWasm 바인딩 (defin.py에서 사용)
    target_cw20: str = Field(default_factory=lambda: os.getenv("TARGET_CW20", "akash1cw20tokenaddressmock1234"))

class WasmBrokerConfig(BaseModel):
    """Execution parameters for the dvm.wasm engine."""
    tier: str = "SYSTEM"

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
    namespace_id: str = os.getenv("DA_NAMESPACE", "0000000000000000000000000000000000000000000000000000d941")

class ExchangeConfig(BaseModel):
    """Master configuration object integrating Network, WASM execution, and External Plugs."""
    mode: NetEnv = CURRENT_ENV
    
    network: NetworkConfig = Field(default_factory=NetworkConfig)
    contracts: ContractRegistry = Field(default_factory=ContractRegistry)
    agents: AgentRegistry = Field(default_factory=AgentRegistry)
    wasm: WasmBrokerConfig = Field(default_factory=WasmBrokerConfig)
    
    export_attestation: ExportAttestationConfig = Field(default_factory=ExportAttestationConfig)
    da_layer: DAConfig = Field(default_factory=DAConfig)
    
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