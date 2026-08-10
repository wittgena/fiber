# phase.epoch.config.builder.phase
import time
import uuid
import random
import json
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from phase.epoch.config.dphi import mock_env

from arch.contract.model.receptor import (
    TradeIngressRequest,
    AnchorProposalRequest,
    ParityTripletSchema
)
from watcher.receptor.contract.model import ExportLogsServiceRequest
from receptor.edge.core import StreamAppendRequest, LedgerEventSchema

class ExtWalletClient:
    def __init__(self, base_url: str = "http://localhost:8000/v1/ext", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self.client = client  # In-memory E2E 테스트를 위한 커스텀 클라이언트 주입 지원
        self.simulate = True  # 기본값, 외부 설정에 의해 덮어씌워짐

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """내부 유틸리티: httpx 세션을 관리하고 공통 예외 처리를 수행합니다."""
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 45.0)

        if self.client:
            response = await self.client.request(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response.json()
        else:
            async with httpx.AsyncClient() as client:
                response = await client.request(method, url, timeout=timeout, **kwargs)
                response.raise_for_status()
                return response.json()

    # --- 1. Wallet & CDP Settlement ---
    async def get_wallet_info(self) -> Dict[str, Any]:
        """Edge 서버에 구성된 Agent 지갑 정보를 조회합니다."""
        return await self._request("GET", "/wallet/info")

    async def process_x402_payment(self, payee_address: str, amount_usdc: str, resource_id: str) -> Dict[str, Any]:
        """Edge 서버에 X402 정산 결제를 요청합니다."""
        payload = {
            "payee_address": payee_address,
            "amount_usdc": amount_usdc,
            "resource_id": resource_id
        }
        return await self._request("POST", "/wallet/pay/x402", json=payload, timeout=60.0)

    # --- 2. EVM Web3 Interactions ---
    async def get_evm_balances(self, address: str) -> Dict[str, Any]:
        """EVM 계정의 Native(ETH) 및 ERC20(WETH) 잔고를 조회합니다."""
        return await self._request("GET", f"/evm/balance?address={address}")

    async def wrap_weth(self, caller_address: str, amount_wei: int, agent_alias: str = "beta") -> Dict[str, Any]:
        """Native ETH를 WETH로 Auto-wrap 스마트 컨트랙트 호출을 서버에 요청합니다."""
        payload = {
            "caller_address": caller_address, 
            "amount_wei": str(amount_wei), 
            "agent_alias": agent_alias
        }
        return await self._request("POST", "/evm/wrap", json=payload, timeout=90.0)


# =====================================================================
# DVM Models & Data Classes
# =====================================================================
@dataclass
class DvmConfig:
    """ EVM Runner 및 Pipeline 실행 시 사용되는 전역 설정 데이터 """
    mode: str = "suite"
    scenario: str = "ERC20_TRANSFER"
    revert: bool = False
    
    rpc_url: str = field(default_factory=lambda: mock_env.network.rpc_url)
    target: str = field(default_factory=lambda: mock_env.contracts.target_erc20)
    caller: str = field(default_factory=lambda: mock_env.agents.alpha.evm_address)
    
    value: str = "0"
    calldata: str = "0x"
    slots: List[str] = field(default_factory=list)


@dataclass
class EvmIntent:
    """ EVM 트랜잭션 실행 의도를 명세하는 정규화된 스키마 """
    target: str
    caller: str
    calldata: str
    scenario_type: str
    value: int = 0
    storage_slots: List[str] = field(default_factory=list)
    requires_access_list: bool = False
    allowance_slot_index: Optional[int] = None
    
    def to_workflow_dict(self) -> dict:
        """ EvmWorkflow가 기대하는 dict 포맷으로 변환 (None 값은 제거) """
        data = asdict(self)
        data.pop("target") # target은 EvmWorkflow 생성자에 별도로 주입되므로 제외
        return {k: v for k, v in data.items() if v is not None}


class NotarySwarm:
    def __init__(self, size: int = 3):
        self.notaries = []
        for i in range(size):
            seed = hashlib.sha256(f"dphi_notary_node_{i}".encode()).digest()
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(seed)
            public_hex = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, 
                format=serialization.PublicFormat.Raw
            ).hex()
            self.notaries.append({"priv": private_key, "pub": public_hex})

    @property
    def public_keys(self) -> List[str]:
        return [node["pub"] for node in self.notaries]

    def attest_payload(self, canonical_hash: bytes) -> List[str]:
        return [node["priv"].sign(canonical_hash).hex() for node in self.notaries]


# =====================================================================
# PhaseBuilder (Payload Factories)
# =====================================================================
class PhaseBuilder:
    __domain_metadata__ = {
        "otlp_payload": "OTel + Datadog/LangSmith. Tracks LLM GenAI metrics (tokens/latency) for billing.",
        "trade_intent": "W3C DID + UniswapX/Fetch.ai. Intent-centric A2A (Agent-to-Agent) resource swap with slippage.",
        "ledger_append": "Celestia/EigenDA + RISC Zero. Immutable DA (Data Availability) & ZK-verifiable compute logs.",
        "anchor_proposal": "Ethereum L2 (OP Stack) Sequencer. Rollup of state roots (Merkle Parity) for global consensus.",
        "evm_user_intent": "Execution intent supporting complex scenarios (ERC4337, Uniswap, Merkle) for WASM EVM.",
        "evm_state_snapshot": "Mock EVM account state (balance, nonce, storage) bypassing actual RPC calls."
    }

    @staticmethod
    def get_testnet_wallet(edge_server_url: str = "http://localhost:8000/v1/ext") -> ExtWalletClient:
        return ExtWalletClient(base_url=edge_server_url)

    @staticmethod
    def ap2_mandate_params(
        agent_pub_hex: str, 
        agent_key: ed25519.Ed25519PrivateKey,
        target_action: str = "M2M_INFERENCE_SWAP",
        max_spend_usdc: str = "0.50",
        is_expired: bool = False  
    ) -> Dict[str, Any]:
        return {
            "requester_id": agent_pub_hex,
            "target_action": target_action,
            "max_spend_usdc": max_spend_usdc,
            "signer_key": agent_key,
            "validity_ms": -3600000 if is_expired else 3600000,
            "delegation_tier": "TIER_2_ORACLE",
            "allowed_networks": ["base-mainnet", "arbitrum-one"],
            "ip_restrictions": ["192.168.1.0/24", "10.0.0.0/8"]
        }

    @staticmethod
    def otlp_payload(
        model_name: str = None, 
        prompt_tokens: int = None,
        completion_tokens: int = None,
        latency_ms: int = None,
        is_malformed: bool = False 
    ) -> Dict[str, Any]:
        models = ["gemini-1.5-pro", "gpt-4o", "claude-3-5-sonnet", "llama-3.1-70b-instruct"]
        providers = ["gcp", "aws", "azure", "together-ai"]
        
        model_name = model_name or random.choice(models)
        prompt_tokens = prompt_tokens or random.randint(100, 150000)
        completion_tokens = completion_tokens or random.randint(10, 4096)
        latency_ms = latency_ms or random.randint(500, 15000)
        
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        agent_did = mock_env.agents.alpha.did 
        
        req = ExportLogsServiceRequest(
            resourceLogs=[{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "xelog-agent-gateway"}},
                        {"key": "cloud.provider", "value": {"stringValue": random.choice(providers)}},
                        {"key": "cloud.region", "value": {"stringValue": "us-west-2"}},
                        {"key": "agent.did", "value": {"stringValue": agent_did}},
                        {"key": "tenant.id", "value": {"stringValue": agent_did}} 
                    ]
                },
                "scopeLogs": [{
                    "scope": {"name": "genai.instrumentation", "version": "1.2.0"},
                    "logRecords": [{
                        "timeUnixNano": str(int(time.time() * 1e9)),
                        "traceId": trace_id,
                        "spanId": span_id,
                        "severityText": "INFO",
                        "body": {"stringValue": f"[{model_name}] LLM Inference completed successfully."},
                        "attributes": [
                            {"key": "llm.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.request.model", "value": {"stringValue": model_name}},
                            {"key": "gen_ai.response.latency_ms", "value": {"intValue": str(latency_ms)}},
                            {"key": "prompt_tokens", "value": {"intValue": str(prompt_tokens)}},
                            {"key": "completion_tokens", "value": {"intValue": str(completion_tokens)}},
                            {"key": "reasoning_tokens", "value": {"intValue": str(random.randint(0, completion_tokens))}},
                            {"key": "gen_ai.request.temperature", "value": {"doubleValue": "0.7"}},
                            {"key": "gen_ai.response.finish_reason", "value": {"stringValue": "stop"}},
                        ]
                    }]
                }]
            }]
        )
        payload = req.model_dump(exclude_none=True)
        
        if is_malformed:
            del payload["resourceLogs"] 
        else:
            estimated_cost = (prompt_tokens * 0.000001) + (completion_tokens * 0.000002)
            payload["genai_metrics"] = {
                "tenant_id": agent_did,
                "model": model_name,
                "usage": {
                    "prompt_tokens": prompt_tokens, 
                    "completion_tokens": completion_tokens,
                    "estimated_cost_usd": round(estimated_cost, 4)
                }
            }
        return payload

    @staticmethod
    def trade_intent(
        action: str = "A2A_COMPUTE_LEASE",
        token: str = "USDC",
        max_fee: str = "2.50",
        slippage: int = 50,
        should_fail_policy: bool = False 
    ) -> Dict[str, Any]:
        if should_fail_policy:
            token = "DOGE"
            slippage = 5000 
            
        req = TradeIngressRequest(
            agent_id=mock_env.agents.alpha.did,
            action=action,
            parameters={
                "target_service": mock_env.agents.beta.did,
                "payment_token": "USDC" if not should_fail_policy else token,
                "max_fee_amount": max_fee,          
                "slippage_tolerance_bps": slippage,      
                "deadline_ts": int(time.time()) + (10 if should_fail_policy else 300),
                "execution_environment": {
                    "hardware": "NVIDIA_H100_80GB",
                    "duration_seconds": 3600,
                    "dataset_cid": "ipfs://QmYwAPJzv5CZsnA625s3Xf2sm5DcgXU1G"
                }
            }
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def ledger_append(action_name: str, root_hash: str, event_count: int = 3) -> Dict[str, Any]:
        events = []
        for i in range(event_count):
            pii_payload = {
                "user_email": f"agent_{i}@dphi.network",
                "kyc_wallet_ip": f"192.168.1.{10+i}",
                "auth_token": f"Bearer eyJhbGci...{uuid.uuid4().hex[:8]}"
            }
            
            event = LedgerEventSchema(
                action=f"{action_name}_STEP_{i+1}",
                user_id="system_clearing_engine",
                pii_data=pii_payload, 
                details=f"State transition step {i+1} for intent hash {root_hash}."
            )
            events.append(event)
            
        req = StreamAppendRequest(
            stream_name=mock_env.da_layer.namespace_id,
            verbose=True,
            events=events
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def anchor_proposal(
        state_roots: Dict[str, str], 
        inject_fault: bool = False 
    ) -> Dict[str, Any]:
        ledger_root = state_roots.get("ledger_root", f"0x{uuid.uuid4().hex}")
        if inject_fault:
            ledger_root = "0xBAD_HASH_CORRUPTED_STATE"
            
        parity = ParityTripletSchema(
            topos_id=f"epoch_{time.strftime('%Y%m%d')}_batch_01",
            phase_id=1,
            nexus_id=14592,
            state_hash=ledger_root
        )
        
        witnesses = mock_env.export_attestation.witness_pubkeys
        mock_signatures = [f"{uuid.uuid4().hex}{uuid.uuid4().hex}" for _ in range(3)]
        req = AnchorProposalRequest(
            receptor_id=mock_env.contracts.nexus_clearing,
            proposed_parity=parity,
            parent_nexus_id=14591,
            self_parent_state="genesis",
            repos={
                "exchange_merkle_root": state_roots.get("exchange_root", "0x00"),
                "otlp_telemetry_root": state_roots.get("otlp_root", "0x00")
            },
            signers=witnesses[:3], 
            signatures=mock_signatures,
            timestamp=int(time.time() * 1000)
        )
        return req.model_dump(exclude_none=True)

    @staticmethod
    def evm_user_intent(
        scenario_type: str = "ERC20_TRANSFER",
        should_revert: bool = False
    ) -> EvmIntent:
        caller = mock_env.agents.alpha.evm_address
        value = 0
        requires_access_list = False
        
        if scenario_type == "ERC20_TRANSFER":
            target = mock_env.contracts.target_erc20
            calldata = "0xa9059cbb" + "000000000000000000000000" + mock_env.agents.beta.evm_address[2:] + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            storage_slots = ["0x0", "0x1", "0x2"]
            requires_access_list = True
            
        elif scenario_type == "ERC4337_HANDLE_OPS":
            target = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
            calldata = "0x1fad948c" + "0000000000000000000000000000000000000000000000000000000000000040" + "0000000000000000000000001111111111111111111111111111111111111111" + ("00" * 32)
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "UNISWAP_EXACT_INPUT":
            target = "0x3bFA4769FB09eefC5a80d6E87c3B9C650f7Ae48E"
            
            token_in = mock_env.contracts.target_erc20.replace("0x", "").zfill(64).lower() 
            token_out = "1c7D4B196Cb0C7B01d743Fbc6116a902379C7238".zfill(64).lower()      
            fee = hex(3000).replace("0x", "").zfill(64)                                    
            recipient = mock_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
            deadline = hex(int(time.time()) + 1800).replace("0x", "").zfill(64)            
            amount_in = hex(int(0.001 * 1e18)).replace("0x", "").zfill(64)                 
            amount_out_min = "0000000000000000000000000000000000000000000000000000000000000000"
            sqrt_price_limit = "0000000000000000000000000000000000000000000000000000000000000000"
            
            params_struct = token_in + token_out + fee + recipient + deadline + amount_in + amount_out_min + sqrt_price_limit
            calldata = "0x414bf389" + "0000000000000000000000000000000000000000000000000000000000000020" + params_struct
            
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "MERKLE_VERIFY":
            target = "0x6EDCE65403992e310A62460808c4b910D972f10f"
            calldata = "0xdeadbeef" + ("aa" * 32)
            storage_slots = []
            requires_access_list = False 
        else:
            raise ValueError(f"Unknown EVM scenario_type: {scenario_type}")

        if should_revert:
            calldata = "0xdeadbeef"
            
        return EvmIntent(
            target=target,
            caller=caller,
            calldata=calldata,
            value=value,
            storage_slots=storage_slots,
            requires_access_list=requires_access_list,
            scenario_type=scenario_type
        )

    @staticmethod
    def evm_state_snapshot(
        address: str, 
        is_contract: bool = False,
        balance_wei: int = int(10 * 1e18),
        should_revert: bool = False
    ) -> Dict[str, Any]:
        if is_contract:
            mock_code = "0xfd" if should_revert else "0x608060405234801561001057600080fd5b506101"
        else:
            mock_code = "0x"
        
        padded_alpha_address = "0x000000000000000000000000" + mock_env.agents.alpha.evm_address[2:]
        
        return {
            "balance": hex(balance_wei),
            "nonce": random.randint(1, 100) if not is_contract else 1,
            "code": mock_code,
            "storage": {
                "0x0": padded_alpha_address,
                "0x1": hex(int(1000 * 1e18)),
                "0x2": "0x0000000000000000000000000000000000000000000000000000000000000001"
            }
        }

    @staticmethod
    def evm_block_context() -> Dict[str, Any]:
        return {
            "timestamp": int(time.time()),
            "block_number": random.randint(19_000_000, 20_000_000),
            "coinbase": "0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5",
            "chain_id": mock_env.network.chain_id
        }