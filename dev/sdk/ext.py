# fiber.dev.sdk.ext
import os
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, ValidationError

from fiber.infra.rpc.client import InternalRpcClient, RpcException

# =====================================================================
# 1. Payload Models (Strict Validation for SDK Consumers)
# =====================================================================

class WalletInfoRequest(BaseModel):
    use_ledger: bool = False

class X402PaymentPayload(BaseModel):
    payee_address: str
    amount_usdc: str
    resource_id: str
    use_ledger: bool = False

class DeferredSettlementPayload(BaseModel):
    agent_address: str
    accrued_debt_usdc: str
    receipt_id: str
    internal_auth_token: str

class EvmBalanceRequest(BaseModel):
    address: str

class EvmWrapPayload(BaseModel):
    caller_address: str
    amount_wei: str  # JSON 직렬화를 위해 내부적으로 str 변환 허용
    agent_alias: str = "beta"

# =====================================================================
# 2. ExtClient Implementation
# =====================================================================

class ExtSdkException(Exception):
    """Base exception for ExtClient SDK errors."""
    pass

class ExtClient:
    """
    SDK Client for interacting with the ExtRpcService.
    Provides strictly typed, async methods for wallet operations, EVM queries, and settlements.
    """
    def __init__(
        self, 
        rpc_queue_topic: str = "internal.rpc.queue", 
        rpc_client: Optional[InternalRpcClient] = None
    ):
        target_queue = os.getenv("RPC_QUEUE_TOPIC", rpc_queue_topic)
        self.rpc = rpc_client or InternalRpcClient(queue_name=target_queue)

    async def _safe_rpc_call(self, method: str, payload: BaseModel, timeout: float) -> Dict[str, Any]:
        """내부 RPC 호출을 수행하고, 예외를 SDK 예외로 래핑하여 안전하게 반환"""
        try:
            # Pydantic 모델을 dict로 변환 (exclude_none을 통해 불필요한 null 필드 제거)
            params = payload.model_dump(exclude_none=True)
            result = await self.rpc.call(method, params, timeout=timeout)
            
            # rpc 핸들러가 반환하는 공통 에러 포맷 {"error": True, "message": "..."} 체크
            if isinstance(result, dict) and result.get("error"):
                raise ExtSdkException(f"RPC Error [{method}]: {result.get('message', 'Unknown Error')}")
            
            return result
        except ValidationError as ve:
            raise ExtSdkException(f"Payload Validation Failed before sending [{method}]: {ve}")
        except RpcException as re:
            raise ExtSdkException(f"RPC Transport Error [{method}]: {re}")
        except Exception as e:
            raise ExtSdkException(f"Unexpected Error during RPC Call [{method}]: {e}")

    async def get_wallet_info(self, use_ledger: bool = False) -> Dict[str, Any]:
        """Agent 지갑 상태, 네트워크 ID 및 현재 모드(Simulated 등) 조회"""
        payload = WalletInfoRequest(use_ledger=use_ledger)
        return await self._safe_rpc_call("ext.wallet.info", payload, timeout=15.0)

    async def process_x402_payment(
        self, 
        payee_address: str, 
        amount_usdc: str, 
        resource_id: str, 
        use_ledger: bool = False
    ) -> Dict[str, Any]:
        """X402 규격에 맞춘 결제/정산 처리 (Instant Push 또는 DVM Ledger Deferred Charge)"""
        payload = X402PaymentPayload(
            payee_address=payee_address,
            amount_usdc=amount_usdc,
            resource_id=resource_id,
            use_ledger=use_ledger
        )
        return await self._safe_rpc_call("ext.wallet.pay.x402", payload, timeout=60.0)

    async def process_deferred_settlement(
        self, 
        agent_address: str, 
        accrued_debt_usdc: str, 
        receipt_id: str,
        internal_auth_token: str
    ) -> Dict[str, Any]:
        """L1/L2 네트워크 상에서 사전 승인된 토큰을 가져오는 Pull 기반 후불 정산(transferFrom)"""
        payload = DeferredSettlementPayload(
            agent_address=agent_address,
            accrued_debt_usdc=accrued_debt_usdc,
            receipt_id=receipt_id,
            internal_auth_token=internal_auth_token
        )
        return await self._safe_rpc_call("ext.wallet.settle.deferred", payload, timeout=90.0)

    async def get_evm_balances(self, address: str) -> Dict[str, Any]:
        """주어진 지갑 주소의 EVM Native(ETH) 및 ERC20(WETH/USDC 등 설정 기반) 잔고 조회"""
        payload = EvmBalanceRequest(address=address)
        return await self._safe_rpc_call("ext.evm.balance", payload, timeout=30.0)

    async def wrap_weth(
        self, 
        caller_address: str, 
        amount_wei: int, 
        agent_alias: str = "beta"
    ) -> Dict[str, Any]:
        """EVM 환경에서 Native 코인을 WETH 등 래핑 토큰으로 변환 (Auto-Wrap 스마트 컨트랙트 호출)"""
        payload = EvmWrapPayload(
            caller_address=caller_address, 
            amount_wei=str(amount_wei), 
            agent_alias=agent_alias
        )
        return await self._safe_rpc_call("ext.evm.wrap", payload, timeout=90.0)