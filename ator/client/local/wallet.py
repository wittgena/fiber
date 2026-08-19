# eco.client.local.wallet
## @lineage: bound.client.local.wallet
import json
import hashlib
from typing import Any, Dict, Optional
import httpx

class LocalWalletClient:
    """
    Ext Edge API (receptor.edge.ext) 와 통신하는 HTTP 클라이언트.
    Agent 지갑 상태 조회, 능동적 X402 결제, WETH 래핑 및 내부 지연 정산 트리거를 지원합니다.
    """
    def __init__(self, base_url: str = "http://localhost:8000/v1/ext", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self.client = client

    async def _request(self, method: str, path: str, headers: Optional[Dict[str, str]] = None, **kwargs) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", 45.0)
        req_headers = headers or {}

        if self.client:
            response = await self.client.request(method, url, timeout=timeout, headers=req_headers, **kwargs)
            response.raise_for_status()
            return response.json()
        else:
            async with httpx.AsyncClient() as client:
                response = await client.request(method, url, timeout=timeout, headers=req_headers, **kwargs)
                response.raise_for_status()
                return response.json()

    # =====================================================================
    # 1. Ext Wallet Endpoints (/wallet)
    # =====================================================================
    async def get_wallet_info(self, use_ledger: bool = False) -> Dict[str, Any]:
        """Agent 지갑 상태 및 주소 조회"""
        # API 쿼리 파라미터에 use_ledger 추가 (true/false)
        use_ledger_str = str(use_ledger).lower()
        return await self._request("GET", f"/wallet/info?use_ledger={use_ledger_str}")

    async def process_x402_payment(
        self, 
        payee_address: str, 
        amount_usdc: str, 
        resource_id: str, 
        use_ledger: bool = False
    ) -> Dict[str, Any]:
        """X402 HTTP 결제 정산 처리 (Agent 주도의 능동 결제)"""
        payload = {
            "payee_address": payee_address,
            "amount_usdc": amount_usdc,
            "resource_id": resource_id,
            "use_ledger": use_ledger
        }
        return await self._request("POST", "/wallet/pay/x402", json=payload, timeout=60.0)

    async def process_deferred_settlement(
        self, 
        agent_address: str, 
        accrued_debt_usdc: str, 
        receipt_id: str,
        internal_auth_token: str
    ) -> Dict[str, Any]:
        """
        [Internal/Worker 용] 지연 정산: 에이전트의 L1 지갑에서 누적 부채를 강제 징수 (transferFrom).
        Ext API의 내부 인증 헤더(internal-auth)가 필요합니다.
        """
        payload = {
            "agent_address": agent_address,
            "accrued_debt_usdc": accrued_debt_usdc,
            "receipt_id": receipt_id
        }
        headers = {
            "internal-auth": internal_auth_token
        }
        return await self._request("POST", "/wallet/settle/deferred", headers=headers, json=payload, timeout=90.0)

    # =====================================================================
    # 2. Ext EVM Endpoints (/evm)
    # =====================================================================
    async def get_evm_balances(self, address: str) -> Dict[str, Any]:
        """EVM Native(ETH) 및 ERC20(WETH) 잔고 조회"""
        return await self._request("GET", f"/evm/balance?address={address}")

    async def wrap_weth(
        self, 
        caller_address: str, 
        amount_wei: int, 
        agent_alias: str = "beta"
    ) -> Dict[str, Any]:
        """ETH -> WETH 변환 (Auto-Wrap 스마트 컨트랙트 호출)"""
        payload = {
            "caller_address": caller_address, 
            "amount_wei": str(amount_wei), 
            "agent_alias": agent_alias
        }
        return await self._request("POST", "/evm/wrap", json=payload, timeout=90.0)