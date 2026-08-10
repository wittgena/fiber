# ext.client
## @lineage: receptor.ext.client
## @lineage: phase.epoch.config.client
import json
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
import httpx

class ExtWalletClient:
    def __init__(self, base_url: str = "http://localhost:8000/v1/ext", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self.client = client
        self.simulate = True

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