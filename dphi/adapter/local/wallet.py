# dphi.adapter.local.wallet
import json
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
import httpx

class LocalWalletClient:
    def __init__(self, base_url: str = "http://localhost:8000/v1/ext", client: Optional[httpx.AsyncClient] = None):
        self.base_url = base_url.rstrip("/")
        self.client = client

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
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
        return await self._request("GET", "/wallet/info")

    async def process_x402_payment(self, payee_address: str, amount_usdc: str, resource_id: str) -> Dict[str, Any]:
        payload = {
            "payee_address": payee_address,
            "amount_usdc": amount_usdc,
            "resource_id": resource_id
        }
        return await self._request("POST", "/wallet/pay/x402", json=payload, timeout=60.0)

    async def get_evm_balances(self, address: str) -> Dict[str, Any]:
        return await self._request("GET", f"/evm/balance?address={address}")

    async def wrap_weth(self, caller_address: str, amount_wei: int, agent_alias: str = "beta") -> Dict[str, Any]:
        payload = {
            "caller_address": caller_address, 
            "amount_wei": str(amount_wei), 
            "agent_alias": agent_alias
        }
        return await self._request("POST", "/evm/wrap", json=payload, timeout=90.0)