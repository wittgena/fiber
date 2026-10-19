# fiber.infra.bound.account.manager
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional, Protocol
from dataclasses import dataclass

from fiber.infra.bound.account.registry import LocalAccountRegistry, DEFAULT_TENANT_ID
from fiber.gateway.rest.token.gen import FinOpsTokenGenerator, FinOpsOpType
from fiber.gateway.rest.security import TokenTranslator
from fiber.infra.watcher.finops import default_finops_watcher

from xphi.node.wasm.broker import DphiBroker
from xphi.arch.bound.adapter.pta import (
    PtaAdapter, 
    PtaTransaction, 
    PtaOutput, 
    PtaInput, 
    NodeSigner
)
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bound.account.manager")
DEFAULT_INITIAL_FUEL = 1000

@dataclass(frozen=True)
class AllocateCommand:
    amount: int

@dataclass(frozen=True)
class SyncQuotaCommand:
    budget_usd: float
    op: FinOpsOpType
    policy_ver: int
    sync_ledger: bool

@dataclass(frozen=True)
class RevokeCommand:
    policy_ver: int

class AccountManagerProtocol(Protocol):
    async def get_status(self) -> Dict[str, Any]: ...
    async def allocate(self, cmd: AllocateCommand) -> Dict[str, Any]: ...
    async def sync_quota(self, cmd: SyncQuotaCommand) -> Dict[str, Any]: ...
    async def revoke(self, cmd: RevokeCommand) -> Dict[str, Any]: ...

class LocalAccountManager(AccountManagerProtocol):
    def __init__(self, tenant_id: str = DEFAULT_TENANT_ID, broker_timeout: float = 5.0):
        self.tenant_id = tenant_id or DEFAULT_TENANT_ID
        self.broker = DphiBroker(timeout=broker_timeout)
        self.pta_adapter = PtaAdapter(broker=self.broker)
        self.token_gen = FinOpsTokenGenerator()
        self.signer = NodeSigner.get_instance()
        
        LocalAccountRegistry.record(self.tenant_id)

    async def close(self) -> None:
        self.pta_adapter.shutdown()
        await self.broker.close()

    async def __aenter__(self) -> "LocalAccountManager":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _get_core_balance(self, asset_type: str = "fuel") -> int:
        return await self.pta_adapter.get_balance(self.tenant_id, asset_type=asset_type)

    async def get_status(self) -> Dict[str, Any]:
        core_balance = await self._get_core_balance("fuel")
        utxos = await self.pta_adapter.get_available_inputs(self.tenant_id, "fuel")
        watcher_quota = default_finops_watcher.local_budget_cache.get(self.tenant_id)
        policy_ver = default_finops_watcher.policy_versions.get(self.tenant_id, 0)

        registry_entry = LocalAccountRegistry.record(
            tenant_id=self.tenant_id,
            ledger_balance=core_balance,
            active_quota=watcher_quota,
            policy_ver=policy_ver
        )

        return {
            "tenant_id": self.tenant_id,
            "registry_file": str(LocalAccountRegistry.get_registry_path()),
            "core_state": {
                "balance": core_balance, 
                "active_allocations": len(utxos)
            },
            "finops_watcher": {
                "active_quota": watcher_quota, 
                "policy_version": policy_ver
            },
            "persisted_meta": {
                "last_op": registry_entry.last_op, 
                "updated_at": registry_entry.updated_at
            }
        }

    async def allocate(self, cmd: AllocateCommand) -> Dict[str, Any]:
        initial_balance = await self._get_core_balance()
        target_balance = initial_balance + cmd.amount

        produce_tx = PtaTransaction(
            inputs=[],
            outputs=[PtaOutput(amount=cmd.amount, owner=self.tenant_id, asset_type="fuel")],
            metadata={"action": "allocate", "tenant_id": self.tenant_id}
        )
        tx_hash = await self.pta_adapter.execute_transaction(produce_tx)

        start_ts = time.time()
        while time.time() - start_ts < 5.0:
            if await self._get_core_balance() >= target_balance:
                break
            await asyncio.sleep(0.2)

        LocalAccountRegistry.record(self.tenant_id, last_op=f"ALLOCATE(+{cmd.amount})")
        
        status = await self.get_status()
        return {"tx_hash": tx_hash, "status": status}

    async def sync_quota(self, cmd: SyncQuotaCommand) -> Dict[str, Any]:
        jwt_token = self.token_gen.generate_token(
            tenant_id=self.tenant_id,
            budget_usd=cmd.budget_usd,
            op=cmd.op,
            policy_ver=cmd.policy_ver
        )
        
        LocalAccountRegistry.record(
            self.tenant_id, 
            policy_ver=cmd.policy_ver, 
            last_op=f"ISSUE_JWT({cmd.op})", 
            last_jwt=jwt_token
        )

        ap2_mandate = await TokenTranslator.exchange(jwt_token)
        
        if cmd.sync_ledger and ap2_mandate:
            target_fuel = int(default_finops_watcher.local_budget_cache.get(self.tenant_id, 0.0))
            diff = target_fuel - await self._get_core_balance()
            if diff > 0:
                await self.allocate(AllocateCommand(amount=diff))

        parsed_ticket = None
        if ap2_mandate:
            try:
                parsed_ticket = json.loads(ap2_mandate)
            except json.JSONDecodeError:
                log.warning(f"Failed to parse AP2 Mandate JSON for tenant '{self.tenant_id}'")

        status = await self.get_status()
        return {
            "jwt": jwt_token,
            "compute_ticket_issued": ap2_mandate is not None,
            "compute_ticket": parsed_ticket,
            "status": status
        }

    async def revoke(self, cmd: RevokeCommand) -> Dict[str, Any]:
        utxos = await self.pta_adapter.get_available_inputs(self.tenant_id, "fuel")
        burn_tx_hash = None
        
        if utxos:
            inputs_to_spend = []
            for u in utxos:
                ptr = u["pointer"]
                payload_dict = {"payload": ptr.to_key()}
                canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
                signature = self.signer.sign_payload(canonical_bytes)
                
                inputs_to_spend.append(
                    PtaInput(pointer=ptr, signature=signature, owner_address=self.signer.pubkey_hex)
                )

            burn_tx = PtaTransaction(
                inputs=inputs_to_spend,
                outputs=[],
                metadata={"action": "revoke_burn", "tenant_id": self.tenant_id, "policy_ver": cmd.policy_ver}
            )
            
            burn_tx_hash = await self.pta_adapter.execute_transaction(burn_tx)
            
            start_ts = time.time()
            while time.time() - start_ts < 5.0:
                if await self._get_core_balance("fuel") <= 0:
                    break
                await asyncio.sleep(0.2)

        tunnel = await TunnelFactory.get_default()
        payload = {
            "type": "update_budget",
            "tenant_id": self.tenant_id,
            "fuel_budget": 0.0,
            "finops": {
                "op": "REVOKE", 
                "budget_usd": 0.0, 
                "exchange_rate": 1000, 
                "policy_ver": cmd.policy_ver
            }
        }
        await tunnel.publish("finops:control_plane", json.dumps(payload))
        
        default_finops_watcher.local_budget_cache[self.tenant_id] = 0.0
        
        LocalAccountRegistry.record(
            self.tenant_id, 
            active_quota=0.0, 
            policy_ver=cmd.policy_ver, 
            last_op="LEDGER_BURN_AND_REVOKE"
        )
        
        log.info(f"[LocalAccountManager] Ledger Burned & 'REVOKE' event published for '{self.tenant_id}'.")
        
        status = await self.get_status()
        return {
            "tenant_id": self.tenant_id, 
            "burn_tx_hash": burn_tx_hash,
            "signal": "REVOKE_PUBLISHED",
            "status": status
        }