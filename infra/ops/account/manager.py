# fiber.infra.ops.account.manager
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import time
from typing import Any, Dict, Optional, Protocol

from fiber.gateway.contract.token.gen import (
    DEFAULT_EXCHANGE_RATE,
    FinOpsOpType,
    FinOpsTokenGenerator,
)
from fiber.gateway.security import TokenTranslator
from fiber.infra.ops.account.registry import DEFAULT_TENANT_ID, LocalAccountRegistry
from fiber.infra.ops.finops.watcher import default_finops_watcher
from fiber.gateway.bridge.schema import (
    AccountAllocateResult,
    AccountMandateResult,
    AccountRevokeResult,
    AccountStatusResult,
)

from xphi.arch.bound.adapter.pta import (
    NodeSigner,
    PtaAdapter,
    PtaInput,
    PtaOutput,
    PtaTransaction,
)
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.state.anchor.key import KeyGen
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("account.manager")
DEFAULT_INITIAL_FUEL = 1_000_000  # $1.00 = 1,000,000 Fuel


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class AllocateCommand:
    amount: int


@dataclass(frozen=True)
class SyncQuotaCommand:
    budget_usd: float
    op: FinOpsOpType = "SYNC_QUOTA"
    policy_ver: Optional[int] = None
    sync_ledger: bool = True


@dataclass(frozen=True)
class RevokeCommand:
    policy_ver: Optional[int] = None


class AccountManagerProtocol(Protocol):
    async def get_status(self) -> AccountStatusResult: ...
    async def allocate(self, cmd: AllocateCommand) -> AccountAllocateResult: ...
    async def sync_quota(self, cmd: SyncQuotaCommand) -> AccountMandateResult: ...
    async def revoke(self, cmd: RevokeCommand) -> AccountRevokeResult: ...


class LocalAccountManager(AccountManagerProtocol):
    def __init__(self, tenant_id: str = DEFAULT_TENANT_ID, broker_timeout: float = 5.0):
        self.tenant_id = tenant_id or DEFAULT_TENANT_ID
        self.broker = DphiBroker(timeout=broker_timeout)
        self.pta_adapter = PtaAdapter(broker=self.broker)
        self.token_gen = FinOpsTokenGenerator()
        self.signer = NodeSigner.get_instance()

    async def close(self) -> None:
        self.pta_adapter.shutdown()
        await self.broker.close()

    async def __aenter__(self) -> "LocalAccountManager":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _get_ledger_balance(self, asset_type: str = "fuel") -> int:
        return await self.pta_adapter.get_balance(self.tenant_id, asset_type=asset_type)

    async def _hydrate_watcher_if_empty(self, current_ledger_balance: int) -> None:
        saved_accounts = LocalAccountRegistry.list_accounts()
        saved_info = saved_accounts.get(self.tenant_id, {})

        if self.tenant_id not in default_finops_watcher.policy_versions:
            saved_ver = _safe_int(saved_info.get("policy_ver"), 0)
            if saved_ver > 0:
                default_finops_watcher.policy_versions[self.tenant_id] = saved_ver

        if self.tenant_id not in default_finops_watcher.local_budget_cache:
            default_finops_watcher.local_budget_cache[self.tenant_id] = float(current_ledger_balance)

    def _resolve_next_version(self, requested_ver: Optional[int], min_floor: int = 1) -> int:
        saved_accounts = LocalAccountRegistry.list_accounts()
        saved_ver = _safe_int(saved_accounts.get(self.tenant_id, {}).get("policy_ver"), 0)
        watcher_ver = default_finops_watcher.policy_versions.get(self.tenant_id, 0)
        current_ver = max(saved_ver, watcher_ver)

        if requested_ver is not None and int(requested_ver) > current_ver:
            return int(requested_ver)
        return max(current_ver + 1, min_floor)

    async def _reconcile_ledger(
        self,
        target_fuel: int,
        action_tag: str = "reconcile",
        policy_ver: Optional[int] = None,
    ) -> Optional[str]:
        current_balance = await self._get_ledger_balance("fuel")
        diff = target_fuel - current_balance

        if diff == 0:
            default_finops_watcher.local_budget_cache[self.tenant_id] = float(target_fuel)
            return None

        stream_id = KeyGen.stream_fuel(self.tenant_id)
        tx_hash: Optional[str] = None
        meta: Dict[str, Any] = {"action": action_tag, "tenant_id": self.tenant_id}
        if policy_ver is not None:
            meta["policy_ver"] = policy_ver

        if diff > 0:
            produce_tx = PtaTransaction(
                inputs=[],
                outputs=[PtaOutput(amount=diff, owner=self.tenant_id, asset_type="fuel")],
                metadata=meta,
            )
            tx_hash = await self.pta_adapter.execute_transaction(produce_tx, stream_id=stream_id)
        else:
            anchors = await self.pta_adapter.get_available_inputs(self.tenant_id, "fuel")
            if anchors:
                inputs_to_spend = []
                for u in anchors:
                    ptr = u["pointer"]
                    payload_dict = {"payload": ptr.to_key()}
                    canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
                    signature = self.signer.sign_payload(canonical_bytes)
                    inputs_to_spend.append(
                        PtaInput(pointer=ptr, signature=signature, owner_address=self.signer.pubkey_hex)
                    )

                outputs = []
                if target_fuel > 0:
                    outputs.append(PtaOutput(amount=target_fuel, owner=self.tenant_id, asset_type="fuel"))

                reconcile_tx = PtaTransaction(
                    inputs=inputs_to_spend,
                    outputs=outputs,
                    metadata=meta,
                )
                tx_hash = await self.pta_adapter.execute_transaction(reconcile_tx, stream_id=stream_id)

        start_ts = time.time()
        while time.time() - start_ts < 5.0:
            curr = await self._get_ledger_balance("fuel")
            if (diff > 0 and curr >= target_fuel) or (diff < 0 and curr <= target_fuel):
                break
            await asyncio.sleep(0.2)

        final_balance = await self._get_ledger_balance("fuel")
        default_finops_watcher.local_budget_cache[self.tenant_id] = float(final_balance)
        return tx_hash

    async def get_status(self) -> AccountStatusResult:
        ledger_balance = await self._get_ledger_balance("fuel")
        inputs = await self.pta_adapter.get_available_inputs(self.tenant_id, "fuel")

        await self._hydrate_watcher_if_empty(ledger_balance)

        default_finops_watcher.local_budget_cache[self.tenant_id] = float(ledger_balance)
        watcher_quota = float(ledger_balance)
        policy_ver = default_finops_watcher.policy_versions.get(self.tenant_id, 0)

        registry_entry = LocalAccountRegistry.record(
            tenant_id=self.tenant_id,
            ledger_balance=ledger_balance,
            active_quota=watcher_quota,
            policy_ver=policy_ver,
        )

        raw_dict = {
            "tenant_id": self.tenant_id,
            "registry_file": str(LocalAccountRegistry.get_registry_path()),
            "core_state": {
                "ledger_balance": ledger_balance,
                "active_allocations": len(inputs),
            },
            "finops_watcher": {
                "active_quota": watcher_quota,
                "policy_version": policy_ver,
            },
            "persisted_meta": {
                "last_op": registry_entry.last_op,
                "updated_at": registry_entry.updated_at,
            },
        }

        return AccountStatusResult(
            tenant_id=self.tenant_id,
            registry_file=raw_dict["registry_file"],
            raw_status=raw_dict,
            last_op=registry_entry.last_op,
            updated_at=registry_entry.updated_at,
            spiffe_id=getattr(registry_entry, "spiffe_id", None),
            ledger_balance=_safe_int(ledger_balance, 0),
            active_allocations=len(inputs),
            active_quota=_safe_int(ledger_balance, 0),
            policy_ver=_safe_int(policy_ver, 0),
        )

    async def allocate(self, cmd: AllocateCommand) -> AccountAllocateResult:
        initial_balance = await self._get_ledger_balance("fuel")
        target_balance = initial_balance + cmd.amount

        tx_hash = await self._reconcile_ledger(
            target_fuel=target_balance,
            action_tag="allocate",
        )

        LocalAccountRegistry.record(self.tenant_id, last_op=f"ALLOCATE(+{cmd.amount})")
        status = await self.get_status()
        return AccountAllocateResult(
            tx_hash=tx_hash,
            allocated_amount=cmd.amount,
            status=status,
        )

    async def sync_quota(self, cmd: SyncQuotaCommand) -> AccountMandateResult:
        current_balance = await self._get_ledger_balance("fuel")
        await self._hydrate_watcher_if_empty(current_balance)

        effective_ver = self._resolve_next_version(cmd.policy_ver, min_floor=1)

        jwt_token = self.token_gen.generate_token(
            tenant_id=self.tenant_id,
            budget_usd=cmd.budget_usd,
            op=cmd.op,
            policy_ver=effective_ver,
            exchange_rate=DEFAULT_EXCHANGE_RATE,
        )

        LocalAccountRegistry.record(
            self.tenant_id,
            policy_ver=effective_ver,
            last_op=f"MANDATE({cmd.op})",
            last_jwt=jwt_token,
        )

        ap2_mandate = await TokenTranslator.exchange(jwt_token)

        if cmd.sync_ledger and ap2_mandate:
            target_fuel = int(default_finops_watcher.local_budget_cache.get(self.tenant_id, 0.0))
            await self._reconcile_ledger(
                target_fuel=target_fuel,
                action_tag=f"jwt_{cmd.op.lower()}",
                policy_ver=effective_ver,
            )

        parsed_ticket = None
        if ap2_mandate:
            try:
                parsed_ticket = json.loads(ap2_mandate)
            except json.JSONDecodeError:
                log.warning(f"Failed to parse AP2 Mandate JSON for tenant '{self.tenant_id}'")

        status = await self.get_status()
        return AccountMandateResult(
            jwt=jwt_token,
            compute_ticket_issued=ap2_mandate is not None,
            compute_ticket=parsed_ticket,
            balance=status.ledger_balance,
            active_quota=status.ledger_balance,
            policy_ver=status.policy_ver,
            status=status,
        )

    async def revoke(self, cmd: RevokeCommand) -> AccountRevokeResult:
        current_balance = await self._get_ledger_balance("fuel")
        await self._hydrate_watcher_if_empty(current_balance)

        effective_ver = self._resolve_next_version(cmd.policy_ver, min_floor=1)

        burn_tx_hash = await self._reconcile_ledger(
            target_fuel=0,
            action_tag="revoke_burn",
            policy_ver=effective_ver,
        )

        tunnel = await TunnelFactory.get_default()
        payload = {
            "type": "update_budget",
            "tenant_id": self.tenant_id,
            "fuel_budget": 0.0,
            "finops": {
                "op": "REVOKE",
                "budget_usd": 0.0,
                "exchange_rate": DEFAULT_EXCHANGE_RATE,
                "policy_ver": effective_ver,
            },
        }
        await tunnel.publish("finops:control_plane", json.dumps(payload))
        default_finops_watcher.local_budget_cache[self.tenant_id] = 0.0
        default_finops_watcher.policy_versions[self.tenant_id] = effective_ver

        LocalAccountRegistry.record(
            self.tenant_id,
            ledger_balance=0,
            active_quota=0.0,
            policy_ver=effective_ver,
            last_op="REVOKE_ZERO",
        )

        log.info(f"[LocalAccountManager] Ledger Burned & 'REVOKE' event published for '{self.tenant_id}' (v{effective_ver}).")

        status = await self.get_status()
        return AccountRevokeResult(
            tenant_id=self.tenant_id,
            burn_tx_hash=burn_tx_hash,
            signal="REVOKE_PUBLISHED",
            status=status,
        )