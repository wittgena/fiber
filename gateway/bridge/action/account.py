# fiber.gateway.bridge.action.account
"""
@desc: LocalAccount CLI & Headless Bridge Action Utility
- Provides typed headless functions for Console UI & programmatic callers.
- Provides terminal interface for FinOps & Quota management.

@cli_usage:
  # 1. Inspect & Registry
  $ fiber account list
  $ fiber account --tenant accounting_dept status
  $ fiber account --tenant accounting_dept demo

  # 2. Direct Fuel Allocation & JWT Mandate ($1.00 = 1,000,000 Fuel)
  $ fiber account --tenant accounting_dept allocate --amount 1000000
  $ fiber account --tenant accounting_dept --spiffe fiber jwt --usd 50.0 --op SYNC_QUOTA
  $ fiber account --tenant accounting_dept jwt --usd 10.0 --op TOP_UP --ver 2

  # 3. Emergency Kill-Switch (Burn ledger to 0 & broadcast REVOKE)
  $ fiber account --tenant accounting_dept kill
"""
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Optional

from fiber.gateway.contract.token.gen import FinOpsOpType
from fiber.gateway.security import TokenTranslator
from fiber.infra.ops.account.manager import (
    DEFAULT_INITIAL_FUEL,
    AllocateCommand,
    LocalAccountManager,
    RevokeCommand,
    SyncQuotaCommand,
)
from fiber.infra.ops.account.registry import DEFAULT_TENANT_ID, LocalAccountRegistry
from fiber.gateway.bridge.action.schema import (
    AccountAllocateResult,
    AccountListResult,
    AccountMandateResult,
    AccountRevokeResult,
    AccountStatusResult,
)

def list_registered_accounts() -> AccountListResult:
    return AccountListResult(
        default_tenant=DEFAULT_TENANT_ID,
        registry_file=str(LocalAccountRegistry.get_registry_path()),
        accounts=LocalAccountRegistry.list_accounts(),
    )


async def fetch_account_status(
    tenant_id: str = DEFAULT_TENANT_ID,
    *,
    spiffe_id: Optional[str] = None,
) -> AccountStatusResult:
    if spiffe_id:
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id)

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        return await mgr.get_status()


async def allocate_account_fuel(
    tenant_id: str = DEFAULT_TENANT_ID,
    *,
    amount: int = DEFAULT_INITIAL_FUEL,
    spiffe_id: Optional[str] = None,
) -> AccountAllocateResult:
    if spiffe_id:
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id)

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        return await mgr.allocate(AllocateCommand(amount=amount))


async def issue_account_mandate(
    tenant_id: str = DEFAULT_TENANT_ID,
    *,
    budget_usd: float = 50.0,
    op: FinOpsOpType = "SYNC_QUOTA",
    policy_ver: Optional[int] = None,
    sync_ledger: bool = True,
    spiffe_id: Optional[str] = None,
) -> AccountMandateResult:
    if spiffe_id:
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id)

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        cmd = SyncQuotaCommand(
            budget_usd=budget_usd,
            op=op,
            policy_ver=policy_ver,
            sync_ledger=sync_ledger,
        )
        return await mgr.sync_quota(cmd)


async def revoke_account_mandate(
    tenant_id: str = DEFAULT_TENANT_ID,
    *,
    policy_ver: Optional[int] = None,
    spiffe_id: Optional[str] = None,
) -> AccountRevokeResult:
    if spiffe_id:
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id)

    async with LocalAccountManager(tenant_id=tenant_id) as mgr:
        return await mgr.revoke(RevokeCommand(policy_ver=policy_ver))


# =====================================================================
# 2. CLI Lifecycle Demo & Entrypoint
# =====================================================================
async def run_lifecycle_demo(tenant_id: str, spiffe_id: Optional[str] = None):
    """Validates full account lifecycle visually via console output using typed schema models."""
    reg_name = LocalAccountRegistry.get_registry_path().name
    print(f"\n=== [LocalAccount Lifecycle Demo: '{tenant_id}' (Registry: {reg_name})] ===")

    async with LocalAccountManager(tenant_id=tenant_id) as manager:
        # 1. Check initial state
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id, last_op="DEMO_START")
        st0 = await manager.get_status()
        print(f"1. Initial Status:\n   {json.dumps(st0.raw_status, indent=2)}")

        # 2. Provision initial quota via JWT (PROVISION: $50 -> 50,000 Fuel, v1)
        res_prov = await manager.sync_quota(
            SyncQuotaCommand(budget_usd=50.0, op="PROVISION", policy_ver=1)
        )
        print(f"\n2. After PROVISION ($50.0, v1):")
        print(f"   JWT (prefix): {res_prov.jwt[:40]}...")
        print(f"   Core Balance: {res_prov.balance} Fuel")
        print(f"   Active Quota: {res_prov.active_quota} Fuel")

        # 3. Top-up quota via JWT (TOP_UP: +$20 -> 70,000 Fuel, v2)
        res_topup = await manager.sync_quota(
            SyncQuotaCommand(budget_usd=20.0, op="TOP_UP", policy_ver=2)
        )
        print(f"\n3. After TOP_UP (+$20.0, v2):")
        print(f"   Core Balance: {res_topup.balance} Fuel")
        print(f"   Active Quota: {res_topup.active_quota} Fuel")
        print(f"   Policy Ver  : {res_topup.policy_ver}")

        # 4. Downsize / Sync quota via JWT (SYNC_QUOTA: $70 -> $30, 30,000 Fuel, v3)
        res_sync = await manager.sync_quota(
            SyncQuotaCommand(budget_usd=30.0, op="SYNC_QUOTA", policy_ver=3)
        )
        print(f"\n4. After SYNC_QUOTA ($30.0 Downsize, v3):")
        print(f"   Core Balance: {res_sync.balance} Fuel (Expected: 30000)")
        print(f"   Active Quota: {res_sync.active_quota} Fuel (Expected: 30000)")
        print(f"   Policy Ver  : {res_sync.policy_ver}")

        # 5. Verify stale token (v1) replay rejection
        stale_ap2 = await TokenTranslator.exchange(res_prov.jwt)
        print(f"\n5. Replay v1 Token Check -> Accepted? {stale_ap2 is not None} (Expected: False)")

        # 6. Execute revocation (REVOKE, v4)
        res_rev = await manager.revoke(RevokeCommand(policy_ver=4))
        print(f"\n6. After REVOKE (v4):")
        print(f"   Auth Ticket Issued? : False (Expected: False)")
        print(f"   Core Balance        : {res_rev.status.ledger_balance} Fuel (Expected: 0)")
        print(f"   Active Quota        : {res_rev.status.active_quota} Fuel (Expected: 0)\n")


async def main_async(args: argparse.Namespace):
    """Routes parsed CLI arguments to headless functions and prints serialized schema output."""
    if args.command == "list":
        res = list_registered_accounts()
        print(json.dumps(res.model_dump(), indent=2, ensure_ascii=False))
        return

    if args.command == "demo":
        await run_lifecycle_demo(args.tenant, args.spiffe)
        return

    result = None
    if args.command == "status":
        st = await fetch_account_status(args.tenant, spiffe_id=args.spiffe)
        # 기존 CLI 출력 포맷(raw_status 구조)과 100% 동일하게 출력하거나 스키마 덤프 출력
        print(json.dumps(st.raw_status, indent=2, ensure_ascii=False))
        return

    elif args.command == "allocate":
        result = await allocate_account_fuel(
            args.tenant, amount=args.amount, spiffe_id=args.spiffe
        )

    elif args.command == "jwt":
        result = await issue_account_mandate(
            args.tenant,
            budget_usd=args.usd,
            op=args.op,
            policy_ver=args.ver,
            spiffe_id=args.spiffe,
        )

    elif args.command == "kill":
        result = await revoke_account_mandate(
            args.tenant, policy_ver=args.ver, spiffe_id=args.spiffe
        )

    if result is not None:
        print(json.dumps(result.model_dump(), indent=2, ensure_ascii=False))


def main(args_list: Optional[list[str]] = None):
    parser = argparse.ArgumentParser(
        prog="fiber account",
        description="Fiber LocalAccount FinOps & Quota CLI",
    )
    parser.add_argument(
        "--tenant",
        type=str,
        default=DEFAULT_TENANT_ID,
        help=f"Target tenant ID (default: {DEFAULT_TENANT_ID})",
    )
    parser.add_argument(
        "--spiffe",
        type=str,
        default=None,
        help="Optional SPIFFE ID to bind with this account (e.g., 'fiber' or 'spiffe://self/fiber')",
    )

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("demo", help="Run full lifecycle walkthrough (Provision -> TopUp -> Downsize -> Revoke)")
    subparsers.add_parser("list", help="List all tracked local accounts from file registry")
    subparsers.add_parser("status", help="Check core system balance and FinOps active quota state")

    p_allocate = subparsers.add_parser("allocate", help="Directly allocate Fuel into the core system")
    p_allocate.add_argument("--amount", type=int, default=DEFAULT_INITIAL_FUEL, help="Fuel amount to allocate")

    p_jwt = subparsers.add_parser("jwt", help="Issue FinOps JWT and apply quota change to ledger & watcher")
    p_jwt.add_argument("--usd", type=float, default=50.0, help="Budget in USD")
    p_jwt.add_argument("--op", type=str, default="SYNC_QUOTA", choices=["PROVISION", "SYNC_QUOTA", "TOP_UP", "REVOKE"])
    p_jwt.add_argument("--ver", type=int, default=None, help="Policy version (auto-increments if omitted)")

    p_kill = subparsers.add_parser("kill", help="Burn ledger fuel and publish real-time REVOKE kill-switch")
    p_kill.add_argument("--ver", type=int, default=None, help="Policy version to enforce revocation (auto-increments if omitted)")

    parsed_args = parser.parse_args(args_list)
    if parsed_args.command is None:
        parsed_args.command = "demo"

    asyncio.run(main_async(parsed_args))


if __name__ == "__main__":
    main()