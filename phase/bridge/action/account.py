# fiber.phase.bridge.action.account
"""
@desc: LocalAccount CLI Utility
- Provides terminal interface for FinOps & Quota management.
- Delegates business logic to fiber.infra.bound.account.manager.LocalAccountManager.

[CLI Command Set & Usage Examples]
fiber account
fiber account demo
fiber account list
fiber account status
fiber account --tenant tenant_client_01 --spiffe fiber allocate --amount 10000
fiber account --tenant tenant_client_01 jwt --usd 100.0 --op SYNC_QUOTA --ver 1 --sync
fiber account --tenant tenant_client_01 kill --ver 99
"""
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Optional

from fiber.infra.bound.account.registry import LocalAccountRegistry, DEFAULT_TENANT_ID
from fiber.infra.bound.account.manager import (
    LocalAccountManager,
    AllocateCommand,
    SyncQuotaCommand,
    RevokeCommand,
    DEFAULT_INITIAL_FUEL
)
from fiber.gateway.security import TokenTranslator


async def run_lifecycle_demo(tenant_id: str, spiffe_id: Optional[str] = None):
    """Validates full account lifecycle visually via console output using Command DTOs."""
    reg_name = LocalAccountRegistry.get_registry_path().name
    print(f"\n=== [LocalAccount Lifecycle Demo: '{tenant_id}' (Registry: {reg_name})] ===")
    
    async with LocalAccountManager(tenant_id=tenant_id) as manager:
        # 1. Check initial state (spiffe_id가 주어졌다면 함께 기록 및 TOTP 유무 체크)
        LocalAccountRegistry.record(tenant_id, spiffe_id=spiffe_id, last_op="DEMO_START")
        st0 = await manager.get_status()
        print(f"1. Initial Status:\n   {json.dumps(st0, indent=2)}")

        # 2. Provision initial quota via JWT (PROVISION: $50 -> 50,000 Fuel, v1)
        cmd_prov = SyncQuotaCommand(budget_usd=50.0, op="PROVISION", policy_ver=1, sync_ledger=True)
        res_prov = await manager.sync_quota(cmd_prov)
        print(f"\n2. After PROVISION ($50.0, v1):")
        print(f"   JWT (prefix): {res_prov['jwt'][:40]}...")
        print(f"   Core Balance: {res_prov['status']['core_state']['ledger_balance']} Fuel")
        print(f"   Active Quota: {res_prov['status']['finops_watcher']['active_quota']} Fuel")

        # 3. Top-up quota via JWT (TOP_UP: +$20 -> 70,000 Fuel, v2)
        cmd_topup = SyncQuotaCommand(budget_usd=20.0, op="TOP_UP", policy_ver=2, sync_ledger=True)
        res_topup = await manager.sync_quota(cmd_topup)
        print(f"\n3. After TOP_UP (+$20.0, v2):")
        print(f"   Core Balance: {res_topup['status']['core_state']['ledger_balance']} Fuel")
        print(f"   Active Quota: {res_topup['status']['finops_watcher']['active_quota']} Fuel")
        print(f"   Policy Ver  : {res_topup['status']['finops_watcher']['policy_version']}")

        # 4. Verify stale token (v1) replay rejection (Bypassing manager to test translator directly)
        stale_ap2 = await TokenTranslator.exchange(res_prov["jwt"])
        print(f"\n4. Replay v1 Token Check -> Accepted? {stale_ap2 is not None} (Expected: False)")

        # 5. Execute revocation (REVOKE, v3)
        cmd_rev = RevokeCommand(policy_ver=3)
        await manager.revoke(cmd_rev)
        
        # 상태 확인을 위해 get_status 재호출
        res_rev_status = await manager.get_status()
        print(f"\n5. After REVOKE (v3):")
        # REVOKE는 PubSub 이벤트이므로 Compute Ticket 발급과 무관함을 표현
        print(f"   Auth Ticket Issued? : False (Expected: False)") 
        print(f"   Core Balance        : {res_rev_status['core_state']['ledger_balance']} Fuel (Expected: 0)")
        print(f"   Active Quota        : {res_rev_status['finops_watcher']['active_quota']} Fuel (Expected: 0.0)\n")


async def main_async(args: argparse.Namespace):
    """Routes parsed CLI arguments to the LocalAccount manager via Command DTOs."""
    if args.command == "list":
        accounts = LocalAccountRegistry.list_accounts()
        print(json.dumps({
            "default_tenant": DEFAULT_TENANT_ID,
            "registry_file": str(LocalAccountRegistry.get_registry_path()),
            "accounts": accounts
        }, indent=2, ensure_ascii=False))
        return

    if args.command == "demo":
        await run_lifecycle_demo(args.tenant, args.spiffe)
        return

    # --spiffe가 입력된 경우 Manager 동작 전에 Registry에 매핑 기록 (TOTP 미등록 시 Warning 출력)
    if args.spiffe:
        LocalAccountRegistry.record(args.tenant, spiffe_id=args.spiffe)

    # Initialize business logic manager
    async with LocalAccountManager(tenant_id=args.tenant) as manager:
        result = None
        
        if args.command == "status":
            result = await manager.get_status()
            
        elif args.command == "allocate":
            cmd = AllocateCommand(amount=args.amount)
            result = await manager.allocate(cmd)
            
        elif args.command == "jwt":
            cmd = SyncQuotaCommand(
                budget_usd=args.usd,
                op=args.op,
                policy_ver=args.ver,
                sync_ledger=args.sync
            )
            result = await manager.sync_quota(cmd)
            
        elif args.command == "kill":
            cmd = RevokeCommand(policy_ver=args.ver)
            result = await manager.revoke(cmd)

        if result is not None:
            print(json.dumps(result, indent=2, ensure_ascii=False))


def main(args_list: Optional[list[str]] = None):
    parser = argparse.ArgumentParser(
        prog="fiber account",
        description="Fiber LocalAccount FinOps & Quota CLI"
    )
    parser.add_argument(
        "--tenant",
        type=str,
        default=DEFAULT_TENANT_ID,
        help=f"Target tenant ID (default: {DEFAULT_TENANT_ID})"
    )
    parser.add_argument(
        "--spiffe",
        type=str,
        default=None,
        help="Optional SPIFFE ID to bind with this account (e.g., 'fiber' or 'spiffe://self/fiber')"
    )

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("demo", help="Run full lifecycle walkthrough (Provision -> TopUp -> Revoke)")
    subparsers.add_parser("list", help="List all tracked local accounts from file registry")
    subparsers.add_parser("status", help="Check core system balance and FinOps active quota state")

    p_allocate = subparsers.add_parser("allocate", help="Directly allocate Fuel into the core system")
    p_allocate.add_argument("--amount", type=int, default=DEFAULT_INITIAL_FUEL, help="Fuel amount to allocate")

    p_jwt = subparsers.add_parser("jwt", help="Issue FinOps JWT and apply quota change")
    p_jwt.add_argument("--usd", type=float, default=50.0, help="Budget in USD")
    p_jwt.add_argument("--op", type=str, default="SYNC_QUOTA", choices=["PROVISION", "SYNC_QUOTA", "TOP_UP", "REVOKE"])
    p_jwt.add_argument("--ver", type=int, default=1, help="Policy version")
    p_jwt.add_argument("--sync", action="store_true", help="Also allocate missing Fuel Receipt in core state")

    p_kill = subparsers.add_parser("kill", help="Publish real-time REVOKE kill-switch via Tunnel Pub/Sub")
    p_kill.add_argument("--ver", type=int, default=99, help="Policy version to enforce revocation")

    parsed_args = parser.parse_args(args_list)
    if parsed_args.command is None:
        parsed_args.command = "demo"

    asyncio.run(main_async(parsed_args))


if __name__ == "__main__":
    main()