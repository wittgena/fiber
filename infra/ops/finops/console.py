# fiber.infra.ops.finops.console
"""
Glossary:
- fuel_budget: Fixed spending quota limit assigned via policy/Mandate (immutable until upgraded).
- ledger_balance: Real-time remaining fuel balance strictly anchored to the PTA SSOT.
- Ensure UI signals and state tracking keep these two concepts strictly decoupled.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from nicegui import ui

from fiber.gateway.bridge.action.account import (
    allocate_account_fuel,
    fetch_account_status,
    issue_account_mandate,
    revoke_account_mandate
)
from fiber.infra.ops.account.state import state, refs, save_state_to_disk
import fiber.dev.console.ui.diag as diag

USD_TO_FUEL_RATE = 1_000_000
REVOKE_OPS = frozenset({
    "REVOKE",
    "REVOKE_ZERO",
    "LEDGER_BURN_AND_REVOKE",
    "ISSUE_JWT(REVOKE)",
    "MANDATE(REVOKE)",
})

class FinOpsConsoleManager:
    def __init__(self) -> None:
        self._restore_task: asyncio.Task[None] | None = None

    @property
    def f(self) -> Any:
        return state.finops

    @property
    def s(self) -> Any:
        return state.security

    def sync_fuel_state(self, fuel_amount: int, *, update_quota: bool = False) -> float:
        f = self.f
        val = max(0, int(fuel_amount))
        f.ledger_balance = val

        if update_quota:
            f.fuel_budget = val
            if val > 0:
                f.jwt_usd = round(val / USD_TO_FUEL_RATE, 2)
        elif getattr(f, "fuel_budget", 0) <= 0 and val > 0:
            f.fuel_budget = val
        return val / USD_TO_FUEL_RATE

    def _next_policy_ver(self) -> int:
        from fiber.infra.ops.finops.watcher import default_finops_watcher

        curr_watcher_ver = default_finops_watcher.policy_versions.get(self.f.tenant_id, 0)
        return max(curr_watcher_ver, self.f.policy_ver) + 1

    def _update_watcher_cache(self, balance: float, policy_ver: int | None = None) -> None:
        from fiber.infra.ops.finops.watcher import default_finops_watcher

        default_finops_watcher.local_budget_cache[self.f.tenant_id] = float(balance)
        if policy_ver is not None:
            default_finops_watcher.policy_versions[self.f.tenant_id] = policy_ver

    def _cancel_restore_task(self) -> None:
        if self._restore_task and not self._restore_task.done():
            self._restore_task.cancel()
        self._restore_task = None

    @staticmethod
    def normalize_spiffe(raw_spiffe: str) -> str:
        raw = raw_spiffe.strip()
        return "" if not raw else (raw if raw.startswith("spiffe://") else f"spiffe://self/{raw}")

    def sync_spiffe_and_check_totp(self, emit_warning: bool = True) -> None:
        f, s = self.f, self.s
        if not f.spiffe_id or not f.spiffe_id.strip():
            f.totp_enrolled = False
            return

        norm_id = self.normalize_spiffe(f.spiffe_id)
        f.spiffe_id = norm_id
        s.admin_user = norm_id
        f.totp_enrolled = norm_id in s.enrolled_admins

        if not f.totp_enrolled and emit_warning:
            warn_msg = (
                f"⚠️ [AccountRegistry] SPIFFE '{norm_id}' bound to '{f.tenant_id}', "
                f"but TOTP is not enrolled (Enroll in Model & Setting tab)."
            )
            refs.push_cli(f"  {warn_msg}")
            refs.push_pubsub("finops:warning", f"UNENROLLED_SPIFFE tenant={f.tenant_id} spiffe={norm_id}")
            ui.notify(warn_msg, type="warning")

    async def refresh_account_status(self, silent: bool = False) -> None:
        f = self.f
        try:
            res = await fetch_account_status(f.tenant_id)
            if (
                silent
                and res.ledger_balance == 0
                and f.ledger_balance > 0
                and (res.last_op not in REVOKE_OPS)
                and (f.last_op not in REVOKE_OPS)
            ):
                alloc_res = await allocate_account_fuel(f.tenant_id, amount=f.ledger_balance)
                res = alloc_res.status

            if res.last_op:
                f.last_op = res.last_op
            if res.spiffe_id:
                f.spiffe_id = res.spiffe_id
            self.sync_spiffe_and_check_totp(emit_warning=not silent)

            should_raise_quota = res.ledger_balance > getattr(f, "fuel_budget", 0)
            bal_usd = self.sync_fuel_state(res.ledger_balance, update_quota=should_raise_quota)
            f.policy_ver = max(1, res.policy_ver)
            f.push_chart_point()
            save_state_to_disk(force=False)

            if not silent:
                quota_usd = f.fuel_budget / USD_TO_FUEL_RATE
                refs.push_cli(
                    f"$ fiber account --tenant {f.tenant_id} status\n  {json.dumps(res.raw_status, ensure_ascii=False)}"
                )
                ui.notify(
                    f"Synced '{f.tenant_id}': Balance={f.ledger_balance:,} / Quota={f.fuel_budget:,} Fuel "
                    f"(${bal_usd:,.2f} / ${quota_usd:,.2f}, v{f.policy_ver})",
                    type="info",
                )
        except Exception:
            if not silent:
                from fiber.gateway.bridge.console import run_fiber_cli
                await run_fiber_cli("account", "--tenant", f.tenant_id, "status")

    async def issue_account_jwt(self) -> None:
        self._cancel_restore_task()
        f = self.f
        spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None
        try:
            if spiffe_arg:
                self.sync_spiffe_and_check_totp(emit_warning=True)

            if f.jwt_op == "REVOKE":
                await self.revoke_account_kill()
                return

            effective_op = "SYNC_QUOTA" if f.jwt_op in ("PROVISION", "SET_BUDGET") else f.jwt_op
            next_ver = self._next_policy_ver()

            res = await issue_account_mandate(
                f.tenant_id,
                budget_usd=f.jwt_usd,
                op=effective_op,
                policy_ver=next_ver,
                sync_ledger=True,
                spiffe_id=spiffe_arg,
            )

            bal_usd = self.sync_fuel_state(res.balance, update_quota=True)
            f.policy_ver = res.policy_ver
            f.last_op = effective_op
            f.last_jwt_preview = f"{res.jwt[:42]}... (v{res.policy_ver}, {effective_op})"
            f.push_chart_point()
            save_state_to_disk(force=True)

            action_desc = "Top-Up (+Added)" if effective_op == "TOP_UP" else "Synced to Quota"
            refs.push_cli(
                f"  ✓ [EdDSA JWT | {action_desc}] {f.last_jwt_preview} -> "
                f"Balance={f.ledger_balance:,} / Quota={f.fuel_budget:,} Fuel (${bal_usd:,.2f})"
            )
            refs.push_pubsub(
                "finops:control_plane",
                f"EVENT={effective_op} tenant={f.tenant_id} balance={f.ledger_balance} quota={f.fuel_budget} v={res.policy_ver}",
            )
            ui.notify(
                f"{action_desc}: {f.ledger_balance:,} Fuel (${bal_usd:,.2f}, v{res.policy_ver})",
                type="positive",
            )
        except Exception:
            from fiber.gateway.bridge.console import run_fiber_cli

            args = ["account", "--tenant", f.tenant_id]
            if spiffe_arg:
                args.extend(["--spiffe", spiffe_arg])
            args.extend(["jwt", "--usd", str(f.jwt_usd), "--op", f.jwt_op])
            await run_fiber_cli(*args)

    async def revoke_account_kill(self) -> None:
        self._cancel_restore_task()
        f = self.f
        spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None
        preserved_quota = f.fuel_budget

        try:
            next_ver = self._next_policy_ver()
            res = await revoke_account_mandate(
                f.tenant_id,
                policy_ver=next_ver,
                spiffe_id=spiffe_arg,
            )
            burn_tx = res.burn_tx_hash
            applied_ver = res.status.policy_ver

            self.sync_fuel_state(0, update_quota=False)
            f.fuel_budget = preserved_quota
            f.policy_ver = applied_ver
            f.last_op = "REVOKE"
            # f.stream_active = False
            f.stream_status_text = "🛑 ZERO BALANCE (REVOKED)"
            f.push_chart_point()
            save_state_to_disk(force=True)

            refs.push_cli(
                f"$ fiber account --tenant {f.tenant_id} kill -> BurnTx={burn_tx} | "
                f"Balance=0 / Quota={f.fuel_budget:,} (v{f.policy_ver})"
            )
            refs.push_pubsub(
                "finops:control_plane",
                f"EVENT=REVOKE tenant={f.tenant_id} balance=0 quota={f.fuel_budget} v={f.policy_ver}",
            )
            ui.notify(f"🚨 Balance Zeroed & Revoked '{f.tenant_id}' (v{f.policy_ver})", type="negative")
        except Exception:
            from fiber.gateway.bridge.console import run_fiber_cli

            await run_fiber_cli("account", "--tenant", f.tenant_id, "kill")

    async def temporary_stream_kill(self, cooldown_sec: float = 10.0) -> None:
        self._cancel_restore_task()
        f = self.f

        saved_quota = f.fuel_budget if f.fuel_budget > 0 else f.ledger_balance
        restore_fuel = f.ledger_balance if f.ledger_balance > 0 else saved_quota

        await self.revoke_account_kill()
        f.fuel_budget = saved_quota
        if restore_fuel <= 0:
            return

        async def _cooldown_and_restore() -> None:
            try:
                total_sec = max(1, int(cooldown_sec))
                for remaining in range(total_sec, 0, -1):
                    f.fuel_budget = saved_quota
                    f.stream_status_text = (
                        f"🛑 BREAK ({remaining}s) ➔ Restoring {restore_fuel:,} / {saved_quota:,} Fuel"
                    )
                    await asyncio.sleep(1.0)

                from xphi.node.space.tunnel.factory import TunnelFactory

                next_ver = self._next_policy_ver()
                spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None

                # 단순 합산(+)인 allocate_account_fuel 대신 SYNC_QUOTA로 원장을 정확히 restore_fuel에 맞춤
                mandate_res = await issue_account_mandate(
                    f.tenant_id,
                    budget_usd=round(restore_fuel / USD_TO_FUEL_RATE, 6),
                    op="SYNC_QUOTA",
                    policy_ver=next_ver,
                    sync_ledger=True,
                    spiffe_id=spiffe_arg,
                )
                restored_balance = mandate_res.balance
                applied_ver = mandate_res.policy_ver

                # 워처의 고정 예산 한도(fuel_budget)는 saved_quota로 복원
                self._update_watcher_cache(saved_quota, policy_ver=applied_ver)
                try:
                    tunnel = await TunnelFactory.get_default()
                    if tunnel:
                        await tunnel.publish(
                            "finops:control_plane",
                            json.dumps({
                                "type": "update_budget",
                                "tenant_id": f.tenant_id,
                                "fuel_budget": float(saved_quota),
                                "finops": {
                                    "op": "SYNC_QUOTA",
                                    "fuel_budget": float(saved_quota),
                                    "budget_usd": round(saved_quota / USD_TO_FUEL_RATE, 6),
                                    "exchange_rate": USD_TO_FUEL_RATE,
                                    "policy_ver": applied_ver,
                                },
                            }),
                        )
                except Exception:
                    pass

                f.ledger_balance = int(restored_balance)
                f.fuel_budget = int(saved_quota)
                f.policy_ver = applied_ver
                f.last_op = "AUTO_RESTORE"
                f.stream_status_text = "♻️ RESTORED TO PRE-BREAK BALANCE"
                f.push_chart_point()
                save_state_to_disk(force=True)

                refs.push_cli(
                    f"  ♻️ [Auto-Restore after {total_sec}s] Balance={f.ledger_balance:,} / "
                    f"Quota={f.fuel_budget:,} Fuel (v{f.policy_ver})"
                )
                refs.push_pubsub(
                    "finops:control_plane",
                    f"EVENT=AUTO_RESTORE tenant={f.tenant_id} balance={f.ledger_balance} quota={f.fuel_budget} v={f.policy_ver}",
                )
                ui.notify(
                    f"♻️ Restored Balance: {f.ledger_balance:,} / Quota: {f.fuel_budget:,} Fuel",
                    type="positive",
                )
            except asyncio.CancelledError:
                pass
            except Exception as e:
                diag.exception("Auto-restore after temporary stream kill failed", exc=e)

        self._restore_task = asyncio.create_task(_cooldown_and_restore())

    def trigger_kill_switch(self) -> None:
        f = self.f
        f.stream_active = False
        f.stream_status_text = "🛑 SEVERED MID-FLIGHT (HTTP 402)"
        if refs.live_stream_log:
            refs.live_stream_log.push("\n🛑 [FUEL BREAKER] Stream physically severed via aclose()!")
            refs.live_stream_log.push("↩ Agent Response: 'HTTP 402: Remaining fuel balance exhausted. Halted safely.'")
        ui.notify("🚨 Kill-Switch Activated! Active stream severed.", type="negative", position="top")

finops_manager = FinOpsConsoleManager()