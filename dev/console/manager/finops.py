# fiber.dev.console.manager.finops
from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from nicegui import ui

import fiber.gateway.bridge.console as console_act
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
            f.fuel_quota = val
            if val > 0:
                f.jwt_usd = round(val / USD_TO_FUEL_RATE, 2)
        elif getattr(f, "fuel_quota", 0) <= 0 and val > 0:
            f.fuel_quota = val
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

    # -------------------------------------------------------------------------
    # 2. SPIFFE ID 정규화 및 TOTP 등록 상태 동기화
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # 3. Account Mandate (Status / Issue JWT / Revoke / Cooldown Restore)
    # -------------------------------------------------------------------------
    async def refresh_account_status(self, silent: bool = False) -> None:
        f = self.f
        try:
            res = await console_act.fetch_account_status(f.tenant_id)
            if (
                silent
                and res.ledger_balance == 0
                and f.ledger_balance > 0
                and (res.last_op not in REVOKE_OPS)
                and (f.last_op not in REVOKE_OPS)
            ):
                if hasattr(console_act, "allocate_account_fuel"):
                    alloc_res = await console_act.allocate_account_fuel(f.tenant_id, amount=f.ledger_balance)
                    res = alloc_res.status
                else:
                    target_usd = round(f.ledger_balance / USD_TO_FUEL_RATE, 6)
                    await console_act.issue_account_mandate(
                        f.tenant_id,
                        budget_usd=target_usd,
                        op="SYNC_QUOTA",
                        policy_ver=max(1, f.policy_ver),
                        sync_ledger=True,
                    )
                    res = await console_act.fetch_account_status(f.tenant_id)

            if res.last_op:
                f.last_op = res.last_op
            if res.spiffe_id:
                f.spiffe_id = res.spiffe_id
            self.sync_spiffe_and_check_totp(emit_warning=not silent)

            should_raise_quota = res.ledger_balance > getattr(f, "fuel_quota", 0)
            bal_usd = self.sync_fuel_state(res.ledger_balance, update_quota=should_raise_quota)
            f.policy_ver = max(1, res.policy_ver)
            f.push_chart_point()
            save_state_to_disk(force=False)

            if not silent:
                quota_usd = f.fuel_quota / USD_TO_FUEL_RATE
                refs.push_cli(
                    f"$ fiber account --tenant {f.tenant_id} status\n  {json.dumps(res.raw_status, ensure_ascii=False)}"
                )
                ui.notify(
                    f"Synced '{f.tenant_id}': Balance={f.ledger_balance:,} / Quota={f.fuel_quota:,} Fuel "
                    f"(${bal_usd:,.2f} / ${quota_usd:,.2f}, v{f.policy_ver})",
                    type="info",
                )
        except Exception:
            if not silent:
                from fiber.gateway.bridge.cli import run_fiber_cli

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

            res = await console_act.issue_account_mandate(
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
                f"Balance={f.ledger_balance:,} / Quota={f.fuel_quota:,} Fuel (${bal_usd:,.2f})"
            )
            refs.push_pubsub(
                "finops:control_plane",
                f"EVENT={effective_op} tenant={f.tenant_id} balance={f.ledger_balance} quota={f.fuel_quota} v={res.policy_ver}",
            )
            ui.notify(
                f"{action_desc}: {f.ledger_balance:,} Fuel (${bal_usd:,.2f}, v{res.policy_ver})",
                type="positive",
            )
        except Exception:
            from fiber.gateway.bridge.cli import run_fiber_cli

            args = ["account", "--tenant", f.tenant_id]
            if spiffe_arg:
                args.extend(["--spiffe", spiffe_arg])
            args.extend(["jwt", "--usd", str(f.jwt_usd), "--op", f.jwt_op])
            await run_fiber_cli(*args)

    async def revoke_account_kill(self) -> None:
        self._cancel_restore_task()
        f = self.f
        spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None
        preserved_quota = f.fuel_quota

        try:
            next_ver = self._next_policy_ver()
            res = await console_act.revoke_account_mandate(
                f.tenant_id,
                policy_ver=next_ver,
                spiffe_id=spiffe_arg,
            )
            burn_tx = res.burn_tx_hash
            applied_ver = res.status.policy_ver

            self.sync_fuel_state(0, update_quota=False)
            f.fuel_quota = preserved_quota
            f.policy_ver = applied_ver
            f.last_op = "REVOKE"
            f.stream_active = False
            f.stream_status_text = "🛑 ZERO BALANCE (REVOKED)"
            f.push_chart_point()
            save_state_to_disk(force=True)

            refs.push_cli(
                f"$ fiber account --tenant {f.tenant_id} kill -> BurnTx={burn_tx} | "
                f"Balance=0 / Quota={f.fuel_quota:,} (v{f.policy_ver})"
            )
            refs.push_pubsub(
                "finops:control_plane",
                f"EVENT=REVOKE tenant={f.tenant_id} balance=0 quota={f.fuel_quota} v={f.policy_ver}",
            )
            ui.notify(f"🚨 Balance Zeroed & Revoked '{f.tenant_id}' (v{f.policy_ver})", type="negative")
        except Exception:
            from fiber.gateway.bridge.cli import run_fiber_cli

            await run_fiber_cli("account", "--tenant", f.tenant_id, "kill")

    async def temporary_stream_kill(self, cooldown_sec: float = 10.0) -> None:
        self._cancel_restore_task()
        f = self.f

        saved_quota = f.fuel_quota if f.fuel_quota > 0 else f.ledger_balance
        restore_fuel = f.ledger_balance if f.ledger_balance > 0 else saved_quota

        await self.revoke_account_kill()
        f.fuel_quota = saved_quota
        if restore_fuel <= 0:
            return

        async def _cooldown_and_restore() -> None:
            try:
                total_sec = max(1, int(cooldown_sec))
                for remaining in range(total_sec, 0, -1):
                    f.fuel_quota = saved_quota
                    f.stream_status_text = (
                        f"🛑 BREAK ({remaining}s) ➔ Restoring {restore_fuel:,} / {saved_quota:,} Fuel"
                    )
                    await asyncio.sleep(1.0)

                from xphi.node.space.tunnel.factory import TunnelFactory

                next_ver = self._next_policy_ver()
                spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None

                if hasattr(console_act, "allocate_account_fuel"):
                    alloc_res = await console_act.allocate_account_fuel(
                        f.tenant_id, amount=restore_fuel, spiffe_id=spiffe_arg
                    )
                    restored_balance = alloc_res.status.ledger_balance
                else:
                    from fiber.infra.ops.account.manager import AllocateCommand, LocalAccountManager

                    async with LocalAccountManager(tenant_id=f.tenant_id) as mgr:
                        alloc_raw = await mgr.allocate(AllocateCommand(amount=restore_fuel))
                    restored_balance = (
                        alloc_raw.status.ledger_balance if hasattr(alloc_raw, "status") else restore_fuel
                    )

                self._update_watcher_cache(restored_balance, policy_ver=next_ver)
                try:
                    tunnel = await TunnelFactory.get_default()
                    if tunnel:
                        await tunnel.publish(
                            "finops:control_plane",
                            json.dumps({
                                "type": "update_budget",
                                "tenant_id": f.tenant_id,
                                "fuel_budget": float(restored_balance),
                                "finops": {
                                    "op": "SYNC_QUOTA",
                                    "budget_usd": round(restored_balance / USD_TO_FUEL_RATE, 6),
                                    "exchange_rate": USD_TO_FUEL_RATE,
                                    "policy_ver": next_ver,
                                },
                            }),
                        )
                except Exception:
                    pass

                f.ledger_balance = int(restored_balance)
                f.fuel_quota = int(saved_quota)
                f.policy_ver = next_ver
                f.last_op = "AUTO_RESTORE"
                f.stream_status_text = "♻️ RESTORED TO PRE-BREAK BALANCE"
                f.push_chart_point()
                save_state_to_disk(force=True)

                refs.push_cli(
                    f"  ♻️ [Auto-Restore after {total_sec}s] Balance={f.ledger_balance:,} / "
                    f"Quota={f.fuel_quota:,} Fuel (v{f.policy_ver})"
                )
                refs.push_pubsub(
                    "finops:control_plane",
                    f"EVENT=AUTO_RESTORE tenant={f.tenant_id} balance={f.ledger_balance} quota={f.fuel_quota} v={f.policy_ver}",
                )
                ui.notify(
                    f"♻️ Restored Balance: {f.ledger_balance:,} / Quota: {f.fuel_quota:,} Fuel",
                    type="positive",
                )
            except asyncio.CancelledError:
                pass
            except Exception as e:
                diag.exception("Auto-restore after temporary stream kill failed", exc=e)

        self._restore_task = asyncio.create_task(_cooldown_and_restore())

    # -------------------------------------------------------------------------
    # 4. FuelBreaker Live Stream Simulation & Kill-Switch
    # -------------------------------------------------------------------------
    def trigger_kill_switch(self) -> None:
        f = self.f
        f.stream_active = False
        f.stream_status_text = "🛑 SEVERED MID-FLIGHT (HTTP 402)"
        if refs.live_stream_log:
            refs.live_stream_log.push("\n🛑 [FUEL BREAKER] Stream physically severed via aclose()!")
            refs.live_stream_log.push("↩ Agent Response: 'HTTP 402: Remaining fuel balance exhausted. Halted safely.'")
        ui.notify("🚨 Kill-Switch Activated! Active stream severed.", type="negative", position="top")

    async def simulate_stream(self) -> None:
        f = self.f
        if f.stream_active:
            return

        if f.ledger_balance <= 0:
            self.trigger_kill_switch()
            return

        f.stream_active = True
        f.stream_status_text = "⚡ STREAMING (2,200 chunks/s)"
        if refs.live_stream_log:
            refs.live_stream_log.clear()
            refs.live_stream_log.push("▶ [acompletion] model='llama_server/gemma-3-1b-it-Q4_K_M.gguf' stream=True")

        try:
            baseline = max(f.fuel_quota, f.ledger_balance, USD_TO_FUEL_RATE)
            burn_step = max(100_000, int(baseline * 0.15))

            for i in range(1, 10):
                if not f.stream_active:
                    break
                await asyncio.sleep(0.35)
                new_balance = max(0, f.ledger_balance - burn_step)

                self.sync_fuel_state(new_balance, update_quota=False)
                self._update_watcher_cache(f.ledger_balance)
                f.push_chart_point(time.strftime("%H:%M:%S"))

                if refs.live_stream_log:
                    refs.live_stream_log.push(
                        f"chunk[{i}]: Coalesced window (100ms)... "
                        f"Balance: {f.ledger_balance:,} / {f.fuel_quota:,} Fuel"
                    )

                if f.ledger_balance <= 0:
                    refs.push_pubsub(
                        "finops:control_plane",
                        f"FUEL_EXHAUSTED (balance=0 / quota={f.fuel_quota:,})",
                    )
                    self.trigger_kill_switch()
                    break
            else:
                f.stream_active = False
                f.stream_status_text = "✓ STREAM COMPLETE"
            save_state_to_disk(force=False)
        except Exception as e:
            f.stream_active = False
            f.stream_status_text = "⚠️ STREAM ERROR"
            diag.exception("Stream simulation crashed", exc=e)


finops_manager = FinOpsConsoleManager()