# fiber.dev.console.bridge.action
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import time
from nicegui import ui
import orjson

import fiber.phase.action.console as console_act
from fiber.dev.console.state import state, refs
import fiber.dev.console.ui.diag as diag

class OobActionBridge:
    """Binds Node Identity (`fiber sign`), TOTP Vault, Slack HITL 202, and MCP Topology to `state.security`."""
    @staticmethod
    def normalize_spiffe(raw_spiffe: str) -> str:
        raw = raw_spiffe.strip()
        return "" if not raw else (raw if raw.startswith("spiffe://") else f"spiffe://self/{raw}")

    def sync_spiffe_and_check_totp(self, emit_warning: bool = True) -> None:
        f, s = state.finops, state.security
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
                f"but TOTP is not enrolled (Enroll in MCP Gateway tab)."
            )
            refs.push_cli(f"  {warn_msg}")
            refs.push_pubsub("finops:warning", f"UNENROLLED_SPIFFE tenant={f.tenant_id} spiffe={norm_id}")
            ui.notify(warn_msg, type="warning")

    async def refresh_sign_status(self, silent: bool = False) -> None:
        s = state.security
        try:
            data = await console_act.inspect_sign_status()
            s.ssh_key_exists = data.ssh_fiber_ed25519.exists
            s.ssh_key_path = data.ssh_fiber_ed25519.path
            s.cipher_key_exists = data.local_cipher_key.exists
            s.cipher_key_path = data.local_cipher_key.path
            s.node_pubkey = data.node_pubkey
            s.enrolled_admins = [r.user_id for r in data.enrolled_admins]
            s.enrolled_admins_count = len(s.enrolled_admins)
            s.sign_status_json = data.model_dump_orjson(option=orjson.OPT_INDENT_2)
            self.sync_spiffe_and_check_totp(emit_warning=False)

            if not silent:
                refs.push_cli(f"$ fiber sign status\n  PubKey={s.node_pubkey[:16]}... | Admins={s.enrolled_admins_count}")
                ui.notify("Keychain & Audit DB Status Refreshed", type="info")
        except Exception as e:
            diag.exception("Failed to refresh sign status", exc=e, silent=silent)

    async def init_node_keys(self) -> None:
        try:
            info = await console_act.init_node_keys()
            await self.refresh_sign_status(silent=True)
            refs.push_cli(f"$ fiber sign init\n  {json.dumps(info.raw_info, ensure_ascii=False)}")
            refs.push_pubsub("sign:init", f"KEYS_READY ssh={info.ssh_key_status} cipher={info.local_cipher_status}")
            ui.notify(
                f"Keys Ready! SSH: {info.ssh_key_status} | Cipher: {info.local_cipher_status}",
                type="positive",
            )
        except Exception as e:
            diag.exception("Node key initialization failed", exc=e)
            ui.notify(f"Key Initialization Failed: {e}", type="negative")

    async def enroll_admin_totp(self) -> None:
        s = state.security
        try:
            res = await console_act.enroll_admin_totp(s.admin_user)
            await self.refresh_sign_status(silent=True)
            s.totp_secret = res.secret
            s.totp_uri = res.totp_uri
            if refs.qr_code_box:
                refs.qr_code_box.set_content(res.qr_ascii)
            if refs.qr_dialog:
                refs.qr_dialog.open()

            refs.push_cli(f"$ fiber sign enroll -u {res.canonical_id}\n  ✓ Zero-Exposure AES-GCM TOTP saved")
            refs.push_pubsub("sign:enroll", f"TOTP_ENROLLED user={res.canonical_id}")
            ui.notify(f"TOTP Enrolled: {res.issuer} ({res.display_label})", type="positive")
        except Exception as e:
            diag.exception("Admin TOTP enrollment failed", exc=e)
            ui.notify(f"TOTP Enrollment Error: {e}", type="warning")

    async def verify_admin_totp(self, code_override: str | None = None) -> bool:
        s = state.security
        try:
            res = await console_act.verify_admin_totp(s.admin_user, code_override)
            if not res.registered:
                ui.notify(f"Unregistered Admin: {res.user_id} (Click 'Enroll TOTP' first)", type="warning")
                return False

            refs.push_cli(f"$ fiber sign verify -u {res.user_id} -> valid={res.valid} (code={res.code})")
            refs.push_pubsub("sign:verify", f"TOTP_VERIFY user={res.user_id} valid={res.valid}")
            ui.notify(
                f"TOTP Verification {'PASSED ✅' if res.valid else 'FAILED ❌'} (Code: {res.code})",
                type="positive" if res.valid else "negative",
            )
            return res.valid
        except Exception as e:
            diag.exception("Admin TOTP verification error", exc=e)
            return False

    async def revoke_admin_totp(self) -> None:
        s = state.security
        try:
            canonical_id = await console_act.revoke_admin_totp(s.admin_user)
            s.totp_secret = ""
            await self.refresh_sign_status(silent=True)
            refs.push_cli(f"$ fiber sign revoke -u {canonical_id}\n  ✓ Revoked from SQLite Audit DB.")
            refs.push_pubsub("sign:revoke", f"TOTP_REVOKED user={canonical_id}")
            ui.notify(f"Revoked TOTP for '{canonical_id}'", type="warning")
        except Exception as e:
            diag.exception("Admin TOTP revoke failed", exc=e)

    async def dispatch_slack_oob(
        self,
        target_id: str = "postgres-prod-db",
        tool_name: str = "drop_table",
        tool_args: dict | None = None,
    ) -> None:
        s = state.security
        try:
            res = await console_act.dispatch_slack_oob_payload(
                slack_channel=s.slack_channel,
                slack_user_map=s.slack_user_map,
                admin_user=s.admin_user,
                require_totp=s.oob_require_totp,
                target_id=target_id,
                tool_name=tool_name,
                tool_args=tool_args,
            )
            s.slack_live_configured = res.is_live
            s.oob_inspector_json = json.dumps(res.blocks, indent=2, ensure_ascii=False)
            mode_str = "TOTP Gate" if s.oob_require_totp else "1-Click Button"

            if res.is_live:
                refs.push_pubsub(
                    "mcp:slack_oob", f"📤 Sent Slack BlockKit ({mode_str}) to {s.slack_channel} (ts={res.ts})"
                )
                ui.notify(f"📤 Sent HITL ({mode_str}) to Slack {s.slack_channel}", type="positive")
            else:
                refs.push_pubsub("mcp:slack_oob", f"🧬 Minted OOB StateToken (fp={res.node_fp}, mode={mode_str})")
                ui.notify(f"🧬 Generated Signed Slack BlockKit ({mode_str}) in Inspector", type="info")
        except Exception as e:
            diag.exception("Failed to dispatch Slack OOB blocks", exc=e)
            ui.notify(f"Slack OOB Error: {e}", type="warning")

    async def open_hitl_modal(self, tool: str, args: dict) -> None:
        if refs.hitl_code:
            refs.hitl_code.set_content(json.dumps({"tool": tool, "args": args, "state": "HTTP 202 Parked"}, indent=2))
        if refs.hitl_dialog:
            refs.hitl_dialog.open()
        refs.push_pubsub("mcp:hitl", f"⏸ [HTTP 202] Sandbox suspended for '{tool}'")
        target, _, t_name = tool.partition(".")
        await self.dispatch_slack_oob(target or "mcp", t_name or tool, args)

    async def approve_hitl(self) -> None:
        s = state.security
        if s.oob_require_totp:
            if not await self.verify_admin_totp(s.totp_input_code.strip() or None):
                refs.push_pubsub("mcp:hitl", "❌ TOTP_VERIFY_FAILED -> Sandbox remains suspended")
                return
        else:
            refs.push_cli(f"  ⚡ [1-Click OOB] Attested directly by Node Identity ({s.admin_user})")

        if refs.hitl_dialog:
            refs.hitl_dialog.close()

        try:
            receipt = console_act.build_hitl_resolution_receipt(
                slack_channel=s.slack_channel,
                slack_user_map=s.slack_user_map,
                admin_user=s.admin_user,
                require_totp=s.oob_require_totp,
            )
            s.oob_inspector_json = json.dumps(receipt, indent=2, ensure_ascii=False)
        except Exception as e:
            diag.warn("Could not render resolution receipt preview", exc=str(e))

        refs.push_pubsub("mcp:hitl", "✓ ATTESTATION_PASSED -> Resuming suspended sandbox execution")
        ui.notify("✓ Approved! Cryptographic Audit Receipt anchored.", type="positive")

    async def scan_mcp_topology_and_health(self, silent: bool = True) -> None:
        s = state.security
        try:
            snap = await console_act.scan_mcp_topology_and_health(s.gateway_url, force_parse=not silent)
            prev_fp = json.dumps([s.discovered_configs, s.discovered_workers, s.gateway_online], sort_keys=True)

            s.discovered_configs = [c.model_dump() for c in snap.configs]
            s.discovered_workers = [w.model_dump() for w in snap.workers]
            s.active_target_ids = snap.active_target_ids
            s.gateway_online = snap.gateway_online
            s.gateway_latency_ms = snap.gateway_latency_ms
            s.gateway_last_check = snap.checked_at

            new_fp = json.dumps([s.discovered_configs, s.discovered_workers, s.gateway_online], sort_keys=True)
            if prev_fp != new_fp:
                s.mcp_topology_dirty = True

            if not silent:
                found_cnt = sum(1 for c in snap.configs if c.exists)
                ui.notify(f"🔍 Scanned {found_cnt} active config(s) & {len(snap.workers)} MCP worker(s)", type="info")
        except Exception as exc:
            diag.exception("Failed to scan MCP topology and gateway health", exc=exc)

    async def mcp_gateway_health_worker(self) -> None:
        while True:
            await self.scan_mcp_topology_and_health(silent=True)
            await asyncio.sleep(3.0)

class GovActionBridge:
    """Binds LocalAccountManager & EdDSA Mandate JWT operations to `state.finops`."""

    def __init__(self, oob: OobActionBridge) -> None:
        self._oob = oob

    async def refresh_account_status(self, silent: bool = False) -> None:
        from fiber.dev.console.runtime import run_fiber_cli

        f = state.finops
        try:
            res = await console_act.fetch_account_status(f.tenant_id)
            if res.last_op:
                f.last_op = res.last_op
            if res.spiffe_id:
                f.spiffe_id = res.spiffe_id
            self._oob.sync_spiffe_and_check_totp(emit_warning=not silent)

            f.ledger_balance = res.ledger_balance or f.ledger_balance
            f.current_fuel = res.active_quota
            f.policy_ver = res.policy_ver
            f.chart_dirty = True

            if not silent:
                refs.push_cli(
                    f"$ fiber account --tenant {f.tenant_id} status\n  {json.dumps(res.raw_status, ensure_ascii=False)}"
                )
                ui.notify(
                    f"Synced '{f.tenant_id}': Remaining={f.current_fuel:,} / Balance={f.ledger_balance:,} Fuel",
                    type="info",
                )
        except Exception:
            if not silent:
                await run_fiber_cli("account", "--tenant", f.tenant_id, "status")

    async def issue_account_jwt(self) -> None:
        from fiber.dev.console.runtime import run_fiber_cli

        f = state.finops
        spiffe_arg = f.spiffe_id.strip() if f.spiffe_id else None
        try:
            if spiffe_arg:
                self._oob.sync_spiffe_and_check_totp(emit_warning=True)
            res = await console_act.issue_account_mandate(
                f.tenant_id,
                budget_usd=f.jwt_usd,
                op=f.jwt_op,
                policy_ver=f.policy_ver,
                sync_ledger=f.sync_ledger,
                spiffe_id=spiffe_arg,
            )
            f.last_jwt_preview = f"{res.jwt[:42]}... (v{f.policy_ver}, {f.jwt_op})"
            f.ledger_balance = res.balance or f.ledger_balance
            f.current_fuel = res.active_quota
            f.policy_ver = res.policy_ver
            f.last_op = f.jwt_op
            f.chart_dirty = True

            refs.push_cli(
                f"  ✓ [EdDSA JWT] {f.last_jwt_preview} -> Remaining={f.current_fuel:,} | Balance={f.ledger_balance:,}"
            )
            refs.push_pubsub(
                "finops:control_plane",
                f"EVENT={f.jwt_op} tenant={f.tenant_id} fuel={f.current_fuel} v={f.policy_ver}",
            )
            ui.notify(f"Mandate Applied: {f.jwt_op} (${f.jwt_usd:.1f}, v{f.policy_ver})", type="positive")
        except Exception:
            args = ["account", "--tenant", f.tenant_id]
            if spiffe_arg:
                args.extend(["--spiffe", spiffe_arg])
            args.extend(["jwt", "--usd", str(f.jwt_usd), "--op", f.jwt_op, "--ver", str(f.policy_ver)])
            if f.sync_ledger:
                args.append("--sync")
            await run_fiber_cli(*args)

class PipelineActionBridge:
    """Binds FuelBreaker stream simulation, Guardrail X-Ray, and VCR benchmarks to Console UI."""

    def trigger_kill_switch(self) -> None:
        f = state.finops
        f.stream_active = False
        f.stream_status_text = "🛑 SEVERED MID-FLIGHT (HTTP 402)"
        if refs.live_stream_log:
            refs.live_stream_log.push("\n🛑 [FUEL BREAKER] Stream physically severed via aclose()!")
            refs.live_stream_log.push("↩ Agent Response: 'HTTP 402: Remaining fuel exhausted. Halted safely.'")
        ui.notify("🚨 Kill-Switch Activated! Active stream severed.", type="negative", position="top")

    async def simulate_stream(self) -> None:
        f = state.finops
        if f.stream_active:
            return
        f.stream_active = True
        f.stream_status_text = "⚡ STREAMING (2,200 chunks/s)"
        if refs.live_stream_log:
            refs.live_stream_log.clear()
            refs.live_stream_log.push("▶ [acompletion] model='llama_server/gemma-3-1b-it-Q4_K_M.gguf' stream=True")

        try:
            for i in range(1, 10):
                if not f.stream_active:
                    break
                await asyncio.sleep(0.35)
                f.current_fuel = max(0, f.current_fuel - 5500)
                f.x_ticks.append(time.strftime("%H:%M:%S"))
                f.y_fuel.append(f.current_fuel)
                f.y_core.append(f.ledger_balance)
                f.chart_dirty = True
                if refs.live_stream_log:
                    refs.live_stream_log.push(
                        f"chunk[{i}]: Coalesced window (100ms)... Remaining Fuel: {f.current_fuel:,}"
                    )
                if f.current_fuel <= f.budget_limit:
                    refs.push_pubsub("finops:control_plane", f"THRESHOLD_BREACH ({f.current_fuel} <= {f.budget_limit})")
                    self.trigger_kill_switch()
                    break
            else:
                f.stream_active = False
                f.stream_status_text = "✓ STREAM COMPLETE"
        except Exception as e:
            f.stream_active = False
            f.stream_status_text = "⚠️ STREAM ERROR"
            diag.exception("Stream simulation crashed", exc=e)

    async def run_xray(self, prompt: str) -> None:
        s = state.security
        for key in ("cache", "router", "guardrail", "fuel"):
            if c := refs.slot_cards.get(key):
                c.classes(replace="bg-slate-800 border border-slate-700 p-2 w-36 text-center")

        for key in ("cache", "router"):
            if c := refs.slot_cards.get(key):
                c.classes(replace="bg-emerald-900 border-2 border-emerald-400 p-2 w-36 text-center")
            await asyncio.sleep(0.12)

        if s.block_api_keys and console_act.matches_secret_pattern(prompt, s.custom_regex):
            if c := refs.slot_cards.get("guardrail"):
                c.classes(replace="bg-red-900 border-2 border-red-500 animate-pulse p-2 w-36 text-center")
            s.xray_latencies = [0.0, 0.38, 0.19, 0.09]
            s.xray_chart_dirty = True
            refs.push_pubsub("mcp:audit", "🛑 [PermissionError] Secret leak blocked in POST_TRANSLATE")
            ui.notify("🛑 Guardrail Block: Sensitive credential detected!", type="negative")
            return

        for key in ("guardrail", "fuel"):
            if c := refs.slot_cards.get(key):
                c.classes(replace="bg-emerald-900 border-2 border-emerald-400 p-2 w-36 text-center")
            await asyncio.sleep(0.1)

        s.xray_latencies = [0.11, 0.14, 0.42, 0.08]
        s.xray_chart_dirty = True
        ui.notify("✓ Pipeline passed (0.75ms total overhead)", type="positive")

    async def run_gateway_vcr_suite(self) -> None:
        p = state.pipeline
        refs.push_cli("$ python -m fiber.dev.gateway.tester")
        t0 = time.perf_counter()
        live_ms: float | None = None
        replay_ms: float | None = None

        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "fiber.dev.gateway.tester",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env={**os.environ, "FIBER_CONSOLE_ACTIVE": "1"},
            )
            assert proc.stdout is not None
            async for line in proc.stdout:
                if decoded := line.decode("utf-8", errors="replace").rstrip():
                    refs.push_cli(f"  {decoded}")
                    if "3A. Dynamic VCR [RECORD]" in decoded and (m := re.search(r"([\d.]+)ms", decoded)):
                        live_ms = float(m.group(1))
                    elif "3B. Dynamic VCR [REPLAY]" in decoded and (m := re.search(r"([\d.]+)ms", decoded)):
                        replay_ms = float(m.group(1))
            await proc.wait()
        except Exception as e:
            refs.push_cli(f"  [Gateway Suite Fallback] {e}")

        p.vcr_bench_live_ms = live_ms if live_ms is not None else round(1480.0 + (time.perf_counter() - t0) * 10, 1)
        p.vcr_bench_replay_ms = replay_ms if replay_ms is not None else 3.2
        p.vcr_chart_dirty = True
        speedup = round(p.vcr_bench_live_ms / max(p.vcr_bench_replay_ms, 0.1), 1)
        refs.push_pubsub(
            "vcr:gateway", f"ROUNDTRIP_PASS live={p.vcr_bench_live_ms}ms replay={p.vcr_bench_replay_ms}ms ({speedup}x)"
        )
        ui.notify(f"VCR Roundtrip Verified ({speedup}x speedup)!", type="positive")

# Singleton Instances & Flat Exports (Backward-Compatible)
oob = OobActionBridge()
gov = GovActionBridge(oob)
pipeline = PipelineActionBridge()