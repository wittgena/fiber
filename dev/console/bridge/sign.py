# fiber.dev.console.bridge.sign
from __future__ import annotations

import json
import time
from typing import Any
from nicegui import ui
import orjson

from fiber.gateway.bridge.action.sign import (
    enroll_admin_totp,
    init_node_keys,
    inspect_sign_status,
    revoke_admin_totp,
    verify_admin_totp,
)
from fiber.gateway.bridge.action.schema import SlackOobDispatchResult
from fiber.infra.ops.account.state import state, refs
import fiber.dev.console.ui.diag as diag
from fiber.infra.ops.finops.console import (
    FinOpsConsoleManager,
    finops_manager,
)


async def dispatch_slack_oob_payload(
    *,
    slack_channel: str,
    slack_user_map: str,
    admin_user: str,
    require_totp: bool,
    target_id: str = "postgres-prod-db",
    tool_name: str = "drop_table",
    tool_args: dict[str, Any] | None = None,
) -> SlackOobDispatchResult:
    from fiber.gateway.bridge.channel.slack import SlackHitlBridge

    args_payload = tool_args or {"table": "customer_ledgers", "cascade": True}
    handle_id = f"h_{int(time.time() * 1000) % 1000000:06d}"

    bridge = SlackHitlBridge(default_channel=slack_channel)
    bridge.register_identity_mapping(slack_user_map.strip() or "*", admin_user)

    yield_event = {
        "event": "MCP_PHASE_YIELD",
        "handle_id": handle_id,
        "target_server_id": f"{target_id}.{tool_name}",
        "agent": admin_user,
        "prompt": {
            "id": f"req_{handle_id}",
            "params": {
                "require_totp": require_totp,
                "message": f"Execute '{tool_name}' with args {json.dumps(args_payload)}",
            },
        },
    }
    blocks = bridge.build_yield_message_blocks(yield_event, channel=slack_channel)
    ts = await bridge.post_yield_notification(yield_event) if bridge.is_live_configured else None
    return SlackOobDispatchResult(
        handle_id=handle_id,
        is_live=bridge.is_live_configured,
        node_fp=bridge.node_fp,
        ts=ts,
        blocks=blocks,
    )


def build_hitl_resolution_receipt(
    *, slack_channel: str, slack_user_map: str, admin_user: str, require_totp: bool
) -> dict[str, Any]:
    from fiber.gateway.bridge.channel.slack import SlackHitlBridge

    bridge = SlackHitlBridge(default_channel=slack_channel)
    token = bridge.mint_approval_state_token("h_active", "postgres-prod-db", "req_1", require_totp=require_totp)
    return bridge.build_resolution_receipt_blocks(
        {"state_token": token, "slack_user_id": slack_user_map or "operator"},
        {"status": "RESUMED", "handle_id": "h_active", "attested_by": admin_user},
    )

class OobActionBridge:
    def __init__(self, finops: FinOpsConsoleManager) -> None:
        self._finops = finops

    @staticmethod
    def normalize_spiffe(raw_spiffe: str) -> str:
        return FinOpsConsoleManager.normalize_spiffe(raw_spiffe)

    def sync_spiffe_and_check_totp(self, emit_warning: bool = True) -> None:
        self._finops.sync_spiffe_and_check_totp(emit_warning=emit_warning)

    async def refresh_sign_status(self, silent: bool = False) -> None:
        s = state.security
        try:
            data = await inspect_sign_status()
            s.ssh_key_exists = data.ssh_fiber_ed25519.exists
            s.ssh_key_path = data.ssh_fiber_ed25519.path
            s.cipher_key_exists = data.local_cipher_key.exists
            s.cipher_key_path = data.local_cipher_key.path
            s.node_pubkey = data.node_pubkey
            s.enrolled_admins = [r.user_id for r in data.enrolled_admins]
            s.enrolled_admins_count = len(s.enrolled_admins)
            s.sign_status_json = data.model_dump_orjson(option=orjson.OPT_INDENT_2)
            self._finops.sync_spiffe_and_check_totp(emit_warning=False)

            if not silent:
                refs.push_cli(
                    f"$ fiber sign status\n  PubKey={s.node_pubkey[:16]}... | Admins={s.enrolled_admins_count}"
                )
                ui.notify("Keychain & Audit DB Status Refreshed", type="info")
        except Exception as e:
            diag.exception("Failed to refresh sign status", exc=e, silent=silent)

    async def init_node_keys(self) -> None:
        try:
            info = await init_node_keys()
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
            res = await enroll_admin_totp(s.admin_user)
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
            res = await verify_admin_totp(s.admin_user, code_override)
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
            canonical_id = await revoke_admin_totp(s.admin_user)
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
            res = await dispatch_slack_oob_payload(
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
            receipt = build_hitl_resolution_receipt(
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


oob = OobActionBridge(finops_manager)