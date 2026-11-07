# fiber.dev.tester.channel.slack
"""
Slack App Manifest & Socket Mode E2E Tester.

Quick Flow:
  1. Create Slack App via `build_slack_manifest_url()` (Socket Mode & scopes pre-configured).
  2. Generate App-Level Token (`xapp-...`) with `connections:write` under Basic Information.
  3. Install App to Workspace and copy Bot User OAuth Token (`xoxb-...`).

Usage Examples:
  # Print Slack App Manifest setup guide & open creation link in browser
  $ python -m fiber.dev.tester.channel.slack guide --open

  # Print raw Slack App Manifest JSON
  $ python -m fiber.dev.tester.channel.slack json

  # Run Slack HITL E2E test (1-Click or --totp)
  $ python -m fiber.dev.tester.channel.slack test --bot-token "xoxb-..." --app-token "xapp-..."
  $ python -m fiber.dev.tester.channel.slack test --bot-token "xoxb-..." --app-token "xapp-..." --totp
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import urllib.parse
import webbrowser
from typing import Any, Dict, Optional

import httpx

from fiber.gateway.bridge.channel.slack import SlackHitlBridge
from fiber.infra.ops.account.state import load_or_init_state, state

SLACK_MANIFEST_DICT: Dict[str, Any] = {
    "display_information": {
        "name": "Fiber HITL Guard",
        "description": "Zero-Trust Out-of-Band Approval Bridge for Fiber MCP Gateway",
        "background_color": "#0f172a",
    },
    "features": {
        "bot_user": {
            "display_name": "Fiber Guard",
            "always_online": True,
        }
    },
    "oauth_config": {
        "scopes": {
            "bot": [
                "chat:write",
                "chat:write.public",
                "channels:read",
                "channels:join",
            ]
        }
    },
    "settings": {
        "interactivity": {"is_enabled": True},
        "org_deploy_enabled": False,
        "socket_mode_enabled": True,
        "token_rotation_enabled": False,
    },
}


def get_local_spiffe_id() -> str:
    try:
        load_or_init_state()
        if state.security.admin_user:
            return state.security.admin_user.strip()
    except Exception:
        pass
    return "spiffe://self/fiber"


def build_slack_manifest_url() -> str:
    manifest_json = json.dumps(SLACK_MANIFEST_DICT, separators=(",", ":"))
    encoded = urllib.parse.quote(manifest_json, safe="")
    return f"https://api.slack.com/apps?new_app=1&manifest_json={encoded}"

def build_slack_onboarding_md(
    manifest_url: Optional[str] = None,
    spiffe_id: Optional[str] = None,
    *,
    compact: bool = False,
) -> str:
    """Reusable Markdown guide for both CLI output and Console GuideModal."""
    url = manifest_url or build_slack_manifest_url()
    spiffe = spiffe_id or get_local_spiffe_id()

    if compact:
        # Console UI용: 거대한 ### 헤딩 제거 & 650자 Raw URL을 깔끔한 링크로 숨겨 가로 깨짐 원천 차단
        return (
            f"**1. Create App:** Click **[1-Click Create Slack App]({url})** above, select your workspace, and click **Create**.\n\n"
            "**2. App-Level Token (`xapp-...`):** In **Basic Information** → **App-Level Tokens**, click **Generate Token and Scopes**, add `connections:write`, and copy `xapp-1-...`.\n\n"
            "**3. Bot Token (`xoxb-...`):** In **Install App** (left sidebar), click **Install to Workspace** and copy `xoxb-...`.\n\n"
            f"*Default Attester SPIFFE ID: `{spiffe}`*"
        )

    # CLI 터미널 출력용 (전체 URL 표시)
    return (
        "[Fiber HITL] Slack App Manifest 30s Setup Guide\n\n"
        "Step 1. Create App from Manifest\n"
        f"  Open URL -> Select Workspace -> Click [Create]:\n  {url}\n\n"
        "Step 2. Generate App-Level Token (xapp-...)\n"
        "  Basic Information -> App-Level Tokens -> [Generate Token and Scopes]\n"
        "  -> Add scope 'connections:write' -> Copy 'xapp-1-...'\n\n"
        "Step 3. Install App to Workspace (xoxb-...)\n"
        "  Left Sidebar 'Install App' -> [Install to Workspace] -> Copy 'xoxb-...'\n\n"
        f"Note: Default SPIFFE identity is bound to '{spiffe}'."
    )

def print_onboarding_checklist(open_browser: bool = False) -> None:
    url = build_slack_manifest_url()
    print("=" * 76)
    print(build_slack_onboarding_md(manifest_url=url))
    print("=" * 76)
    if open_browser:
        webbrowser.open(url)

def validate_slack_tokens(bot_token: str, app_token: str) -> tuple[str, str]:
    b_tok, a_tok = bot_token.strip(), app_token.strip()
    if b_tok.startswith("xapp-") and a_tok.startswith("xoxb-"):
        print("[!] Swapped tokens detected (xapp <-> xoxb). Auto-correcting...")
        b_tok, a_tok = a_tok, b_tok

    if not b_tok.startswith("xoxb-"):
        raise ValueError(
            f"Invalid Bot Token prefix ('{b_tok[:8]}...').\n"
            "  -> Go to 'Install App' -> [Install to Workspace] and copy the 'xoxb-...' token."
        )
    if not a_tok.startswith("xapp-"):
        raise ValueError(
            f"Invalid App-Level Token prefix ('{a_tok[:8]}...').\n"
            "  -> Go to 'Basic Information' -> 'App-Level Tokens', add 'connections:write', and copy 'xapp-...'."
        )
    return b_tok, a_tok


async def discover_slack_channel(bot_token: str) -> Optional[str]:
    headers = {"Authorization": f"Bearer {bot_token}"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        auth_res = (await client.post("https://slack.com/api/auth.test", headers=headers)).json()
        if not auth_res.get("ok"):
            print(f"[!] Slack auth.test failed: {auth_res.get('error')}")
            return None
        print(f"[✓] Slack Bot Authenticated: @{auth_res.get('user')} (Workspace: {auth_res.get('team')})")

        list_res = (
            await client.get(
                "https://slack.com/api/conversations.list",
                headers=headers,
                params={"types": "public_channel", "exclude_archived": "true", "limit": 20},
            )
        ).json()

        channels = list_res.get("channels", [])
        if not channels:
            return input("Enter Slack Channel ID manually (e.g. C01234567): ").strip()

        print("\n--- Available Public Slack Channels ---")
        for idx, ch in enumerate(channels, 1):
            member_tag = " (Joined)" if ch.get("is_member") else ""
            print(f"  [{idx}] #{ch['name']} -> {ch['id']}{member_tag}")

        choice = input(f"Select channel [1-{len(channels)}] or paste Channel ID (Enter=1): ").strip()
        if not choice:
            selected = channels[0]
        elif choice.isdigit() and 1 <= int(choice) <= len(channels):
            selected = channels[int(choice) - 1]
        else:
            return choice

        ch_id = selected["id"]
        if not selected.get("is_member"):
            await client.post("https://slack.com/api/conversations.join", headers=headers, json={"channel": ch_id})
        return ch_id


class LocalTOTPVerifiedMockRpc:
    def __init__(self, require_totp: bool) -> None:
        self.require_totp = require_totp
        self.resolved_event = asyncio.Event()
        self.last_payload: Dict[str, Any] = {}

    async def publish_intent(self, channel: str, payload: Dict[str, Any]) -> None:
        self.last_payload = payload
        print(f"\n[MockRPC] Captured Intent on '{channel}':")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

        if payload.get("action") == "RESUME" and self.require_totp:
            result = payload.get("payload", {}).get("result", {})
            otp_code = str(result.get("value", "")).strip()
            spiffe_id = str(result.get("attested_user_id", ""))
            try:
                from fiber.gateway.bridge.action.sign import verify_admin_totp

                vres = await verify_admin_totp(spiffe_id, otp_code)
                if not vres.registered:
                    print(f"[TOTP Check] Unregistered SPIFFE ID '{spiffe_id}' (Enroll TOTP in console first).")
                elif vres.valid:
                    print(f"[TOTP Check] Local RFC-6238 TOTP verified! (User: {spiffe_id}, Code: {otp_code})")
                else:
                    print(f"[TOTP Check] Invalid TOTP code! (Input: {otp_code})")
            except Exception as exc:
                print(f"[TOTP Check] Skipped local TOTP check ({exc})")

        self.resolved_event.set()


async def run_slack_hitl_test(
    bot_token: str,
    app_token: str,
    channel_id: str = "",
    spiffe_id: str = "",
    require_totp: bool = False,
    timeout_sec: int = 120,
) -> bool:
    b_tok, a_tok = validate_slack_tokens(bot_token, app_token)
    effective_spiffe = spiffe_id.strip() or get_local_spiffe_id()
    target_channel = channel_id.strip() or await discover_slack_channel(b_tok)
    if not target_channel:
        print("[!] Channel ID is required.")
        return False

    rpc = LocalTOTPVerifiedMockRpc(require_totp=require_totp)
    bridge = SlackHitlBridge(
        rpc=rpc,  # type: ignore[arg-type]
        bot_token=b_tok,
        app_token=a_tok,
        default_channel=target_channel,
    )
    bridge.register_identity_mapping("*", effective_spiffe)

    handle_id = f"test_slk_{int(time.time()) % 10000}"
    yield_event = {
        "event": "MCP_PHASE_YIELD",
        "handle_id": handle_id,
        "target_server_id": "postgres-sandbox.drop_table",
        "agent": effective_spiffe,
        "prompt": {
            "id": f"prompt_{handle_id}",
            "params": {
                "require_totp": require_totp,
                "message": "Simulated DROP TABLE customer_ledgers CASCADE",
            },
        },
    }

    bridge.running = True
    daemon_task = asyncio.create_task(bridge.run_socket_mode_daemon())
    await asyncio.sleep(1.2)

    mode_str = "TOTP (6-Digit)" if require_totp else "1-Click Button"
    print(f"\n[SlackTester] Posting HITL card (Mode: {mode_str}, SPIFFE: {effective_spiffe}) -> {target_channel}")
    ts = await bridge.post_yield_notification(yield_event)
    if not ts:
        print("[!] Failed to send Slack message. Check Bot Token & scopes.")
        bridge.running = False
        daemon_task.cancel()
        return False

    print(f"[SlackTester] Waiting up to {timeout_sec}s for Approve/Deny click in Slack...")
    try:
        await asyncio.wait_for(rpc.resolved_event.wait(), timeout=timeout_sec)
        await asyncio.sleep(1.0)
        print("[SlackTester] E2E Slack HITL round-trip succeeded!")
        return True
    except asyncio.TimeoutError:
        print("[SlackTester] Timed out waiting for button interaction.")
        return False
    finally:
        bridge.running = False
        daemon_task.cancel()
        await asyncio.gather(daemon_task, return_exceptions=True)


async def _async_main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m fiber.dev.tester.channel.slack",
        description="Isolated Slack App-Manifest & Auto-Discovery HITL Tester",
    )
    parser.add_argument(
        "mode",
        nargs="?",
        default="guide",
        choices=["guide", "json", "test", "slack"],
        help="'guide': print setup checklist & URL | 'json': print Manifest JSON | 'test': run E2E test",
    )
    parser.add_argument("--bot-token", default="", help="Slack Bot Token (xoxb-...) or FIBER_TEST_SLACK_BOT_TOKEN")
    parser.add_argument("--app-token", default="", help="Slack App-Level Token (xapp-...) or FIBER_TEST_SLACK_APP_TOKEN")
    parser.add_argument("--channel", default="", help="Slack Channel ID (auto-discovers channels if omitted)")
    parser.add_argument("--spiffe", default="", help="Mapped SPIFFE ID (defaults to local state.security.admin_user)")
    parser.add_argument("--totp", action="store_true", help="Test 6-digit TOTP mode (default: 1-Click button mode)")
    parser.add_argument("--open", action="store_true", help="Open Slack Manifest creation URL in browser immediately")
    args = parser.parse_args()

    if args.mode == "json":
        print(json.dumps(SLACK_MANIFEST_DICT, indent=2, ensure_ascii=False))
        return

    if args.mode == "guide":
        print_onboarding_checklist(open_browser=args.open)
        return

    if args.mode in ("test", "slack"):
        bot_token = (args.bot_token or os.environ.get("FIBER_TEST_SLACK_BOT_TOKEN", "")).strip()
        app_token = (args.app_token or os.environ.get("FIBER_TEST_SLACK_APP_TOKEN", "")).strip()

        if not bot_token or not app_token:
            print_onboarding_checklist(open_browser=args.open)
            if not bot_token:
                bot_token = input("\n1) Enter Slack Bot User OAuth Token (xoxb-...): ").strip()
            if not app_token:
                app_token = input("2) Enter Slack App-Level Token (xapp-...): ").strip()

        try:
            await run_slack_hitl_test(
                bot_token=bot_token,
                app_token=app_token,
                channel_id=args.channel,
                spiffe_id=args.spiffe,
                require_totp=args.totp,
            )
        except ValueError as err:
            print(f"\n[!] Slack Token Configuration Error:\n{err}")

def main() -> None:
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        print("\n[Tester] Stopped.")


if __name__ == "__main__":
    main()