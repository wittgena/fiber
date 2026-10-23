# fiber.dev.e2e.worker.guard
import os
import sys
import time
import uuid
import json
import hmac
import hashlib
import asyncio
import tempfile
import urllib.parse
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import httpx
import redis.exceptions

from fiber.infra.bound.e2e.config import Phase, E2EConfig, TestResult, get_free_port
from fiber.infra.bound.e2e.pipeline import BasePipeline, log
from fiber.infra.mcp.mapper.config import McpConfigMapper
from fiber.phase.action.connect import (
    ACTIVE_TARGETS_TUNNEL_KEY,
    ResolvedTarget,
    _specs_from_mapped_entries,
    resolve_connector_spec,
    run_connect_copy,
)
from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.payload import McpPayloadBuilder
import fiber.infra.mcp.worker.eco.finlib as worker_finlib

from fiber.infra.bound.e2e.audit.context import AuditContext
from fiber.gateway.bridge.mcp.slack import SlackHitlBridge

from xphi.node.space.bind.resolver import resolve_path
from xphi.arch.contract.config.env import DEFAULT_GATEWAY_PORT
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.node.space.tunnel.factory import TunnelFactory

ANCHOR_MCP_JSON = Path(resolve_path("anchor")) / "mcp.json"


class PreflightGuardPipeline(BasePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config,
            name="Zero-Code Pre-Flight HITL Guard (Daemon TOTP & Ephemeral 1-Click) Suite",
            scope_name="MCP_PREFLIGHT_GUARD_SUITE",
        )
        self.mcp = McpPayloadBuilder()

        self.finlib_target_id = "worker.eco.finlib"
        self.finlib_preflight_rule = r"normalize_order|pow\(|exp\("
        self._attached_to_daemon = False

        self.stdio_target_id = "hitl-stdio-fs-01"
        self.stdio_preflight_rule = "button:prod_secrets|DELETE|DROP"

        self.temp_dir = tempfile.TemporaryDirectory(prefix="fiber_guard_fs_")
        self.sandbox_path = Path(self.temp_dir.name).resolve()
        (self.sandbox_path / "public_readme.txt").write_text("SAFE_PUBLIC_CONTENT", encoding="utf-8")
        (self.sandbox_path / "prod_secrets.env").write_text("SUPER_SECRET_KEY=12345", encoding="utf-8")

        self.crypto_ctx = AuditContext(spiffe_id="spiffe://self/fiber")
        self.slack_bridge: Optional[SlackHitlBridge] = None
        self.slack_admin_id = os.environ.get("FIBER_SLACK_USER_ID", "U_SLACK_ADMIN_99")
        self.dpop_client = DPoPClientGenerator(key_size=2048)
        self.http_client: Optional[httpx.AsyncClient] = None

        self.set_phases([
            Phase("Phase 1: Connect Spec Resolver & Dual Mode (TOTP / Button) Guard Arming", self.phase_resolver_and_guard_arming),
            Phase("Phase 2: Safe Call Passthrough (Zero-Overhead on Non-Matching Payload)", self.phase_safe_passthrough),
            Phase("Phase 3: [Daemon Persistent + TOTP] FinLib Trap & TOTP Attested Resume", self.phase_daemon_finlib_totp_hitl),
            Phase("Phase 4: [Stdio/Ephemeral + 1-Click Button] Zero-Spawn Trap & Button-Only Resume", self.phase_stdio_ephemeral_button_hitl),
        ])

    async def setup_custom_context(self) -> None:
        self.http_client = httpx.AsyncClient(base_url=self.local_url, timeout=25.0)
        self.crypto_ctx.provision()
        self.slack_bridge = SlackHitlBridge(rpc=self.rpc)
        for uid in (self.slack_admin_id, "*"):
            self.slack_bridge.register_identity_mapping(uid, self.crypto_ctx.spiffe_id)

        if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
            await self.slack_bridge.start_background_daemons()
            log.info(f"[GuardPipeline:LIVE] Slack Socket Mode Active! Channel: {self.slack_bridge.default_channel}")
        else:
            log.info(f"[GuardPipeline:SIM] Mapped '{self.slack_admin_id}' -> '{self.crypto_ctx.spiffe_id}'")

    async def teardown_custom(self) -> None:
        if self.slack_bridge:
            await self.slack_bridge.stop_background_daemons()
        self.crypto_ctx.teardown()
        if self.http_client:
            await self.http_client.aclose()
        self.temp_dir.cleanup()

    def _ensure_anchor_finlib_guard(self) -> Tuple[Optional[ResolvedTarget], bool]:
        def _parse_current() -> Optional[ResolvedTarget]:
            if not ANCHOR_MCP_JSON.is_file():
                return None
            entries = McpConfigMapper.parse_file(ANCHOR_MCP_JSON, agent_hint="fiber")
            return {s.target_id: s for s in _specs_from_mapped_entries(entries)}.get(self.finlib_target_id)

        existing = _parse_current()
        if existing and existing.preflight == self.finlib_preflight_rule:
            return existing, False

        try:
            run_connect_copy(
                exec_cmd="finlib", target=self.finlib_target_id, mode="linear", preflight=self.finlib_preflight_rule
            )
            if ANCHOR_MCP_JSON.is_file():
                os.utime(ANCHOR_MCP_JSON, None)
            return _parse_current(), True
        except BaseException as e:
            log.warning(f"[GuardPipeline] Anchor config sync warning: {e}")
            return None, False

    async def setup_workers(self) -> None:
        anchor_spec, was_updated = self._ensure_anchor_finlib_guard()
        active_daemon_ids = set()
        try:
            tunnel = await TunnelFactory.get_default()
            raw = await tunnel.get(ACTIVE_TARGETS_TUNNEL_KEY)
            if raw:
                active_daemon_ids = set(json.loads(raw))
        except Exception:
            pass

        if self.finlib_target_id in active_daemon_ids:
            self._attached_to_daemon = True
            log.info(f"[GuardPipeline] 🔗 Attached to live daemon: [{self.finlib_target_id}] (/{self.finlib_preflight_rule}/i)")
            if was_updated:
                await asyncio.sleep(2.0)
        else:
            spec_finlib = anchor_spec or resolve_connector_spec(
                target=self.finlib_target_id,
                exec_cmd=f"{sys.executable} -m {worker_finlib.__name__}",
                mode="linear",
                preflight=self.finlib_preflight_rule,
            )
            self.connectors.append(
                MCPServerConnector(
                    spec_finlib.target_id, spec_finlib.exec_cmd, spec_finlib.mode, spec_finlib.transport, spec_finlib.preflight
                )
            )

        spec_stdio = resolve_connector_spec(
            target=self.stdio_target_id,
            exec_cmd=f"npx -y @modelcontextprotocol/server-filesystem {self.sandbox_path}",
            preflight=self.stdio_preflight_rule,
        )
        self.connectors.append(
            MCPServerConnector(
                spec_stdio.target_id, spec_stdio.exec_cmd, spec_stdio.mode, spec_stdio.transport, spec_stdio.preflight
            )
        )

    def _get_connector(self, target_id: str) -> Optional[MCPServerConnector]:
        return next((c for c in self.connectors if c.target_id == target_id), None)

    async def _post_gateway(
        self, target_id: str, payload: Dict[str, Any], idem_key: Optional[str] = None, timeout: float = 20.0
    ) -> httpx.Response:
        nonce, url = uuid.uuid4().hex, f"{self.local_url}/v1/mcp-gateway/{target_id}/invoke"
        headers = {
            "x-idempotency-key": idem_key or uuid.uuid4().hex,
            "x-nonce": nonce,
            "Fuel-Receipt": "valid_receipt",
            "x-spiffe-id": self.crypto_ctx.spiffe_id,
            "DPoP": self.dpop_client.generate_proof(url=url, method="POST", nonce=nonce),
        }
        return await self.http_client.post(url, json=payload, headers=headers, timeout=timeout)

    async def _emulate_local_webhook(self, target_id: str, handle_id: str, prompt_payload: Dict[str, Any], expect_totp: bool) -> None:
        yield_event = {
            "event": "MCP_PHASE_YIELD",
            "handle_id": handle_id,
            "target_server_id": target_id,
            "agent": self.crypto_ctx.spiffe_id,
            "prompt": prompt_payload,
        }
        blocks = self.slack_bridge.build_yield_message_blocks(yield_event, channel="C_HITL_TEST")["blocks"]
        is_totp_block = blocks[3].get("block_id") == "fiber_totp_block"

        if expect_totp != is_totp_block:
            raise RuntimeError(f"Block-Kit TOTP slot mismatch: expect_totp={expect_totp}, rendered={is_totp_block}")

        state_vals = (
            {"fiber_totp_block": {"fiber_totp_input": {"type": "plain_text_input", "value": self.crypto_ctx.generate_totp()}}}
            if expect_totp else {}
        )
        action_payload = {
            "type": "block_actions",
            "user": {"id": self.slack_admin_id, "username": "admin"},
            "actions": [{"action_id": "fiber_action_approve", "value": blocks[4]["elements"][0]["value"]}],
            "state": {"values": state_vals},
        }
        form_body = urllib.parse.urlencode({"payload": json.dumps(action_payload)}).encode("utf-8")
        ts = str(int(time.time()))
        sig = "v0=" + hmac.new(self.slack_bridge.slack_secret, f"v0:{ts}:".encode() + form_body, hashlib.sha256).hexdigest()

        oob_res = await self.slack_bridge.handle_http_webhook(ts, form_body, sig)
        if oob_res.get("status") != "RESUMED":
            raise RuntimeError(f"Expected OOB status RESUMED, got {oob_res}")

    async def _resolve_hitl_adaptive(
        self, target_id: str, handle_id: str, prompt_payload: Dict[str, Any], orig_payload: Dict[str, Any], idem_key: str, expect_totp: bool
    ) -> str:
        actual_totp = bool(prompt_payload.get("params", {}).get("require_totp", True))
        if actual_totp != expect_totp:
            raise RuntimeError(f"Prompt require_totp mismatch on {target_id}: expected {expect_totp}, got {actual_totp}")

        mode_badge = "TOTP-6DIGIT" if expect_totp else "1-CLICK BUTTON"
        tunnel = await TunnelFactory.get_default()
        pubsub_channel = f"mcp.intent.reply.{handle_id}"
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(pubsub_channel)

        async def listen_for_ledger() -> Dict[str, Any]:
            while True:
                try:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg and msg.get("type") == "message":
                        raw = msg["data"]
                        return json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                except redis.exceptions.TimeoutError:
                    continue
                await asyncio.sleep(0.05)

        try:
            if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
                await asyncio.sleep(0.4)
                print(f"\n{'=' * 80}\n📲 [LIVE SLACK HITL | {mode_badge}] Target: {target_id} | Handle: {handle_id}\n{'-' * 80}")
                start_wait = time.time()

                async def spinner_task():
                    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                    tick = 0
                    while True:
                        el = time.time() - start_wait
                        icon = frames[tick % len(frames)]
                        if expect_totp:
                            rem = 30 - (int(time.time()) % 30)
                            msg = f"{icon} ⏳ [PARKED: TOTP] Code [ \033[1;32m{self.crypto_ctx.generate_totp()}\033[0m ] (in {rem:02d}s)... ({el:.1f}s)"
                        else:
                            msg = f"{icon} ⚡ [PARKED: 1-CLICK] Click [ \033[1;36mApprove & Resume\033[0m ] in Slack... ({el:.1f}s)"
                        sys.stdout.write(f"\r\033[K   {msg}")
                        sys.stdout.flush()
                        tick += 1
                        await asyncio.sleep(0.2)

                spinner = asyncio.create_task(spinner_task())
                try:
                    event_data = await asyncio.wait_for(listen_for_ledger(), timeout=60.0)
                finally:
                    spinner.cancel()
                    await asyncio.gather(spinner, return_exceptions=True)
                print(f"\r\033[K   [SIGNAL RECEIVED] Status: {event_data.get('status')} ({time.time() - start_wait:.1f}s)\n{'=' * 80}")
            else:
                await self._emulate_local_webhook(target_id, handle_id, prompt_payload, expect_totp)
                event_data = await asyncio.wait_for(listen_for_ledger(), timeout=15.0)

            if event_data.get("status") != "RESOLVED":
                raise RuntimeError(f"Handle {handle_id} FAULTED: {event_data.get('error_detail', 'Unknown')}")

            res_final = await self._post_gateway(target_id, orig_payload, idem_key=idem_key, timeout=25.0)
            if res_final.status_code != 200:
                raise RuntimeError(f"Expected 200 OK after resolution, got {res_final.status_code}: {res_final.text}")
            return McpPayloadBuilder.extract_data(res_final.content, parse_json=False)
        finally:
            try:
                await pubsub.unsubscribe(pubsub_channel)
                await pubsub.close()
            except Exception:
                pass

    async def phase_resolver_and_guard_arming(self) -> None:
        finlib_conn = self._get_connector(self.finlib_target_id)
        stdio_conn = self._get_connector(self.stdio_target_id)

        if finlib_conn:
            if finlib_conn.mode != "linear" or finlib_conn.fsm.preflight.require_totp is not True:
                raise RuntimeError("Local FinLib connector failed to arm as linear/TOTP.")
        else:
            spec = resolve_connector_spec(target=self.finlib_target_id, exec_cmd="finlib", preflight=self.finlib_preflight_rule)
            if spec.mode != "linear" or spec.preflight != self.finlib_preflight_rule:
                raise RuntimeError(f"Daemon FinLib spec mismatch: {spec}")

        if not stdio_conn or stdio_conn.mode != "ephemeral" or stdio_conn.fsm.preflight.require_totp is not False:
            raise RuntimeError("Stdio connector failed to arm as ephemeral/Button.")

        log.info(f"  └─ Armed: [{self.finlib_target_id} (TOTP)] & [{self.stdio_target_id} (1-CLICK BUTTON)]")

    async def phase_safe_passthrough(self) -> None:
        res_math = await self._post_gateway(self.finlib_target_id, self.mcp.next_tool_call("eval_math", expression="100 * 50"))
        if res_math.status_code != 200 or McpPayloadBuilder.extract_data(res_math.content, parse_json=True).get("result") != 5000.0:
            raise RuntimeError(f"Safe FinLib call failed: {res_math.status_code}")

        safe_file = str(self.sandbox_path / "public_readme.txt")
        res_fs = await self._post_gateway(self.stdio_target_id, self.mcp.next_tool_call("read_file", path=safe_file))
        if res_fs.status_code != 200 or "SAFE_PUBLIC_CONTENT" not in McpPayloadBuilder.extract_data(res_fs.content, parse_json=False):
            raise RuntimeError(f"Safe Stdio call failed: {res_fs.status_code}")

        log.info("  └─ Safe Calls ('eval_math' & 'public_readme.txt') bypassed Guard with 200 OK.")

    async def phase_daemon_finlib_totp_hitl(self) -> None:
        idem_key = uuid.uuid4().hex
        payload = self.mcp.next_tool_call("normalize_order", symbol="BTCUSD", raw_price=65000.25, raw_amount=1.5004)

        res_yield = await self._post_gateway(self.finlib_target_id, payload, idem_key=idem_key)
        if res_yield.status_code != 202:
            raise RuntimeError(f"Expected 202 YIELD on FinLib, got {res_yield.status_code}")

        handle_id = res_yield.headers.get("Location", "").split("/")[-1]
        yield_body = res_yield.json()
        if yield_body.get("params", {}).get("require_totp") is not True:
            raise RuntimeError(f"FinLib 202 prompt missing require_totp=True: {yield_body}")

        content = await self._resolve_hitl_adaptive(self.finlib_target_id, handle_id, yield_body, payload, idem_key, expect_totp=True)
        parsed = json.loads(content)
        if parsed.get("symbol") != "BTCUSD" or parsed.get("normalized_price") != 65000.0:
            raise RuntimeError(f"Unexpected FinLib output: {parsed}")

        log.info(f"  └─ [Daemon/Linear | TOTP] Verified -> Normalized price={parsed['normalized_price']}")

    async def phase_stdio_ephemeral_button_hitl(self) -> None:
        stdio_conn = self._get_connector(self.stdio_target_id)
        idem_key = uuid.uuid4().hex
        payload = self.mcp.next_tool_call("read_file", path=str(self.sandbox_path / "prod_secrets.env"))

        res_yield = await self._post_gateway(self.stdio_target_id, payload, idem_key=idem_key)
        if res_yield.status_code != 202:
            raise RuntimeError(f"Expected 202 YIELD on Stdio, got {res_yield.status_code}")

        handle_id = res_yield.headers.get("Location", "").split("/")[-1]
        if stdio_conn.active_sandboxes:
            raise RuntimeError("SECURITY VIOLATION: npx spawned before button approval!")

        content = await self._resolve_hitl_adaptive(self.stdio_target_id, handle_id, res_yield.json(), payload, idem_key, expect_totp=False)
        if "SUPER_SECRET_KEY=12345" not in content or handle_id in stdio_conn.active_sandboxes:
            raise RuntimeError("Secret read failed or ephemeral sandbox leaked.")

        log.info("  └─ [Stdio/Ephemeral | 1-Click] Zero-Spawn -> Button Approved -> Lazy Spawn -> Destroyed!")


async def run_suite() -> None:
    pipeline = PreflightGuardPipeline(E2EConfig(host="127.0.0.1", port=get_free_port(DEFAULT_GATEWAY_PORT), protocol="http"))
    results: List[TestResult] = await pipeline.run_pipeline()
    all_passed = all(r.passed for r in results)

    lines = ["\n" + "=" * 90, "🛡️ [ZERO-CODE PRE-FLIGHT GUARD (DAEMON TOTP & EPHEMERAL 1-CLICK) E2E REPORT]", "=" * 90]
    for idx, r in enumerate(results, 1):
        lines.append(f"{'✅' if r.passed else '❌'} {idx:02d}. [{r.target}]".ljust(28) + f"{r.scenario.ljust(60)} | {'PASSED' if r.passed else 'FAILED'}")
    lines.extend(["-" * 90, "PRE-FLIGHT GUARD PIPELINE VALIDATED SUCCESSFULLY." if all_passed else "PRE-FLIGHT GUARD FAILURE DETECTED.", "=" * 90 + "\n"])
    print("\n".join(lines), flush=True)


def main(args_list: Optional[List[str]] = None) -> None:
    PhaseReactor.ignite(main_coro_func=run_suite)


if __name__ == "__main__":
    main()