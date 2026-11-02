# fiber.gateway.bridge.channel.discord
from __future__ import annotations

import os
import uuid
import json
import time
import hmac
import hashlib
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set

import httpx

from fiber.gateway.bridge.channel.oob import OutOfBandApprovalBridge
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("gateway.bridge.channel.discord")

class DiscordInteractionType:
    PING = 1
    MESSAGE_COMPONENT = 3
    MODAL_SUBMIT = 5

class DiscordCallbackType:
    PONG = 1
    CHANNEL_MESSAGE_WITH_SOURCE = 4
    DEFERRED_UPDATE_MESSAGE = 6
    UPDATE_MESSAGE = 7
    MODAL = 9

class DiscordHitlBridge(OutOfBandApprovalBridge):
    """
    Out-of-Band HITL Approval Bridge over Discord Gateway v10 & Interactions API.
    Maintains 100% behavioral parity with SlackHitlBridge:
      - TOTP Mode: Button Click -> Ephemeral Popup Modal (Type 9) -> Modal Submit -> Audit Receipt
      - 1-Click Mode: Button Click -> Immediate Ed25519 Attestation -> Audit Receipt
      - RBAC Guard: Rejects unauthorized users via Ephemeral Message (flags=64) without mutating the card.
    """
    DISCORD_API_BASE = "https://discord.com/api/v10"
    DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"

    def __init__(
        self,
        rpc: Optional[InternalRpcClient] = None,
        bot_token: Optional[str] = None,
        default_channel: Optional[str] = None,
        allowed_role_ids: Optional[List[str]] = None,
        signing_secret: Optional[str] = None,
    ):
        super().__init__(rpc=rpc, slack_signing_secret=signing_secret)
        self.bot_token = (bot_token or os.environ.get("FIBER_DISCORD_BOT_TOKEN", "")).strip()
        self.default_channel = (default_channel or os.environ.get("FIBER_DISCORD_CHANNEL_ID", "")).strip()

        env_roles = os.environ.get("FIBER_DISCORD_ALLOWED_ROLES", "").strip()
        self.allowed_roles: Set[str] = set(allowed_role_ids or (env_roles.split(",") if env_roles else []))

        self.node_fp = self.signer.pubkey_hex[:16]
        self.bridge_id = uuid.uuid4().hex[:8]
        self._owned_handles: Set[str] = set()
        self._token_cache: Dict[str, Dict[str, Any]] = {}

        self.running = False
        self._bg_tasks: List[asyncio.Task] = []
        self._last_seq: Optional[int] = None

    @property
    def is_live_configured(self) -> bool:
        return bool(self.bot_token and self.default_channel)

    def _bot_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bot {self.bot_token}",
            "Content-Type": "application/json",
        }

    def mint_approval_state_token(
        self,
        handle_id: str,
        target_id: str,
        prompt_id: str,
        ttl_sec: int = 300,
        require_totp: bool = True,
    ) -> Dict[str, Any]:
        self._owned_handles.add(handle_id)
        exp = int(time.time()) + ttl_sec
        envelope = {
            "handle_id": handle_id,
            "target_id": target_id,
            "prompt_id": prompt_id,
            "node_fp": self.node_fp,
            "bridge_id": self.bridge_id,
            "require_totp": bool(require_totp),
            "exp": exp,
        }
        canonical = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        sig = self.signer.sign_payload(canonical)
        state_token = {"envelope": envelope, "sig": sig}
        self._token_cache[handle_id] = state_token
        return state_token

    def is_owned_by_this_node(self, interaction_payload: Dict[str, Any]) -> bool:
        envelope = interaction_payload.get("state_token", {}).get("envelope", {})
        token_fp = envelope.get("node_fp")
        if token_fp and token_fp != self.node_fp:
            return False

        token_bridge_id = envelope.get("bridge_id")
        if token_bridge_id and token_bridge_id != self.bridge_id:
            return False

        handle_id = envelope.get("handle_id")
        if self._owned_handles and handle_id not in self._owned_handles:
            return False

        return True

    def _is_managed_agent(self, agent_uri: Optional[str]) -> bool:
        managed_spiffes = set(self.identity_map.values())
        if not managed_spiffes:
            return True
        return str(agent_uri or "") in managed_spiffes

    def _check_user_authorized(self, user_id: str, member_roles: List[str]) -> bool:
        """검증: 1) Allowed Role ID가 설정된 경우 역할 보유 여부 확인, 2) identity_map 매핑 확인"""
        if self.allowed_roles and not (self.allowed_roles & set(member_roles)):
            return False
        if user_id not in self.identity_map and "*" in self.identity_map:
            self.identity_map[user_id] = self.identity_map["*"]
        return user_id in self.identity_map

    def build_yield_message_payload(self, yield_event: Dict[str, Any]) -> Dict[str, Any]:
        handle_id = yield_event["handle_id"]
        target_id = yield_event["target_server_id"]
        agent_uri = yield_event.get("agent", "spiffe://public/agent")
        prompt_obj = yield_event.get("prompt", {})
        prompt_id = prompt_obj.get("id", f"prompt_{handle_id}")
        prompt_params = prompt_obj.get("params", {})

        require_totp = bool(prompt_params.get("require_totp", True))
        default_msg = (
            "Destructive operation requires 6-digit TOTP via Popup Modal."
            if require_totp
            else "High-risk operation requires 1-Click operator approval."
        )
        prompt_msg = prompt_params.get("message", default_msg)

        self.mint_approval_state_token(
            handle_id=handle_id,
            target_id=target_id,
            prompt_id=prompt_id,
            require_totp=require_totp,
        )

        # Discord custom_id limit is 100 chars: "fbr:{action}:{bridge_id}:{handle_id}"
        approve_custom_id = f"fbr:approve:{self.bridge_id}:{handle_id}"[:100]
        deny_custom_id = f"fbr:deny:{self.bridge_id}:{handle_id}"[:100]
        approve_label = "🔐 Approve (Enter TOTP)" if require_totp else "🛡️ Approve & Resume"

        return {
            "content": f"🚨 **[Fiber HITL]** Approval required for `{handle_id}` on `{target_id}`",
            "embeds": [
                {
                    "title": "🚨 Fiber HITL: High-Risk Execution Parked",
                    "color": 0xE67E22,
                    "description": f"**Intercepted Prompt:** _{prompt_msg}_",
                    "fields": [
                        {"name": "Target MCP", "value": f"`{target_id}`", "inline": True},
                        {"name": "Node Fingerprint", "value": f"`{self.node_fp}`", "inline": True},
                        {"name": "Handle ID", "value": f"`{handle_id}`", "inline": True},
                        {"name": "Requester", "value": f"`{agent_uri}`", "inline": True},
                        {
                            "name": "Auth Mode",
                            "value": "`RFC-6238 TOTP Modal`" if require_totp else "`⚡ 1-Click Attestation`",
                            "inline": False,
                        },
                    ],
                }
            ],
            "components": [
                {
                    "type": 1,  # ActionRow
                    "components": [
                        {
                            "type": 2,  # Button
                            "style": 3,  # Success (Green)
                            "label": approve_label,
                            "custom_id": approve_custom_id,
                        },
                        {
                            "type": 2,  # Button
                            "style": 4,  # Danger (Red)
                            "label": "🛑 Deny & Rollback",
                            "custom_id": deny_custom_id,
                        },
                    ],
                }
            ],
        }

    def build_totp_modal_response(self, handle_id: str, target_id: str) -> Dict[str, Any]:
        """TOTP 모드에서 Approve 클릭 시 호출자에게만 띄우는 팝업 모달 (Interaction Callback Type 9)"""
        modal_custom_id = f"fbr:modal:{self.bridge_id}:{handle_id}"[:100]
        return {
            "type": DiscordCallbackType.MODAL,
            "data": {
                "custom_id": modal_custom_id,
                "title": f"🔐 Fiber Sudo: {target_id[:20]}",
                "components": [
                    {
                        "type": 1,  # ActionRow
                        "components": [
                            {
                                "type": 4,  # TextInput
                                "custom_id": "fiber_totp_input",
                                "label": "Authenticator 6-Digit TOTP Code",
                                "style": 1,  # Short single-line input
                                "min_length": 6,
                                "max_length": 6,
                                "placeholder": "000000",
                                "required": True,
                            }
                        ],
                    }
                ],
            },
        }

    def build_resolution_receipt_payload(
        self, normalized: Dict[str, Any], result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """승인/거부 직후 기존 버튼을 제거(components=[])하고 감사 영수증 Embed로 원본 메시지를 교체합니다."""
        is_approved = result.get("status") == "RESUMED"
        envelope = normalized.get("state_token", {}).get("envelope", {})
        sig_preview = normalized.get("state_token", {}).get("sig", "")[:24]
        handle_id = result.get("handle_id") or envelope.get("handle_id", "unknown")
        target_id = envelope.get("target_id", "legacy-01")
        require_totp = envelope.get("require_totp", True)
        discord_user = normalized.get("slack_user_id", "operator")
        spiffe_id = result.get("attested_by", "spiffe://self/fiber")
        utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        if is_approved:
            title = "✅ Fiber HITL: Cryptographic Sudo Granted"
            color = 0x2ECC71
            status_badge = "🟢 `RESUMED -> EXECUTING`"
            seal_type = "RFC-6238 TOTP + Ed25519" if require_totp else "1-Click Button + Ed25519"
            desc = (
                f"🛡️ **Zero-Trust Attestation Receipt**\n"
                f"• **Attested Identity:** `{spiffe_id}` (by <@{discord_user}>)\n"
                f"• **Cryptographic Seal:** `{seal_type} ({sig_preview}...)`\n"
                f"• **Action Taken:** Parked sandbox unlocked & destructive payload released."
            )
        else:
            title = "🛑 Fiber HITL: Execution Aborted & Rolled Back"
            color = 0xE74C3C
            status_badge = "🔴 `ROLLED_BACK (FAULTED)`"
            desc = (
                f"🛑 **Security Intervention Receipt**\n"
                f"• **Rejected By:** <@{discord_user}>\n"
                f"• **Action Taken:** `FORCE_ROLLBACK` dispatched. Parked sandbox terminated immediately."
            )

        return {
            "content": f"{title} (`{handle_id}`)",
            "embeds": [
                {
                    "title": title,
                    "color": color,
                    "description": desc,
                    "fields": [
                        {"name": "Target MCP", "value": f"`{target_id}`", "inline": True},
                        {"name": "Phase Transition", "value": status_badge, "inline": True},
                        {"name": "Handle ID", "value": f"`{handle_id}`", "inline": True},
                        {"name": "Node Fingerprint", "value": f"`{self.node_fp}`", "inline": True},
                    ],
                    "footer": {"text": f"🔒 Immutable PTA state transition anchored at {utc_now}"},
                }
            ],
            "components": [],  # 버튼 제거 (재클릭 원천 방지)
        }

    def parse_discord_interaction(self, interaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        itype = interaction.get("type")
        data = interaction.get("data", {})
        custom_id = str(data.get("custom_id", ""))
        if not custom_id.startswith("fbr:"):
            return None

        parts = custom_id.split(":", 3)
        if len(parts) != 4:
            return None
        _, action_kind, target_bridge_id, handle_id = parts

        if target_bridge_id != self.bridge_id:
            return {"action": "IGNORE_FOREIGN_NODE"}

        state_token = self._token_cache.get(handle_id)
        if not state_token:
            return {"action": "EXPIRED_OR_UNKNOWN_HANDLE", "handle_id": handle_id}

        member = interaction.get("member") or {}
        user_obj = member.get("user") or interaction.get("user") or {}
        user_id = str(user_obj.get("id", ""))
        member_roles = [str(r) for r in member.get("roles", [])]

        if not self._check_user_authorized(user_id, member_roles):
            return {"action": "UNAUTHORIZED_USER", "user_id": user_id}

        require_totp = state_token.get("envelope", {}).get("require_totp", True)
        target_id = state_token.get("envelope", {}).get("target_id", "legacy-01")

        # Case A: 버튼 클릭 (MESSAGE_COMPONENT)
        if itype == DiscordInteractionType.MESSAGE_COMPONENT:
            if action_kind == "approve" and require_totp:
                return {
                    "action": "OPEN_MODAL",
                    "handle_id": handle_id,
                    "target_id": target_id,
                }

            decision = "APPROVE" if action_kind == "approve" else "DENY"
            return {
                "action": "DISPATCH",
                "state_token": state_token,
                "slack_user_id": user_id,  # OutOfBandApprovalBridge 호환 키
                "decision": decision,
                "otp_code": "BUTTON_APPROVED" if decision == "APPROVE" else "",
            }

        # Case B: TOTP 팝업 모달 제출 (MODAL_SUBMIT)
        if itype == DiscordInteractionType.MODAL_SUBMIT and action_kind == "modal":
            otp_code = ""
            for row in data.get("components", []):
                for comp in row.get("components", []):
                    if comp.get("custom_id") == "fiber_totp_input":
                        otp_code = str(comp.get("value", "")).strip()

            return {
                "action": "DISPATCH",
                "state_token": state_token,
                "slack_user_id": user_id,
                "decision": "APPROVE",
                "otp_code": otp_code,
            }

        return None

    async def _send_interaction_callback(
        self, interaction_id: str, interaction_token: str, callback_payload: Dict[str, Any]
    ) -> None:
        url = f"{self.DISCORD_API_BASE}/interactions/{interaction_id}/{interaction_token}/callback"
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json=callback_payload)
            if resp.status_code not in (200, 204):
                log.warning(f"[DiscordBridge] Interaction callback HTTP {resp.status_code}: {resp.text}")

    async def process_gateway_interaction(self, interaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        parsed = self.parse_discord_interaction(interaction)
        if not parsed or parsed.get("action") == "IGNORE_FOREIGN_NODE":
            return None

        interaction_id = str(interaction.get("id", ""))
        interaction_token = str(interaction.get("token", ""))
        action = parsed.get("action")

        # 1. 권한 없는 유저가 클릭한 경우: 원본 메시지는 건드리지 않고 본인에게만 Ephemeral(flags=64) 경고 표시
        if action == "UNAUTHORIZED_USER":
            if interaction_id and interaction_token:
                await self._send_interaction_callback(
                    interaction_id,
                    interaction_token,
                    {
                        "type": DiscordCallbackType.CHANNEL_MESSAGE_WITH_SOURCE,
                        "data": {
                            "content": f"⛔ **Access Denied:** Discord user `<@{parsed['user_id']}>` is not enrolled in Fiber's SPIFFE/RBAC identity map.",
                            "flags": 64,  # Ephemeral (Only visible to the clicker)
                        },
                    },
                )
            return {"status": "UNAUTHORIZED_USER"}

        # 2. 만료되었거나 이미 처리된 핸들인 경우
        if action == "EXPIRED_OR_UNKNOWN_HANDLE":
            if interaction_id and interaction_token:
                await self._send_interaction_callback(
                    interaction_id,
                    interaction_token,
                    {
                        "type": DiscordCallbackType.CHANNEL_MESSAGE_WITH_SOURCE,
                        "data": {
                            "content": "⚠️ This HITL transaction handle has already been resolved or expired.",
                            "flags": 64,
                        },
                    },
                )
            return {"status": "EXPIRED_HANDLE"}

        # 3. TOTP 모드에서 Approve 클릭 시: 즉시 TOTP 입력 모달창(Type 9) 팝업
        if action == "OPEN_MODAL":
            modal_payload = self.build_totp_modal_response(parsed["handle_id"], parsed["target_id"])
            if interaction_id and interaction_token:
                await self._send_interaction_callback(interaction_id, interaction_token, modal_payload)
            return {"status": "MODAL_OPENED", "handle_id": parsed["handle_id"]}

        # 4. 최종 승인(모달 제출 / 1-Click) 또는 거부(Deny) -> 부모 OutOfBandApprovalBridge로 위임
        ts = str(int(time.time()))
        raw_bytes = json.dumps(parsed, separators=(",", ":")).encode("utf-8")
        sig = "v0=" + hmac.new(self.slack_secret, f"v0:{ts}:".encode() + raw_bytes, hashlib.sha256).hexdigest()

        res_status = await self.handle_slack_interaction(ts, raw_bytes, sig, parsed)

        # 소비 완료된 핸들은 캐시에서 제거하고 원본 Discord 메시지를 감사 영수증으로 업데이트(Type 7)
        handle_id = parsed["state_token"]["envelope"]["handle_id"]
        self._token_cache.pop(handle_id, None)

        if interaction_id and interaction_token:
            receipt_data = self.build_resolution_receipt_payload(parsed, res_status)
            await self._send_interaction_callback(
                interaction_id,
                interaction_token,
                {
                    "type": DiscordCallbackType.UPDATE_MESSAGE,
                    "data": receipt_data,
                },
            )

        return res_status

    # =========================================================================
    # 4. Egress Notification & Outbound Gateway WebSocket Daemon
    # =========================================================================
    async def post_yield_notification(self, yield_event: Dict[str, Any]) -> Optional[str]:
        msg_payload = self.build_yield_message_payload(yield_event)
        if not self.is_live_configured:
            return None

        url = f"{self.DISCORD_API_BASE}/channels/{self.default_channel}/messages"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, headers=self._bot_headers(), json=msg_payload)
            if resp.status_code not in (200, 201):
                log.error(f"[DiscordBridge] Failed to post message ({resp.status_code}): {resp.text}")
                return None
            data = resp.json()
            msg_id = data.get("id")
            mode_tag = "TOTP-Modal" if yield_event.get("prompt", {}).get("params", {}).get("require_totp", True) else "1-Click"
            log.info(f"[DiscordBridge] 📤 Sent HITL ({mode_tag}) prompt to Discord channel {self.default_channel} (id: {msg_id})")
            return msg_id

    async def listen_telemetry_and_notify(self) -> None:
        tunnel = await TunnelFactory.get_default()
        pubsub = tunnel.pubsub()
        await pubsub.subscribe("eco.telemetry.events")
        log.info(
            f"[DiscordBridge] Listening for MCP_PHASE_YIELD on eco.telemetry.events "
            f"(Node FP: {self.node_fp}, Bridge ID: {self.bridge_id})"
        )
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg and msg["type"] == "message":
                    try:
                        event_data = json.loads(msg["data"])
                        if event_data.get("event") == "MCP_PHASE_YIELD":
                            if self._is_managed_agent(event_data.get("agent")):
                                await self.post_yield_notification(event_data)
                    except Exception as e:
                        log.error(f"[DiscordBridge] Failed to process yield telemetry: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("eco.telemetry.events")
            await pubsub.close()

    async def run_gateway_daemon(self) -> None:
        """Connects to Discord Gateway v10 WebSocket, maintains heartbeat, and processes INTERACTION_CREATE."""
        if not self.bot_token:
            return

        import websockets

        while self.running:
            heartbeat_task: Optional[asyncio.Task] = None
            try:
                async with websockets.connect(self.DISCORD_GATEWAY_URL, max_size=4 * 1024 * 1024) as ws:
                    # 1. Receive Opcode 10 (Hello) -> extract heartbeat_interval
                    hello_raw = await ws.recv()
                    hello = json.loads(hello_raw)
                    if hello.get("op") != 10:
                        raise RuntimeError(f"Expected Discord Gateway OP 10 Hello, got: {hello}")

                    hb_interval_sec = hello["d"]["heartbeat_interval"] / 1000.0

                    # 2. Send Opcode 2 (Identify) - Intents=0 is sufficient to receive INTERACTION_CREATE
                    identify_payload = {
                        "op": 2,
                        "d": {
                            "token": self.bot_token,
                            "intents": 0,
                            "properties": {"os": "linux", "browser": "fiber", "device": "fiber"},
                        },
                    }
                    await ws.send(json.dumps(identify_payload))

                    async def _heartbeat_loop():
                        while self.running:
                            await asyncio.sleep(hb_interval_sec)
                            await ws.send(json.dumps({"op": 1, "d": self._last_seq}))

                    heartbeat_task = asyncio.create_task(_heartbeat_loop())
                    log.info("[DiscordBridge:Gateway] 🔌 Connected to Discord Outbound Gateway v10.")

                    # 3. Event Loop
                    async for raw_frame in ws:
                        if not self.running:
                            break
                        frame = json.loads(raw_frame)
                        op = frame.get("op")
                        seq = frame.get("s")
                        if seq is not None:
                            self._last_seq = seq

                        if op == 1:  # Server requested immediate heartbeat
                            await ws.send(json.dumps({"op": 1, "d": self._last_seq}))
                        elif op in (7, 9):  # Reconnect / Invalid Session
                            break
                        elif op == 0 and frame.get("t") == "INTERACTION_CREATE":
                            interaction_data = frame.get("d", {})
                            try:
                                await self.process_gateway_interaction(interaction_data)
                            except Exception as act_err:
                                log.error(f"[DiscordBridge:Gateway] Interaction rejected: {act_err}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"[DiscordBridge:Gateway] Reconnecting in 3s after error: {e}")
                await asyncio.sleep(3.0)
            finally:
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()
                    await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def start_background_daemons(self) -> None:
        self.running = True
        self._bg_tasks.append(asyncio.create_task(self.listen_telemetry_and_notify()))
        if self.bot_token:
            self._bg_tasks.append(asyncio.create_task(self.run_gateway_daemon()))

    async def stop_background_daemons(self) -> None:
        self.running = False
        for t in self._bg_tasks:
            if not t.done():
                t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()