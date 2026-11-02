# fiber.gateway.bridge.channel.slack
import os
import uuid
import json
import time
import hmac
import hashlib
import asyncio
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set

import httpx

from fiber.gateway.bridge.channel.oob import OutOfBandApprovalBridge
from xphi.arch.bound.client.rpc import InternalRpcClient
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bridge.channel.slack")


class SlackHitlBridge(OutOfBandApprovalBridge):
    SLACK_API_BASE = "https://slack.com/api"

    def __init__(
        self,
        rpc: Optional[InternalRpcClient] = None,
        bot_token: Optional[str] = None,
        app_token: Optional[str] = None,
        default_channel: Optional[str] = None,
        slack_signing_secret: Optional[str] = None,
    ):
        super().__init__(rpc=rpc, slack_signing_secret=slack_signing_secret)
        self.bot_token = (bot_token or os.environ.get("FIBER_SLACK_BOT_TOKEN", "")).strip()
        self.app_token = (app_token or os.environ.get("FIBER_SLACK_APP_TOKEN", "")).strip()
        self.default_channel = (default_channel or os.environ.get("FIBER_SLACK_CHANNEL_ID", "")).strip()
        self.node_fp = self.signer.pubkey_hex[:16]
        self.bridge_id = uuid.uuid4().hex[:8]
        self._owned_handles: Set[str] = set()
        self.running = False
        self._bg_tasks: List[asyncio.Task] = []

    @property
    def is_live_configured(self) -> bool:
        return bool(self.bot_token and self.default_channel)

    # Node Fingerprint, Bridge ID 및 인증 모드(require_totp)가 각인된 State Token 발급
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
        return {"envelope": envelope, "sig": sig}

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
        """이 브릿지 인스턴스에 등록된 SPIFFE ID가 요청한 이벤트인지 판별합니다."""
        managed_spiffes = set(self.identity_map.values())
        if not managed_spiffes:
            return True
        return str(agent_uri or "") in managed_spiffes

    def _resolve_wildcard_identity(self, normalized: Dict[str, Any]) -> None:
        """Socket Mode와 HTTP Webhook 모두에서 동일하게 '*' 와일드카드 ID 매핑을 바인딩합니다."""
        slack_uid = normalized.get("slack_user_id", "")
        if slack_uid and slack_uid not in self.identity_map and "*" in self.identity_map:
            self.identity_map[slack_uid] = self.identity_map["*"]

    # Block Kit UI 조립 (TOTP 모드 vs 1-Click Button 모드 지원 & blocks[4] 인덱스 호환성 유지)
    def build_yield_message_blocks(
        self, yield_event: Dict[str, Any], channel: Optional[str] = None
    ) -> Dict[str, Any]:
        handle_id = yield_event["handle_id"]
        target_id = yield_event["target_server_id"]
        agent_uri = yield_event.get("agent", "spiffe://public/agent")
        prompt_obj = yield_event.get("prompt", {})
        prompt_id = prompt_obj.get("id", f"prompt_{handle_id}")
        prompt_params = prompt_obj.get("params", {})

        require_totp = bool(prompt_params.get("require_totp", True))
        default_msg = (
            "Destructive operation requires TOTP."
            if require_totp
            else "High-risk operation requires operator approval."
        )
        prompt_msg = prompt_params.get("message", default_msg)

        state_token = self.mint_approval_state_token(
            handle_id=handle_id,
            target_id=target_id,
            prompt_id=prompt_id,
            require_totp=require_totp,
        )
        token_str = json.dumps(state_token, separators=(",", ":"))

        # blocks[3] 슬롯: TOTP 필수 시 입력창 렌더링, 버튼 단독 모드 시 1-Click 안내 컨텍스트 렌더링
        # (이를 통해 fiber_decision_block이 항상 blocks[4]에 위치하여 기존 테스트 코드와 100% 호환됨)
        if require_totp:
            auth_slot_block: Dict[str, Any] = {
                "type": "input",
                "block_id": "fiber_totp_block",
                "element": {
                    "type": "plain_text_input",
                    "action_id": "fiber_totp_input",
                    "placeholder": {"type": "plain_text", "text": "Enter 6-digit TOTP code"},
                    "min_length": 6,
                    "max_length": 6,
                },
                "label": {"type": "plain_text", "text": "🔐 Authenticator TOTP Code"},
            }
        else:
            auth_slot_block = {
                "type": "context",
                "block_id": "fiber_button_mode_hint",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "⚡ *1-Click Approval Mode Active:* TOTP input is bypassed for this guard. Click below to attest with your Node Identity.",
                    }
                ],
            }

        return {
            "channel": channel or self.default_channel,
            "text": f"🚨 [Fiber HITL] Approval required for {handle_id} on {target_id}",
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "🚨 Fiber HITL: High-Risk Execution Parked"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Target MCP:*\n`{target_id}`"},
                        {"type": "mrkdwn", "text": f"*Node Fingerprint:*\n`{self.node_fp}`"},
                        {"type": "mrkdwn", "text": f"*Handle ID:*\n`{handle_id}`"},
                        {"type": "mrkdwn", "text": f"*Requester:*\n`{agent_uri}`"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Intercepted Prompt:* _{prompt_msg}_"},
                },
                auth_slot_block,
                {
                    "type": "actions",
                    "block_id": "fiber_decision_block",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "🛡️ Approve & Resume"},
                            "style": "primary",
                            "action_id": "fiber_action_approve",
                            "value": token_str,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "🛑 Deny & Rollback"},
                            "style": "danger",
                            "action_id": "fiber_action_deny",
                            "value": token_str,
                        },
                    ],
                },
            ],
        }

    def build_resolution_receipt_blocks(
        self, normalized: Dict[str, Any], result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """승인/거부 직후 슬랙의 입력창/버튼을 지우고 엔터프라이즈 감사 영수증(Audit Receipt) 카드로 교체합니다."""
        is_approved = result.get("status") == "RESUMED"
        envelope = normalized.get("state_token", {}).get("envelope", {})
        sig_preview = normalized.get("state_token", {}).get("sig", "")[:24]
        handle_id = result.get("handle_id") or envelope.get("handle_id", "unknown")
        target_id = envelope.get("target_id", "legacy-01")
        require_totp = envelope.get("require_totp", True)
        slack_user = normalized.get("slack_user_id", "operator")
        spiffe_id = result.get("attested_by", "spiffe://self/fiber")
        utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        if is_approved:
            header_text = "✅ Fiber HITL: Cryptographic Sudo Granted"
            status_badge = "🟢 `RESUMED -> EXECUTING`"
            seal_type = "RFC-6238 TOTP + Ed25519" if require_totp else "1-Click Button + Ed25519"
            detail_md = (
                f"🛡️ *Zero-Trust Attestation Receipt*\n"
                f"• *Attested Identity:* `{spiffe_id}` (by <@{slack_user}>)\n"
                f"• *Cryptographic Seal:* `{seal_type} ({sig_preview}...)`\n"
                f"• *Action Taken:* Parked sandbox unlocked & destructive payload released."
            )
        else:
            header_text = "🛑 Fiber HITL: Execution Aborted & Rolled Back"
            status_badge = "🔴 `ROLLED_BACK (FAULTED)`"
            detail_md = (
                f"🛑 *Security Intervention Receipt*\n"
                f"• *Rejected By:* <@{slack_user}>\n"
                f"• *Action Taken:* `FORCE_ROLLBACK` dispatched. Parked sandbox terminated immediately."
            )

        return {
            "replace_original": True,
            "text": f"{header_text} ({handle_id})",
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": header_text},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Target MCP:*\n`{target_id}`"},
                        {"type": "mrkdwn", "text": f"*Phase Transition:*\n{status_badge}"},
                        {"type": "mrkdwn", "text": f"*Handle ID:*\n`{handle_id}`"},
                        {"type": "mrkdwn", "text": f"*Node Fingerprint:*\n`{self.node_fp}`"},
                    ],
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": detail_md},
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"🔒 Immutable PTA state transition anchored at `{utc_now}`",
                        }
                    ],
                },
            ],
        }

    # Payload 추출 및 HTTP/SocketMode 핸들러
    @staticmethod
    def extract_interaction_from_block_actions(slack_payload: Dict[str, Any]) -> Dict[str, Any]:
        if "state_token" in slack_payload and "slack_user_id" in slack_payload:
            return slack_payload

        user_id = slack_payload.get("user", {}).get("id", "")
        actions = slack_payload.get("actions", [])
        if not actions:
            raise ValueError("Missing actions array in Slack block_actions payload")

        clicked = actions[0]
        action_id = clicked.get("action_id", "")
        decision = "APPROVE" if action_id == "fiber_action_approve" else "DENY"

        raw_value = clicked.get("value", "{}")
        state_token = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
        require_totp = state_token.get("envelope", {}).get("require_totp", True)

        state_values = slack_payload.get("state", {}).get("values", {})
        otp_code = (
            state_values.get("fiber_totp_block", {})
            .get("fiber_totp_input", {})
            .get("value")
        )
        cleaned_otp = (otp_code or "").strip()
        if not cleaned_otp and not require_totp:
            cleaned_otp = "BUTTON_APPROVED"

        return {
            "state_token": state_token,
            "slack_user_id": user_id,
            "decision": decision,
            "otp_code": cleaned_otp,
            "response_url": slack_payload.get("response_url"),
        }

    async def handle_http_webhook(self, timestamp: str, raw_body: bytes, slack_signature: str) -> Dict[str, Any]:
        body_str = raw_body.decode("utf-8").strip()
        if body_str.startswith("payload="):
            parsed_qs = urllib.parse.parse_qs(body_str)
            slack_json = json.loads(parsed_qs["payload"][0])
        else:
            slack_json = json.loads(body_str)

        normalized = self.extract_interaction_from_block_actions(slack_json)
        if not self.is_owned_by_this_node(normalized):
            return {"status": "IGNORED_FOREIGN_NODE"}

        self._resolve_wildcard_identity(normalized)

        res = await self.handle_slack_interaction(
            timestamp=timestamp,
            raw_body=raw_body,
            slack_signature=slack_signature,
            interaction_payload=normalized,
        )

        if normalized.get("response_url"):
            asyncio.create_task(
                self._update_slack_message_via_response_url(normalized["response_url"], normalized, res)
            )

        return res

    async def start_background_daemons(self) -> None:
        self.running = True
        self._bg_tasks.append(asyncio.create_task(self.listen_telemetry_and_notify()))
        if self.app_token:
            self._bg_tasks.append(asyncio.create_task(self.run_socket_mode_daemon()))

    async def stop_background_daemons(self) -> None:
        self.running = False
        for t in self._bg_tasks:
            if not t.done():
                t.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    async def post_yield_notification(self, yield_event: Dict[str, Any]) -> Optional[str]:
        msg_payload = self.build_yield_message_blocks(yield_event)
        if not self.is_live_configured:
            return None

        headers = {"Authorization": f"Bearer {self.bot_token}", "Content-Type": "application/json; charset=utf-8"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{self.SLACK_API_BASE}/chat.postMessage", headers=headers, json=msg_payload)
            data = resp.json()
            if not data.get("ok"):
                log.error(f"[SlackBridge] chat.postMessage failed: {data.get('error')}")
                return None
            mode_tag = "TOTP" if yield_event.get("prompt", {}).get("params", {}).get("require_totp", True) else "1-Click"
            log.info(
                f"[SlackBridge] 📤 Sent HITL ({mode_tag}) prompt to Slack channel {msg_payload['channel']} (ts: {data.get('ts')})"
            )
            return data.get("ts")

    async def _update_slack_message_via_response_url(
        self, response_url: str, normalized: Dict[str, Any], result: Dict[str, Any]
    ):
        receipt_payload = self.build_resolution_receipt_blocks(normalized, result)
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(response_url, json=receipt_payload)
        except Exception as e:
            log.debug(f"[SlackBridge] Failed to update original message via response_url: {e}")

    async def listen_telemetry_and_notify(self):
        tunnel = await TunnelFactory.get_default()
        pubsub = tunnel.pubsub()
        await pubsub.subscribe("eco.telemetry.events")
        log.info(
            f"[SlackBridge] Listening for MCP_PHASE_YIELD on eco.telemetry.events "
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
                        log.error(f"[SlackBridge] Failed to process yield telemetry: {e}")
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("eco.telemetry.events")
            await pubsub.close()

    async def run_socket_mode_daemon(self):
        if not self.app_token:
            return

        import websockets

        headers = {"Authorization": f"Bearer {self.app_token}"}
        while self.running:
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    res = await client.post(f"{self.SLACK_API_BASE}/apps.connections.open", headers=headers)
                    conn_info = res.json()
                    if not conn_info.get("ok"):
                        raise RuntimeError(f"apps.connections.open failed: {conn_info.get('error')}")
                    ws_url = conn_info["url"]

                log.info("[SlackBridge:SocketMode] 🔌 Connected to Slack Outbound WebSocket.")
                async with websockets.connect(ws_url) as ws:
                    async for raw_frame in ws:
                        if not self.running:
                            break
                        frame = json.loads(raw_frame)
                        frame_type = frame.get("type")
                        envelope_id = frame.get("envelope_id")

                        if envelope_id:
                            await ws.send(json.dumps({"envelope_id": envelope_id}))

                        if frame_type == "disconnect":
                            break

                        if frame_type in ("interactive", "block_actions"):
                            slack_payload = frame.get("payload", {})
                            if slack_payload.get("type") != "block_actions":
                                continue

                            normalized = self.extract_interaction_from_block_actions(slack_payload)
                            if not self.is_owned_by_this_node(normalized):
                                continue

                            self._resolve_wildcard_identity(normalized)

                            ts = str(int(time.time()))
                            raw_bytes = json.dumps(normalized, separators=(",", ":")).encode("utf-8")
                            sig = "v0=" + hmac.new(self.slack_secret, f"v0:{ts}:".encode() + raw_bytes, hashlib.sha256).hexdigest()

                            try:
                                res_status = await self.handle_slack_interaction(ts, raw_bytes, sig, normalized)
                                if normalized.get("response_url"):
                                    asyncio.create_task(
                                        self._update_slack_message_via_response_url(
                                            normalized["response_url"], normalized, res_status
                                        )
                                    )
                            except Exception as act_err:
                                log.error(f"[SlackBridge:SocketMode] Interaction rejected: {act_err}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"[SlackBridge:SocketMode] Reconnecting in 3s after error: {e}")
                await asyncio.sleep(3.0)