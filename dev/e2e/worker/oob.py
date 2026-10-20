# fiber.dev.e2e.worker.oob
import os
import sys
import json
import time
import hmac
import uuid
import hashlib
import asyncio
import urllib.parse
from typing import List, Optional

from fiber.infra.bound.e2e.config import Phase, E2EConfig, TestResult
from fiber.infra.bound.e2e.pipeline import BaseBridgePipeline, log
import fiber.infra.mcp.worker.deployer as worker_deployer

from fiber.infra.mcp.connector import MCPServerConnector
from fiber.gateway.bridge.mcp.slack import SlackHitlBridge
from fiber.dev.e2e.worker.fixture import AuditCryptoContext, MCPGatewayTestClient
from xphi.state.phase.reactor import PhaseReactor


class OutOfBandAuditPipeline(BaseBridgePipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config,
            name="Out-of-Band (Slack/CLI) Zero-Trust Approval Suite",
            scope_name="MCP_OOB_SUITE",
        )
        self.deploy_id = "legacy-01"
        self.slack_admin_id = os.environ.get("FIBER_SLACK_USER_ID", "U_SLACK_ADMIN_99")

        self.crypto_ctx = AuditCryptoContext(spiffe_id="spiffe://self/fiber")
        self.gw_client: Optional[MCPGatewayTestClient] = None
        self.slack_bridge: Optional[SlackHitlBridge] = None

        self.set_phases([
            Phase("Phase 1: Webhook Spoofing & Token Forgery Defense", self.phase_webhook_security_defense),
            Phase("Phase 2: Out-of-Band Slack Approval & Sandbox Resume", self.phase_oob_slack_resume),
        ])

    async def setup_custom_context(self):
        self.crypto_ctx.provision()
        self.gw_client = MCPGatewayTestClient(
            base_url=self.local_url,
            target_id=self.deploy_id,
            spiffe_id=self.crypto_ctx.spiffe_id,
        )
        self.slack_bridge = SlackHitlBridge(rpc=self.rpc)
        self.slack_bridge.register_identity_mapping(self.slack_admin_id, self.crypto_ctx.spiffe_id)
        self.slack_bridge.register_identity_mapping("*", self.crypto_ctx.spiffe_id)

        if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
            await self.slack_bridge.start_background_daemons()
            log.info(f"[OOBPipeline:LIVE] Live Slack Socket Mode Active! Target Channel: {self.slack_bridge.default_channel}")
        else:
            log.info(f"[OOBPipeline:SIM] Mapped Slack User '{self.slack_admin_id}' -> '{self.crypto_ctx.spiffe_id}' (Local Emulation Mode)")

    async def setup_workers(self):
        deploy_cmd = f"{sys.executable} -m {worker_deployer.__name__}"
        self.connectors.append(
            MCPServerConnector(target_id=self.deploy_id, execution_target=deploy_cmd, mode="ephemeral")
        )

    async def teardown_custom(self):
        if self.slack_bridge:
            await self.slack_bridge.stop_background_daemons()
        self.crypto_ctx.teardown()

    def _sign_mock_slack_body(self, raw_body: bytes, ts_override: Optional[int] = None) -> tuple[str, str]:
        ts = str(ts_override or int(time.time()))
        basestring = f"v0:{ts}:".encode("utf-8") + raw_body
        sig = "v0=" + hmac.new(self.slack_bridge.slack_secret, basestring, hashlib.sha256).hexdigest()
        return ts, sig

    # Phase 1: 가짜 웹훅 서명, 만료된 타임스탬프, 위조된 State Token 방어 검증
    async def phase_webhook_security_defense(self):
        state_token = self.slack_bridge.mint_approval_state_token(
            handle_id="txn_dummy_001", target_id=self.deploy_id, prompt_id="prompt_777"
        )
        interaction = {
            "state_token": state_token,
            "slack_user_id": self.slack_admin_id,
            "decision": "APPROVE",
            "otp_code": "000000",
        }
        raw_body = json.dumps(interaction, separators=(",", ":")).encode("utf-8")
        ts, valid_sig = self._sign_mock_slack_body(raw_body)

        # 잘못된 HMAC 서명 차단 검증
        try:
            await self.slack_bridge.handle_slack_interaction(ts, raw_body, "v0=invalid_forged_sig", interaction)
            raise RuntimeError("Failed to block forged Slack HMAC signature!")
        except PermissionError as e:
            assert "INVALID_WEBHOOK_SIGNATURE" in str(e)

        # 10분 지난 Stale Timestamp (Replay Attack) 차단 검증
        stale_ts, stale_sig = self._sign_mock_slack_body(raw_body, ts_override=int(time.time()) - 600)
        try:
            await self.slack_bridge.handle_slack_interaction(stale_ts, raw_body, stale_sig, interaction)
            raise RuntimeError("Failed to block stale webhook replay!")
        except PermissionError as e:
            assert "INVALID_WEBHOOK_SIGNATURE" in str(e)

        # 공격자가 handle_id를 변조한 경우 (NodeSigner State Token 서명 불일치) 차단 검증
        tampered = dict(interaction)
        tampered["state_token"] = {
            "envelope": {**state_token["envelope"], "handle_id": "txn_hijacked_999"},
            "sig": state_token["sig"],
        }
        t_body = json.dumps(tampered, separators=(",", ":")).encode("utf-8")
        t_ts, t_sig = self._sign_mock_slack_body(t_body)
        try:
            await self.slack_bridge.handle_slack_interaction(t_ts, t_body, t_sig, tampered)
            raise RuntimeError("Failed to block tampered handle_id in State Token!")
        except PermissionError as e:
            assert "FORGED_STATE_TOKEN" in str(e)

        log.info("   └─ Forged HMAC, Timestamp Replay, and Handle Hijacking blocked 100%.")

    # Phase 2: Adaptive (Local Block-Kit Emulation OR Clean Live Slack HUD)
    async def phase_oob_slack_resume(self):
        idem_key = uuid.uuid4().hex
        payload = self.gw_client.build_migration_payload(
            req_id=999, service_name="payment_ledger", sql_script="DROP TABLE"
        )

        # 에이전트가 파괴적 명령 호출 -> HTTP 202 (YIELD) 파킹 및 telemetry 이벤트 자동 발생
        res1 = await self.gw_client.invoke(payload, idempotency_key=idem_key)
        if res1.status_code != 202:
            raise RuntimeError(f"Expected 202 YIELD, got {res1.status_code}")

        location = res1.headers.get("Location", "")
        handle_id = location.split("/")[-1]
        prompt_payload = res1.json()

        # 분기 A: Live Slack 모드
        if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
            await asyncio.sleep(0.4)

            print("\n" + "=" * 80)
            print(f"📲 [LIVE SLACK HITL] Out-of-Band Cryptographic Sudo Requested")
            print(f"   • Target Channel : {self.slack_bridge.default_channel}  |  Handle : {handle_id}")
            print("-" * 80)

            frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            start_wait = time.time()

            for tick in range(300):
                state_obj = await self.mock_store.query_state(handle_id)
                cur_status = state_obj.metadata.get("status") if state_obj else "YIELD"

                if cur_status == "RESOLVED":
                    elapsed = time.time() - start_wait
                    sys.stdout.write(
                        f"\r\033[K   [SIGNAL RECEIVED] Slack Approval Verified & Sandbox Unlocked! ({elapsed:.1f}s)\n"
                    )
                    sys.stdout.flush()
                    print("=" * 80 + "\n")

                    res_final = await self.gw_client.invoke(payload, idempotency_key=idem_key)
                    if res_final.status_code == 200:
                        log.info("   └─ ✨ [LIVE SLACK] Interactive Button Click -> Sandbox 200 OK Verified!")
                        return
                    raise RuntimeError(f"Expected 200 OK after resolution, got {res_final.status_code}")

                if cur_status == "FAULTED":
                    sys.stdout.write("\r\033[K   [ABORTED] Execution Denied or Faulted from Slack!\n")
                    sys.stdout.flush()
                    print("=" * 80 + "\n")
                    raise RuntimeError("Execution rolled back or faulted by operator in Slack.")

                totp_now = self.crypto_ctx.generate_totp()
                rem_sec = 30 - (int(time.time()) % 30)
                elapsed = time.time() - start_wait
                spinner = frames[tick % len(frames)]

                sys.stdout.write(
                    f"\r\033[K   {spinner} ⏳ [PARKED: YIELD] Enter TOTP [ \033[1;32m{totp_now}\033[0m ] "
                    f"(renews in {rem_sec:02d}s) in Slack... ({elapsed:.1f}s)"
                )
                sys.stdout.flush()
                await asyncio.sleep(0.2)

            print("\n" + "=" * 80)
            raise RuntimeError("Timed out waiting for live Slack approval click (60s).")

        # 분기 B: 토큰 미설정 시 -> 로컬 에뮬레이션 모드
        yield_event = {
            "event": "MCP_PHASE_YIELD",
            "handle_id": handle_id,
            "target_server_id": self.deploy_id,
            "agent": self.crypto_ctx.spiffe_id,
            "prompt": prompt_payload,
        }
        block_kit_msg = self.slack_bridge.build_yield_message_blocks(yield_event, channel="C_MOCK_CHAN")
        approve_btn_val = block_kit_msg["blocks"][4]["elements"][0]["value"]

        valid_totp = self.crypto_ctx.generate_totp()
        real_slack_block_actions = {
            "type": "block_actions",
            "user": {"id": self.slack_admin_id, "username": "admin"},
            "actions": [{"action_id": "fiber_action_approve", "value": approve_btn_val}],
            "state": {
                "values": {
                    "fiber_totp_block": {
                        "fiber_totp_input": {"type": "plain_text_input", "value": valid_totp}
                    }
                }
            },
        }
        form_encoded_body = urllib.parse.urlencode(
            {"payload": json.dumps(real_slack_block_actions)}
        ).encode("utf-8")
        ts, slack_sig = self._sign_mock_slack_body(form_encoded_body)

        oob_res = await self.slack_bridge.handle_http_webhook(ts, form_encoded_body, slack_sig)
        assert oob_res["status"] == "RESUMED"

        try:
            await self.slack_bridge.handle_http_webhook(ts, form_encoded_body, slack_sig)
            raise RuntimeError("Failed to block duplicate button click!")
        except PermissionError as e:
            assert "DUPLICATE_APPROVAL_REPLAY" in str(e)

        for _ in range(20):
            state_obj = await self.mock_store.query_state(handle_id)
            if state_obj and state_obj.metadata.get("status") == "RESOLVED":
                res_poll = await self.gw_client.invoke(payload, idempotency_key=idem_key)
                if res_poll.status_code == 200:
                    log.info("   └─ ✨ Slack Block-Kit & Form-Urlencoded Webhook -> Bus RESUME -> 200 OK Verified.")
                    return
            await asyncio.sleep(0.2)

        raise RuntimeError("Sandbox did not resolve after Out-of-Band RESUME.")


class OOBSuiteRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        net_config = E2EConfig(host="127.0.0.1", port=8355, protocol="http")
        self.results.extend(await OutOfBandAuditPipeline(config=net_config).run_pipeline())
        self._print_report()

    def _print_report(self):
        self.log.info("\n" + "=" * 80)
        self.log.info("[OUT-OF-BAND SLACK HITL BENCHMARK REPORT]")
        self.log.info("=" * 80)
        all_passed = all(r.passed for r in self.results)

        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            self.log.info(
                f"{status_icon} {idx:02d}. [{res.target}]".ljust(22)
                + f"{res.scenario.ljust(50)} | {'PASSED' if res.passed else 'FAILED'}"
            )

        self.log.info("-" * 80)
        if all_passed:
            self.log.info("OUT-OF-BAND HITL PIPELINE VALIDATED SUCCESSFULLY.")
        else:
            self.log.critical("SECURITY FRACTURE DETECTED. Check logs for details.")
        self.log.info("=" * 80 + "\n")

def main():
    app = OOBSuiteRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()