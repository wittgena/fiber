# fiber.dev.e2e.gateway.worker.hitl
## @lineage: fiber.dev.e2e.worker.hitl
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
from typing import List, Dict, Any, Optional

import httpx
import redis.exceptions

from fiber.dev.base.workflow.config import Phase, E2EConfig, TestResult, get_free_port
from fiber.dev.base.workflow.pipeline import MockPipeline, log
from fiber.dev.base.workflow.mcp import MockMcpServer
from fiber.gateway.bridge.action.connect import resolve_connector_spec
from fiber.infra.mcp.connector import MCPServerConnector
from fiber.infra.mcp.payload import McpPayloadBuilder

from fiber.dev.base.workflow.audit.context import AuditContext
from fiber.gateway.bridge.slack import SlackHitlBridge

from xphi.arch.contract.config.env import DEFAULT_GATEWAY_PORT
from xphi.state.phase.reactor import PhaseReactor
from xphi.arch.bound.adapter.gateway import DPoPClientGenerator
from xphi.node.space.tunnel.factory import TunnelFactory


class ThirdPartyPreflightHitlPipeline(MockPipeline):
    def __init__(self, config: E2EConfig):
        super().__init__(
            config=config,
            name="3rd-Party Zero-Code Pre-Flight HITL (TOTP & 1-Click Button) Suite",
            scope_name="MCP_PREFLIGHT_HITL_SUITE",
        )
        self.mcp = McpPayloadBuilder()

        # 검증 대상 타겟 ID 2종 (Network/Multiplex + TOTP vs Stdio/Ephemeral + 1-Click Button)
        self.net_target_id = "net-context7-remote-01"
        self.stdio_target_id = "hitl-stdio-fs-01"

        # 1. Network Target (Context7) 설정
        self.env_key_name = "CONTEXT7_API_KEY"
        user_key = os.environ.get(self.env_key_name, "").strip()
        self._injected_temp_env = not bool(user_key)
        self.active_api_key = user_key or f"ctx7_live_e2e_{uuid.uuid4().hex[:16]}"
        os.environ[self.env_key_name] = self.active_api_key

        self.mock_mcp = MockMcpServer(host="127.0.0.1", port=8360, expected_api_key=self.active_api_key)
        self.context7_url = os.environ.get("CONTEXT7_MCP_URL", "").strip() or self.mock_mcp.context7_url

        # 2. Stdio Target (npx @modelcontextprotocol/server-filesystem) 용 샌드박스 디렉토리 준비
        self.temp_dir = tempfile.TemporaryDirectory(prefix="fiber_hitl_fs_")
        self.sandbox_path = Path(self.temp_dir.name).resolve()
        (self.sandbox_path / "public_readme.txt").write_text("SAFE_PUBLIC_CONTENT", encoding="utf-8")
        (self.sandbox_path / "prod_secrets.env").write_text("SUPER_SECRET_KEY=12345", encoding="utf-8")

        # 3. HITL 서명 및 TOTP 검증 컨텍스트
        self.crypto_ctx = AuditContext(spiffe_id="spiffe://self/fiber")
        self.slack_bridge: Optional[SlackHitlBridge] = None
        self.slack_admin_id = os.environ.get("FIBER_SLACK_USER_ID", "U_SLACK_ADMIN_99")
        self.dpop_client = DPoPClientGenerator(key_size=2048)
        self.http_client: Optional[httpx.AsyncClient] = None

        self.set_phases([
            Phase("Phase 1: Connect Spec Resolver & Dual Mode (TOTP / Button) Guard Arming", self.phase_resolver_and_guard_arming),
            Phase("Phase 2: Safe Call Passthrough (Zero-Overhead on Non-Matching Payload)", self.phase_safe_passthrough),
            Phase("Phase 3: [Network/Multiplex + TOTP] Context7 Trap & TOTP Attested Resume", self.phase_network_multiplex_totp_hitl),
            Phase("Phase 4: [Stdio/Ephemeral + 1-Click Button] Zero-Spawn Trap & Button-Only Resume", self.phase_stdio_ephemeral_button_hitl),
        ])

    async def setup_custom_context(self) -> None:
        self.http_client = httpx.AsyncClient(base_url=self.local_url, timeout=25.0)
        await self.mock_mcp.start()

        self.crypto_ctx.provision()
        self.slack_bridge = SlackHitlBridge(rpc=self.rpc)
        self.slack_bridge.register_identity_mapping(self.slack_admin_id, self.crypto_ctx.spiffe_id)
        self.slack_bridge.register_identity_mapping("*", self.crypto_ctx.spiffe_id)

        if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
            await self.slack_bridge.start_background_daemons()
            log.info(f"[HITLPipeline:LIVE] Live Slack Socket Mode Active! Target Channel: {self.slack_bridge.default_channel}")
        else:
            log.info(f"[HITLPipeline:SIM] Mapped Slack User '{self.slack_admin_id}' -> '{self.crypto_ctx.spiffe_id}' (Local Emulation Mode)")

    async def teardown_custom(self) -> None:
        if self.slack_bridge:
            await self.slack_bridge.stop_background_daemons()
        self.crypto_ctx.teardown()
        if self._injected_temp_env:
            os.environ.pop(self.env_key_name, None)
        if self.http_client:
            await self.http_client.aclose()
        await self.mock_mcp.stop()
        self.temp_dir.cleanup()

    async def setup_workers(self) -> None:
        stdio_npx_cmd = f"npx -y @modelcontextprotocol/server-filesystem {self.sandbox_path}"

        targets = [
            # 구조 1: URL 기반 (network/multiplex) + 기본 TOTP 모드 (require_totp=True)
            (self.net_target_id, self.context7_url, "RESTRICTED_TOPIC|DROP"),
            # 구조 2: CLI 기반 (stdio/ephemeral) + "button:" 접두어를 통한 1-Click 버튼 단독 모드 (require_totp=False)
            (self.stdio_target_id, stdio_npx_cmd, "button:prod_secrets|DELETE|DROP"),
        ]

        for tid, exec_cmd, preflight_rule in targets:
            spec = resolve_connector_spec(target=tid, exec_cmd=exec_cmd, preflight=preflight_rule)
            self.connectors.append(
                MCPServerConnector(
                    target_id=spec.target_id,
                    execution_target=spec.exec_cmd,
                    mode=spec.mode,
                    transport_type=spec.transport,
                    preflight_pattern=spec.preflight,
                )
            )

    def _get_connector(self, target_id: str) -> MCPServerConnector:
        return next(c for c in self.connectors if c.target_id == target_id)

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

    async def _resolve_hitl_adaptive(
        self,
        target_id: str,
        handle_id: str,
        prompt_payload: Dict[str, Any],
        orig_payload: Dict[str, Any],
        idem_key: str,
        expect_totp: bool,
    ) -> str:
        """
        [이벤트 구동 & 경계 분리 리팩토링 적용]
        UniversalPubSub(`tunnel.pubsub()`)을 통해 원장 상태 확정(Pub/Sub)과 
        샌드박스 실행(Gateway Timeout)의 타임아웃 경계를 명확하게 분리합니다.
        """
        prompt_params = prompt_payload.get("params", {})
        actual_require_totp = bool(prompt_params.get("require_totp", True))
        if actual_require_totp != expect_totp:
            raise RuntimeError(
                f"Prompt require_totp mismatch on {target_id}: expected {expect_totp}, got {actual_require_totp}"
            )

        mode_badge = "TOTP-6DIGIT" if expect_totp else "1-CLICK BUTTON"

        # 0. 레이스 컨디션 방지를 위해 액션 전에 미리 Pub/Sub 객체 생성 및 채널 구독
        tunnel = await TunnelFactory.get_default()
        pubsub_channel = f"mcp.intent.reply.{handle_id}"
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(pubsub_channel)

        async def listen_for_ledger() -> Dict[str, Any]:
            # UniversalFacade의 socket_timeout(5.0s)으로 인해 장시간 대기 시 발생할 수 있는
            # redis.exceptions.TimeoutError를 무시하고 get_message 폴링/대기를 이어갑니다.
            while True:
                try:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg and msg.get("type") == "message":
                        raw_data = msg["data"]
                        return json.loads(raw_data) if isinstance(raw_data, (str, bytes)) else raw_data
                except redis.exceptions.TimeoutError:
                    continue
                await asyncio.sleep(0.05)

        try:
            # 분기 A: Live Slack 모드 (실제 Slack 메시지 발송 및 사용자 액션 대기)
            if self.slack_bridge.is_live_configured and self.slack_bridge.app_token:
                await asyncio.sleep(0.4)

                print("\n" + "=" * 80)
                print(f"📲 [LIVE SLACK PRE-FLIGHT HITL | Mode: {mode_badge}] Target: {target_id}")
                print(f"   • Target Channel : {self.slack_bridge.default_channel}  |  Handle : {handle_id}")
                print("-" * 80)

                start_wait = time.time()

                async def spinner_task():
                    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                    tick = 0
                    while True:
                        elapsed = time.time() - start_wait
                        spinner_icon = frames[tick % len(frames)]
                        if expect_totp:
                            totp_now = self.crypto_ctx.generate_totp()
                            rem_sec = 30 - (int(time.time()) % 30)
                            sys.stdout.write(
                                f"\r\033[K   {spinner_icon} ⏳ [PARKED: TOTP] Enter TOTP [ \033[1;32m{totp_now}\033[0m ] "
                                f"(renews in {rem_sec:02d}s) in Slack... ({elapsed:.1f}s)"
                            )
                        else:
                            sys.stdout.write(
                                f"\r\033[K   {spinner_icon} ⚡ [PARKED: 1-CLICK] Click [ \033[1;36mApprove & Resume\033[0m ] "
                                f"button in Slack (No TOTP required)... ({elapsed:.1f}s)"
                            )
                        sys.stdout.flush()
                        tick += 1
                        await asyncio.sleep(0.2)

                spinner = asyncio.create_task(spinner_task())

                try:
                    # 60초간 사용자 액션 및 원장 합의 이벤트를 대기
                    event_data = await asyncio.wait_for(listen_for_ledger(), timeout=60.0)
                except asyncio.TimeoutError:
                    print("\n" + "=" * 80)
                    raise RuntimeError("Timed out waiting for live Slack approval click (60s).")
                finally:
                    spinner.cancel()
                    await asyncio.gather(spinner, return_exceptions=True)

                status = event_data.get("status")
                if status == "RESOLVED":
                    elapsed = time.time() - start_wait
                    sys.stdout.write(
                        f"\r\033[K   [SIGNAL RECEIVED] Slack {mode_badge} Verified & Pre-Flight Unlocked! ({elapsed:.1f}s)\n"
                    )
                    sys.stdout.flush()
                    print("=" * 80 + "\n")
                elif status == "FAULTED":
                    err_detail = event_data.get("error_detail", "Unknown")
                    sys.stdout.write(f"\r\033[K   [ABORTED] Execution Denied or Faulted: {err_detail}\n")
                    sys.stdout.flush()
                    print("=" * 80 + "\n")
                    raise RuntimeError(f"Execution faulted on {target_id}: {err_detail}")

            # 분기 B: 로컬 웹훅 에뮬레이션 모드 (UI 블록 구조 및 State payload 정밀 검증)
            else:
                yield_event = {
                    "event": "MCP_PHASE_YIELD",
                    "handle_id": handle_id,
                    "target_server_id": target_id,
                    "agent": self.crypto_ctx.spiffe_id,
                    "prompt": prompt_payload,
                }
                block_kit_msg = self.slack_bridge.build_yield_message_blocks(yield_event, channel="C_HITL_TEST")
                auth_slot_block = block_kit_msg["blocks"][3]
                approve_btn_val = block_kit_msg["blocks"][4]["elements"][0]["value"]

                if expect_totp:
                    if auth_slot_block.get("block_id") != "fiber_totp_block":
                        raise RuntimeError("Expected 'fiber_totp_block' input in Block-Kit for TOTP mode.")
                    state_values = {
                        "fiber_totp_block": {
                            "fiber_totp_input": {"type": "plain_text_input", "value": self.crypto_ctx.generate_totp()}
                        }
                    }
                else:
                    if auth_slot_block.get("block_id") == "fiber_totp_block":
                        raise RuntimeError("TOTP input block should NOT be rendered in 1-Click Button mode!")
                    state_values = {}

                action_payload = {
                    "type": "block_actions",
                    "user": {"id": self.slack_admin_id, "username": "admin"},
                    "actions": [{"action_id": "fiber_action_approve", "value": approve_btn_val}],
                    "state": {"values": state_values},
                }
                form_body = urllib.parse.urlencode({"payload": json.dumps(action_payload)}).encode("utf-8")
                ts = str(int(time.time()))
                sig = "v0=" + hmac.new(self.slack_bridge.slack_secret, f"v0:{ts}:".encode() + form_body, hashlib.sha256).hexdigest()

                oob_res = await self.slack_bridge.handle_http_webhook(ts, form_body, sig)
                if oob_res.get("status") != "RESUMED":
                    raise RuntimeError(f"Expected OOB status RESUMED ({mode_badge}), got {oob_res}")

                # [경계 1] 원장 합의 대기 (순수 원장 I/O 및 실행 완료 통보 대기)
                try:
                    event_data = await asyncio.wait_for(listen_for_ledger(), timeout=15.0)
                except asyncio.TimeoutError:
                    raise RuntimeError(f"Handle {handle_id} on {target_id} did not resolve within timeout (AnchorStore Consensus stalled).")

                status = event_data.get("status")
                if status == "RESOLVED":
                    log.info(f"  ├─ [Boundary 1] AnchorStore Consensus verified via PubSub ({handle_id}).")
                elif status == "FAULTED":
                    err_detail = event_data.get("error_detail", "Unknown")
                    raise RuntimeError(f"Handle {handle_id} FAULTED after RESUME ({mode_badge}): {err_detail}")

            # [경계 2] 샌드박스 실행 및 게이트웨이 응답 대기 (OS 종속적: 느리고 불확정적인 구간, Timeout 25.0초)
            res_final = await self._post_gateway(target_id, orig_payload, idem_key=idem_key, timeout=25.0)
            if res_final.status_code == 200:
                log.info(f"  ├─ [Boundary 2] External Sandbox Execution completed.")
                return McpPayloadBuilder.extract_data(res_final.content, parse_json=False)

            raise RuntimeError(f"Expected 200 OK after resolution, got {res_final.status_code}: {res_final.text}")

        finally:
            try:
                await pubsub.unsubscribe(pubsub_channel)
                await pubsub.close()
            except Exception:
                pass

    # =========================================================================
    # Phase 1: resolve_connector_spec 추론 및 Dual Mode(TOTP / Button) 장착 검증
    # =========================================================================
    async def phase_resolver_and_guard_arming(self) -> None:
        net_conn = self._get_connector(self.net_target_id)
        stdio_conn = self._get_connector(self.stdio_target_id)

        if (
            net_conn.transport_type != "network"
            or net_conn.mode != "multiplex"
            or not net_conn.fsm.preflight.guard_regex
            or net_conn.fsm.preflight.require_totp is not True
        ):
            raise RuntimeError("Network connector failed to arm as network/multiplex with TOTP mode (require_totp=True).")

        if (
            stdio_conn.transport_type != "stdio"
            or stdio_conn.mode != "ephemeral"
            or not stdio_conn.fsm.preflight.guard_regex
            or stdio_conn.fsm.preflight.require_totp is not False
        ):
            raise RuntimeError("Stdio connector failed to arm as stdio/ephemeral with Button mode (require_totp=False).")

        log.info(
            f"  └─ Guard Armed: [{self.net_target_id} -> NETWORK/MULTIPLEX (Mode: TOTP)] & "
            f"[{self.stdio_target_id} -> STDIO/EPHEMERAL (Mode: 1-CLICK BUTTON)]"
        )

    # =========================================================================
    # Phase 2: 정규식 미매칭(Safe) 요청의 무정지 통과(200 OK) 검증
    # =========================================================================
    async def phase_safe_passthrough(self) -> None:
        safe_file = str(self.sandbox_path / "public_readme.txt")
        payload = self.mcp.next_tool_call("read_file", path=safe_file)

        res = await self._post_gateway(self.stdio_target_id, payload)
        if res.status_code != 200:
            raise RuntimeError(f"Safe call should return 200 OK immediately, got {res.status_code}")

        content = McpPayloadBuilder.extract_data(res.content, parse_json=False)
        if "SAFE_PUBLIC_CONTENT" not in content:
            raise RuntimeError(f"Unexpected file content: {content}")

        log.info("  └─ Safe Stdio Call ('public_readme.txt') bypassed Pre-Flight Guard with 200 OK.")

    # =========================================================================
    # Phase 3: [Network/Multiplex + TOTP 모드] Context7 차단 및 TOTP 검증 재개
    # =========================================================================
    async def phase_network_multiplex_totp_hitl(self) -> None:
        net_conn = self._get_connector(self.net_target_id)
        idem_key = uuid.uuid4().hex
        payload = self.mcp.next_tool_call(
            "get-library-docs",
            context7CompatibleLibraryID="/duckdb/duckdb",
            libraryId="/duckdb/duckdb",
            libraryName="duckdb",
            topic="RESTRICTED_TOPIC",  # Guard 정규식("RESTRICTED_TOPIC|DROP") 매칭 -> TOTP 요구
            query="RESTRICTED_TOPIC",
        )

        # 1. Pre-flight 차단 (202 YIELD) 및 Stash 검증
        res_yield = await self._post_gateway(self.net_target_id, payload, idem_key=idem_key)
        if res_yield.status_code != 202:
            raise RuntimeError(f"Expected 202 YIELD on Context7 preflight, got {res_yield.status_code}")

        handle_id = res_yield.headers.get("Location", "").split("/")[-1]
        stashed = net_conn.fsm.preflight.stashed_payloads.get(handle_id)
        if not stashed or stashed.get("require_totp") is not True:
            raise RuntimeError("Context7 payload was not stashed with require_totp=True.")

        log.info(f"  ├─ [Network/Multiplex | TOTP] Trapped 'RESTRICTED_TOPIC' prior to HTTP I/O (Handle: {handle_id})")

        # 2. 6자리 TOTP 승인 및 Shared Transport 재개 검증
        content = await self._resolve_hitl_adaptive(
            self.net_target_id, handle_id, res_yield.json(), payload, idem_key, expect_totp=True
        )
        log.info(f"  └─ [Network/Multiplex | TOTP] 6-Digit TOTP Verified -> Context7 Stream Resumed ({len(content)} bytes)")

    # =========================================================================
    # Phase 4: [Stdio/Ephemeral + 1-Click Button 모드] Zero-Spawn 차단 및 버튼 단독 승인 재개
    # =========================================================================
    async def phase_stdio_ephemeral_button_hitl(self) -> None:
        stdio_conn = self._get_connector(self.stdio_target_id)
        idem_key = uuid.uuid4().hex
        secret_file = str(self.sandbox_path / "prod_secrets.env")

        # Guard 정규식("button:prod_secrets|DELETE|DROP") 매칭 -> 1-Click Button 요구
        payload = self.mcp.next_tool_call("read_file", path=secret_file)

        # 1. Pre-flight 차단 (202 YIELD)
        res_yield = await self._post_gateway(self.stdio_target_id, payload, idem_key=idem_key)
        if res_yield.status_code != 202:
            raise RuntimeError(f"Expected 202 YIELD on Stdio preflight, got {res_yield.status_code}")

        handle_id = res_yield.headers.get("Location", "").split("/")[-1]

        # [핵심 검증] 승인 전에는 npx 서브프로세스가 아예 생성되지 않아야 함 (Zero-Spawn)
        if handle_id in stdio_conn.active_sandboxes or len(stdio_conn.active_sandboxes) > 0:
            raise RuntimeError("SECURITY VIOLATION: npx process was spawned before Pre-Flight Button approval!")

        stashed = stdio_conn.fsm.preflight.stashed_payloads.get(handle_id)
        if not stashed or stashed.get("require_totp") is not False:
            raise RuntimeError("Stdio payload was not stashed with require_totp=False.")

        log.info(f"  ├─ [Stdio/Ephemeral | 1-Click] Zero-Spawn Confirmed (active_sandboxes=0 while parked at {handle_id})")

        # 2. TOTP 입력 없이 Approve 버튼 단독 클릭 승인 -> Lazy Spawn 동작 검증
        content = await self._resolve_hitl_adaptive(
            self.stdio_target_id, handle_id, res_yield.json(), payload, idem_key, expect_totp=False
        )

        if "SUPER_SECRET_KEY=12345" not in content:
            raise RuntimeError(f"Expected secret file content after button approval, got: {content}")

        # 3. Ephemeral 프로세스 종료(Teardown) 확인
        if handle_id in stdio_conn.active_sandboxes:
            raise RuntimeError("Ephemeral sandbox was not cleaned up after resolution.")

        log.info("  └─ [Stdio/Ephemeral | 1-Click] Button-Only Approved -> Lazy npx Spawn -> File Read -> Sandbox Destroyed!")


async def run_suite() -> None:
    pipeline = ThirdPartyPreflightHitlPipeline(
        E2EConfig(host="127.0.0.1", port=get_free_port(DEFAULT_GATEWAY_PORT), protocol="http")
    )
    results: List[TestResult] = await pipeline.run_pipeline()
    all_passed = all(r.passed for r in results)

    lines = ["\n" + "=" * 90, "🛡️ [3RD-PARTY ZERO-CODE PRE-FLIGHT HITL (TOTP & 1-CLICK BUTTON) E2E REPORT]", "=" * 90]
    for idx, r in enumerate(results, 1):
        lines.append(
            f"{'✅' if r.passed else '❌'} {idx:02d}. [{r.target}]".ljust(22)
            + f"{r.scenario.ljust(60)} | {'PASSED' if r.passed else 'FAILED'}"
        )
    lines.extend([
        "-" * 90,
        "PRE-FLIGHT HITL PIPELINE VALIDATED SUCCESSFULLY." if all_passed else "PRE-FLIGHT HITL FAILURE DETECTED.",
        "=" * 90 + "\n",
    ])
    print("\n".join(lines), flush=True)


def main(args_list: Optional[List[str]] = None) -> None:
    PhaseReactor.ignite(main_coro_func=run_suite)


if __name__ == "__main__":
    main()