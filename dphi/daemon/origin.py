# fiber.dphi.daemon.origin
## @lineage: fiber.dphi.infra.daemon.origin
import os
import json
import time
import asyncio
import hashlib
from contextlib import suppress
from typing import Dict, Any, Optional

import aiohttp

from fiber.dphi.eco.config.exchange import dphi_env
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.adapter.sign import NodeSigner
from xphi.kernel.adapter.state import StateAdapter
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.origin")

# =========================================================================
# Origin Delegator Daemon
# =========================================================================
@contract.daemon("origin_delegator")
class OriginDelegatorDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("OriginDelegatorDaemon")
        self.ctx = ctx
        
        # 1. 인프라 및 라우팅 설정 로드
        self.origin_url = dphi_env.delegation.origin_url
        self.allow_fallback = dphi_env.delegation.allow_fallback
        
        # 2. 오리진 무결성 검증을 위한 신뢰된 위원회 공개키
        self.trusted_witnesses = set(dphi_env.export_attestation.witness_pubkeys)
        
        # 3. 게이트웨이 자체의 신원(Signer) - 오리진에 위탁 시 DPoP 래핑에 사용
        # 시스템 마스터 키를 가져오거나 기본 NodeSigner 인스턴스 사용
        self.signer = NodeSigner.get_instance() 
        
        self.tunnel = None
        self.client_session: Optional[aiohttp.ClientSession] = None
        self.listen_channel = "mcp.intent.queue.origin"
        self._tasks = set()

    async def run(self):
        log.info(f"[{self.name}] Initiating Origin Delegation Sidecar...")
        
        if not self.allow_fallback:
            log.warning(f"[{self.name}] Origin fallback is DISABLED in config. Daemon will hibernate.")
            return

        try:
            self.tunnel = await TunnelFactory.get_default()
            self.client_session = aiohttp.ClientSession()
            
            pubsub = self.tunnel.pubsub()
            await pubsub.subscribe(self.listen_channel)
            log.info(f"[{self.name}] 🚀 Listening for delegation intents on: {self.listen_channel}")
            log.info(f"[{self.name}] Target Origin URL: {self.origin_url}")

            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg and msg["type"] == "message":
                    intent_data = json.loads(msg["data"])
                    # 병목 방지를 위해 위탁 처리 로직을 비동기 Task로 분리 (Multiplexing)
                    task = asyncio.create_task(self._process_delegation(intent_data))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal execution error: {e}", exc_info=True)
        finally:
            await self._teardown()

    async def _process_delegation(self, intent_data: Dict[str, Any]):
        """오리진 위탁, 통신, 검증을 관장하는 핵심 파이프라인"""
        handle_id = intent_data.get("handle_id")
        action = intent_data.get("action", "EXECUTE")
        payload = intent_data.get("payload", {})
        
        if not handle_id:
            return

        reply_channel = f"mcp.intent.reply.{handle_id}"
        log.info(f"[Delegator:{handle_id}] Forwarding intent to Origin -> {action}")

        try:
            # @phase.1: DPoP Wrapping & X402 Injection
            # 오리진으로 보낼 요청을 게이트웨이의 프라이빗 키로 서명하여 무결성 보장
            target_endpoint = f"{self.origin_url}/v1/mcp-gateway/invoke"
            wrapped_headers = self._build_delegation_headers(target_endpoint, payload)

            # @phase.2: Execute Upstream Request
            async with self.client_session.post(
                target_endpoint, 
                json=payload, 
                headers=wrapped_headers,
                timeout=30.0
            ) as resp:
                resp.raise_for_status()
                origin_response = await resp.json()

            # @phase.3: Verify Origin Attestation (수학적 무결성 검증)
            verified_payload = self._verify_and_unwrap(origin_response)

            # @phase.4: Resolve Local State
            # 성공 시 로컬 브릿지(TransitionBridge)가 기다리는 채널로 결과 발행
            log.info(f"[Delegator:{handle_id}] ✨ Origin response verified successfully.")
            await self.tunnel.publish(reply_channel, json.dumps({
                "status": "RESOLVED",
                "executable_payload": verified_payload,
                "error_detail": ""
            }))

        except Exception as e:
            log.error(f"[Delegator:{handle_id}] Delegation Failed: {str(e)}")
            # 에러 발생 시 로컬 게이트웨이가 무한 대기하지 않도록 FAULT 상태 발행
            await self.tunnel.publish(reply_channel, json.dumps({
                "status": "FAULTED",
                "executable_payload": {},
                "error_detail": f"Origin Delegation Error: {str(e)}"
            }))

    def _build_delegation_headers(self, target_url: str, payload: dict) -> Dict[str, str]:
        """로컬 요청을 오리진으로 위임하기 위한 헤더 세트 구성 (Gateway Identity)"""
        canonical_payload = StateAdapter.to_canonical_bytes(payload).decode('utf-8')
        payload_hash = hashlib.sha256(canonical_payload.encode()).hexdigest()
        
        # DPoP 토큰 구성 요소
        nonce = os.urandom(16).hex()
        timestamp = int(time.time() * 1000)
        
        dpop_message = f"POST:{target_url}:{payload_hash}:{nonce}:{timestamp}"
        signature = self.signer.sign_payload(dpop_message.encode())
        
        # 게이트웨이 법인의 마스터 결제 영수증 (설정에서 주입)
        master_receipt = os.getenv("GATEWAY_MASTER_X402_RECEIPT", "gateway_sponsored_x402_mock")

        return {
            "Content-Type": "application/json",
            "x-spiffe-id": f"spiffe://gateway/{self.signer.pubkey_hex[:16]}",
            "x-nonce": nonce,
            "x-timestamp": str(timestamp),
            "DPoP": signature,
            "X-X402-Receipt": master_receipt,
            "X-Delegated-From": "local_mesh"
        }

    def _verify_and_unwrap(self, origin_response: dict) -> dict:
        """오리진에서 반환한 결과가 위조되지 않았음을 수학적으로 증명"""
        # 1. 일반적인 엣지 에러나 거절(Reject)인 경우 검증 패스 (에러는 바로 던짐)
        if origin_response.get("error"):
            raise RuntimeError(origin_response.get("error").get("message", "Origin Returned Error"))

        # 2. 오리진은 성공 시 반드시 attestation(증명) 객체를 포함해야 함
        attestation = origin_response.get("attestation")
        if not attestation:
            log.warning("Origin response missing 'attestation' block. Assuming untrusted or legacy response.")
            # 강경한 Zero-Trust 정책에 따라 증명이 없으면 거부
            raise ValueError("Missing cryptographic attestation from Origin.")

        canonical_root = attestation.get("canonical_root")
        signature = attestation.get("signature")

        # 3. 서명 검증 (로컬에 저장된 위원회 공개키 활용)
        is_valid = False
        # (Mock) 실제 암호학적 검증 로직은 xphi.kernel.adapter.sign 에 의존
        for pubkey in self.trusted_witnesses:
            if self.signer.verify_signature_static(canonical_root.encode(), signature, pubkey):
                is_valid = True
                break
                
        if not is_valid:
            raise ValueError("Cryptographic verification failed: Origin signature is forged or unrecognized.")

        # 4. 검증 통과 시 순수 데이터(Observation/Payload)만 추출하여 반환
        return origin_response.get("observation", origin_response.get("payload", origin_response))

    async def _teardown(self):
        log.info(f"[{self.name}] Releasing Origin Delegator resources...")
        
        if self.client_session and not self.client_session.closed:
            await self.client_session.close()
            
        for task in list(self._tasks):
            if not task.done():
                task.cancel()
                
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        log.info(f"[{self.name}] Teardown complete.")