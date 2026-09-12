# fiber.dphi.daemon.pricing
import asyncio
import json
from typing import Dict, Any
from contextlib import suppress

from fiber.dphi.eco.client.rpc import InternalRpcClient
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.ops.daemon.base import AbstractDaemon
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("daemon.pricing")

class TelemetrySentinel:
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.pubsub = tunnel.pubsub()

    async def run_listening_loop(self, callback_func):
        """텔레메트리 버스를 감시하다가 데이터가 오면 콜백(Vault)을 트리거합니다."""
        await self.pubsub.subscribe("eco.telemetry.events")
        try:
            async for msg in self.pubsub.listen():
                if msg and msg["type"] == "message":
                    telemetry_data = json.loads(msg["data"])
                    await callback_func(telemetry_data)
        finally:
            await self.pubsub.unsubscribe()

class PricingRiskManager:
    def __init__(self, rpc_client: InternalRpcClient):
        self.rpc = rpc_client
        self.MAX_PRICE_USD = 0.5   ## 상한선 (Hard Cap)
        self.MIN_PRICE_USD = 0.001 ## 하한선 (Hard Floor)

    async def calculate_and_verify_price(self, telemetry: Dict[str, Any]) -> float:
        """
        [개선] 내부 순수 RPC 망(eco.margin.calculate)을 통해 마진 연산을 수행하고, 
        하드 코딩된 리스크 캡으로 단가를 검증합니다.
        """
        target_id = telemetry.get("target", "unknown_worker")
        
        try:
            # 1. MCP 외부 껍데기(tools/call) 우회를 중단하고, 순수 내부 RPC 호출
            margin_res = await self.rpc.call("eco.margin.calculate", telemetry)
            
            # 2. 순수 딕셔너리 반환이 보장되므로, 억지 파싱 없이 직관적 접근 가능
            unit_economics = margin_res.get("unit_economics", {}) if isinstance(margin_res, dict) else {}
            proposed_fee = float(unit_economics.get("effective_fee_usd", self.MIN_PRICE_USD))
            
            # 3. 리스크 매니지먼트 (캡 적용)
            verified_fee = max(self.MIN_PRICE_USD, min(proposed_fee, self.MAX_PRICE_USD))
            if proposed_fee != verified_fee:
                log.warning(f"[Risk Alert] Agent proposed unsafe fee (\({proposed_fee}) for {target_id}. Clamped to\){verified_fee}.")
                
            return verified_fee
            
        except Exception as e:
            log.error(f"Failed to calculate price for {target_id}. Fallback to floor. Error: {e}")
            return self.MIN_PRICE_USD

class PricingVault:
    def __init__(self, risk_manager: PricingRiskManager, tunnel):
        self.risk = risk_manager
        self.tunnel = tunnel
        self.sentinel = TelemetrySentinel(tunnel)

    async def deploy_daemon(self):
        log.info("[Vault] Pricing Sentinel deployed. Awaiting telemetry streams...")
        await self.sentinel.run_listening_loop(self._process_telemetry)

    async def _process_telemetry(self, telemetry: Dict[str, Any]):
        target_id = telemetry.get("target")
        if not target_id: 
            return

        ## 1. 리스크 검증을 거친 최종 단가 획득
        safe_price = await self.risk.calculate_and_verify_price(telemetry)
        
        ## 2. Redis 전광판(요금표) 업데이트 (Atomic operation)
        await self.tunnel.set(f"eco:price_tag:{target_id}", safe_price)
        
        log.info(f"⚖️ [Pricing Enforcer] Updated X402 Fee for {target_id} -> ${safe_price:.4f}")

@contract.daemon("dynamic_pricing")
class DynamicPricingDaemon(AbstractDaemon):
    def __init__(self, ctx):
        super().__init__("DynamicPricingDaemon")
        self.ctx = ctx
        self.tunnel = getattr(self.ctx, 'tunnel', None)
        self.rpc = getattr(self.ctx, 'rpc', None) 
        self.vault = None

    async def _init_dependencies(self):
        """비동기 컨텍스트 내에서 인프라 의존성을 안전하게 확보합니다."""
        if not self.tunnel:
            self.tunnel = await TunnelFactory.get_default()
        if not self.rpc:
            self.rpc = InternalRpcClient()
            
        risk_manager = PricingRiskManager(self.rpc)
        self.vault = PricingVault(risk_manager, self.tunnel)

    async def run(self):
        log.info(f"[{self.name}] Autonomous Pricing Daemon Started.")
        
        # 비동기 인프라(Tunnel, RPC) 주입 및 Vault 초기화
        await self._init_dependencies()
        
        vault_task = asyncio.create_task(self.vault.deploy_daemon())
        
        try:
            while self.running:
                await asyncio.sleep(1.0)
                if vault_task.done():
                    exc = vault_task.exception()
                    if exc:
                        log.error(f"[{self.name}] Vault Task crashed: {exc}", exc_info=exc)
                    break
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancellation signal received.")
        finally:
            if not vault_task.done():
                vault_task.cancel()
                with suppress(asyncio.CancelledError):
                    await vault_task
            log.info(f"[{self.name}] Daemon evaporated cleanly.")