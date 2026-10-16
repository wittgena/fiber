# fiber.infra.watcher.finops
import asyncio
import json
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from pydantic import BaseModel

from fiber.llm.types.provider.general import ModelInfo
from fiber.llm.provider.cost.registry import ModelCostRegistry 
from fiber.llm.provider.cost.unit import UnitCostCalculator
from fiber.llm.provider.cost.policy import CostPolicy
from fiber.llm.model.usage import Usage

from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.arch.contract.config.resolver import config as global_config
from xphi.arch.bound.event.next import LogEvent
from xphi.watcher.plane.emitter import flow_scope, register_interceptor, get_emitter

logger = logging.getLogger("infra.watcher.finops")
finops_emitter = get_emitter("tenant.finops", phase="BILLING")

"""Usage Tracker & Tenant Eco (Outbound Billing System)"""
class UsageTracker:
    def __init__(self):
        self.usage_data = defaultdict(list)

    def _flatten_usage_entry(self, usage_entry: dict[str, Any]) -> dict[str, Any]:
        result = {}
        for key, value in usage_entry.items():
            if isinstance(value, BaseModel):
                result[key] = value.model_dump()
            else:
                result[key] = value
        return result

    def _merge_usage_entries(
        self, usage_entry1: dict[str, Any] | None, usage_entry2: dict[str, Any] | None
    ) -> dict[str, Any]:
        if not usage_entry1:
            return dict(usage_entry2 or {})
        if not usage_entry2:
            return dict(usage_entry1)

        result = dict(usage_entry2)
        for k, v in usage_entry1.items():
            current_v = result.get(k)
            if isinstance(v, dict) or isinstance(current_v, dict):
                result[k] = self._merge_usage_entries(current_v, v)
            elif current_v is not None or v is not None:
                result[k] = (current_v or 0) + (v or 0)
        return result

    def add_usage(self, lm: str, usage_entry: dict[str, Any]) -> None:
        if len(usage_entry) > 0:
            self.usage_data[lm].append(self._flatten_usage_entry(usage_entry))

    def get_total_tokens(self) -> dict[str, dict[str, Any]]:
        total_usage_by_lm = {}
        for lm, usage_entries in self.usage_data.items():
            total_usage = {}
            for usage_entry in usage_entries:
                total_usage = self._merge_usage_entries(total_usage, usage_entry)
            total_usage_by_lm[lm] = total_usage
        return total_usage_by_lm


class TenantEco:
    def __init__(self, cost_policy: Optional[CostPolicy] = None):
        self.policy = cost_policy or CostPolicy()
        self.usage_tracker = UsageTracker()

    def _fetch_model_info(self, model_name: str, provider: Optional[str] = None) -> ModelInfo:
        try:
            info = ModelCostRegistry.lookup_base_model_info(
                model=model_name,
                custom_llm_provider=provider
            )
            return info
        except Exception as e:
            logger.warning(f"[EcoService] Failed to fetch model info for {model_name}. Using free fallback. Error: {e}")
            return {
                "input_cost_per_token": 0.0,
                "output_cost_per_token": 0.0,
            }

    def _normalize_usage(self, raw_usage: Any) -> Usage:
        if isinstance(raw_usage, Usage):
            return raw_usage
        if isinstance(raw_usage, dict):
            return Usage(**raw_usage)
        if hasattr(raw_usage, "model_dump"):
            return Usage(**raw_usage.model_dump())
        
        return Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)

    async def calculate_tenant_billing(
        self, 
        tenant_id: str, 
        usage: Any, 
        model_name: str = "default-model",
        provider: Optional[str] = None
    ) -> dict:
        try:
            normalized_usage = self._normalize_usage(usage)
            self.usage_tracker.add_usage(model_name, normalized_usage.model_dump())

            model_info = self._fetch_model_info(model_name, provider)
            prompt_cost, completion_cost = UnitCostCalculator.generic_cost_per_token(
                model_info=model_info,
                usage=normalized_usage
            )
            base_total_cost = prompt_cost + completion_cost

            cost_breakdown = self.policy.apply_cost_modifiers(
                base_cost=base_total_cost,
                custom_llm_provider=provider
            )
            final_cost = cost_breakdown.final_cost
            amount_to_consume = int(final_cost)
            
            telemetry_payload = {
                "gen_ai.system": provider or "unknown",
                "gen_ai.request.model": model_name,
                "gen_ai.usage.input_tokens": normalized_usage.prompt_tokens,
                "gen_ai.usage.output_tokens": normalized_usage.completion_tokens,
                "gen_ai.usage.total_tokens": normalized_usage.total_tokens,
                "ChargeableEntity": tenant_id,          
                "UsageUnit": "Tokens",                  
                "ListCost": base_total_cost,            
                "BilledCost": final_cost,               
            }

            finops_emitter.info(
                f"Billing resolved for tenant: {tenant_id}",
                **telemetry_payload
            )

            logger.info(
                f"[EcoServ] Tenant: {tenant_id} | Model: {model_name} ({provider}) | "
                f"Tokens: {normalized_usage.total_tokens} | "
                f"Base Cost: {base_total_cost:.6f} -> Consume Intent: {amount_to_consume} Fuel"
            )
            
            # PTA 원장에 기록될 영구 감사 메타데이터 생성
            audit_metadata = {
                "action": "llm_inference_consume",
                "model": model_name,
                "usage": normalized_usage.model_dump(),
                "applied_modifiers": [m.model_dump() for m in cost_breakdown.modifiers],
                "timestamp_ms": int(time.time() * 1000)
            }
            
            return {
                "status": "success",
                "consume_intent": {
                    "tenant_id": tenant_id,
                    "amount_to_consume": amount_to_consume,
                    "preferred_assets": ["fuel"],
                    "metadata": audit_metadata
                }
            }
            
        except Exception as e:
            logger.error(f"[EcoService] Failed to calculate billing for tenant {tenant_id}: {str(e)}")
            return {
                "status": "error", 
                "message": str(e)
            }

"""FinOps Watcher (Inbound: 외부 정책 실시간 동기화)"""
class FinOpsWatcher:
    def __init__(self):
        self.local_budget_cache: Dict[str, float] = {}
        self._sync_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self.is_ready = asyncio.Event()

    def start(self):
        if self._sync_task is None or self._sync_task.done():
            self._running = True
            self.is_ready.clear()
            self._sync_task = asyncio.create_task(self._watch_finops_events())
            logger.info("[FinOpsWatcher] Started background policy sync daemon.")

    def stop(self):
        self._running = False
        self.is_ready.clear()
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            logger.info("[FinOpsWatcher] Stopped background policy sync daemon.")

    async def wait_until_ready(self, timeout: float = 5.0):
        """메시지를 발행(Publish)하기 전에 이 메서드를 호출하여 구독(Subscription)이 완전히 맺어질 때까지 대기"""
        try:
            await asyncio.wait_for(self.is_ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[FinOpsWatcher] Timeout waiting for watcher to be ready.")

    async def _watch_finops_events(self):
        while self._running:
            pubsub = None
            try:
                tunnel = await TunnelFactory.get_default()
                pubsub = tunnel.pubsub()
                await pubsub.subscribe("finops:control_plane")
                
                self.is_ready.set()
                logger.info("[FinOpsWatcher] Connected to FinOps Control Plane via Tunnel.")
                async for message in pubsub.listen():
                    if not self._running:
                        break
                    
                    if message["type"] == "message":
                        self._handle_event(message["data"])
                        
            except asyncio.CancelledError:
                logger.info("[FinOpsWatcher] Watcher task cancelled.")
                break
            except Exception as e:
                self.is_ready.clear()  # 에러 발생 시 준비 상태 해제
                logger.warning(f"[FinOpsWatcher] Tunnel connection lost. Retrying in 5s... Error: {e}")
                await asyncio.sleep(5.0)
            finally:
                self.is_ready.clear()  # 구독 루프 탈출 시 준비 상태 해제
                if pubsub is not None:
                    try:
                        await pubsub.unsubscribe("finops:control_plane")
                        await pubsub.close()
                    except Exception:
                        pass

    def _handle_event(self, raw_data: Any):
        try:
            payload = json.loads(raw_data) if isinstance(raw_data, (str, bytes)) else raw_data
            event_type = payload.get("type")

            if event_type == "update_policy_discount":
                provider = payload.get("provider")
                discount = payload.get("discount_pct")
                
                if provider and discount is not None:
                    current_config = getattr(global_config, "cost_discount_config", {})
                    if not isinstance(current_config, dict):
                        current_config = {}
                        
                    new_config = current_config.copy()
                    new_config[provider] = float(discount)
                    global_config.cost_discount_config = new_config
                    
                    logger.info(f"[FinOpsWatcher] Updated policy: {provider} discount to {discount}")

            elif event_type == "update_policy_margin":
                provider = payload.get("provider")
                margin_pct = payload.get("margin_pct", 0.0)
                fixed_amount = payload.get("fixed_amount", 0.0)
                
                if provider:
                    current_config = getattr(global_config, "cost_margin_config", {})
                    if not isinstance(current_config, dict):
                        current_config = {}
                        
                    new_config = current_config.copy()
                    new_config[provider] = {
                        "percentage": float(margin_pct),
                        "fixed_amount": float(fixed_amount)
                    }
                    global_config.cost_margin_config = new_config
                    
                    logger.info(f"[FinOpsWatcher] Updated margin policy: {provider} (pct: {margin_pct}, fixed: {fixed_amount})")

            elif event_type == "update_budget":
                tenant_id = payload.get("tenant_id")
                fuel_budget = payload.get("fuel_budget")
                
                if tenant_id and fuel_budget is not None:
                    self.local_budget_cache[tenant_id] = float(fuel_budget)
                    logger.info(f"[FinOpsWatcher] Updated budget: Tenant {tenant_id} -> {fuel_budget}")

        except Exception as e:
            logger.error(f"[FinOpsWatcher] Failed to process event: {e}", exc_info=True)


"""Interceptors & Exportable Singleton Instances"""
def _usage_tracking_interceptor(event: LogEvent):
    ctx = event.context or {}
    tracker = ctx.get("usage_tracker")
    usage_data = ctx.get("usage_metrics")
    
    if tracker and isinstance(tracker, UsageTracker) and usage_data:
        model_name = ctx.get("model_name", "default-model")
        tracker.add_usage(model_name, usage_data)

register_interceptor(_usage_tracking_interceptor)

@contextmanager
def track_usage() -> Generator[UsageTracker, None, None]:
    tracker = UsageTracker()
    with flow_scope(usage_tracker=tracker):
        yield tracker

default_finops_watcher = FinOpsWatcher()
default_tenant_eco = TenantEco()