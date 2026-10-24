# fiber.infra.watcher.finops
## @lineage: fiber.infra.bound.watcher.finops
from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import contextmanager
import json
import time
from typing import Any, Dict, Generator, Optional, Set

from pydantic import BaseModel

from fiber.dev.console.observer.signal import UIKey, UIMetric, UISlot, UISlotStatus
from fiber.llm.model.usage import Usage
from fiber.llm.provider.cost.exchange import FuelExchange, default_exchange
from fiber.llm.provider.cost.policy import CostPolicy
from fiber.llm.provider.cost.registry import ModelCostRegistry
from fiber.llm.provider.cost.unit import UnitCostCalculator
from fiber.llm.types.provider.general import ModelInfo

from xphi.arch.contract.config.resolver import config as global_config
from xphi.arch.contract.event.next import LogEvent
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import flow_scope, get_emitter, register_interceptor

finops_emitter = get_emitter("watcher.finops", phase="BILLING")

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
    def __init__(
        self,
        cost_policy: Optional[CostPolicy] = None,
        exchange: Optional[FuelExchange] = None,
    ):
        self.policy = cost_policy or CostPolicy()
        self.exchange = exchange or default_exchange
        self.usage_tracker = UsageTracker()

    def _fetch_model_info(self, model_name: str, provider: Optional[str] = None) -> ModelInfo:
        try:
            return ModelCostRegistry.lookup_base_model_info(
                model=model_name,
                custom_llm_provider=provider,
            )
        except Exception as e:
            finops_emitter.debug(
                f"[EcoService] Model cost entry not found for '{model_name}' ({e}). "
                f"Falling back to 1:1 token-to-fuel par.",
                model=model_name,
                provider=provider or "unknown",
            )
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
        provider: Optional[str] = None,
    ) -> dict:
        try:
            normalized_usage = self._normalize_usage(usage)
            self.usage_tracker.add_usage(model_name, normalized_usage.model_dump())

            model_info = self._fetch_model_info(model_name, provider)
            prompt_cost, completion_cost = UnitCostCalculator.generic_cost_per_token(
                model_info=model_info,
                usage=normalized_usage,
            )
            base_total_cost = prompt_cost + completion_cost

            cost_breakdown = self.policy.apply_cost_modifiers(
                base_cost=base_total_cost,
                custom_llm_provider=provider,
            )
            final_cost = cost_breakdown.final_cost

            # FuelExchange를 통해 정산:
            # 1) 상용 단가(final_cost > 0)가 존재하면 USD 환율(기본 1000)을 곱해 Fuel 산출
            # 2) 로컬 모델 등 단가가 0.0이면 기본 1 Token = 1 Fuel 비율로 산출
            amount_to_consume = self.exchange.settle_consumption(
                total_tokens=normalized_usage.total_tokens,
                billed_cost_usd=final_cost,
            )
            saved_discount_usd = max(0.0, float(base_total_cost - final_cost))

            # 로컬 워처 캐시가 보유한 현재 잔여 쿼터와 동기화하여 차감 반영
            cached_quota = default_finops_watcher.local_budget_cache.get(tenant_id)
            if cached_quota is not None and amount_to_consume > 0:
                remaining_quota = max(0.0, cached_quota - amount_to_consume)
                default_finops_watcher.local_budget_cache[tenant_id] = remaining_quota
            else:
                remaining_quota = cached_quota

            ui_context: Dict[str, Any] = {
                "mode": "RAW",
                "gen_ai.system": provider or "unknown",
                "gen_ai.request.model": model_name,
                "gen_ai.usage.input_tokens": normalized_usage.prompt_tokens,
                "gen_ai.usage.output_tokens": normalized_usage.completion_tokens,
                "gen_ai.usage.total_tokens": normalized_usage.total_tokens,
                "ChargeableEntity": tenant_id,
                "UsageUnit": "Tokens",
                "ListCost": base_total_cost,
                "BilledCost": final_cost,
                UIKey.SLOT.value: UISlot.FUEL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.TENANT_ID.value: tenant_id,
                UIKey.CHUNKS.value: normalized_usage.total_tokens,
            }
            if remaining_quota is not None:
                ui_context[UIKey.METRIC.value] = UIMetric.FUEL_TICK.value
                ui_context[UIKey.ACTIVE_QUOTA.value] = int(remaining_quota)
            if saved_discount_usd > 0.0:
                ui_context[UIKey.SAVED_USD.value] = round(saved_discount_usd, 6)

            finops_emitter.info(
                f"[EcoServ] Tenant: {tenant_id} | Model: {model_name} ({provider or 'auto'}) | "
                f"Tokens: {normalized_usage.total_tokens} | "
                f"Cost: ${final_cost:.6f} -> Consume: {amount_to_consume} Fuel",
                **ui_context,
            )

            audit_metadata = {
                "action": "llm_inference_consume",
                "model": model_name,
                "usage": normalized_usage.model_dump(),
                "billed_cost_usd": final_cost,
                "exchange_rate": self.exchange.usd_rate,
                "applied_modifiers": [m.model_dump() for m in cost_breakdown.modifiers],
                "timestamp_ms": int(time.time() * 1000),
            }

            return {
                "status": "success",
                "consume_intent": {
                    "tenant_id": tenant_id,
                    "amount_to_consume": amount_to_consume,
                    "preferred_assets": ["fuel"],
                    "metadata": audit_metadata,
                },
            }

        except Exception as e:
            finops_emitter.error(
                f"[EcoService] Failed to calculate billing for tenant '{tenant_id}': {e}",
                exc_info=True,
                **{UIKey.TENANT_ID.value: tenant_id},
            )
            return {
                "status": "error",
                "message": str(e),
            }


class FinOpsWatcher:
    def __init__(self, exchange: Optional[FuelExchange] = None):
        self.exchange = exchange or default_exchange
        self.local_budget_cache: Dict[str, float] = {}
        self.processed_jtis: Set[str] = set()
        self.policy_versions: Dict[str, int] = {}

        self._sync_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self.is_ready = asyncio.Event()

    def start(self):
        if self._sync_task is None or self._sync_task.done():
            self._running = True
            self.is_ready.clear()
            self._sync_task = asyncio.create_task(self._watch_finops_events())
            finops_emitter.info("[FinOpsWatcher] Started background policy sync daemon.")

    def stop(self):
        self._running = False
        self.is_ready.clear()
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            finops_emitter.info("[FinOpsWatcher] Stopped background policy sync daemon.")

    async def wait_until_ready(self, timeout: float = 5.0):
        """메시지를 발행(Publish)하기 전에 이 메서드를 호출하여 구독(Subscription)이 완전히 맺어질 때까지 대기"""
        try:
            await asyncio.wait_for(self.is_ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            finops_emitter.warning("[FinOpsWatcher] Timeout waiting for watcher to be ready.")

    def is_version_valid(self, tenant_id: str, policy_ver: int) -> bool:
        """구버전(Stale) JWT 재사용 여부 검증"""
        return policy_ver >= self.policy_versions.get(tenant_id, 0)

    def apply_finops_spec(
        self,
        tenant_id: str,
        op: str = "SYNC_QUOTA",
        budget_usd: Optional[float] = None,
        fuel_budget: Optional[float] = None,
        exchange_rate: Optional[int] = None,
        policy_ver: Optional[int] = None,
        jti: Optional[str] = None,
    ) -> bool:
        # 1. 버전 검증: 이미 더 높은 버전의 정책이 적용되었으면 무시/거부
        current_ver = self.policy_versions.get(tenant_id, 0)
        if policy_ver is not None and policy_ver < current_ver:
            finops_emitter.warning(
                f"[FinOpsWatcher] Stale policy replay rejected for '{tenant_id}' (ver {policy_ver} < {current_ver}).",
                **{
                    "mode": "RAW",
                    UIKey.SLOT.value: UISlot.GUARDRAIL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.BLOCK.value,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.POLICY_VER.value: current_ver,
                },
            )
            return False

        # 2. 멱등성(Idempotency) 검증: 이미 처리된 JTI라면 중복 생성/충전 스킵
        if jti and jti in self.processed_jtis:
            finops_emitter.debug(
                f"[FinOpsWatcher] Duplicate JTI '{jti}' ignored for tenant '{tenant_id}'.",
                **{UIKey.TENANT_ID.value: tenant_id, UIKey.IDEM_KEY.value: jti},
            )
            return False

        # 3. 목표 Fuel 계산 (fuel_budget 직접 지정 또는 FuelExchange를 통한 budget_usd 환산)
        if fuel_budget is not None:
            target_fuel = float(fuel_budget)
        elif budget_usd is not None:
            target_fuel = self.exchange.usd_to_fuel(budget_usd, rate_override=exchange_rate)
        else:
            target_fuel = 0.0

        # 4. 처리 완료된 JTI 및 최신 버전 기록
        if jti:
            self.processed_jtis.add(jti)
        if policy_ver is not None:
            self.policy_versions[tenant_id] = int(policy_ver)
        effective_ver = self.policy_versions.get(tenant_id, 1)

        # 5. 오퍼레이션(op) 별 상태 변이 및 구조화 UI 시그널 발행
        op_upper = op.upper()
        if op_upper == "REVOKE":
            self.local_budget_cache[tenant_id] = 0.0
            finops_emitter.signal(
                f"[FinOpsWatcher] 🚨 REVOKE executed: Tenant '{tenant_id}' budget frozen to 0.0 Fuel (v{effective_ver}).",
                **{
                    UIKey.METRIC.value: UIMetric.KILL_SWITCH.value,
                    UIKey.SLOT.value: UISlot.FUEL.value,
                    UIKey.SLOT_STATUS.value: UISlotStatus.SEVER.value,
                    UIKey.TENANT_ID.value: tenant_id,
                    UIKey.ACTIVE_QUOTA.value: 0,
                    UIKey.LEDGER_BALANCE.value: 0,
                    UIKey.POLICY_VER.value: effective_ver,
                },
            )
            return True

        if op_upper == "TOP_UP":
            current_fuel = self.local_budget_cache.get(tenant_id, 0.0)
            new_quota = current_fuel + target_fuel
            self.local_budget_cache[tenant_id] = new_quota
            msg = f"[FinOpsWatcher] TOP_UP executed: Tenant '{tenant_id}' +{int(target_fuel):,} -> {int(new_quota):,} Fuel (v{effective_ver})."
        elif op_upper == "PROVISION":
            if tenant_id not in self.local_budget_cache:
                self.local_budget_cache[tenant_id] = target_fuel
                new_quota = target_fuel
                msg = f"[FinOpsWatcher] PROVISION executed: Tenant '{tenant_id}' initialized with {int(new_quota):,} Fuel (v{effective_ver})."
            else:
                new_quota = self.local_budget_cache[tenant_id]
                finops_emitter.debug(
                    f"[FinOpsWatcher] PROVISION skipped: Tenant '{tenant_id}' already exists ({int(new_quota):,} Fuel).",
                    **{UIKey.TENANT_ID.value: tenant_id, UIKey.ACTIVE_QUOTA.value: int(new_quota)},
                )
                return True
        else:
            self.local_budget_cache[tenant_id] = target_fuel
            new_quota = target_fuel
            msg = f"[FinOpsWatcher] SYNC_QUOTA executed: Tenant '{tenant_id}' -> {int(new_quota):,} Fuel (v{effective_ver})."

        finops_emitter.info(
            msg,
            **{
                "mode": "RAW",
                UIKey.METRIC.value: UIMetric.LEDGER_SYNC.value,
                UIKey.SLOT.value: UISlot.FUEL.value,
                UIKey.SLOT_STATUS.value: UISlotStatus.PASS.value,
                UIKey.TENANT_ID.value: tenant_id,
                UIKey.ACTIVE_QUOTA.value: int(new_quota),
                UIKey.POLICY_VER.value: effective_ver,
            },
        )
        return True

    async def _watch_finops_events(self):
        while self._running:
            pubsub = None
            try:
                tunnel = await TunnelFactory.get_default()
                pubsub = tunnel.pubsub()
                await pubsub.subscribe("finops:control_plane")

                self.is_ready.set()
                finops_emitter.info("[FinOpsWatcher] Connected to FinOps Control Plane via Tunnel.")
                async for message in pubsub.listen():
                    if not self._running:
                        break
                    if message["type"] == "message":
                        self._handle_event(message["data"])

            except asyncio.CancelledError:
                finops_emitter.info("[FinOpsWatcher] Watcher task cancelled.")
                break
            except Exception as e:
                self.is_ready.clear()
                finops_emitter.warning(f"[FinOpsWatcher] Tunnel connection lost. Retrying in 5s... Error: {e}")
                await asyncio.sleep(5.0)
            finally:
                self.is_ready.clear()
                if pubsub is not None:
                    try:
                        await pubsub.unsubscribe("finops:control_plane")
                        await pubsub.close()
                    except Exception:
                        pass

    def _handle_event(self, raw_data: Any):
        try:
            payload = json.loads(raw_data) if isinstance(raw_data, (str, bytes)) else raw_data
            if not isinstance(payload, dict):
                return
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
                    finops_emitter.info(
                        f"[FinOpsWatcher] Updated policy: {provider} discount to {discount}",
                        provider=provider,
                        discount_pct=float(discount),
                    )

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
                        "fixed_amount": float(fixed_amount),
                    }
                    global_config.cost_margin_config = new_config
                    finops_emitter.info(
                        f"[FinOpsWatcher] Updated margin policy: {provider} (pct: {margin_pct}, fixed: {fixed_amount})",
                        provider=provider,
                        margin_pct=float(margin_pct),
                        fixed_amount=float(fixed_amount),
                    )

            elif event_type == "update_budget":
                tenant_id = payload.get("tenant_id")
                if not tenant_id:
                    return

                finops_block = payload.get("finops", {})
                op = finops_block.get("op") or payload.get("op", "SYNC_QUOTA")
                fuel_budget = payload.get("fuel_budget")
                budget_usd = (
                    finops_block.get("budget_usd") if "budget_usd" in finops_block else payload.get("budget_usd")
                )
                exchange_rate = finops_block.get("exchange_rate") or payload.get("exchange_rate")
                policy_ver = finops_block.get("policy_ver") or payload.get("policy_ver")
                jti = payload.get("jti")

                if fuel_budget is not None and float(fuel_budget) <= 0.0:
                    op = "REVOKE"

                self.apply_finops_spec(
                    tenant_id=tenant_id,
                    op=op,
                    budget_usd=budget_usd,
                    fuel_budget=fuel_budget,
                    exchange_rate=exchange_rate,
                    policy_ver=policy_ver,
                    jti=jti,
                )

        except Exception as e:
            finops_emitter.error(f"[FinOpsWatcher] Failed to process event: {e}", exc_info=True)


# Interceptors & Exportable Singleton Instances
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