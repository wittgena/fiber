# fiber.llm.provider.cost.exchange
"""
@desc: Canonical Micro-Dollar Fuel Exchange & Dynamic Registry Sampler.
       - Supply Par:  $1.00 USD = 1,000,000 Fuel (`DEFAULT_USD_TO_FUEL_RATE = 1_000_000`, 1 Fuel = 1 micro-USD)
       - Token Par:   1 Token = 1 Fuel (`DEFAULT_TOKEN_TO_FUEL_PAR = 1.0` for local/unmapped models)
       - Natural Alignment: A model priced at $1.00 / 1M tokens consumes exactly 1.0 Fuel per token.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from fiber.llm.provider.cost.filter import CORE_KEYWORDS
from fiber.llm.provider.cost.registry import ModelCostRegistry
from xphi.arch.model.surge.model import SurgeBaseModel

# 1 USD = 1,000,000 Fuel (1M Fuel) / 1 Token = 1 Fuel (Base Fallback)
DEFAULT_USD_TO_FUEL_RATE: int = 1_000_000
DEFAULT_TOKEN_TO_FUEL_PAR: float = 1.0

# 레지스트리 파일 누락 등 극단적 예외 시에도 에러가 나지 않도록 보장하는 최소 1줄 폴백
_FALLBACK_ENTRY: Dict[str, Any] = {
    "provider_name": "local",
    "input_cost_per_token": 0.0,
    "output_cost_per_token": 0.0,
}


class ModelRateEntry(SurgeBaseModel):
    model: str
    provider: str
    context_window: int = 0
    input_usd_per_1m: float = 0.0
    output_usd_per_1m: float = 0.0
    input_fuel_per_tok: float = 1.0
    output_fuel_per_tok: float = 1.0
    input_fuel_per_1k: float = 1000.0
    output_fuel_per_1k: float = 1000.0
    billing_mode: str = "1:1 Token Par"


class FuelExchange(SurgeBaseModel):
    usd_rate: int = DEFAULT_USD_TO_FUEL_RATE
    token_par: float = DEFAULT_TOKEN_TO_FUEL_PAR

    def usd_to_fuel(self, usd: float, rate_override: Optional[int] = None) -> float:
        """Mandate 충전 시 USD($) -> Fuel 환산 ($1.00 = 1,000,000 Fuel)"""
        rate = rate_override if (rate_override and rate_override > 0) else self.usd_rate
        return round(float(usd) * rate, 2)

    def fuel_to_usd(self, fuel: float, rate_override: Optional[int] = None) -> float:
        """Fuel -> USD($) 역산 (1,000,000 Fuel = $1.00)"""
        rate = rate_override if (rate_override and rate_override > 0) else self.usd_rate
        return round(float(fuel) / rate, 6)

    def token_rates_to_fuel_weights(
        self,
        input_cost_per_token: float = 0.0,
        output_cost_per_token: float = 0.0,
        rate_override: Optional[int] = None,
    ) -> tuple[float, float, bool]:
        """
        (input_cost_per_token, output_cost_per_token)을 토큰당 Fuel 가중치로 변환.
        - $1 = 1M Fuel 기준이므로, ($/token * 1,000,000)이 곧 1토큰당 소모 Fuel이 됨 (예: $3/1M -> 3.0 Fuel/tok).
        - 단가가 0.0이면 (1.0, 1.0, False)를 반환하여 1 Token = 1 Fuel 유지.
        """
        in_c = float(input_cost_per_token or 0.0)
        out_c = float(output_cost_per_token or 0.0)
        if in_c <= 0.0 and out_c <= 0.0:
            return self.token_par, self.token_par, False

        rate = rate_override if (rate_override and rate_override > 0) else self.usd_rate
        return round(in_c * rate, 4), round(out_c * rate, 4), True

    def settle_consumption(
        self,
        *,
        total_tokens: int,
        billed_cost_usd: float = 0.0,
        rate_override: Optional[int] = None,
    ) -> int:
        """
        단일 공식 기반 런타임 소비 정산:
        1) 상용 단가(billed_cost_usd > 0) 산출 시: ceil(billed_cost_usd * 1,000,000)
        2) 로컬/무료 모델(billed_cost_usd == 0.0) 시: ceil(total_tokens * 1.0) -> 1 Token = 1 Fuel
        """
        rate = rate_override if (rate_override and rate_override > 0) else self.usd_rate
        if billed_cost_usd and billed_cost_usd > 0.0:
            return max(1, math.ceil(float(billed_cost_usd) * rate))
        if total_tokens and total_tokens > 0:
            return max(1, int(round(int(total_tokens) * self.token_par)))
        return 0

    def build_catalog_rows(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        `ModelCostRegistry`에서 `cost.filter.CORE_KEYWORDS` 프로바이더별로 주요 모델을
        고르게(Round-Robin) 샘플링하여 UI 단가 테이블 행(Row) 리스트를 생성합니다.
        """
        cost_map = ModelCostRegistry._model_cost or {"default-local": _FALLBACK_ENTRY}
        buckets: Dict[str, List[tuple[str, Dict[str, Any]]]] = {kw: [] for kw in CORE_KEYWORDS}
        others: List[tuple[str, Dict[str, Any]]] = []

        for model_key, spec in cost_map.items():
            if not isinstance(spec, dict):
                continue
            prov = str(
                spec.get("provider_name")
                or spec.get("model_provider")
                or (model_key.split("/")[0] if "/" in model_key else "local")
            ).lower()

            matched_kw = next(
                (kw for kw in CORE_KEYWORDS if prov.startswith(kw) or model_key.lower().startswith(kw)),
                None,
            )
            if matched_kw:
                buckets[matched_kw].append((model_key, spec))
            else:
                others.append((model_key, spec))

        # 프로바이더별 Round-Robin 추출 (특정 프로바이더 편중 방지)
        selected: List[tuple[str, Dict[str, Any]]] = []
        active_buckets = [b for b in buckets.values() if b]
        idx = 0
        while len(selected) < limit and active_buckets:
            exhausted = []
            for b in active_buckets:
                if idx < len(b):
                    selected.append(b[idx])
                    if len(selected) >= limit:
                        break
                else:
                    exhausted.append(b)
            for eb in exhausted:
                active_buckets.remove(eb)
            idx += 1

        if len(selected) < limit:
            selected.extend(others[: limit - len(selected)])
        if not selected:
            selected = [("default-local", _FALLBACK_ENTRY)]

        rows: List[Dict[str, Any]] = []
        for model_key, spec in selected:
            in_cost = float(spec.get("input_cost_per_token", 0.0) or 0.0)
            out_cost = float(spec.get("output_cost_per_token", 0.0) or 0.0)
            in_w, out_w, is_priced = self.token_rates_to_fuel_weights(in_cost, out_cost)

            prov = str(
                spec.get("provider_name")
                or spec.get("model_provider")
                or (model_key.split("/")[0] if "/" in model_key else "local")
            )
            ctx_win = int(spec.get("max_input_tokens") or spec.get("max_output_tokens") or 0)

            rows.append(
                ModelRateEntry(
                    model=model_key,
                    provider=prov,
                    context_window=ctx_win,
                    input_usd_per_1m=round(in_cost * 1_000_000, 4),
                    output_usd_per_1m=round(out_cost * 1_000_000, 4),
                    input_fuel_per_tok=in_w,
                    output_fuel_per_tok=out_w,
                    input_fuel_per_1k=round(in_w * 1000, 1),
                    output_fuel_per_1k=round(out_w * 1000, 1),
                    billing_mode="Cost-Weighted" if is_priced else "1:1 Token Par",
                ).model_dump()
            )
        return rows


default_exchange = FuelExchange()