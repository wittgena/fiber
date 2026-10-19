# fiber.infra.bound.oracle.receptor
import os
import time
import hashlib
import statistics
import asyncio
import httpx
from typing import Dict, Any, List, Optional

from xphi.arch.bound.adapter.state import StateAdapter
from xphi.arch.bound.adapter.pta import NodeSigner
from xphi.watcher.plane.emitter import get_emitter
from fiber.infra.bound.oracle.binance import kline as binance_kline
from fiber.infra.bound.oracle.coinbase import kline as coinbase_kline

class ProvableOracleAggregator:
    ADAPTER_REGISTRY = {
        "arn:bound:oracle:binance:kline:v1.0.0": binance_kline,
        "arn:bound:oracle:coinbase:kline:v1.0.0": coinbase_kline
    }

    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("oracle.aggregator")

    @staticmethod
    def _extract_source_hash(module_or_file: Any) -> str:
        path = os.path.abspath(getattr(module_or_file, '__file__', module_or_file))
        if path.endswith('.pyc'):
            path = path[:-1]
        if not os.path.exists(path):
            raise FileNotFoundError(f"[Oracle Security] Target source not found: {path}")
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    @property
    def aggregator_code_hash(self) -> str:
        return self._extract_source_hash(__file__)

    def aggregate_candles(self, observations: List[List[Dict[str, float]]], strategy: str) -> List[Dict[str, float]]:
        """N개의 거래소 데이터를 시간(ts) 기준으로 병합하는 무결성 함수"""
        # [핵심 개선] 빈 배열 반환 시 IndexError를 방지하는 강건한(Robust) 교집합 처리
        valid_obs = [obs for obs in observations if obs]
        if not valid_obs:
            raise ValueError("All exchange observations returned empty data")
            
        if len(valid_obs) == 1:
            return valid_obs[0]

        # 거래소 간 공통된 시간대(ts)만 추출하여 안전하게 집계
        ts_sets = [set(c["ts"] for c in obs) for obs in valid_obs]
        common_ts = set.intersection(*ts_sets) if ts_sets else set()
        
        if not common_ts:
            # 타임스탬프가 엇갈려 교집합이 없으면, 가장 데이터가 풍부한 첫 거래소 반환 (Fallback)
            return valid_obs[0]

        aggregated = []
        for ts in sorted(common_ts):
            matched_candles = []
            for obs in valid_obs:
                for c in obs:
                    if c["ts"] == ts:
                        matched_candles.append(c)
                        break
            
            closes = [c["c"] for c in matched_candles]
            final_c = statistics.mean(closes) if strategy == "mean" else statistics.median(closes)
            
            # 고가(h)와 저가(l)는 가장 보수적인 값으로 병합
            base_candle = matched_candles[0]
            aggregated.append({
                "ts": ts, 
                "o": base_candle["o"],
                "h": max(c["h"] for c in matched_candles),
                "l": min(c["l"] for c in matched_candles),
                "c": final_c,
                "v": sum(c["v"] for c in matched_candles)
            })
            
        return aggregated

    async def _fetch_single_source(self, client: httpx.AsyncClient, arn: str, symbol: str, end_time_ms: int) -> Dict[str, Any]:
        adapter = self.ADAPTER_REGISTRY.get(arn)
        if not adapter:
            raise ValueError(f"Unsupported adapter ARN: {arn}")
            
        adapter_code_hash = self._extract_source_hash(adapter)
        interval_param = "1m" if "binance" in arn else 60
        intent_params = adapter.build_intent_params(symbol, interval_param, 1, end_time_ms)
        param_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(intent_params)).hexdigest()
        
        source_meta = {
            "adapter_code_hash": adapter_code_hash,
            "param_hash": param_hash,
            "request_params": intent_params
        }
        
        max_retries = 3
        retry_delay = 0.5  # 초기 대기 시간 (초)
        response = None
        
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.get(intent_params["url"], params=intent_params["query"])
                response.raise_for_status()
                break
            except httpx.RequestError as e:
                if attempt == max_retries:
                    self.log.error(f"[Aggregator] Source {arn} failed after {max_retries} attempts: {str(e)}")
                    raise  # 최종 실패 시 에러 전파 (Fail-fast 복귀)
                
                self.log.warning(f"[Aggregator] Source {arn} attempt {attempt} failed ({str(e)}). Retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # 0.5s -> 1.0s -> 2.0s
            
        parsed_data = adapter.parse_observation(response.json())
        obs_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(parsed_data)).hexdigest()
        
        return {
            "arn": arn,
            "source_meta": source_meta,
            "parsed_data": parsed_data,
            "obs_hash": obs_hash
        }

    async def fetch_aggregate_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        fetch_time = int(time.time())
        end_time_ms = (fetch_time // 60 * 60 * 1000) - 1
        self.log.info(f"[Aggregator] Initiating async multi-source fetch for {symbol} (Strategy: {strategy})")

        composite_sources = {}
        raw_observations = []
        individual_hashes = {}

        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [
                self._fetch_single_source(client, arn, symbol, end_time_ms)
                for arn in target_arns
            ]
            results = await asyncio.gather(*tasks)

        for res in results:
            arn = res["arn"]
            composite_sources[arn] = res["source_meta"]
            raw_observations.append(res["parsed_data"])
            individual_hashes[arn] = res["obs_hash"]

        aggregated_data = self.aggregate_candles(raw_observations, strategy)
        aggregated_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(aggregated_data)).hexdigest()

        context = {
            "oracle_id": "provable_aggregator_v2.0_async",
            "timestamp": fetch_time,
            "signer_pubkey": getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        }

        recipe = {
            "aggregator_code_hash": self.aggregator_code_hash,
            "strategy": strategy,
            "sources": composite_sources
        }

        observation = {
            "individual_hashes": individual_hashes,
            "aggregated_hash": aggregated_hash,
            "payload": aggregated_data
        }

        attestation_payload = {
            "context": context,
            "recipe_hashes": {
                "aggregator_code_hash": recipe["aggregator_code_hash"],
                "sources_root": hashlib.sha256(StateAdapter.to_canonical_bytes(composite_sources)).hexdigest()
            },
            "observation_root": observation["aggregated_hash"]
        }
        
        canonical_root = StateAdapter.to_canonical_bytes(attestation_payload)
        signature = self.signer.sign_payload(canonical_root)

        return {
            "context": context,
            "recipe": recipe,
            "observation": observation,
            "attestation": {
                "canonical_root": hashlib.sha256(canonical_root).hexdigest(),
                "signature": signature
            }
        }

class OracleReceptor:
    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.universal")
        self.aggregator = ProvableOracleAggregator(self.signer, self.log)

    async def fetch_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        self.log.info(f"[Receptor] Forwarding async fetch request for {symbol}")
        payload = await self.aggregator.fetch_aggregate_and_seal(symbol, target_arns, strategy)
        self.log.info(f"  └─ [Universal Async Seal] Sig: {payload['attestation']['signature'][:12]}...")
        return payload