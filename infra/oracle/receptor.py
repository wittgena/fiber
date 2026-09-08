# fiber.infra.oracle.receptor
## @lineage: fiber.infra.observer.oracle.receptor
## @lineage: fiber.agent.infra.observer.oracle.receptor
import os
import time
import hashlib
import statistics
import asyncio
import httpx
from typing import Dict, Any, List, Optional

from xphi.kernel.adapter.state import StateAdapter
from xphi.kernel.adapter.sign import NodeSigner
from xphi.watcher.plane.emitter import get_emitter

from fiber.infra.oracle.proof.binance import kline as binance_kline
from fiber.infra.oracle.proof.coinbase import kline as coinbase_kline

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
        """모듈(어댑터) 또는 파일의 순수 .py 바이트코드를 추출하여 해시화"""
        path = os.path.abspath(getattr(module_or_file, '__file__', module_or_file))
        if path.endswith('.pyc'):
            path = path[:-1]
            
        if not os.path.exists(path):
            raise FileNotFoundError(f"[Oracle Security] Target source not found: {path}")

        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    @property
    def aggregator_code_hash(self) -> str:
        """집계기(Aggregator) 로직 자체의 무결성 증명 해시"""
        return self._extract_source_hash(__file__)

    def aggregate_candles(self, observations: List[List[Dict[str, float]]], strategy: str) -> List[Dict[str, float]]:
        """N개의 거래소 데이터를 시간(ts) 기준으로 병합하는 순수 함수 (CPU Bound)"""
        if not observations:
            raise ValueError("No observations to aggregate")
            
        if len(observations) == 1:
            return observations[0]

        aggregated = []
        for i in range(len(observations[0])):
            closes = [obs[i]["c"] for obs in observations]
            final_c = statistics.mean(closes) if strategy == "mean" else statistics.median(closes)
            
            aggregated.append({
                "ts": observations[0][i]["ts"], 
                "o": observations[0][i]["o"],
                "h": observations[0][i]["h"],
                "l": observations[0][i]["l"],
                "c": final_c,
                "v": observations[0][i]["v"]
            })
            
        return aggregated

    async def _fetch_single_source(self, client: httpx.AsyncClient, arn: str, symbol: str, end_time_ms: int) -> Dict[str, Any]:
        """개별 거래소 데이터를 비동기적으로 페치하고 파싱하는 코루틴"""
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
        
        try:
            # [핵심] 외부 네트워크 I/O 비동기 대기
            response = await client.get(intent_params["url"], params=intent_params["query"])
            response.raise_for_status()
        except httpx.RequestError as e:
            self.log.error(f"[Aggregator] Source {arn} failed: {str(e)}")
            raise # Fail-fast 원칙 유지
            
        parsed_data = adapter.parse_observation(response.json())
        obs_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(parsed_data)).hexdigest()
        
        return {
            "arn": arn,
            "source_meta": source_meta,
            "parsed_data": parsed_data,
            "obs_hash": obs_hash
        }

    async def fetch_aggregate_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        """@phase: Composite Attestation (복합 증명 조립 및 서명 - 비동기 버전)"""
        fetch_time = int(time.time())
        end_time_ms = (fetch_time // 60 * 60 * 1000) - 1
        
        self.log.info(f"[Aggregator] Initiating async multi-source fetch for {symbol} (Strategy: {strategy})")

        composite_sources = {}
        raw_observations = []
        individual_hashes = {}

        # 1. 분산 소스 데이터 병렬 수집 (Concurrent Fetch)
        # 여러 거래소 API를 찔러놓고, 가장 늦게 오는 응답 속도에만 맞추면 됩니다.
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [
                self._fetch_single_source(client, arn, symbol, end_time_ms)
                for arn in target_arns
            ]
            # 모든 소스의 응답을 병렬로 기다림
            results = await asyncio.gather(*tasks)

        # 수집된 병렬 데이터 정리
        for res in results:
            arn = res["arn"]
            composite_sources[arn] = res["source_meta"]
            raw_observations.append(res["parsed_data"])
            individual_hashes[arn] = res["obs_hash"]

        # 2. 데이터 집계 및 최종 해시 산출 (동기적 연산)
        aggregated_data = self.aggregate_candles(raw_observations, strategy)
        aggregated_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(aggregated_data)).hexdigest()

        # 3. 복합 영수증(Composite Receipt) 조립 및 노드 서명 씰링
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
        """
        워커에서 호출되는 진입점.
        이제 워커는 이 메서드를 await 함으로써, 외부 API 대기 시간 동안 이벤트 루프를 양보합니다.
        """
        self.log.info(f"[Receptor] Forwarding async fetch request for {symbol}")
        
        # Aggregator의 핵심 비동기 로직 호출
        payload = await self.aggregator.fetch_aggregate_and_seal(symbol, target_arns, strategy)
        
        self.log.info(f"  └─ [Universal Async Seal] Sig: {payload['attestation']['signature'][:12]}...")
        return payload