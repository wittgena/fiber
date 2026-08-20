# bound.observer.oracle.aggregator
## @lineage: eco.observer.oracle.aggregator
## @lineage: bound.proof.oracle.aggregator
## @lineage: bound.exchange.capital.aggregator
import os
import time
import hashlib
import statistics
import requests
from typing import List, Dict, Any, Optional

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

from bound.observer.proof.binance import kline as binance_kline
from bound.observer.proof.coinbase import kline as coinbase_kline

class ProvableOracleAggregator:
    # ARN 기반 모듈 레지스트리
    ADAPTER_REGISTRY = {
        "arn:bound:oracle:binance:kline:v1.0.0": binance_kline,
        "arn:bound:oracle:coinbase:kline:v1.0.0": coinbase_kline
    }

    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("oracle.aggregator")

    @staticmethod
    def _extract_source_hash(module_or_file: Any) -> str:
        """
        @desc: 모듈(어댑터) 또는 현재 파일(집계기)의 순수 .py 바이트코드를 추출하여 해시화합니다.
               (.pyc 캐싱으로 인한 해시 불일치 원천 차단)
        """
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
        """N개의 거래소 데이터를 시간(ts) 기준으로 병합하는 순수 함수"""
        if not observations:
            raise ValueError("No observations to aggregate")
            
        if len(observations) == 1:
            return observations[0]

        aggregated = []
        # 시계열 인덱스 병합 로직 (각 거래소의 데이터 길이가 같다는 전제 하의 심플 모델)
        for i in range(len(observations[0])):
            # 모든 소스에서 i번째 캔들의 종가(c)를 추출
            closes = [obs[i]["c"] for obs in observations]
            
            final_c = statistics.mean(closes) if strategy == "mean" else statistics.median(closes)
            
            aggregated.append({
                "ts": observations[0][i]["ts"], 
                "o": observations[0][i]["o"], # 데모를 위해 첫 소스 기준 (실제로는 o,h,l,v 모두 전략 적용 필요)
                "h": observations[0][i]["h"],
                "l": observations[0][i]["l"],
                "c": final_c,
                "v": observations[0][i]["v"]
            })
            
        return aggregated

    def fetch_aggregate_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        """
        @phase: Composite Attestation (복합 증명 조립 및 서명)
        """
        fetch_time = int(time.time())
        end_time_ms = (fetch_time // 60 * 60 * 1000) - 1
        
        composite_sources = {}
        raw_observations = []
        individual_hashes = {}
        
        self.log.info(f"[Aggregator] Initiating multi-source fetch for {symbol} (Strategy: {strategy})")

        # =========================================================================
        # 1. 분산 소스 데이터 수집 및 개별 증명 체인 구축
        # =========================================================================
        for arn in target_arns:
            adapter = self.ADAPTER_REGISTRY.get(arn)
            if not adapter:
                raise ValueError(f"Unsupported adapter ARN: {arn}")
                
            # A. 하위 어댑터 코드 해시 추출
            adapter_code_hash = self._extract_source_hash(adapter)
            
            # B. 거래소별 파라미터 매핑 및 Intent 해시
            interval_param = "1m" if "binance" in arn else 60
            intent_params = adapter.build_intent_params(symbol, interval_param, 1, end_time_ms)
            param_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(intent_params)).hexdigest()
            
            composite_sources[arn] = {
                "adapter_code_hash": adapter_code_hash,
                "param_hash": param_hash,
                "request_params": intent_params
            }
            
            # C. 실제 HTTP 요청 수행
            try:
                response = requests.get(intent_params["url"], params=intent_params["query"], timeout=10)
                response.raise_for_status()
            except requests.RequestException as e:
                self.log.error(f"[Aggregator] Source {arn} failed: {str(e)}")
                raise # 엄격한 합의를 위해 하나의 소스라도 실패하면 전체 롤백 (Fail-fast)

            # D. 데이터 파싱 및 개별 관측 해시 생성
            parsed_data = adapter.parse_observation(response.json())
            raw_observations.append(parsed_data)
            
            obs_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(parsed_data)).hexdigest()
            individual_hashes[arn] = obs_hash

        # =========================================================================
        # 2. 데이터 집계 및 최종 해시 산출
        # =========================================================================
        aggregated_data = self.aggregate_candles(raw_observations, strategy)
        aggregated_hash = hashlib.sha256(StateAdapter.to_canonical_bytes(aggregated_data)).hexdigest()

        # =========================================================================
        # 3. 복합 영수증(Composite Receipt) 조립 및 노드 서명 씰링
        # =========================================================================
        context = {
            "oracle_id": "provable_aggregator_v1.0",
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

        # 서명용 Canonical Root 생성 (Context + Recipe + Observation)
        attestation_payload = {
            "context": context,
            "recipe_hashes": {
                "aggregator_code_hash": recipe["aggregator_code_hash"],
                # 하위 소스들의 해시 상태만 모아서 상위 해시 생성 (Merkle 트리 구조화)
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