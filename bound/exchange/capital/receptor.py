# bound.exchange.capital.receptor
## @lineage: bound.capital.exchange.receptor
import time
import hashlib
from typing import Dict, Any, List, Optional

from bound.exchange.capital.aggregator import ProvableOracleAggregator
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

class OracleReceptor:
    """
    @desc: 정책(Policy) 기반으로 다중 오라클 어댑터를 동적 라우팅하고, 
           수집/집계된 복합 상태 트리를 최종 씰링(Sealing)하는 범용 리셉터.
    """
    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.universal")
        
        # 순수 실행 및 해시 트리 생성을 담당하는 집계기 인스턴스화
        self.aggregator = ProvableOracleAggregator()

    def fetch_and_seal(self, symbol: str, target_arns: List[str], strategy: str = "mean") -> Dict[str, Any]:
        """
        @param target_arns: 실행할 어댑터의 ARN 목록 (단일 값 입력 시 기존 binance 전용처럼 동작)
        @param strategy: 집계 전략 (mean, median 등)
        """
        fetch_time = int(time.time())
        self.log.info(f"[Receptor] Executing policy for {symbol} | Sources: {len(target_arns)} | Strategy: {strategy}")

        # =========================================================================
        # 1. Aggregator에게 데이터 수집, 집계 및 하위 해시 트리(Merkle root) 생성 위임
        # =========================================================================
        # aggregator.execute_policy는 서명이 없는 순수 데이터와 해시 트리만 반환하도록 수정되었다고 가정
        aggregator_result = self.aggregator.execute_policy(symbol, target_arns, strategy, fetch_time)

        # =========================================================================
        # 2. Context 정의 (오라클 노드의 서명 환경 정보)
        # =========================================================================
        context = {
            "oracle_id": "universal_receptor_v2.0",
            "timestamp": fetch_time,
            "signer_pubkey": getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        }

        # =========================================================================
        # 3. Recipe (의도 증명) - 단일 해시가 아닌 복합 해시(Composite Hashes) 구조
        # =========================================================================
        recipe = {
            "strategy": strategy,
            "aggregator_code_hash": aggregator_result["aggregator_code_hash"],
            "sources": aggregator_result["composite_sources"]
        }

        # =========================================================================
        # 4. Observation (상태 관측) - 개별 관측 해시와 최종 집계 결과
        # =========================================================================
        observation = {
            "individual_hashes": aggregator_result["individual_hashes"],
            "payload": aggregator_result["aggregated_data"],
            "aggregated_hash": aggregator_result["aggregated_hash"]
        }

        # =========================================================================
        # 5. Attestation (최종 씰링 및 부인방지 서명)
        # =========================================================================
        # 블록체인 온체인 검증의 효율성을 위해, 전체 데이터가 아닌 Root Hash들만 모아서 서명합니다.
        recipe_root_bytes = StateAdapter.to_canonical_bytes(recipe)
        
        attestation_payload = {
            "context": context,
            "recipe_root": hashlib.sha256(recipe_root_bytes).hexdigest(),
            "observation_root": observation["aggregated_hash"]
        }
        
        canonical_root = StateAdapter.to_canonical_bytes(attestation_payload)
        signature = self.signer.sign_payload(canonical_root)

        self.log.info(f"  └─ [Universal Seal] Sig: {signature[:12]}... | Root: {hashlib.sha256(canonical_root).hexdigest()[:8]}")

        return {
            "context": context,
            "recipe": recipe,
            "observation": observation,
            "attestation": {
                "canonical_root": hashlib.sha256(canonical_root).hexdigest(),
                "signature": signature
            }
        }