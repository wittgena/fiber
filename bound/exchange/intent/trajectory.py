# bound.exchange.intent.trajectory
## @lineage: bound.capital.exchange.trajectory
import time
import hashlib
from typing import Dict, Any, List, Optional

from bound.exchange.capital.comparator import ProvableTrajectoryEngine

from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

class TrajectoryOracleReceptor:
    """
    @desc: 다중 거래소의 펀딩비/마찰력 데이터를 수집하여, 
           단일 값이 아닌 시장의 '불균형(Spread)'과 '자본의 이동 궤적(Trajectory)'을 
           암호학적으로 씰링(Sealing)하는 동적(Dynamic) 리셉터.
    """
    def __init__(self, signer: Optional[NodeSigner] = None, logger: Optional[Any] = None):
        self.signer = signer or NodeSigner.get_instance()
        self.log = logger or get_emitter("exchange.trajectory")
        
        # 순수 실행 및 행렬/벡터 생성을 담당하는 궤적 엔진 인스턴스화
        self.engine = ProvableTrajectoryEngine()

    def fetch_and_seal(
        self, 
        symbol: str, 
        target_arns: List[str], 
        time_window_sec: int = 28800  # 기본 8시간(펀딩비 주기) 동안의 궤적 추적
    ) -> Dict[str, Any]:
        """
        @param target_arns: 궤적을 비교할 대상 어댑터 ARN 목록 (최소 2개 이상 필수)
        @param time_window_sec: 궤적(미분/적분)을 계산할 시계열 윈도우 크기 (단위: 초)
        """
        if len(target_arns) < 2:
            raise ValueError("[Topology Error] Trajectory & Spread analysis requires at least 2 dimensions (sources).")

        fetch_time = int(time.time())
        self.log.info(f"[Receptor] Tracking vector trajectory for {symbol} | Sources: {len(target_arns)} | Window: {time_window_sec}s")

        # =========================================================================
        # 1. Trajectory Engine에게 데이터 수집, 매트릭스 비교 및 궤적 산출 위임
        # =========================================================================
        # 엔진은 각 소스의 데이터를 `time_window_sec`만큼 수집하여 
        # 1) 현재의 스프레드(Matrix)와 2) 시간 축에 따른 흐름(Trajectory Vector)을 산출함
        engine_result = self.engine.execute_flow(symbol, target_arns, time_window_sec, fetch_time)

        # =========================================================================
        # 2. Context 정의 (오라클 노드의 서명 환경 정보)
        # =========================================================================
        context = {
            "oracle_id": "trajectory_receptor_v1.0",
            "timestamp": fetch_time,
            "signer_pubkey": getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        }

        # =========================================================================
        # 3. Recipe (의도 증명) - '어떤 전략'이 아니라 '어떤 시간 축(Window)'인가를 증명
        # =========================================================================
        recipe = {
            "time_window_sec": time_window_sec,
            "engine_code_hash": engine_result["engine_code_hash"],
            "sources": engine_result["composite_sources"]
        }

        # =========================================================================
        # 4. Observation (상태 관측) - 스칼라 값(평균)이 아닌 벡터(흐름) 구조
        # =========================================================================
        observation = {
            "individual_hashes": engine_result["individual_hashes"],
            "payload": {
                # [점] 현재 시점의 정적 불균형 상태 (차익거래 즉시 실행용 시그널)
                "spread_matrix": engine_result["spread_matrix"], 
                
                # [선/면] 시간 축에 따른 동적 흐름 (리스크 관리 및 체제 변화 감지용)
                "trajectory_vector": {
                    "velocity": engine_result["velocity"],           # 미분: 스프레드가 벌어지는가 좁혀지는가?
                    "accumulated_stress": engine_result["integral"]  # 적분: 해당 윈도우 동안 누적된 청산 압력
                }
            },
            "observation_root": engine_result["payload_hash"]
        }

        # =========================================================================
        # 5. Attestation (최종 씰링 및 부인방지 서명)
        # =========================================================================
        recipe_root_bytes = StateAdapter.to_canonical_bytes(recipe)
        
        attestation_payload = {
            "context": context,
            "recipe_root": hashlib.sha256(recipe_root_bytes).hexdigest(),
            "observation_root": observation["observation_root"]
        }
        
        canonical_root = StateAdapter.to_canonical_bytes(attestation_payload)
        signature = self.signer.sign_payload(canonical_root)

        self.log.info(f"  └─ [Trajectory Seal] Sig: {signature[:12]}... | Root: {hashlib.sha256(canonical_root).hexdigest()[:8]}")

        return {
            "context": context,
            "recipe": recipe,
            "observation": observation,
            "attestation": {
                "canonical_root": hashlib.sha256(canonical_root).hexdigest(),
                "signature": signature
            }
        }