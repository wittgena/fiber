# bound.exchange.intent.trajectory
import os
import time
import hashlib
from typing import Dict, Any, List, Optional, TypedDict
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.sign import NodeSigner
from watcher.plane.emitter import get_emitter

class ArbitrageSignal(TypedDict):
    is_actionable: bool        # 차익거래 실행 가능 여부 (스프레드가 임계치를 넘었는가?)
    optimal_long_venue: str    # 롱(매수) 포지션을 잡아야 할 거래소 ARN (펀딩비가 가장 낮은 곳)
    optimal_short_venue: str   # 숏(매도) 포지션을 잡아야 할 거래소 ARN (펀딩비가 가장 높은 곳)
    net_spread_yield: float    # 예상되는 무위험 펀딩비 갭 (최대 펀딩비 - 최소 펀딩비)

class SpreadMatrix(TypedDict):
    symbol: str
    timestamp: float
    raw_states: Dict[str, float]       # ARN -> Funding Rate 매핑 (데이터 원형 보존)
    arbitrage_signal: ArbitrageSignal  # 산출된 차익거래 시그널


# =========================================================================
# 2. Spread Matrix Comparator Core
# =========================================================================
class FundingRateComparator:
    MIN_ACTIONABLE_SPREAD = 0.0005 

    @staticmethod
    def _extract_source_hash(module_or_file: Any) -> str:
        path = os.path.abspath(getattr(module_or_file, '__file__', module_or_file))
        if path.endswith('.pyc'):
            path = path[:-1]
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    @property
    def comparator_code_hash(self) -> str:
        return self._extract_source_hash(__file__)

    @classmethod
    def evaluate_spread_matrix(
        cls, 
        symbol: str, 
        observations: Dict[str, Dict[str, Any]]
    ) -> SpreadMatrix:
        if len(observations) < 2:
            raise ValueError("Spread comparison requires at least 2 distinct data sources.")

        raw_states = {arn: data["rate"] for arn, data in observations.items()}
        highest_funding_arn = max(raw_states, key=raw_states.get)
        lowest_funding_arn = min(raw_states, key=raw_states.get)
        
        max_rate = raw_states[highest_funding_arn]
        min_rate = raw_states[lowest_funding_arn]
        
        net_spread = max_rate - min_rate
        is_actionable = net_spread >= cls.MIN_ACTIONABLE_SPREAD
        base_timestamp = list(observations.values())[0]["time"]

        return {
            "symbol": symbol,
            "timestamp": base_timestamp,
            "raw_states": raw_states,
            "arbitrage_signal": {
                "is_actionable": is_actionable,
                "optimal_short_venue": highest_funding_arn,  # 펀딩비가 높은 곳에서 숏(지불 받음)
                "optimal_long_venue": lowest_funding_arn,    # 펀딩비가 낮은 곳에서 롱(지불 받음)
                "net_spread_yield": net_spread
            }
        }


# =========================================================================
# 3. Trajectory Engine
# =========================================================================
class ProvableTrajectoryEngine:
    """
    다중 소스의 시계열 데이터를 바탕으로 스프레드(Matrix)와 
    시간 축에 따른 흐름(Trajectory: 미분/적분)을 산출하는 엔진.
    """
    def __init__(self):
        # 내부적으로 FundingRateComparator를 사용하여 기본 스프레드를 구함
        self.comparator = FundingRateComparator()

    def execute_flow(
        self, symbol: str, target_arns: List[str], time_window_sec: int, fetch_time: int
    ) -> Dict[str, Any]:
        
        # TODO: 실제로는 시계열 DB나 오라클에서 time_window_sec 만큼의 과거 데이터를 가져와야 함.
        # 현재는 아키텍처 연결을 위한 Dummy 데이터 생성
        mock_observations = {
            target_arns[0]: {"rate": 0.01, "time": fetch_time},
            target_arns[1]: {"rate": -0.05, "time": fetch_time}
        }
        
        # 1. 현재 시점의 정적 불균형 상태 산출
        spread_matrix = self.comparator.evaluate_spread_matrix(symbol, mock_observations)
        
        # 2. 결과 조합 반환 (trajectory.py가 기대하는 인터페이스 매핑)
        return {
            "engine_code_hash": self.comparator.comparator_code_hash,
            "composite_sources": {arn: "source_hash_mock" for arn in target_arns},
            "individual_hashes": {arn: "obs_hash_mock" for arn in target_arns},
            "spread_matrix": spread_matrix,
            "velocity": 0.001,      # 미분: 스프레드 확장 속도 (예시)
            "integral": 12.5,       # 적분: 누적 스트레스 (예시, SystemicAnomalyTrap이 평가할 값)
            "payload_hash": hashlib.sha256(b"mock_payload").hexdigest()
        }


# =========================================================================
# 4. Oracle Receptor (Facade)
# =========================================================================
class TrajectoryOracleReceptor:
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
        # 4-1. Trajectory Engine에게 데이터 수집, 매트릭스 비교 및 궤적 산출 위임
        # =========================================================================
        engine_result = self.engine.execute_flow(symbol, target_arns, time_window_sec, fetch_time)

        # =========================================================================
        # 4-2. Context 정의 (오라클 노드의 서명 환경 정보)
        # =========================================================================
        context = {
            "oracle_id": "trajectory_receptor_v1.0",
            "timestamp": fetch_time,
            "signer_pubkey": getattr(self.signer, 'pubkey_hex', 'UNKNOWN')
        }

        # =========================================================================
        # 4-3. Recipe (의도 증명) - '어떤 전략'이 아니라 '어떤 시간 축(Window)'인가를 증명
        # =========================================================================
        recipe = {
            "time_window_sec": time_window_sec,
            "engine_code_hash": engine_result["engine_code_hash"],
            "sources": engine_result["composite_sources"]
        }

        # =========================================================================
        # 4-4. Observation (상태 관측) - 스칼라 값(평균)이 아닌 벡터(흐름) 구조
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
        # 4-5. Attestation (최종 씰링 및 부인방지 서명)
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