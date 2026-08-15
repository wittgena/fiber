# bound.exchange.capital.comparator
## @lineage: bound.capital.oracle.comparator
"""
@arn: arn:bound:oracle:comparator:funding_spread:v1.0.0
@desc: Relational matrix builder for multi-exchange Funding Rates to detect arbitrage opportunities.
@security: The raw bytes of this source file (.py) are cryptographically hashed for execution integrity.
@constraint: Do not modify whitespace, comments, or logic after network deployment.
"""
import os
import hashlib
from typing import List, Dict, Any, TypedDict

# =========================================================================
# 1. Structural Schemas (스칼라가 아닌 벡터 형태의 데이터 구조)
# =========================================================================

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
    """
    @desc: 다중 거래소의 펀딩비를 병합(Aggregate)하지 않고, 
           상태의 차이(Spread)를 분석하여 시장의 불균형을 데이터 구조화하는 엔진.
    """
    
    # 펀딩비 차익거래를 실행하기 위한 최소 마진 임계치 (예: 0.05% 이상 차이가 나야 실행)
    # 거래 수수료와 슬리피지를 고려한 방어적 수치
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
        """비교기 로직 자체의 무결성 증명 해시"""
        return self._extract_source_hash(__file__)

    @classmethod
    def evaluate_spread_matrix(
        cls, 
        symbol: str, 
        observations: Dict[str, Dict[str, Any]]
    ) -> SpreadMatrix:
        """
        @phase: Matrix Evaluation (행렬 평가 및 스프레드 산출)
        @param observations: { "arn:binance...": {"rate": 0.01, "time": 123}, "arn:coinbase...": {"rate": -0.05, "time": 123} }
        """
        if len(observations) < 2:
            raise ValueError("Spread comparison requires at least 2 distinct data sources.")

        # 상태 보존: 원본 데이터를 뭉개지 않고 1차원 벡터로 나열
        raw_states = {arn: data["rate"] for arn, data in observations.items()}
        
        # 최적의 차익거래 포지션 탐색 (Max/Min 탐색)
        highest_funding_arn = max(raw_states, key=raw_states.get)
        lowest_funding_arn = min(raw_states, key=raw_states.get)
        
        max_rate = raw_states[highest_funding_arn]
        min_rate = raw_states[lowest_funding_arn]
        
        # 마찰력(격차) 계산
        net_spread = max_rate - min_rate
        
        # 임계치 돌파 여부 확인 (이 시그널이 True면 스마트 컨트랙트가 자금을 움직임)
        is_actionable = net_spread >= cls.MIN_ACTIONABLE_SPREAD

        # (참고) 타임스탬프는 동기화 검증이 끝났다고 가정하고 첫 번째 소스의 시간을 사용
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