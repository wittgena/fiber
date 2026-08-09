# receptor.dataloader.binance
import time
import hashlib
import requests
from typing import Dict, Any

from watcher.plane.emitter import get_emitter
from kernel.dphi.adapter.state import StateAdapter

log = get_emitter("receptor.binance")

class BinanceDataLoader:
    """
    Web2 바이낸스 시장 데이터를 Web3 파이프라인(WASM Sandbox & Multi-sig)에서 
    신뢰하고 검증할 수 있도록 3계층 해시(Code, Param, Data)로 씰링하는 리셉터 클래스입니다.
    """
    BASE_URL = "https://api.binance.com/api/v3/klines"
    
    # 1. Code Binding: 이 로더의 로직 버전을 명시하여 향후 코드 변경에 따른 무결성을 보장합니다.
    CODE_VERSION_TAG = "binance_dataloader_v1.0.0_jcs_canonical"

    @classmethod
    def _compute_code_hash(cls) -> str:
        """데이터를 추출하는 로직 자체의 해시(Code Hash)를 반환합니다."""
        return hashlib.sha256(cls.CODE_VERSION_TAG.encode('utf-8')).hexdigest()

    @classmethod
    def _compute_param_hash(cls, symbol: str, interval: str, limit: int, fetch_time: int) -> str:
        """
        요청 파라미터의 해시(Param Hash)를 반환합니다.
        서로 다른 노드들이 1분(60초) 이내에 호출했다면 동일한 파라미터 해시를 갖도록 
        Time Window(시간 단위 절사)를 적용하여 실용적 합의가 가능하게 합니다.
        """
        time_window = fetch_time // 60 * 60  # 60초 단위 정규화
        param_dict = {
            "symbol": symbol, 
            "interval": interval, 
            "limit": limit, 
            "time_window": time_window
        }
        canonical_bytes = StateAdapter.to_canonical_bytes(param_dict)
        return hashlib.sha256(canonical_bytes).hexdigest()

    @classmethod
    def fetch_and_seal(cls, symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 20) -> Dict[str, Any]:
        """
        실제 데이터를 가져와 경량화한 후, 검증 가능한 암호학적 영수증(Receipt) 형태로 반환합니다.
        """
        fetch_time = int(time.time())
        log.info(f"[BinanceReceptor] Fetching {limit} klines for {symbol} ({interval})...")
        
        # 1. Fetch Data
        response = requests.get(cls.BASE_URL, params={"symbol": symbol, "interval": interval, "limit": limit})
        response.raise_for_status()
        raw_data = response.json()

        # 2. Extract & Lightweight
        lightweight_data = [
            {
                "ts": candle[0], 
                "o": float(candle[1]), 
                "h": float(candle[2]), 
                "l": float(candle[3]), 
                "c": float(candle[4]), 
                "v": float(candle[5])
            }
            for candle in raw_data
        ]

        # 3. Compute Data Hash (JCS Canonicalization)
        canonical_data = StateAdapter.to_canonical_bytes(lightweight_data)
        data_hash = hashlib.sha256(canonical_data).hexdigest()

        # 4. Generate Hashes
        code_hash = cls._compute_code_hash()
        param_hash = cls._compute_param_hash(symbol, interval, limit, fetch_time)

        # 5. Build Execution Receipt
        receipt = {
            "recipe": {
                "code_hash": code_hash,
                "param_hash": param_hash,
                "timestamp_window": fetch_time // 60 * 60
            },
            "data": {
                "data_hash": data_hash,
                "raw_payload": lightweight_data,
                "canonical_string": canonical_data.decode('utf-8')
            }
        }
        
        log.info(f"  └─ [Receptor Sealed] Code: {code_hash[:8]} | Param: {param_hash[:8]} | Data: {data_hash[:8]}")
        return receipt