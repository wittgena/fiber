import time
import json
import hashlib
import asyncio
import requests
from typing import Dict, Any, List
from dataclasses import dataclass
import logging

# =========================================================================
# 0. Lightweight Mocks & Logging Setup (for Standalone Execution)
# =========================================================================
logging.basicConfig(level=logging.INFO, format=" LOG | TEST_EXECUTION | INFO  |  %(message)s")
log = logging.getLogger("binance.suite")

class StateAdapterMock:
    """프레임워크의 JCS (RFC 8785) 캐노니컬라이저를 모방합니다."""
    @staticmethod
    def to_canonical_bytes(data: Any) -> bytes:
        return json.dumps(data, separators=(',', ':'), sort_keys=True).encode('utf-8')

# =========================================================================
# 1. Target Module: Binance Receptor
# =========================================================================
class BinanceDataLoader:
    """테스트 대상: 바이낸스 실데이터를 3계층 해시로 씰링하는 리셉터"""
    BASE_URL = "https://api.binance.com/api/v3/klines"
    CODE_VERSION_TAG = "binance_dataloader_v1.0.0_jcs_canonical"

    @classmethod
    def _compute_code_hash(cls) -> str:
        return hashlib.sha256(cls.CODE_VERSION_TAG.encode('utf-8')).hexdigest()

    @classmethod
    def _compute_param_hash(cls, symbol: str, interval: str, limit: int, fetch_time: int) -> str:
        time_window = fetch_time // 60 * 60  # 60초 단위 정규화
        param_dict = {"symbol": symbol, "interval": interval, "limit": limit, "time_window": time_window}
        return hashlib.sha256(StateAdapterMock.to_canonical_bytes(param_dict)).hexdigest()

    @classmethod
    def fetch_and_seal(cls, symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 5) -> Dict[str, Any]:
        fetch_time = int(time.time())
        res = requests.get(cls.BASE_URL, params={"symbol": symbol, "interval": interval, "limit": limit})
        res.raise_for_status()
        
        lightweight_data = [
            {"ts": c[0], "o": float(c[1]), "h": float(c[2]), "l": float(c[3]), "c": float(c[4]), "v": float(c[5])}
            for c in res.json()
        ]

        canonical_data = StateAdapterMock.to_canonical_bytes(lightweight_data)
        data_hash = hashlib.sha256(canonical_data).hexdigest()

        return {
            "recipe": {
                "code_hash": cls._compute_code_hash(),
                "param_hash": cls._compute_param_hash(symbol, interval, limit, fetch_time),
                "timestamp_window": fetch_time // 60 * 60
            },
            "data": {
                "data_hash": data_hash,
                "raw_payload": lightweight_data,
                "canonical_string": canonical_data.decode('utf-8')
            }
        }

# =========================================================================
# 2. Test Framework (Patterned after dphi.workflow & phase.entry)
# =========================================================================
@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool
    message: str = ""

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success

class ReceptorTestSuite:
    """Orchestrates the Verification of the Binance DataLoader"""
    def __init__(self):
        self.results: List[TestResult] = []

    async def execute(self):
        log.info("\n" + "="*80)
        log.info("🧪 [RECEPTOR MASTER SUITE] Commencing Zero-Trust Data Loader Tests")
        log.info("="*80)

        await self.phase_1_golden_path()
        await self.phase_2_time_window_consensus()
        await self.phase_3_negative_path_invalid_symbol()
        await self.phase_4_data_integrity_check()

        self._print_report()

    # --- Test Phases ---

    async def phase_1_golden_path(self):
        log.info("\n--- [Phase 1] Golden Path: Fetch & Seal Real Market Data ---")
        try:
            receipt = BinanceDataLoader.fetch_and_seal("BTCUSDT", "1m", 5)
            
            # 검증 로직
            assert "recipe" in receipt and "data" in receipt
            assert len(receipt["data"]["raw_payload"]) == 5
            
            log.info(f"  └─ [SEALED] Code: {receipt['recipe']['code_hash'][:8]} | Param: {receipt['recipe']['param_hash'][:8]}")
            self.results.append(TestResult("RECEPTOR_API", "Fetch & Generate 3-Layer Receipt", True, True))
        except Exception as e:
            self.results.append(TestResult("RECEPTOR_API", "Fetch & Generate 3-Layer Receipt", False, True, str(e)))

    async def phase_2_time_window_consensus(self):
        log.info("\n--- [Phase 2] Consensus: Param Hash Time-Window Determinism ---")
        try:
            # 시뮬레이션: 동일한 60초 윈도우 내에서 10초 간격으로 노드 A와 노드 B가 호출
            hash_node_a = BinanceDataLoader._compute_param_hash("ETHUSDT", "1m", 10, fetch_time=1700000010)
            hash_node_b = BinanceDataLoader._compute_param_hash("ETHUSDT", "1m", 10, fetch_time=1700000025)
            
            # 시뮬레이션: 윈도우를 벗어난 호출
            hash_node_c = BinanceDataLoader._compute_param_hash("ETHUSDT", "1m", 10, fetch_time=1700000065)

            assert hash_node_a == hash_node_b, "Time window failed to normalize param hash."
            assert hash_node_a != hash_node_c, "Param hash should change across different windows."
            
            log.info("  └─ [VALIDATED] Nodes within same minute generate identical Param Hash.")
            self.results.append(TestResult("DETERMINISM", "Time-Window Param Hash Normalization", True, True))
        except Exception as e:
            self.results.append(TestResult("DETERMINISM", "Time-Window Param Hash Normalization", False, True, str(e)))

    async def phase_3_negative_path_invalid_symbol(self):
        log.info("\n--- [Phase 3] Negative Path: Handling Invalid API Requests ---")
        try:
            BinanceDataLoader.fetch_and_seal("INVALID_COIN", "1m", 5)
            self.results.append(TestResult("RECEPTOR_API", "Reject Invalid Symbol Request", True, False, "Should have failed"))
        except requests.exceptions.HTTPError as e:
            log.info(f"  └─ [DEFLECTED] Expected Error Caught: {e.response.status_code}")
            self.results.append(TestResult("RECEPTOR_API", "Reject Invalid Symbol Request", False, False)) # Expected to fail

    async def phase_4_data_integrity_check(self):
        log.info("\n--- [Phase 4] Data Integrity: Format and Field Validation ---")
        try:
            receipt = BinanceDataLoader.fetch_and_seal("SOLUSDT", "1m", 1)
            payload = receipt["data"]["raw_payload"][0]
            
            # WASM 샌드박스가 파싱할 수 있는 필수 필드가 모두 존재하는지 확인
            required_keys = ["ts", "o", "h", "l", "c", "v"]
            for key in required_keys:
                assert key in payload, f"Missing required key: {key}"
            
            # 캐노니컬 데이터가 정확히 해싱되었는지 교차 검증
            re_hashed = hashlib.sha256(receipt["data"]["canonical_string"].encode('utf-8')).hexdigest()
            assert receipt["data"]["data_hash"] == re_hashed, "Data Hash mismatch!"
            
            log.info("  └─ [VALIDATED] Data structure is lightweight and strictly hashed.")
            self.results.append(TestResult("INTEGRITY", "Lightweight Format & Cryptographic Binding", True, True))
        except Exception as e:
            self.results.append(TestResult("INTEGRITY", "Lightweight Format & Cryptographic Binding", False, True, str(e)))

    # --- Reporting ---

    def _print_report(self):
        log.info("\n" + "="*80)
        log.info("📊 [MASTER TEST SUITE REPORT]")
        log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            log.info(f"{status_icon} {idx:02d}. {target_label.ljust(15)} {res.scenario.ljust(45)} | Result: {status_text}")
            if res.message:
                log.info(f"      └─ Note: {res.message}")
            
        log.info("-" * 80)
        if all_passed:
            log.info("🎉 ALL RECEPTOR TESTS EXECUTED SUCCESSFULLY.")
        else:
            log.error("💥 SOME TESTS FAILED. Check the execution logs for details.")
        log.info("="*80 + "\n")

if __name__ == "__main__":
    # Async 진입점 실행
    app = ReceptorTestSuite()
    asyncio.run(app.execute())