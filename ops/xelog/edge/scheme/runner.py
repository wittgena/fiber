# ops.xelog.edge.scheme.runner
import time
import json
import hashlib
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.runner")

class EdgeRoute:
    OTLP_INGRESS = "/v1/logs"
    D3FI_INGRESS = "/d3fi/v1/order/ingress"
    D3FI_CLEARING = "/d3fi/v1/clearing/receipt/generate"
    STREAM_APPEND = "/v1/ledger/stream/append" 
    ANCHOR_SEAL = "/anchor/v1/seal"

class WebSchemeRunner:
    """비동기 HTTP 클라이언트 기반의 E2E 시나리오 실행기"""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context = []

    async def _run_api_case(self, title: str, method: str, endpoint: str, payload: dict, expected_status: int = 200) -> httpx.Response | None:
        log.info(f"\n[TEST] {title} ({method} {endpoint})")
        start_time = time.time()
        
        try:
            res = await self.client.request(method, endpoint, json=payload)
            elapsed_ms = (time.time() - start_time) * 1000
            
            if res.status_code == expected_status:
                output_msg = res.text[:150]
                log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | HTTP {res.status_code} | Output: {output_msg}")
                self.success_count += 1
                return res
            else:
                log.info(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Expected: {expected_status}, Got: {res.status_code}")
                log.info(f"    Details: {res.text}")
                self.fail_count += 1
                self.last_failed_context.append(f"Endpoint: {endpoint} | Error: {res.text}")
                return res
        except Exception as e:
            self.fail_count += 1
            log.info(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            return None

    def report(self):
        log.info(f"\n=== [DONE] E2E Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.info("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.info(f" - {ctx}")

class TrustlessWebScenario(WebSchemeRunner):
    """다중 서명(Multi-sig) 및 상태 해시를 처리하는 상위 시나리오 클래스"""
    def __init__(self, base_url: str):
        super().__init__(base_url)
        ## 3명의 Validator Committee 모사 (Ring 3 외부 검증자)
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    def _sign_payload(self, signers: list, payload_dict: dict) -> list:
        ## JSON 구조를 정규화(Canonicalize)하여 해시 생성 후 서명
        raw_json_bytes = json.dumps(payload_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]

class E2EScenarioOrchestrator(TrustlessWebScenario):
    async def run_genai_otlp(self) -> str:
        payload = {
            "resourceLogs": [{"resource": {"attributes": [{"key": "tenant_id", "value": {"stringValue": "tenant-01"}}]}}],
            "genai_metrics": {"tenant_id": "tenant-01", "model": "gpt-4", "usage": {"tokens": 2048}}
        }
        res = await self._run_api_case("OTLP Billing Ingress", "POST", EdgeRoute.OTLP_INGRESS, payload, 200)
        return res.headers.get("x-edge-content-hash", "failed") if res else "failed"

    async def run_d3fi_trade(self) -> str:
        ## 의도 주입 (Ingress)
        ingress_req = {"agent_id": "agent-x", "action": "SWAP", "amount": "5000", "slippage": "0.005"}
        await self._run_api_case("D3Fi Trade Ingress", "POST", EdgeRoute.D3FI_INGRESS, ingress_req, 200)
        
        ## 거래 정산 영수증(Clearing) 발급 검증
        entangled_state = "d3fi_state_hash_0x88"
        dummy_agent_key = ed25519.Ed25519PrivateKey.generate()
        signatures = self._sign_payload([dummy_agent_key], {"state": entangled_state})
        
        clearing_req = {"entangled_state": entangled_state, "signatures": signatures, "cost_metrics": {"gas": 21000}}
        res = await self._run_api_case("D3Fi Receipt Generation", "POST", EdgeRoute.D3FI_CLEARING, clearing_req, 200)
        return entangled_state if res and res.status_code == 200 else "failed"

    async def run_ledger_stream_append(self) -> str:
        """
        [Agentic Ledger Paradigm]
        - Ledger Stream Append 검증
        - 'timeout'과 'leak' 키워드를 삽입하여 커널(GatekeeperEngine)의 Tension(압력) 계산을 유도하고, ToposGateway가 이를 승인하는지 검증
        """
        payload = {
            "stream_name": "stream_core_infrastructure",
            "events": [
                {
                    "action": "SYSTEM_WARNING",
                    "user_id": "autonomous_agent_1",
                    "pii_data": None,
                    "details": "node_lock_timeout occurred during state transition"  # Triggers tension score
                },
                {
                    "action": "MEMORY_MONITOR",
                    "user_id": "autonomous_agent_1",
                    "pii_data": None,
                    "details": "minor token_leak detected in worker_pool"  # Triggers tension score
                }
            ],
            "verbose": True  # ZK Merkle Proof 발급 요청
        }
        res = await self._run_api_case("Ledger Stream Append & Tension Test", "POST", EdgeRoute.STREAM_APPEND, payload, 200)
        
        if res and res.status_code == 200:
            return res.json().get("result", {}).get("hash", "failed")
        return "failed"

    async def run_global_anchor(self, state_roots: dict):
        """앞선 3개 도메인의 상태 해시들을 묶어 글로벌 Epoch으로 씰링(Seal)합니다."""
        proposed_parity = {"state_roots": state_roots}
        signatures = self._sign_payload(self.committee_keys, proposed_parity)
        anchor_payload = {
            "receptor_id": "e2e-validator-node",
            "proposed_parity": proposed_parity,
            "parent_nexus_id": "nexus-epoch-1000",
            "repos": [],
            "signers": self.committee_pubs,
            "signatures": signatures,
            "timestamp": int(time.time() * 1000)
        }
        await self._run_api_case("Anchor Epoch Seal (Multi-sig & Continuity)", "POST", EdgeRoute.ANCHOR_SEAL, anchor_payload, 200)