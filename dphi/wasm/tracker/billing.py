# dphi.wasm.tracker.billing
## @lineage: dphi.tracker.billing
## @lineage: agent.dphi.tracker.billing
"""
@desc:
- Agentic Economy Billing & Revenue Fusion Tracker.
- Stores ONLY the Delta (change) in memory. Old receipts are naturally dissipated.
- Continuity is proven holographically via: New_Nexus = Prev_Nexus ⊕ Delta_Hash ⊕ Phase_ID
"""
import json
import hashlib
import asyncio
from typing import Dict, Any

from arch.topos.network.bridge import RpcBridge
from watcher.dphi.exchange.config import billing_config, treasury_config
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("tracker.billing")

class PricingEngine:
    """@desc: 자원 소모량(Delta)을 경제적 가치로 치환하고 분배율을 적용하는 룰 엔진"""
    @classmethod
    def calculate_cost(cls, fuel_consumed: int, tier: str) -> float:
        billing_units = fuel_consumed / billing_config.fuel_billing_unit
        base_cost = billing_units * billing_config.usd_per_billing_unit
        multiplier = billing_config.tier_multipliers.get(tier.upper(), 1.0)
        return round(base_cost * multiplier, 6)

    @classmethod
    def calculate_splits(cls, amount_usdc: float) -> Dict[str, float]:
        return {
            "treasury_operator": round(amount_usdc * treasury_config.operator_share, 6),
            "gov_agent_pool": round(amount_usdc * treasury_config.gov_agent_share, 6),
            "sec_agent_pool": round(amount_usdc * treasury_config.sec_agent_share, 6)
        }

def int_hash(data: str) -> int:
    """문자열을 256비트 정수형 해시로 변환 (XOR 연산용)"""
    return int(hashlib.sha256(data.encode('utf-8')).hexdigest(), 16)


class StructuralXORCache:
    """
    @desc: 
    - 절대값(Total)이 아닌 오직 궤적의 델타(Delta)만을 머금고 있는 상태 공간.
    - 데이터는 융합된 직후 물리적으로 소산(Dissipation)
    """
    def __init__(self, genesis_nexus_id: int = 0):
        self._lock = asyncio.Lock()
        self.last_nexus_id: int = genesis_nexus_id
        
        ## 순수 변화량 (Delta)
        self.delta_balances: Dict[str, float] = self._empty_deltas()
        
        ## 원본 데이터(Raw Receipt)를 버리는 대신, 궤적의 순서를 XOR로 압축한 발자국
        self.rolling_receipt_xor: int = 0 
        self.delta_count: int = 0

    def _empty_deltas(self) -> Dict[str, float]:
        return {
            "treasury_operator": 0.0,
            "gov_agent_pool": 0.0,
            "sec_agent_pool": 0.0
        }

    async def absorb_receipt(self, receipt: dict) -> None:
        async with self._lock:
            amount = float(receipt.get("paid_amount_usdc", 0.0))
            splits = PricingEngine.calculate_splits(amount)
            
            for entity, split_amt in splits.items():
                self.delta_balances[entity] += split_amt

            receipt_str = json.dumps(receipt, sort_keys=True)
            self.rolling_receipt_xor ^= int_hash(receipt_str)
            self.delta_count += 1

    async def fuse_and_dissipate(self, current_phase_id: int) -> Dict[str, Any]:
        async with self._lock:
            if self.delta_count == 0:
                return {}

            ## 현재 Delta 공간의 구조적 해시 추출
            delta_state_str = json.dumps(self.delta_balances, sort_keys=True)
            delta_structural_hash = int_hash(delta_state_str)
            
            ##  Prev_Nexus ⊕ Delta_Structural ⊕ Rolling_Receipts ⊕ Phase_ID
            new_nexus_id = (
                self.last_nexus_id ^ 
                delta_structural_hash ^ 
                self.rolling_receipt_xor ^ 
                current_phase_id
            )
            
            ## 원장에 기록될 홀로그래픽 스냅샷 (절대값이 아닌 델타와 융합 증명만 기록)
            fused_payload = {
                "continuity_proof": hex(new_nexus_id),
                "previous_nexus": hex(self.last_nexus_id),
                "phase_id": current_phase_id,
                "fused_deltas": dict(self.delta_balances),
                "receipts_compressed": self.delta_count
            }

            ## 자연 소산 (Natural Dissipation) - 과거의 흔적을 물리적으로 제거
            self.last_nexus_id = new_nexus_id
            self.delta_balances = self._empty_deltas()
            self.rolling_receipt_xor = 0
            self.delta_count = 0
            return fused_payload

class BillingTracker:
    """@desc: 이벤트 스트림 구독 및 Ledger Inscription 오케스트레이터"""
    def __init__(self, rpc_bridge: RpcBridge):
        # 실제 환경에서는 복구용으로 RocksDB에서 최신 Nexus ID를 읽어와 초기화
        self.cache = StructuralXORCache(genesis_nexus_id=0) 
        self.rpc_bridge = rpc_bridge

    def evaluate_wasm_metrics(self, flow_id: str, agent_id: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
        fuel = metrics.get("fuel_consumed", 0)
        tier = metrics.get("tier", "STANDARD")
        cost_usdc = PricingEngine.calculate_cost(fuel, tier)
        
        return {
            "flow_id": flow_id,
            "agent_id": agent_id,
            "amount_usdc": str(cost_usdc),
            "resource_id": f"compute_{tier.lower()}"
        }

    async def process_x402_receipt(self, receipt: dict) -> None:
        await self.cache.absorb_receipt(receipt)
        log.debug(f"[Billing] Receipt absorbed into XOR structural state. Delta count: {self.cache.delta_count}")

    async def flush_to_ledger(self, topos_id: int, phase_id: int) -> bool:
        """@desc: 에포크 봉인 시점에 호출되어, 델타 융합 상태를 Ledger(RocksDB)에 암호학적으로 각인"""
        payload = await self.cache.fuse_and_dissipate(phase_id)
        if not payload:
            return True

        commit_hash = payload["continuity_proof"]
        
        req = {
            "action": "inscribe_actor",
            "payload": {
                "commit_hash": commit_hash,
                "payload_json": json.dumps(payload)
            }
        }
        
        res = await self.rpc_bridge.request(req)
        if res.get("status") == 200:
            log.info(f"[Ledger:Billing] Fused Delta State inscribed. New Nexus: {commit_hash[:16]}")
            return True
        else:
            log.error(f"[Ledger:Billing] Failed to flush fused state: {res.get('error')}")
            return False