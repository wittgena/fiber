# phase.kernel.attach.surge
## @lineage: dphi.node.attach.surge
import time
import asyncio
import random
import json
from typing import Any

from fiber.workflow.scene.anchor import ActorIdentity 
from xphi.kernel.dphi.method import DphiMethod
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("attach.surge")

class MarketSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity

    async def ignite(self):
        log.info(f"\n🔥 [Market Surge] Igniting Compute Market Chaos (Capacity: {self.capacity})")
        code_normal = "print('NORMAL_OK')"
        code_oom_trap = "arr = []\nwhile True: arr.append('A' * 1024 * 1024)" # Fuel/Mem Trap
        code_fp_math = "import math\nprint(sum(math.sin(i)*math.cos(i) for i in range(1000)))" # Determinism
        
        # Deserialization Bomb (깊이 100의 중첩 JSON)
        json_bomb = {"level_0": "payload"}
        for i in range(100): json_bomb = {f"level_{i+1}": json_bomb}

        total_requests = max(200, self.capacity * 4)
        log.info(f" └─ Injecting {total_requests} mixed tasks (Valid vs Fuel-Burners/Bombs)...")
        
        tasks = []
        expected_valid = 0
        
        for _ in range(total_requests):
            choice = random.randint(1, 4)
            if choice == 1:
                tasks.append(self.broker.execute(code=code_normal, tier="STANDARD", timeout=5.0))
                expected_valid += 1
            elif choice == 2:
                tasks.append(self.broker.execute(code=code_fp_math, tier="STANDARD", timeout=5.0))
                expected_valid += 1
            elif choice == 3:
                tasks.append(self.broker.execute(code=code_oom_trap, tier="STANDARD", timeout=5.0))
            else:
                tasks.append(self.broker.invoke(DphiMethod.VERIFY_PACKET.value, json_bomb, tier="STANDARD", timeout=5.0))
                
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes, trapped = 0, 0
        for res in results:
            if isinstance(res, Exception) or not getattr(res, 'success', False):
                trapped += 1
            else:
                successes += 1
                
        log.info(f" └─ 🎯 Demand Mix -> Valid Compute: {expected_valid} | Malicious/Bombs: {total_requests - expected_valid}")
        log.info(f" └─ 📊 Result     -> Processed: {successes} | Trapped/Burned: {trapped}")
        
        if successes == expected_valid:
            log.info(f" └─ 🟢 [Economy] {elapsed:.2f}ms | Perfect Resilience. Toxic tasks exhausted fuel/trapped instantly.")
        else:
            log.info(f" └─ 🔴 [Distortion] {elapsed:.2f}ms | Market choked. Valid tasks dropped.")

class LedgerSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity
        self.committee = [ActorIdentity(f"Com_{i}") for i in range(3)]
        self.rogue = ActorIdentity("Rogue")

    async def ignite(self):
        log.info(f"\n🛡️ [Ledger Surge] Bombarding Consensus Engine (Capacity: {self.capacity})")
        
        burst_size = max(150, self.capacity * 3)
        log.info(f" └─ Injecting {burst_size} concurrent SEAL_EPOCH intents (Valid vs Sybil/Rogue)...")
        
        tasks = []
        expected_sealed = 0
        
        for i in range(burst_size):
            parity = StateAdapter.build_parity_triplet("surge_topos", 1, i)
            commit = StateAdapter.build_anchor_commit(parity, 0, "gen", {"repo": "hash"}, {})
            
            choice = random.randint(1, 3)
            if choice == 1: # Valid (2-of-3)
                signers = [self.committee[0], self.committee[1]]
                expected_sealed += 1
            elif choice == 2: # Threshold Fail (1-of-3)
                signers = [self.committee[0]]
            else: # Sybil Attack (Duplicate signatures)
                signers = [self.committee[0], self.committee[0]]
                
            sig_pubs = [s.pubkey_hex for s in signers]
            sigs = [s.sign(commit) for s in signers]
            
            payload = StateAdapter.build_seal_epoch_payload(
                parity, 0, "gen", {"repo": "hash"}, {}, int(time.time()),
                sig_pubs, sigs, 2, [c.pubkey_hex for c in self.committee]
            )
            tasks.append(self.broker.invoke(DphiMethod.SEAL_EPOCH.value, payload, tier="SYSTEM", timeout=10.0))

        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        sealed, rejected = 0, 0
        for res in results:
            if isinstance(res, Exception) or not getattr(res, 'success', False): rejected += 1
            else: sealed += 1
                
        log.info(f" └─ 🎯 Intent Mix -> Valid Seals: {expected_sealed} | Sybil/Invalid: {burst_size - expected_sealed}")
        log.info(f" └─ 📊 Result     -> Sealed: {sealed} | Rejected: {rejected}")
        
        if sealed == expected_sealed:
            log.info(f" └─ 🟢 [Security] {elapsed:.2f}ms | Byzantine defense held up perfectly under load.")
        else:
            log.info(f" └─ 🔴 [Contention] {elapsed:.2f}ms | Ledger deadlock or valid seals dropped.")

class EcoSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity

    async def ignite(self):
        log.info(f"\n🌪️ [Eco Surge] Flooding P2P Exchange & Pipeline (Capacity: {self.capacity})")
        
        burst_size = max(200, self.capacity * 5)
        log.info(f" └─ Injecting {burst_size} high-frequency Trade Ingress Intents...")
        
        # P2P Exchange Ingress 폭격 (Step 1)
        tasks = [
            self.broker.invoke(DphiMethod.INIT_EPOCH.value, {
                "ts": int(time.time() * 1000) + i, "topo": 777, "press": 5, "rupture": False, "injected_tick": None
            }, tier="SYSTEM", timeout=10.0)
            for i in range(burst_size)
        ]
        
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes = sum(1 for r in results if getattr(r, 'success', False))
        overloads = sum(1 for r in results if not getattr(r, 'success', False) and "OVERLOADED" in str(getattr(r, 'error', '')))
        
        handled = successes + overloads
        if handled >= burst_size * 0.9: 
            log.info(f" └─ 🟢 [Fluidity] {elapsed:.2f}ms | Ingress Processed {successes} | Shed {overloads}")
        else:
            log.info(f" └─ 🔴 [Choked] {elapsed:.2f}ms | Ingress Processed: {successes}/{burst_size}")