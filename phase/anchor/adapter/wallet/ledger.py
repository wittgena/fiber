# phase.anchor.adapter.wallet.ledger
## @lineage: bound.client.wallet.ledger
## @lineage: bound.exchange.wallet.ledger
import os
import time
import hashlib
import json
from typing import Dict, Any

from phase.anchor.config.dphi import dphi_env
from kernel.dphi.ledger.consensus import KernelLedger, ToposBlob
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.broker import DphiBroker
from watcher.plane.emitter import get_emitter

log = get_emitter("wallet.ledger")

class LedgerWalletAdapter:
    def __init__(self, agent_alias: str = "system_clearing", simulate: bool = False):
        self.agent_alias = agent_alias
        self.simulate = simulate
        
        # 지연 정산 구조에서는 DPHI 청산소 마스터 계정이 주체가 됨
        agent_config = getattr(dphi_env.agents, agent_alias)
        self.clearing_address = agent_config.evm_address.lower()
        self.network_id = "dvm-rollup-chain"
        
        self.ledger = KernelLedger()
        self.broker = DphiBroker()
        log.info(f"[Ledger Wallet] Initialized for {agent_alias}: {self.clearing_address} (Simulate: {simulate})")

    async def process_deferred_charge(self, agent_address: str, amount_str: str, asset: str = "usdc") -> str:
        """
        [지연 정산 / Pull] 
        에이전트의 오프체인 잔고(또는 Allowance)를 DVM 내부에서 강제 차감(transferFrom)하는 섀도우 연산
        """
        if self.simulate:
            log.warning(f"[Ledger Wallet] Simulated deferred charge: {amount_str} {asset} from {agent_address}")
            return f"0x_simulated_charge_{os.urandom(8).hex()}"

        log.info(f"[Ledger Wallet] Initiating DVM Deferred Charge for {amount_str} {asset.upper()} from {agent_address}...")

        decimals = 6 if asset.lower() == "usdc" else 18
        amount_wei = int(float(amount_str) * (10 ** decimals))
        
        # 1. Calldata 생성: transferFrom(address from, address to, uint256 amount)
        clean_from = agent_address.lower().replace("0x", "").rjust(64, "0")
        clean_to = self.clearing_address.replace("0x", "").rjust(64, "0")
        clean_amount = hex(amount_wei).replace("0x", "").rjust(64, "0")
        
        # 0x23b872dd == transferFrom Selector
        calldata = f"0x23b872dd{clean_from}{clean_to}{clean_amount}"

        target_contract = getattr(dphi_env.contracts, f"target_{asset.lower()}").lower()

        # 🌟 Mock ERC20 바이트코드 업데이트: 0x23b872dd (transferFrom) 호출 시 STOP 하도록 패치
        valid_mock_erc20_bytecode = "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806323b872dd14602d575b600080fd5b00"

        # 2. DVM 상태 스냅샷 구성 (에이전트의 잔고와 위임 상태를 포함)
        state_snapshot = {
            self.clearing_address: {"balance": hex(10**18), "nonce": 1},
            target_contract: {"balance": "0x0", "code": valid_mock_erc20_bytecode},
            agent_address.lower(): {"balance": hex(amount_wei * 100), "nonce": 0}
        }

        # 3. DVM 실행 페이로드 조립 (Caller는 에이전트가 아니라 DPHI 청산소)
        gas_price = hex(10**9) # 1 Gwei
        dvm_payload = {
            "vm_target": "EVM",
            "target_address": target_contract,
            "calldata": calldata,
            "gas_limit": 150000,
            "gas_price": gas_price,
            "state_snapshot": state_snapshot,
            "caller_address": self.clearing_address
        }
        
        # 4. Broker를 통한 비동기 워커 호출
        try:
            result = await self.broker.execute(
                code=dvm_payload,
                tier="STANDARD",
                timeout=5.0
            )
            
            if not result.success:
                err_msg = result.error.msg if result.error else "Unknown Broker Error"
                raise RuntimeError(f"Broker Execution Failed: {err_msg}")
                
            data = json.loads(result.output)
            if not data.get("success"):
                raise RuntimeError(f"REVM Halted: {data.get('revert_reason', 'Unknown')}")
                
        except Exception as e:
            log.error(f"[Ledger Wallet] Broker Execution Exception: {e}")
            raise RuntimeError(f"Off-chain deferred charge failed: {str(e)}")

        # 5. KernelLedger 상태 기록 (Rollup Seal)
        state_diff = data.get("state_diff", {})
        gas_used = data.get("gas_used", 0)

        payload_dict = {
            "caller": self.clearing_address,
            "contract": target_contract,
            "state_diff": state_diff,
            "gas": gas_used,
            "timestamp": time.time()
        }

        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        rollup_hash = f"0x{hashlib.sha256(canonical_bytes).hexdigest()}"

        blob = ToposBlob(
            action="DEFERRED_SETTLEMENT_CHARGE",
            from_state="dvm.wasm.execution",
            to_state="ledger.sealed",
            tension=0.5,
            details=f"Gas: {gas_used} | Modified: {len(state_diff)}"
        )
        
        self.ledger.save_transition(blob)
        log.info(f"✨ [Ledger Wallet] Local Rollup Hash generated for Charge: {rollup_hash[:18]}...")

        return rollup_hash